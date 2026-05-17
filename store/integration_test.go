package store

import (
	"bytes"
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"testing"

	"github.com/eliothedeman/place/index"
	"github.com/eliothedeman/place/mover"
	"github.com/eliothedeman/place/segment"
)

// Alias the tier constant for terser test code.
const TierCold = index.TierCold

// TestTorrentLikeRandomOrderWrites simulates a torrent client landing pieces
// in random order at piece-aligned offsets, then verifies the assembled file
// matches what we'd get from a sequential write.
func TestTorrentLikeRandomOrderWrites(t *testing.T) {
	s := newStore(t)
	_, h, err := s.Create(RootInode, "movie.mkv", 0o644, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	const fileSize = 8 << 20 // 8 MiB
	const pieceSize = 256 << 10
	expect := make([]byte, fileSize)
	if _, err := rand.Read(expect); err != nil {
		t.Fatal(err)
	}

	pieces := fileSize / pieceSize
	order := mathrand.Perm(pieces)
	for _, p := range order {
		off := int64(p) * pieceSize
		if _, err := h.WriteAt(expect[off:off+pieceSize], off); err != nil {
			t.Fatal(err)
		}
	}

	got := make([]byte, fileSize)
	rn, err := h.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rn != fileSize {
		t.Fatalf("read n=%d want %d", rn, fileSize)
	}
	if !bytes.Equal(got, expect) {
		t.Fatalf("torrent reassembly mismatch")
	}
}

// TestEvictUnderPressureKeepsDataReadable writes a few hundred KiB to a file,
// runs the mover with HotMaxBytes=1 (forcing every stripe to evict to cold),
// then verifies the file is still byte-for-byte correct.
func TestEvictUnderPressureKeepsDataReadable(t *testing.T) {
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 19})
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	s, err := Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	_, h, _ := s.Create(RootInode, "evict-me", 0o644, 0, 0)
	defer h.Close()

	const size = 500 << 10 // divides evenly by len("EVICT")=5
	data := bytes.Repeat([]byte("EVICT"), size/5)
	if _, err := h.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	mv := mover.Start(mover.Config{
		Index:          idx,
		HotMaxBytes:    1,
		HotTargetBytes: 0,
		Tick:           1 << 30,
	})
	defer mv.Stop()
	for i := 0; i < 3; i++ {
		if err := mv.RunOnce(); err != nil {
			t.Fatal(err)
		}
	}

	// Every stripe should now report cold-only.
	err = idx.IterStripes(func(si index.StripeInfo) bool {
		if si.HotBytes != 0 {
			t.Errorf("after evict: inode=%d stripe=%d still has hot bytes=%d", si.Inode, si.StripeID, si.HotBytes)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// Bytes still match.
	got := make([]byte, size)
	if _, err := h.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("post-evict bytes differ")
	}
}

// TestPersistsAcrossReopen verifies the whole stack (segments + bbolt + dir
// tree) round-trips through Close/Open without losing data.
func TestPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Mkdir(RootInode, "dir", 0o755, 0, 0); err != nil {
		t.Fatal(err)
	}
	d, _ := s.Lookup(RootInode, "dir")
	want := []byte("durable bytes")
	_, h, _ := s.Create(d.Inode, "f", 0o644, 0, 0)
	if _, err := h.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	idx2, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := Open(idx2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	n, err := s2.LookupPath("/dir/f")
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := s2.OpenInode(n.Inode, 0)
	defer h2.Close()
	got := make([]byte, len(want))
	if _, err := h2.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("post-reopen: got %q want %q", got, want)
	}
}

// TestBulkWriterRoundTrip exercises the bulk-ingest path used by the
// migrator: write many chunks without per-chunk fsync, Commit once, and
// confirm bytes/size/mtime all line up.
func TestBulkWriterRoundTrip(t *testing.T) {
	s := newStore(t)
	n, h, err := s.Create(RootInode, "bulk.bin", 0o644, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Pre-write metadata, capture mtime.
	preStat, _ := s.Stat(n.Inode)

	bw := h.NewBulkWriter(TierCold)
	chunk := bytes.Repeat([]byte("BULK"), 32<<10) // 128 KiB
	for i := 0; i < 16; i++ {
		if err := bw.Write(int64(i)*int64(len(chunk)), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Commit(); err != nil {
		t.Fatal(err)
	}

	postStat, err := s.Stat(n.Inode)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(16) * int64(len(chunk))
	if postStat.Size != want {
		t.Errorf("Size=%d want %d", postStat.Size, want)
	}
	if postStat.Mtime <= preStat.Mtime {
		t.Errorf("Mtime not bumped after Commit (pre=%d post=%d)", preStat.Mtime, postStat.Mtime)
	}

	// Bytes match a full sequential read.
	got := make([]byte, want)
	if _, err := h.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		dst := got[int64(i)*int64(len(chunk)) : int64(i+1)*int64(len(chunk))]
		if !bytes.Equal(dst, chunk) {
			t.Fatalf("chunk %d mismatched", i)
		}
	}
}

// TestBulkWriterLandsInTargetTier confirms bytes go where requested,
// not the default tier.
func TestBulkWriterLandsInTargetTier(t *testing.T) {
	s := newStore(t)
	n, h, err := s.Create(RootInode, "tiered.bin", 0o644, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	bw := h.NewBulkWriter(TierCold)
	if err := bw.Write(0, bytes.Repeat([]byte("X"), 4096)); err != nil {
		t.Fatal(err)
	}
	if err := bw.Commit(); err != nil {
		t.Fatal(err)
	}
	hotBytes := int64(0)
	coldBytes := int64(0)
	s.Index().IterStripes(func(si index.StripeInfo) bool {
		if si.Inode != n.Inode {
			return true
		}
		hotBytes += si.HotBytes
		coldBytes += si.ColdBytes
		return true
	})
	if hotBytes != 0 {
		t.Errorf("hot bytes %d, want 0 (BulkWriter targeted cold)", hotBytes)
	}
	if coldBytes == 0 {
		t.Errorf("cold bytes 0, want > 0")
	}
}

// TestTornTailRecovery corrupts a segment's tail and verifies that
// (a) RepairTail in index.Open trims it cleanly, and (b) data committed
// before the corruption is still readable.
func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := Open(idx)
	_, h, _ := s.Create(RootInode, "x", 0o644, 0, 0)
	data := bytes.Repeat([]byte("X"), 4096)
	h.WriteAt(data, 0)
	h.Sync()
	h.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Append garbage to every hot segment file.
	hotSegDir := dir + "/hot/.placefs/segments"
	entries, _ := readDirNames(hotSegDir)
	for _, name := range entries {
		f, err := openAppend(hotSegDir + "/" + name)
		if err != nil {
			t.Fatal(err)
		}
		f.Write([]byte("GARBAGE-NOT-A-RECORD-MAGIC"))
		f.Close()
	}

	idx2, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := Open(idx2)
	defer s2.Close()
	n, _ := s2.Lookup(RootInode, "x")
	h2, _ := s2.OpenInode(n.Inode, 0)
	got := make([]byte, len(data))
	if _, err := h2.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("after corruption-and-recover: data mismatch")
	}
	_ = fmt.Sprintf
	_ = segment.TierHot
}
