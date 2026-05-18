package store

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eliothedeman/place/index"
)

// TestSetattrTruncateAtomicity exercises the "fragments first, size last"
// ordering. We can't kill the process mid-Setattr in a unit test, but we
// can simulate the worst-case bookkeeping skew: if the size update never
// happened (we treat the file as if Setattr crashed mid-call), data past
// the would-be-new size has already been dropped, and a subsequent read
// clamps to the OLD size, returning the bytes that survived. The
// invariant that matters: there is no window where the index advertises
// more bytes than fragments cover.
func TestSetattrTruncateAtomicity(t *testing.T) {
	s := newStore(t)
	n, h, _ := s.Create(RootInode, "atomicity", 0o644, 0, 0)
	body := bytes.Repeat([]byte("Q"), 4096)
	if _, err := h.WriteAt(body, 0); err != nil {
		t.Fatal(err)
	}
	// The coalescer holds the write in memory; force it to disk so
	// the index-side stripe check below sees fragments. Without this
	// the test only proves the in-memory invariant, which isn't the
	// one the durability ordering claim cares about.
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	h.Close()

	preStripes, _ := s.idx.StripesOf(n.Inode)
	if len(preStripes) == 0 {
		t.Fatalf("expected at least one stripe after write")
	}

	target := int64(100)
	if _, err := s.Setattr(n.Inode, SetAttr{Size: &target}); err != nil {
		t.Fatal(err)
	}

	// Post-truncate read: must be exactly `target` bytes of body[0:target],
	// no junk past it.
	hh, _ := s.OpenInode(n.Inode, 0)
	defer hh.Close()
	got := make([]byte, 4096)
	rn, err := hh.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}
	if int64(rn) != target {
		t.Fatalf("post-truncate read n=%d want %d", rn, target)
	}
	if !bytes.Equal(got[:rn], body[:target]) {
		t.Fatalf("post-truncate read mismatch")
	}

	// PlanRead at the now-out-of-range region must see nothing claiming
	// to live there.
	plan, err := s.idx.PlanRead(n.Inode, target, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, sl := range plan {
		if !sl.Sparse {
			t.Fatalf("PlanRead past truncate point returned a non-sparse slice: %+v", sl)
		}
	}
}

// TestOrphanSegmentBytesAfterCrashSurfaceAsSparseReads simulates the
// "we wrote bytes to disk but never recorded the fragment in bbolt"
// case — that's what would happen if the process died after the segment
// fsync but before the bbolt tx in AppendTo. We mimic by writing directly
// to a freshly-rotated active segment file (bypassing AppendTo), then
// reopening the index and confirming the file looks empty.
func TestOrphanSegmentBytesAfterCrashSurfaceAsSparseReads(t *testing.T) {
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	n, h, _ := s.Create(RootInode, "orphans", 0o644, 0, 0)
	// Force at least one segment to exist by writing then truncating to 0.
	h.WriteAt([]byte{0}, 0)
	zero := int64(0)
	s.Setattr(n.Inode, SetAttr{Size: &zero})
	h.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Now append raw "ghost" payload bytes (no record framing) onto the
	// segment file — these never get a bbolt fragment, so they should
	// be invisible to readers and reapable by GC.
	hotSegDir := filepath.Join(dir, "hot", ".placefs", "segments")
	entries, _ := os.ReadDir(hotSegDir)
	var hadSeg bool
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		hadSeg = true
		f, err := os.OpenFile(filepath.Join(hotSegDir, e.Name()), os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		f.Write([]byte("GHOSTPAYLOADWITHNOREFINBBOLT"))
		f.Close()
	}
	if !hadSeg {
		t.Skip("no hot segment file produced; skipping (file system layout changed?)")
	}

	// Reopen. The torn-tail recovery should trim the un-framed bytes.
	idx2, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := Open(idx2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	nn, err := s2.Lookup(RootInode, "orphans")
	if err != nil {
		t.Fatal(err)
	}
	st, _ := s2.Stat(nn.Inode)
	if st.Size != 0 {
		t.Fatalf("size after crash-style ghost bytes: %d, want 0", st.Size)
	}
	// A read of any length should produce 0 bytes (no data, file is empty).
	hh, _ := s2.OpenInode(nn.Inode, 0)
	defer hh.Close()
	buf := make([]byte, 128)
	rn, err := hh.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rn != 0 {
		t.Fatalf("read of empty file returned n=%d bytes", rn)
	}
}

// TestBulkWriterAbortLeavesNoVisibleData confirms that aborting a bulk
// write session — what happens when a partial migration fails and rolls
// back — leaves the file unmodified from the reader's point of view.
func TestBulkWriterAbortLeavesNoVisibleData(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "abort.bin", 0o644, 0, 0)
	defer h.Close()
	h.WriteAt([]byte("HELLO"), 0)
	preStat, _ := s.Stat(h.Inode())

	bw := h.NewBulkWriter(TierCold)
	if err := bw.Write(0, bytes.Repeat([]byte("X"), 1<<20)); err != nil {
		t.Fatal(err)
	}
	bw.Abort()

	postStat, _ := s.Stat(h.Inode())
	if postStat.Size != preStat.Size {
		t.Errorf("Size changed after Abort: pre=%d post=%d", preStat.Size, postStat.Size)
	}
	if postStat.Mtime != preStat.Mtime {
		t.Errorf("Mtime changed after Abort: pre=%d post=%d", preStat.Mtime, postStat.Mtime)
	}
	got := make([]byte, 5)
	if _, err := h.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != "HELLO" {
		t.Fatalf("read after Abort returned %q, want %q (aborted bytes leaked)", got, "HELLO")
	}
}
