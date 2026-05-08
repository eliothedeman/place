package audit_tests

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// frameRec mirrors segment.go's frameRecordInto so tests can stamp a cold
// record without going through the unexported recordType arg of Append.
//
// Layout:
//   [magic:4 "PLSE"][type:1=1 data][rel_len:2][rel:N][log_off:8]
//   [payload_len:4][payload:L][crc32:4]  (crc covers type..payload)
//
// Returns (frame bytes, payload start offset within frame).
func frameRec(rel string, logicalOff int64, payload []byte) ([]byte, int) {
	const recordTypeData = 1
	total := 4 + 1 + 2 + len(rel) + 8 + 4 + len(payload) + 4
	buf := make([]byte, total)
	off := 0
	copy(buf[off:], []byte{'P', 'L', 'S', 'E'})
	off += 4
	buf[off] = recordTypeData
	off++
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(rel)))
	off += 2
	off += copy(buf[off:], rel)
	binary.LittleEndian.PutUint64(buf[off:], uint64(logicalOff))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(payload)))
	off += 4
	payloadStart := off
	off += copy(buf[off:], payload)
	crc := crc32.ChecksumIEEE(buf[4:off])
	binary.LittleEndian.PutUint32(buf[off:], crc)
	return buf, payloadStart
}

// stampCold appends a framed record to a cold-tier segment file (creating
// the file if needed) and returns the absolute offset of the payload bytes
// within the file. Bypasses Segment.Append (whose first arg is unexported).
//
// IMPORTANT: After calling this we re-open the SegmentSet so the in-memory
// Segment's tracked size matches the file's actual size; otherwise reads
// using SegmentSet.Get(...).ReadAt would still work (ReadAt uses absolute
// offsets), but writes to the segment via Append would clobber our bytes.
// In these tests we don't append to the same cold segment again post-stamp.
func stampCold(t *testing.T, coldDir string, segID uint32, rel string, logicalOff int64, payload []byte) int64 {
	t.Helper()
	frame, payloadStart := frameRec(rel, logicalOff, payload)
	segDir := filepath.Join(coldDir, ".place", "segments")
	if err := os.MkdirAll(segDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(segDir, segFileBaseName(segID))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	startOff := st.Size()
	if _, err := f.WriteAt(frame, startOff); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	return startOff + int64(payloadStart)
}

func segFileBaseName(id uint32) string {
	out := []byte("00000000.seg")
	v := id
	for i := 7; i >= 0; i-- {
		out[i] = byte('0' + v%10)
		v /= 10
	}
	return string(out)
}

// recLenFor returns the framed record byte length, matching segment.go's
// formula (headerFixedSize=19, trailerSize=4).
func recLenFor(rel string, payload int) int64 {
	return int64(19 + len(rel) + payload + 4)
}

// TestOverwriteAfterReplicateThenEvict guards against the HasColdCopy
// data-loss regression: an overwrite that lands in hot for a range cold
// also covers must NOT be eligible for eviction, even though cold spans
// [0, Size).
//
// Sequence:
//  1. Write "AAAA..." (32 bytes) to file. Hot=[0,32). Size=32.
//  2. Replicate (simulated): cold=[0,32) with the original "A" bytes;
//     ColdDirty cleared to mirror real replicateOne.
//  3. Overwrite at offset 8, 8 bytes "BBBBBBBB". This sets ColdDirty=true
//     on the inode.
//  4. HasColdCopy must return FALSE so eviction skips this file.
//  5. Read [0..32] returns the live "AAAA AAAA BBBB BBBB AAAA ..." bytes.
func TestOverwriteAfterReplicateThenEvict(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()

	// Stamp the cold segment file BEFORE creating the SegmentSet so loadExisting
	// picks it up with the correct in-memory size.
	const coldSegID uint32 = 0
	const N = 32
	originalA := bytes.Repeat([]byte("A"), N)
	coldPayloadOff := stampCold(t, cold, coldSegID, "doomed", 0, originalA)

	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()
	reader := place.NewReader(hotSegs, coldSegs, meta)

	// Step 1: Create "doomed" and write "AAAA..." via the real writer.
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "doomed",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("doomed", 0, originalA); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	// Verify hot read.
	buf := make([]byte, N)
	if _, err := reader.ReadAt("doomed", buf, 0); err != nil {
		t.Fatalf("read original: %v", err)
	}
	if !bytes.Equal(buf, originalA) {
		t.Fatalf("read original: got %q want all A", buf)
	}

	// Step 2: Splice the manually-stamped cold record into the FileMeta.
	// Mirrors what replicateOne does on success: install ColdFragments
	// AND clear ColdDirty in the same tx.
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "doomed")
		if err != nil {
			return err
		}
		fm.ColdFragments = []place.Fragment{{
			LogicalOffset: 0,
			Length:        N,
			Tier:          place.TierCold,
			SegmentID:     coldSegID,
			SegmentOffset: coldPayloadOff,
		}}
		fm.ColdDirty = false
		sm, err := place.GetSegmentTx(tx, place.TierCold, coldSegID)
		if err != nil {
			return err
		}
		if sm == nil {
			sm = &place.SegmentMeta{ID: coldSegID, Tier: place.TierCold, CreatedAt: time.Now().UnixNano()}
		}
		sm.Total += recLenFor("doomed", N)
		sm.Live += int64(N)
		if err := place.PutSegmentTx(tx, sm); err != nil {
			return err
		}
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatalf("simulate replicate: %v", err)
	}

	// Sanity: with cold installed and clean, HasColdCopy reports true.
	if fm := readFile(t, meta, "doomed"); !fm.HasColdCopy() {
		t.Fatalf("post-replicate: expected HasColdCopy=true, got false (ColdFragments=%v ColdDirty=%v)",
			fm.ColdFragments, fm.ColdDirty)
	}

	// Sanity: read with both hot+cold present — hot still wins.
	if _, err := reader.ReadAt("doomed", buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, originalA) {
		t.Fatalf("read post-replicate-sim: got %q want %q", buf, originalA)
	}

	// Step 3: Overwrite at offset 8 with "BBBBBBBB".
	overwrite := []byte("BBBBBBBB")
	if err := w.Submit("doomed", 8, overwrite); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	want := bytes.Repeat([]byte("A"), N)
	copy(want[8:], overwrite)
	got := make([]byte, N)
	if _, err := reader.ReadAt("doomed", got, 0); err != nil {
		t.Fatalf("read after overwrite: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read after overwrite (with hot live): got %q want %q", got, want)
	}

	// Step 4: HasColdCopy must report FALSE — the unreplicated overwrite
	// in hot makes cold stale even though it still spans [0, Size).
	fm := readFile(t, meta, "doomed")
	if fm.HasColdCopy() {
		t.Fatalf("HasColdCopy=true after overwrite — eviction would drop the live bytes; "+
			"ColdFragments=%v ColdDirty=%v Size=%d", fm.ColdFragments, fm.ColdDirty, fm.Size)
	}

	// Step 5: Mirror evict.go:dropCached — only drop hot when HasColdCopy
	// reports true. With the fix in place this is a no-op.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "doomed")
		if err != nil {
			return err
		}
		if !fm.HasColdCopy() {
			return nil
		}
		dead := fm.HotFragments
		if err := place.AddLiveBytesTx(tx, dead, -1); err != nil {
			return err
		}
		fm.HotFragments = nil
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Step 6: Read returns the live post-overwrite bytes.
	got2 := make([]byte, N)
	if _, err := reader.ReadAt("doomed", got2, 0); err != nil {
		t.Fatalf("read after evict: %v", err)
	}
	if !bytes.Equal(got2, want) {
		t.Fatalf("read after evict: got %q, want live %q (eviction dropped newer hot bytes)", got2, want)
	}
}

// TestOverwriteFullFileThenEvict is the full-file variant of the same
// regression: write A's, replicate (cold clean), overwrite [0, Size) with
// B's, attempt evict — read must return B's.
func TestOverwriteFullFileThenEvict(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()

	const coldSegID uint32 = 0
	const N = 16
	original := bytes.Repeat([]byte("A"), N)
	coldPayloadOff := stampCold(t, cold, coldSegID, "f", 0, original)

	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()
	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()
	reader := place.NewReader(hotSegs, coldSegs, meta)

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "f",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("f", 0, original); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Splice cold record into FileMeta and clear ColdDirty (mirrors
	// replicateOne's atomic swap-in of newCold).
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		fm.ColdFragments = []place.Fragment{{
			LogicalOffset: 0,
			Length:        N,
			Tier:          place.TierCold,
			SegmentID:     coldSegID,
			SegmentOffset: coldPayloadOff,
		}}
		fm.ColdDirty = false
		sm, _ := place.GetSegmentTx(tx, place.TierCold, coldSegID)
		if sm == nil {
			sm = &place.SegmentMeta{ID: coldSegID, Tier: place.TierCold, CreatedAt: time.Now().UnixNano()}
		}
		sm.Total += recLenFor("f", N)
		sm.Live += int64(N)
		_ = place.PutSegmentTx(tx, sm)
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Full-file overwrite at offset 0.
	newBs := bytes.Repeat([]byte("B"), N)
	if err := w.Submit("f", 0, newBs); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	fm := readFile(t, meta, "f")
	if fm.HasColdCopy() {
		t.Fatalf("HasColdCopy=true after overwrite — eviction would drop the live bytes; "+
			"ColdFragments=%v ColdDirty=%v Size=%d", fm.ColdFragments, fm.ColdDirty, fm.Size)
	}

	got := make([]byte, N)
	if _, err := reader.ReadAt("f", got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newBs) {
		t.Fatalf("pre-evict read: got %q want %q", got, newBs)
	}

	// Mirror evict.go:dropCached — gated on HasColdCopy.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		if !fm.HasColdCopy() {
			return nil
		}
		dead := fm.HotFragments
		_ = place.AddLiveBytesTx(tx, dead, -1)
		fm.HotFragments = nil
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	got2 := make([]byte, N)
	if _, err := reader.ReadAt("f", got2, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, newBs) {
		t.Fatalf("read after evict: got %q want live %q (eviction dropped newer hot bytes)",
			got2, newBs)
	}
	_ = original
}
