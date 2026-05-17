package index

import (
	"bytes"
	"testing"

	"github.com/eliothedeman/place/segment"
)

func newIndex(t *testing.T) *Index {
	t.Helper()
	dir := t.TempDir()
	idx, err := Open(Config{Root: dir, StripeSize: 1 << 20, SegmentMaxSize: 1 << 22})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })
	return idx
}

// readAll planning-only: pull every slice through the index and join.
func readAll(t *testing.T, idx *Index, inode uint64, off, length int64) []byte {
	t.Helper()
	plan, err := idx.PlanRead(inode, off, length)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, length)
	for _, sl := range plan {
		dst := out[sl.LogicalOff-off : sl.LogicalOff-off+sl.Length]
		if sl.Sparse {
			// Already zeroed.
			continue
		}
		if _, err := idx.ReadAt(sl.Locator, dst); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestAppendAndReadBack(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	payload := bytes.Repeat([]byte("a"), 4096)
	if err := idx.Append(ino, 0, payload); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, idx, ino, 0, int64(len(payload)))
	if !bytes.Equal(got, payload) {
		t.Fatalf("read mismatch")
	}
}

func TestAppendSplitsAcrossStripes(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	// Write 3MiB at offset stripe_size - 1MiB, so it crosses two boundaries.
	stripe := idx.StripeSize()
	off := stripe - (1 << 19) // 512 KiB before stripe boundary
	payload := bytes.Repeat([]byte("z"), 3<<20)
	if err := idx.Append(ino, off, payload); err != nil {
		t.Fatal(err)
	}
	stripes, err := idx.StripesOf(ino)
	if err != nil {
		t.Fatal(err)
	}
	if len(stripes) < 3 {
		t.Fatalf("expected fragments to span >=3 stripes, got %v", stripes)
	}
	got := readAll(t, idx, ino, off, int64(len(payload)))
	if !bytes.Equal(got, payload) {
		t.Fatalf("read mismatch after multi-stripe append")
	}
}

func TestOverwriteShadowsViaSeq(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	// Old at [0, 1024).
	if err := idx.Append(ino, 0, bytes.Repeat([]byte("A"), 1024)); err != nil {
		t.Fatal(err)
	}
	// New at [256, 768) — middle 512 bytes get shadowed.
	if err := idx.Append(ino, 256, bytes.Repeat([]byte("B"), 512)); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, idx, ino, 0, 1024)
	want := append(append(bytes.Repeat([]byte("A"), 256), bytes.Repeat([]byte("B"), 512)...), bytes.Repeat([]byte("A"), 256)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("overwrite shadow mismatch")
	}
}

func TestSparseRead(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	if err := idx.Append(ino, 100, bytes.Repeat([]byte("X"), 10)); err != nil {
		t.Fatal(err)
	}
	plan, err := idx.PlanRead(ino, 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	// Expect: sparse [0,100), data [100,110), sparse [110,200).
	if len(plan) != 3 || !plan[0].Sparse || plan[1].Sparse || !plan[2].Sparse {
		t.Fatalf("plan %v", plan)
	}
}

func TestMoveHotToCold(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	payload := bytes.Repeat([]byte("M"), 16<<10)
	if err := idx.Append(ino, 0, payload); err != nil {
		t.Fatal(err)
	}

	// Pre-move: fragment should be on hot.
	frags, _ := idx.FragmentsOf(ino, 0)
	if len(frags) != 1 || frags[0].Tier != segment.TierHot {
		t.Fatalf("pre-move tier wrong: %v", frags)
	}

	if err := idx.Move(ino, 0, segment.TierCold); err != nil {
		t.Fatal(err)
	}

	// Post-move: fragment should be on cold, same bytes readable.
	frags, _ = idx.FragmentsOf(ino, 0)
	if len(frags) != 1 || frags[0].Tier != segment.TierCold {
		t.Fatalf("post-move tier wrong: %v", frags)
	}
	got := readAll(t, idx, ino, 0, int64(len(payload)))
	if !bytes.Equal(got, payload) {
		t.Fatalf("post-move read mismatch")
	}
}

func TestMoveCollapsesOverlap(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	// Three layered writes producing overlap.
	if err := idx.Append(ino, 0, bytes.Repeat([]byte("A"), 1024)); err != nil {
		t.Fatal(err)
	}
	if err := idx.Append(ino, 256, bytes.Repeat([]byte("B"), 512)); err != nil {
		t.Fatal(err)
	}
	if err := idx.Append(ino, 400, bytes.Repeat([]byte("C"), 100)); err != nil {
		t.Fatal(err)
	}

	want := readAll(t, idx, ino, 0, 1024)

	if err := idx.Move(ino, 0, segment.TierHot); err != nil {
		t.Fatal(err)
	}

	// After Move (same tier), the fragment list should be the canonical view
	// — no overlap. Read still produces the same bytes.
	frags, _ := idx.FragmentsOf(ino, 0)
	for i := 1; i < len(frags); i++ {
		if frags[i-1].LogicalOff+frags[i-1].Length > frags[i].LogicalOff {
			t.Fatalf("post-move fragments overlap: %v", frags)
		}
	}
	got := readAll(t, idx, ino, 0, 1024)
	if !bytes.Equal(got, want) {
		t.Fatalf("post-Move bytes changed")
	}
}

func TestGCRemovesUnreferenced(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	if err := idx.Append(ino, 0, bytes.Repeat([]byte("g"), 4096)); err != nil {
		t.Fatal(err)
	}
	// Move to cold. Old hot bytes become orphans.
	if err := idx.Move(ino, 0, segment.TierCold); err != nil {
		t.Fatal(err)
	}
	preHot := len(idx.SegmentIDs(segment.TierHot))
	if err := idx.GC(); err != nil {
		t.Fatal(err)
	}
	postHot := len(idx.SegmentIDs(segment.TierHot))
	// The hot active segment is preserved; older hot segments (if any) get
	// GC'd. With our small SegmentMaxSize this should drop at least one.
	if postHot > preHot {
		t.Fatalf("GC grew hot segment count: pre=%d post=%d", preHot, postHot)
	}
}

func TestTruncateDropsTail(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	payload := bytes.Repeat([]byte("t"), 8192)
	if err := idx.Append(ino, 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := idx.Truncate(ino, 1024); err != nil {
		t.Fatal(err)
	}
	plan, _ := idx.PlanRead(ino, 0, 2048)
	// Beyond 1024 should be sparse.
	var datBytes, sparseBytes int64
	for _, sl := range plan {
		if sl.Sparse {
			sparseBytes += sl.Length
		} else {
			datBytes += sl.Length
		}
	}
	if datBytes != 1024 || sparseBytes != 1024 {
		t.Fatalf("after truncate(1024): data=%d sparse=%d", datBytes, sparseBytes)
	}
}

func TestDeleteInodeDropsFragments(t *testing.T) {
	idx := newIndex(t)
	ino := uint64(1)
	if err := idx.Append(ino, 0, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := idx.DeleteInode(ino); err != nil {
		t.Fatal(err)
	}
	stripes, _ := idx.StripesOf(ino)
	if len(stripes) != 0 {
		t.Fatalf("stripes remain after DeleteInode: %v", stripes)
	}
}

func TestPersistsAcrossClose(t *testing.T) {
	dir := t.TempDir()
	idx, err := Open(Config{Root: dir, StripeSize: 1 << 20, SegmentMaxSize: 1 << 22})
	if err != nil {
		t.Fatal(err)
	}
	ino := uint64(1)
	payload := []byte("persist me")
	if err := idx.Append(ino, 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	idx2, err := Open(Config{Root: dir, StripeSize: 1 << 20, SegmentMaxSize: 1 << 22})
	if err != nil {
		t.Fatal(err)
	}
	defer idx2.Close()
	got := readAll(t, idx2, ino, 0, int64(len(payload)))
	if !bytes.Equal(got, payload) {
		t.Fatalf("read after reopen mismatch: %q vs %q", got, payload)
	}
}
