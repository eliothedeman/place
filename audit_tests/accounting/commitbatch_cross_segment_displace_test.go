package audit_tests

import (
	"syscall"
	"testing"
	"time"

	"github.com/eliothedeman/place"
)

// TestCommitBatchCrossSegmentDisplaceLive pins the cross-segment case after
// the new-segment Live over-count fix: a write into seg B that displaces a
// fragment in seg A must debit A.Live and credit B.Live. The pre-fix code
// path already handled this correctly (A's overlay entry exists by the
// time the displacement runs), but the fix reordered Phase A (segment
// credit) and Phase B (per-request displacement debit) in commitBatch, so
// guard against accidentally regressing the cross-segment side.
func TestCommitBatchCrossSegmentDisplaceLive(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	// Segment cap small enough that a 64 KiB filler fills segment A; the
	// next write rotates into segment B.
	hotSegs := newHot(t, hot, 64<<10)
	defer hotSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "foo",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now,
		Ctime: now,
		Atime: now,
		Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	const payload = 8
	first := make([]byte, payload)
	for i := range first {
		first[i] = 'A'
	}
	// Write 1: 8 bytes at offset 0 → seg A.
	if err := w.Submit("foo", 0, first); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Fill seg A so the next write rotates into a fresh seg B. Each filler
	// chunk is well under the 64 KiB cap and goes to a different logical
	// offset (so it doesn't displace the offset-0 fragment). A few chunks
	// totalling > 64 KiB push the active segment to the rotation threshold.
	const fillerChunk = 16 << 10
	filler := make([]byte, fillerChunk)
	for i := 0; i < 5; i++ {
		off := int64(1<<20) + int64(i)*int64(fillerChunk)
		if err := w.Submit("foo", off, filler); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Write 2: 8 bytes at offset 0 again → lands in seg B, displaces seg A's
	// fragment.
	second := make([]byte, payload)
	for i := range second {
		second[i] = 'B'
	}
	if err := w.Submit("foo", 0, second); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	segs, err := meta.ListSegments(place.TierHot)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("expected ≥2 hot segments after rotation, got %d", len(segs))
	}

	fm, _ := meta.GetFile("foo")
	// Two surviving fragments: the offset-0 displacer (seg B) and the
	// offset-1MiB filler (whichever seg it ended up in).
	var byOff = map[int64]place.Fragment{}
	for _, f := range fm.HotFragments {
		byOff[f.LogicalOffset] = f
	}
	frag0, ok := byOff[0]
	if !ok || frag0.Length != payload {
		t.Fatalf("expected an %d-byte fragment at offset 0, got %v", payload, fm.HotFragments)
	}
	segB := frag0.SegmentID

	// Sum Live across all segments. Across the whole file we have payload
	// bytes at offset 0 plus 60 KiB at offset 1 MiB — total Live should
	// equal HotBytes(). The pre-fix bug appears as Live > HotBytes for
	// the displacing segment.
	var totalLive int64
	var segBLive int64
	for _, sm := range segs {
		totalLive += sm.Live
		if sm.ID == segB {
			segBLive = sm.Live
		}
	}
	if want := fm.HotBytes(); totalLive != want {
		t.Errorf("sum(Live) across segs = %d, want HotBytes()=%d", totalLive, want)
	}
	// segB hosts the new offset-0 fragment. Its Live must include those 8
	// bytes — it MAY also host the filler if both ended up there, in
	// which case Live is payload + 60 KiB. Assert the lower bound.
	if segBLive < payload {
		t.Errorf("seg B Live = %d, want ≥ %d (the displacing fragment lives here)", segBLive, payload)
	}
}
