package audit_tests

import (
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/eliothedeman/place"
)

// TestCommitBatchNewSegmentOverCountsLive exhibits a Live over-count when two
// writes in the SAME commit batch land in a BRAND-NEW hot segment AND one
// displaces the other (e.g., overlapping writes to the same logical offset).
//
// commitBatch's WithOverlay loop processes dead fragments BEFORE the new
// segment's SegmentMeta is created (the segDeltaTotal/Live loop runs after
// the per-request loop). For a brand-new segment, getSegLocked returns nil
// (overlay miss + bbolt miss), so the dead-fragment decrement is silently
// skipped (writer.go:251 "if sm != nil"). The subsequent segDeltaTotal loop
// then credits the FULL Live (sum of all writes) to the new segment, with
// no offset for the displaced bytes.
//
// Net effect: Live for the new segment is over-counted by the displaced
// bytes' length. The displaced fragment is not in any FileMeta (mergeFragment
// dropped it), so the over-count can never be reclaimed by normal eviction —
// only by forward-compaction (which removes the segment wholesale).
//
// This is a latent disk leak: segment retained longer than it should be.
func TestCommitBatchNewSegmentOverCountsLive(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	// Use a single, very large hot segment so all writes land in it (no
	// rotation between them).
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	// Create a regular file "foo".
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

	// Fan out N concurrent writes to the SAME logical offset of the same
	// file. Each write displaces the previous. With enough concurrency they
	// pile up in the writer's channel and get drained into a single batch.
	const N = 64
	const payload = 8
	data := make([]byte, payload)
	for i := range data {
		data[i] = 'A'
	}
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_ = w.Submit("foo", 0, data)
		}()
	}
	wg.Wait()

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Inspect SegmentMeta.
	segs, err := meta.ListSegments(place.TierHot)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("expected exactly 1 hot segment, got %d", len(segs))
	}
	sm := segs[0]

	// File should have exactly one fragment of len `payload` (last write
	// won), so Live MUST be `payload` bytes if accounting is correct.
	fm, _ := meta.GetFile("foo")
	if len(fm.HotFragments) != 1 || fm.HotFragments[0].Length != payload {
		t.Fatalf("expected one %d-byte fragment, got %v", payload, fm.HotFragments)
	}

	// The actual on-disk records: N records of `payload` bytes each.
	// Total should be N * recordSize and Live should be exactly `payload`.
	t.Logf("seg: Total=%d, Live=%d (file has %d live bytes via 1 fragment)",
		sm.Total, sm.Live, payload)

	if sm.Live != payload {
		t.Errorf("Live OVER-COUNT: expected sm.Live=%d (one live fragment), got sm.Live=%d "+
			"(over-count = %d bytes from displaced same-batch writes)",
			payload, sm.Live, sm.Live-payload)
	}
}
