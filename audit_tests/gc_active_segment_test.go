package audit_tests

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestGCPassNeverDeletesActiveSegment is a regression guard for bug #22.
//
// gcPass historically captured set.Active() once before iterating segs and
// used that snapshot for the whole pass. If the writer rotated mid-pass —
// or, more interestingly, if a Live=0 SegmentMeta existed for a segment
// that became the active between snapshot and deletion — the post-fix
// in-tx re-check is what keeps gc from unlinking a file the writer is
// about to commit into.
//
// The test stages exactly that race outcome by:
//   1. Forcing a rotation (so seg 0 is sealed and seg 1 is the next id).
//   2. Manually seeding a SegmentMeta(id=1, Live=0, Total>0). This is the
//      shape ListSegments will return for a freshly-active segment that
//      had its first commitBatch leave Live=0 (e.g., a write that landed
//      no payload bytes).
//   3. Calling Active() so the SegmentSet promotes id=1 into the active
//      slot.
//   4. RunGCPass. With the in-iteration AND in-tx activeID checks, id=1
//      must be filtered out and its .seg file must remain on disk.
func TestGCPassNeverDeletesActiveSegment(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	coldPath := filepath.Join(root, "cold")
	for _, p := range []string{filepath.Join(hotPath, ".place"), coldPath} {
		if err := os.MkdirAll(p, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	dbPath := filepath.Join(hotPath, ".place", "meta.db")

	meta, err := place.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()

	hotSegs, err := place.NewSegmentSet(place.TierHot, hotPath, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer hotSegs.CloseAll()
	if err := hotSegs.AttachMeta(meta); err != nil {
		t.Fatal(err)
	}
	coldSegs, err := place.NewSegmentSet(place.TierCold, coldPath, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer coldSegs.CloseAll()
	if err := coldSegs.AttachMeta(meta); err != nil {
		t.Fatal(err)
	}

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	// Land a regular file in seg 0 so the segment is non-empty and stays
	// alive after rotation (Live > 0 keeps it out of deadIDs; Total > 0
	// keeps it from being skipped by the Total==0 filter).
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "anchor", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("anchor", 0, []byte("anchor bytes")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Force-rotate; the next active segment gets a fresh id.
	if _, _, err := hotSegs.RotateIfFull(1 << 30); err != nil {
		t.Fatal(err)
	}
	if _, err := hotSegs.Active(); err != nil {
		t.Fatal(err)
	}
	// Find the active id by taking the highest id in the set — newActive
	// always allocates the next monotonic id (durable counter ensures no
	// reuse), so the latest open segment is the active one.
	all := hotSegs.All()
	if len(all) == 0 {
		t.Fatal("expected at least one segment after rotation")
	}
	activeID := all[len(all)-1]
	if activeID == 0 {
		t.Fatalf("expected active id != 0 after rotation (got %d)", activeID)
	}

	// Seed a Live=0 SegmentMeta for the current active. This is what an
	// older / future writer code path could leave behind: the SegmentMeta
	// exists (so ListSegments returns it) but Live==0 (so deadIDs picks
	// it up if the activeID guard is missing).
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.PutSegmentTx(tx, &place.SegmentMeta{
			ID:    activeID,
			Tier:  place.TierHot,
			Total: 64, // > 0 so the Total==0 filter doesn't skip
			Live:  0,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.Flush(); err != nil {
		t.Fatal(err)
	}

	activePath := filepath.Join(hotSegs.Dir(), segName(activeID))
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active seg file should exist before gc: %v", err)
	}

	// Run gc. The active segment must NOT be deleted.
	reader := place.NewReader(hotSegs, coldSegs, meta)
	ev := place.NewEvictorForTest(nil, meta, hotSegs, coldSegs, 0.9, 0.8)
	c := place.NewCompactorForTest(reader, hotSegs, coldSegs, meta, ev)
	c.RunGCPass(place.TierHot)

	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("gc unlinked the current active segment %d (path=%s): %v — "+
			"the activeID guard in gcPass let a Live=0 SegmentMeta promote "+
			"into deadIDs", activeID, activePath, err)
	}
	// And the SegmentMeta must still be there — gcPass should have skipped
	// the DeleteSegmentTx for activeID.
	sm := getSegMeta(t, meta, place.TierHot, activeID)
	if sm == nil {
		t.Fatalf("gc deleted SegmentMeta for active seg %d", activeID)
	}

	// Sanity: a follow-up write must still land in the active segment.
	if err := w.Submit("anchor", 1024, []byte("post-gc")); err != nil {
		t.Fatalf("post-gc write to active seg failed: %v "+
			"(implies the active fd was clobbered)", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("post-gc flush: %v", err)
	}
}
