package audit_tests

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestGCForwardCompactOrdering_RelocationsDurableBeforeUnlink drives the
// real forwardCompact path and asserts that FileMeta fragment relocations
// are durable on disk before the source .seg is unlinked.
//
// Pre-fix bug (#4): forwardCompact's tail did
//   1. UpdateLocked rewriting frags → new seg (NoSync, in-memory only)
//   2. set.Remove(target) — physical unlink
//   3. UpdateLocked deleting source SegmentMeta (also NoSync)
// A crash between (1) and the next background sync would roll bbolt back
// to FileMetas still pointing at target.ID — but the .seg file was gone.
// Bytes lost forever.
//
// Post-fix: forwardCompact calls meta.Flush() between the relocation tx
// and set.Remove. After the synchronous gc pass returns, both the
// relocations and the SegmentMeta deletion are durable on disk.
//
// The assertion is end-to-end: open a fresh bbolt handle on a snapshot
// of the db after RunGCPass and verify (a) the FileMeta no longer
// references the target seg and (b) the SegmentMeta is gone.
func TestGCForwardCompactOrdering_RelocationsDurableBeforeUnlink(t *testing.T) {
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

	// Bigger segment cap so we can fit multiple writes in one segment;
	// we control rotation explicitly via RotateIfFull.
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

	// Layout: alive + killed both write into seg 0 (1 MiB cap, easily
	// fits both). Then we force-rotate so seg 1 is the active segment.
	// Then we delete killed so seg 0 is partially-dead — forward-compact
	// will run on seg 0, RELOCATE alive's fragment to seg 1.
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "alive",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now,
		Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "killed",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now,
		Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("alive", 0, []byte("alive bytes preserved across compact!")); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("killed", 0, make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	// Force-rotate: the next active segment must NOT be seg 0 (gcPass
	// skips the active segment).
	if _, _, err := hotSegs.RotateIfFull(1 << 30); err != nil {
		t.Fatal(err)
	}
	// Land at least one byte in the new active so it has Total > 0 and
	// is not skipped by reconcile.
	if err := w.Submit("alive", 2000, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	fmAlive, err := meta.GetFile("alive")
	if err != nil {
		t.Fatal(err)
	}
	fmKilled, err := meta.GetFile("killed")
	if err != nil {
		t.Fatal(err)
	}
	// alive's fragment at offset 0 lives in seg 0 (the soon-to-be-target).
	target := fmAlive.HotFragments[0].SegmentID
	t.Logf("alive frags=%v killed frags=%v target=%d", fmAlive.HotFragments, fmKilled.HotFragments, target)
	// Sanity: target must NOT be the current active seg — gcPass skips it.
	active, err := hotSegs.Active()
	if err != nil {
		t.Fatal(err)
	}
	_ = active

	// Detach killed's fragments and credit segment Live. After this,
	// seg `target` has only alive's small fragment alive — > 30% dead,
	// triggering forward-compact.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		if err := place.AddLiveBytesTx(tx, fmKilled.HotFragments, -1); err != nil {
			return err
		}
		return place.DeleteFileTx(tx, "killed")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.Flush(); err != nil {
		t.Fatal(err)
	}

	// Confirm seg `target` is now > 30% dead.
	var targetSM *place.SegmentMeta
	err = meta.DB().View(func(tx *bolt.Tx) error {
		sm, err := place.GetSegmentTx(tx, place.TierHot, target)
		targetSM = sm
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if targetSM == nil {
		t.Fatal("expected SegmentMeta for target")
	}
	dead := float64(targetSM.Total-targetSM.Live) / float64(targetSM.Total)
	t.Logf("target seg %d: Total=%d Live=%d dead=%.2f%%", target, targetSM.Total, targetSM.Live, dead*100)
	if dead < 0.3 {
		t.Fatalf("test setup broken: target dead ratio %.2f < 0.3", dead)
	}

	targetPath := filepath.Join(hotSegs.Dir(), segName(target))
	if _, err := os.Stat(targetPath); err != nil {
		t.Fatalf("target file should exist before gc: %v", err)
	}

	// Snapshot the bbolt file at the EXACT moment forwardCompact is about
	// to unlink target.seg. The fix's pre-delete + post-delete Flush
	// guarantee that by this instant the on-disk db has no remaining
	// reference to the segment. If Flush is missing, the snapshot will
	// still hold either alive's relocation point at target.SegID or the
	// SegmentMeta itself.
	snapshot := filepath.Join(t.TempDir(), "snapshot.db")
	hotSegs.SetPreRemoveHookForTest(func(id uint32) {
		if id != target {
			return
		}
		if err := copyFile(dbPath, snapshot); err != nil {
			t.Errorf("snapshot at unlink: %v", err)
		}
	})

	reader := place.NewReader(hotSegs, coldSegs, meta)
	ev := place.NewEvictorForTest(nil, meta, hotSegs, coldSegs, 0.9, 0.8)
	c := place.NewCompactorForTest(reader, hotSegs, coldSegs, meta, ev)
	c.RunGCPass(place.TierHot)

	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("target.seg should be unlinked after fwd-compact, stat=%v", err)
	}
	if _, err := os.Stat(snapshot); err != nil {
		t.Fatalf("pre-remove hook never fired (gc didn't reach unlink for target=%d)", target)
	}
	snapDB, err := bolt.Open(snapshot, 0o600, &bolt.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snapDB.Close()
	err = snapDB.View(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "alive")
		if err != nil {
			return err
		}
		if fm == nil {
			t.Fatal("alive file gone from snapshot")
		}
		for _, f := range fm.HotFragments {
			if f.SegmentID == target {
				t.Fatalf("snapshot still references unlinked seg %d in alive.HotFragments — "+
					"fsync ordering broken; a crash at unlink would lose the relocated bytes. "+
					"frags=%v", target, fm.HotFragments)
			}
		}
		sm, err := place.GetSegmentTx(tx, place.TierHot, target)
		if err != nil {
			return err
		}
		if sm != nil {
			t.Fatalf("snapshot still has SegmentMeta for unlinked seg %d — "+
				"post-delete Flush missing or out of order: %+v", target, sm)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// segName returns the on-disk filename for a segment id, matching segment.go's
// segFileName.
func segName(id uint32) string {
	return formatID(id) + ".seg"
}

func formatID(id uint32) string {
	const w = 8
	s := []byte("00000000")
	for i := w - 1; i >= 0; i-- {
		s[i] = byte('0' + id%10)
		id /= 10
	}
	return string(s)
}

func copyFile(src, dst string) error {
	in, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, in, 0o600)
}

// shutPlaceholder forces an unused-import-style guard so the file compiles
// even if certain branches are eliminated.
var _ atomic.Int32
