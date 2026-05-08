package audit_tests

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestForwardCompactHardlinkUnlinkPreservesData covers bug #1: when a
// hardlinked file's primary path is unlinked, fm.Rel becomes stale.
// forwardCompact then walks the inodes bucket, looks up via paths[fm.Rel],
// finds nothing, treats the inode as gone, and drops the source segment
// without relocating its fragments. The surviving hardlink loses its data.
//
// Sequence:
//  1. Create "primary" — write some bytes; inode 1, fm.Rel="primary".
//  2. Hardlink "alias" → inode 1; Nlink=2.
//  3. Write a second file "killed" into the same hot segment so the segment
//     becomes partially dead after we delete killed.
//  4. Force-rotate so a new active segment is in use.
//  5. Unlink "primary" — inode 1 survives (Nlink=1) but in the buggy
//     world, fm.Rel still says "primary".
//  6. Detach "killed" so its segment goes >30% dead.
//  7. RunGCPass(TierHot) → forwardCompact runs on the partially-dead
//     segment.
//  8. Read "alias" → must return the original bytes.
func TestForwardCompactHardlinkUnlinkPreservesData(t *testing.T) {
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

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "primary", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutFile(&place.FileMeta{
		Rel: "killed", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	preserved := []byte("PRESERVED across compact via hardlink alias!")
	if err := w.Submit("primary", 0, preserved); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("killed", 0, make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Snapshot primary's inodeID and the segment its data lives in.
	fmPrimary, err := meta.GetFile("primary")
	if err != nil || fmPrimary == nil {
		t.Fatalf("get primary: %v %v", fmPrimary, err)
	}
	inodeID := fmPrimary.InodeID
	if len(fmPrimary.HotFragments) == 0 {
		t.Fatal("primary has no hot fragments")
	}
	target := fmPrimary.HotFragments[0].SegmentID

	// Add hardlink "alias" → inode 1, Nlink=2.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		var idBytes [8]byte
		binary.LittleEndian.PutUint64(idBytes[:], inodeID)
		if err := tx.Bucket([]byte("paths")).Put([]byte("/alias"), idBytes[:]); err != nil {
			return err
		}
		fm, err := place.GetFileTx(tx, "primary")
		if err != nil {
			return err
		}
		fm.Nlink = 2
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Force-rotate so the partially-dead seg is no longer active.
	if _, _, err := hotSegs.RotateIfFull(1 << 30); err != nil {
		t.Fatal(err)
	}
	// Land at least one byte in the new active so reconcile/gc don't skip it.
	if err := w.Submit("alias", int64(len(preserved)), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Unlink "primary" — inode survives via "alias". This is where the
	// pre-fix code leaves fm.Rel="primary" stale.
	fmKilled, err := meta.GetFile("killed")
	if err != nil {
		t.Fatal(err)
	}
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		// Standard unlink path doesn't subtract live bytes when Nlink>1
		// (segments still reference the inode's fragments).
		return place.DeleteFileTx(tx, "primary")
	})
	if err != nil {
		t.Fatal(err)
	}

	// Detach killed so its segment crosses the dead threshold.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		if err := place.AddLiveBytesTx(meta, tx, fmKilled.HotFragments, -1); err != nil {
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

	// Confirm seg `target` is partially dead.
	var sm *place.SegmentMeta
	err = meta.DB().View(func(tx *bolt.Tx) error {
		got, e := place.GetSegmentTx(tx, place.TierHot, target)
		sm = got
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if sm == nil || sm.Total == 0 {
		t.Fatalf("setup: expected seg %d to have meta", target)
	}
	dead := float64(sm.Total-sm.Live) / float64(sm.Total)
	t.Logf("target seg %d: total=%d live=%d dead=%.2f%%", target, sm.Total, sm.Live, dead*100)
	if dead < 0.3 {
		t.Fatalf("test setup broken: target dead ratio %.2f < 0.3", dead)
	}

	// Run forwardCompact via the real Compactor.
	reader := place.NewReader(hotSegs, coldSegs, meta)
	ev := place.NewEvictorForTest(nil, meta, hotSegs, coldSegs, 0.9, 0.8)
	c := place.NewCompactorForTest(reader, hotSegs, coldSegs, meta, ev)
	c.RunGCPass(place.TierHot)

	// Read "alias" — bytes must round-trip through forwardCompact.
	got := make([]byte, len(preserved))
	if _, err := reader.ReadAt("alias", got, 0); err != nil {
		t.Fatalf("read alias post-compact: %v (data lost — fm.Rel staleness caused forwardCompact to drop the segment)", err)
	}
	if !bytes.Equal(got, preserved) {
		t.Fatalf("read alias post-compact: got %q want %q", got, preserved)
	}

	// Verify fm.Rel no longer points at the deleted path.
	fmAlias, err := meta.GetFile("alias")
	if err != nil || fmAlias == nil {
		t.Fatalf("get alias: %v %v", fmAlias, err)
	}
	if fmAlias.Rel == "primary" {
		t.Errorf("DeleteFileTx didn't repoint fm.Rel: still %q after primary was unlinked", fmAlias.Rel)
	}
}

// TestRenameRaceLeavesValidRel covers bug #9: forwardCompact captures
// fm.Rel during its scan; if a Rename of the inode's primary path lands
// before rewriteRefs runs, the stale rel resolves to nothing and
// forwardCompact drops the segment. With compaction operating on inodeID,
// the rename is harmless — rewriteRefs re-fetches via getInodeTx and uses
// the (now-updated) fm.Rel.
//
// We exercise this by interleaving manually: do the inode-bucket scan
// (capturing fm.Rel="foo"), rename foo→bar (movePathTx updates fm.Rel),
// then run forwardCompact and verify "bar" is still readable.
func TestRenameRaceLeavesValidRel(t *testing.T) {
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

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "foo", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutFile(&place.FileMeta{
		Rel: "killed", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	preserved := []byte("RENAME race must not lose this data")
	if err := w.Submit("foo", 0, preserved); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("killed", 0, make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	fmFoo, err := meta.GetFile("foo")
	if err != nil || fmFoo == nil {
		t.Fatalf("get foo: %v %v", fmFoo, err)
	}
	target := fmFoo.HotFragments[0].SegmentID

	if _, _, err := hotSegs.RotateIfFull(1 << 30); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("foo", int64(len(preserved)), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Rename foo→bar BEFORE the compact pass. Mirrors a window where the
	// scan captured fm.Rel="foo" and rewriteRefs hadn't yet run. With the
	// fix in place, compaction re-fetches via inode id so the rename is
	// transparent.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		// Replicate node.go:Rename's body just enough to move the path.
		pb := tx.Bucket([]byte("paths"))
		v := pb.Get([]byte("/foo"))
		if v == nil {
			return syscall.ENOENT
		}
		var idBytes [8]byte
		copy(idBytes[:], v)
		if err := pb.Put([]byte("/bar"), idBytes[:]); err != nil {
			return err
		}
		if err := pb.Delete([]byte("/foo")); err != nil {
			return err
		}
		// Update fm.Rel="bar" — mirrors movePathTx's tail behaviour.
		fm, err := place.GetFileTx(tx, "bar")
		if err != nil {
			return err
		}
		if fm == nil {
			return syscall.ENOENT
		}
		fm.Rel = "bar"
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Detach killed.
	fmKilled, err := meta.GetFile("killed")
	if err != nil {
		t.Fatal(err)
	}
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		if err := place.AddLiveBytesTx(meta, tx, fmKilled.HotFragments, -1); err != nil {
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

	var sm *place.SegmentMeta
	_ = meta.DB().View(func(tx *bolt.Tx) error {
		got, _ := place.GetSegmentTx(tx, place.TierHot, target)
		sm = got
		return nil
	})
	if sm == nil {
		t.Fatalf("setup: missing seg %d", target)
	}
	dead := float64(sm.Total-sm.Live) / float64(sm.Total)
	if dead < 0.3 {
		t.Fatalf("test setup: dead ratio %.2f < 0.3", dead)
	}

	reader := place.NewReader(hotSegs, coldSegs, meta)
	ev := place.NewEvictorForTest(nil, meta, hotSegs, coldSegs, 0.9, 0.8)
	c := place.NewCompactorForTest(reader, hotSegs, coldSegs, meta, ev)
	c.RunGCPass(place.TierHot)

	got := make([]byte, len(preserved))
	if _, err := reader.ReadAt("bar", got, 0); err != nil {
		t.Fatalf("read bar after rename+compact: %v", err)
	}
	if !bytes.Equal(got, preserved) {
		t.Fatalf("read bar: got %q want %q", got, preserved)
	}
}
