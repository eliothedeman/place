package audit_tests

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestCreateThenWriteThenCrashLosesFile demonstrates that node.go's Create
// runs UpdateLocked WITHOUT calling meta.Flush — the new file's metadata
// lands in bbolt's NoSync state. The subsequent Writer.Submit appends bytes
// to a hot segment (also unsynced). If a crash happens before any sync:
//
//   - bbolt rolls back: paths bucket has no entry for "newfile".
//   - Hot segment file has the bytes (CRC-valid framed records).
//   - On reconcile, the hot segment is registered in bbolt with Live=0 (or
//     truncated, depending on whether bbolt has any record of it).
//   - The bytes are unrecoverable: rebuildMetaFromCold (recover.go:81) walks
//     COLD segments only, never hot. There is no "rebuild from hot" path.
//
// POSIX permits this for non-fsync'd metadata, but most production
// filesystems (ext4, xfs) durably journal directory entries. place defers
// metadata durability to the next syncInterval (1s) which means up to 1s of
// silent file-creation loss after a power event.
//
// Severity: latent / data-loss (bounded by syncInterval window).
//
// We simulate the crash by snapshotting the bbolt file at the durable point
// (before Create) and restoring after.
func TestCreateThenWriteThenCrashLosesFile(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	hotSegs := newHot(t, hot, 1<<30)
	w := place.NewWriter(hotSegs, meta, nil)

	// Initial sync — capture the durable snapshot at the empty state.
	if err := meta.Flush(); err != nil {
		t.Fatal(err)
	}
	snap := snapshotDB(t, hot)

	// Create "newfile" via UpdateLocked (mirrors node.go:Create's tx body).
	now := time.Now().UnixNano()
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.PutFileTx(meta, tx, &place.FileMeta{
			Rel: "newfile", Mode: syscall.S_IFREG | 0o644,
			Mtime: now, Ctime: now, Atime: now, Nlink: 1,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	// Submit a write — the bytes land in the hot segment.
	if err := w.Submit("newfile", 0, []byte("AAAAAAAA")); err != nil {
		t.Fatal(err)
	}
	// CRASH: do NOT Flush, do NOT Close.
	// Restore bbolt to the pre-Create durable snapshot — simulates power loss.
	hotSegs.CloseAll()
	if err := meta.DB().Close(); err != nil {
		t.Fatal(err)
	}
	restoreDB(t, snap, hot)

	// Restart.
	meta2 := openMeta(t, hot)
	defer meta2.Close()
	hotSegs2 := newHot(t, hot, 1<<30)
	defer hotSegs2.CloseAll()
	reconcileForTest(t, meta2, hotSegs2, place.TierHot)

	// File is gone from bbolt.
	fm := readFile(t, meta2, "newfile")
	if fm != nil {
		t.Fatalf("expected file gone after rollback, got %+v", fm)
	}

	// Hot segment file exists — bytes are still on disk, but no metadata
	// references them. They are orphaned.
	segPath := fmt.Sprintf("%s/.place/segments/00000000.seg", hot)
	st, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("seg 0 missing after recovery: %v", err)
	}
	if st.Size() == 0 {
		// Reconcile may have truncated the seg to bbolt-recorded Total=0 (since
		// bbolt has no record of the seg). Either way, bytes are gone.
		t.Logf("ORPHAN BYTES LOST: hot segment was truncated by reconcile to 0; "+
			"the user-visible file 'newfile' is gone. POSIX-OK without fsync, "+
			"but a window of silent loss equal to syncInterval (1s).")
	} else {
		// Bytes still on disk but no rebuild-from-hot path exists.
		var sm *place.SegmentMeta
		_ = meta2.DB().View(func(tx *bolt.Tx) error {
			got, e := place.GetSegmentTx(tx, place.TierHot, 0)
			sm = got
			return e
		})
		t.Logf("ORPHAN BYTES: seg 0 has %d bytes; SegmentMeta=%+v. No FileMeta "+
			"references them; they will be GC'd as dead. The user's data is "+
			"lost. (rebuildMetaFromCold doesn't walk hot tier.)", st.Size(), sm)
	}
}

// TestUnlinkBeforeFsyncResurrectsFile demonstrates that Unlink (via
// node.go:Unlink → UpdateLocked) is also unsynced. A file that was created+
// fsync'd, then unlinked, then power-loss before next syncTick → on restart
// the file resurrects. POSIX-OK without fsync of the parent dir; note that
// places like rsync rely on rename being durable on close though.
//
// Severity: latent (bounded silent resurrection window).
func TestUnlinkBeforeFsyncResurrectsFile(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	hotSegs := newHot(t, hot, 1<<30)
	w := place.NewWriter(hotSegs, meta, nil)

	// Create + write + flush (durable point).
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "doomed", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("doomed", 0, []byte("XXXXXXXX")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	snap := snapshotDB(t, hot)

	// Unlink — mirrors node.go:Unlink body.
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "doomed")
		if err != nil {
			return err
		}
		if fm == nil {
			return syscall.ENOENT
		}
		if err := place.AddLiveBytesTx(meta, tx, fm.HotFragments, -1); err != nil {
			return err
		}
		return place.DeleteFileTx(tx, "doomed")
	})
	if err != nil {
		t.Fatal(err)
	}

	// CRASH: no Flush.
	w.Close()
	hotSegs.CloseAll()
	meta.DB().Close()
	restoreDB(t, snap, hot)

	// Restart.
	meta2 := openMeta(t, hot)
	defer meta2.Close()
	hotSegs2 := newHot(t, hot, 1<<30)
	defer hotSegs2.CloseAll()
	reconcileForTest(t, meta2, hotSegs2, place.TierHot)

	fm := readFile(t, meta2, "doomed")
	if fm == nil {
		t.Fatal("expected resurrected file, got nil")
	}
	t.Logf("FILE RESURRECTED: 'doomed' was unlinked but a power-loss before "+
		"the next syncInterval (1s) brought it back. fm=%+v", fm)
}

// TestUpdateLockedSyncFsyncsBeforeReturn pins the fix for the create-without-
// fsync window: each path-altering FUSE op (Create / Mkdir / Unlink / Rmdir /
// Rename / Symlink / Link / Setattr / Open-with-O_TRUNC) routes through
// Meta.UpdateLockedSync, which must call db.Sync before returning so a
// crash within the 1s syncInterval no longer rolls the metadata back.
//
// We can't simulate "power loss" in-process well enough to distinguish
// page-cache-visible from durable: a same-process snapshotDB always sees
// the latest mmap'd bytes whether or not bbolt fsync'd. Instead, this test
// asserts the contract directly: Meta.FlushCount() — which Flush bumps on
// success — increments on every UpdateLockedSync. If the field doesn't
// move, the op landed in NoSync limbo and a real crash would lose it.
func TestUpdateLockedSyncFsyncsBeforeReturn(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	mkOp := func(rel string) func(*bolt.Tx) error {
		now := time.Now().UnixNano()
		return func(tx *bolt.Tx) error {
			return place.PutFileTx(meta, tx, &place.FileMeta{
				Rel: rel, Mode: syscall.S_IFREG | 0o644,
				Mtime: now, Ctime: now, Atime: now, Nlink: 1,
			})
		}
	}

	for _, name := range []string{"a", "b", "c"} {
		before := meta.FlushCount()
		if err := meta.UpdateLockedSync(mkOp(name)); err != nil {
			t.Fatalf("UpdateLockedSync(%q): %v", name, err)
		}
		after := meta.FlushCount()
		if after <= before {
			t.Errorf("UpdateLockedSync(%q) did not fsync: FlushCount %d -> %d",
				name, before, after)
		}
	}

	// Sanity-check the negative: plain UpdateLocked must NOT bump
	// FlushCount — that's the bug-window the fix is supposed to close, and
	// a passing assertion here keeps the contract honest if someone later
	// folds Sync into UpdateLocked itself.
	before := meta.FlushCount()
	if err := meta.UpdateLocked(mkOp("d")); err != nil {
		t.Fatal(err)
	}
	if after := meta.FlushCount(); after != before {
		t.Errorf("UpdateLocked unexpectedly fsync'd: FlushCount %d -> %d", before, after)
	}
}
