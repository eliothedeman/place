package hardlink

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestConcurrentHardlinkWritesLoseData demonstrates a data-loss bug:
// when two paths point at the same inode (hardlink) and a process writes
// to BOTH paths concurrently, the writer's overlay (m.files map keyed on
// rel) ends up with two FileMeta copies that BOTH carry fm.Rel = the
// inode's stored primary name. flushOverlayLocked iterates the map and
// calls PutFileTx(fm), which looks up the inode via fm.Rel — so both
// writes land in the same inode bucket key, and only the LAST one wins.
// The other writer's fragments are forgotten (the bytes sit in the
// segment but no FileMeta references them).
func TestConcurrentHardlinkWritesLoseData(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	if err := mkdir(hotPath); err != nil {
		t.Fatal(err)
	}
	if err := mkdir(filepath.Join(hotPath, ".place")); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(hotPath, ".place", "meta.db")

	meta, err := place.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()

	hotSegs, err := place.NewSegmentSet(place.TierHot, hotPath, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer hotSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	// Create the primary path "foo" — a regular file, empty.
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

	// Look up foo's inodeID.
	fm, err := meta.GetFile("foo")
	if err != nil || fm == nil {
		t.Fatalf("get foo: %v %v", fm, err)
	}
	inodeID := fm.InodeID
	if inodeID == 0 {
		t.Fatal("missing inodeID for foo")
	}

	// Add hardlink "bar" → same inodeID; bump Nlink to 2.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte("paths"))
		ib := tx.Bucket([]byte("inodes"))
		if pb == nil || ib == nil {
			t.Fatal("missing bucket")
		}
		var idBytes [8]byte
		binary.LittleEndian.PutUint64(idBytes[:], inodeID)
		// "bar" has an extra '/' prefix in relKey encoding.
		if err := pb.Put([]byte("/bar"), idBytes[:]); err != nil {
			return err
		}
		// Bump Nlink. Re-read inode, decode, mutate, encode, put.
		// Easier: use PutFileTx after loading and bumping.
		fm2, err := place.GetFileTx(tx, "foo")
		if err != nil {
			return err
		}
		fm2.Nlink = 2
		return place.PutFileTx(meta, tx, fm2)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Confirm both paths resolve to the same inode.
	fmFoo, _ := meta.GetFile("foo")
	fmBar, _ := meta.GetFile("bar")
	if fmFoo == nil || fmBar == nil {
		t.Fatalf("expected both paths: foo=%v bar=%v", fmFoo, fmBar)
	}
	if fmFoo.InodeID != fmBar.InodeID {
		t.Fatalf("hardlinks should share inode, got foo=%d bar=%d",
			fmFoo.InodeID, fmBar.InodeID)
	}

	// Concurrent writes to BOTH paths at non-overlapping offsets.
	dataFoo := []byte("AAAAAAAA") // 8 bytes at offset 0
	dataBar := []byte("BBBBBBBB") // 8 bytes at offset 100
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := w.Submit("foo", 0, dataFoo); err != nil {
			t.Errorf("submit foo: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := w.Submit("bar", 100, dataBar); err != nil {
			t.Errorf("submit bar: %v", err)
		}
	}()
	wg.Wait()

	// Force overlay flush.
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Read back the inode and inspect fragments.
	fmAfter, err := meta.GetFile("foo")
	if err != nil {
		t.Fatal(err)
	}
	fmAfter2, _ := meta.GetFile("bar")

	t.Logf("after flush: foo.HotFragments=%v size=%d", fmAfter.HotFragments, fmAfter.Size)
	t.Logf("after flush: bar.HotFragments=%v size=%d", fmAfter2.HotFragments, fmAfter2.Size)

	// Both writes should be persisted: we expect 2 fragments covering
	// offsets [0, 8) and [100, 108). The shared inode should reflect
	// BOTH writes.
	want := map[int64]int64{0: 8, 100: 8} // logical offset → length
	got := map[int64]int64{}
	for _, f := range fmAfter.HotFragments {
		got[f.LogicalOffset] = f.Length
	}
	if len(got) != len(want) {
		t.Errorf("DATA LOSS: expected fragments at offsets %v, got %v (one writer's data was overwritten)",
			want, got)
	}
	for off, ln := range want {
		if got[off] != ln {
			t.Errorf("DATA LOSS: missing fragment at offset %d len %d (got len %d)", off, ln, got[off])
		}
	}

	// Also: hardlinks should always show identical content/fragments.
	if len(fmAfter.HotFragments) != len(fmAfter2.HotFragments) {
		t.Errorf("hardlinks diverged: foo has %d frags, bar has %d frags",
			len(fmAfter.HotFragments), len(fmAfter2.HotFragments))
	}
}

// TestSequentialHardlinkWritesShareOverlay drives writes through two rels
// to the same inode in sequence, with explicit Flush() between them, and
// asserts the second write sees the first write's fragment list. This
// pins the per-inode overlay invariant from a different angle than the
// concurrent test: even after one path's writes have been flushed, a
// later write through the sibling rel must materialise on the same inode
// (paths share a FileMeta).
func TestSequentialHardlinkWritesShareOverlay(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	if err := mkdir(hotPath); err != nil {
		t.Fatal(err)
	}
	if err := mkdir(filepath.Join(hotPath, ".place")); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(hotPath, ".place", "meta.db")

	meta, err := place.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()

	hotSegs, err := place.NewSegmentSet(place.TierHot, hotPath, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer hotSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	// Create "x" and hardlink "y" → same inode.
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "x",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	fm, _ := meta.GetFile("x")
	id := fm.InodeID
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		var idBytes [8]byte
		binary.LittleEndian.PutUint64(idBytes[:], id)
		if err := tx.Bucket([]byte("paths")).Put([]byte("/y"), idBytes[:]); err != nil {
			return err
		}
		fm2, _ := place.GetFileTx(tx, "x")
		fm2.Nlink = 2
		return place.PutFileTx(meta, tx, fm2)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write through "x" at offset 0, flush to bbolt (overlay drains).
	if err := w.Submit("x", 0, []byte("XXXXXXXX")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Now write through "y" at offset 200. The writer reloads fm via
	// getFileLocked("y") → resolves to the same inode and accumulates
	// onto the inode's existing fragment list.
	if err := w.Submit("y", 200, []byte("YYYYYYYY")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	got, _ := meta.GetFile("x")
	if got == nil {
		t.Fatal("missing x")
	}
	want := map[int64]int64{0: 8, 200: 8}
	have := map[int64]int64{}
	for _, f := range got.HotFragments {
		have[f.LogicalOffset] = f.Length
	}
	if len(have) != len(want) {
		t.Errorf("expected fragments %v, got %v", want, have)
	}
	for off, ln := range want {
		if have[off] != ln {
			t.Errorf("missing fragment at offset %d len %d (got len %d)", off, ln, have[off])
		}
	}
	if got.Size != 208 {
		t.Errorf("size: want 208, got %d", got.Size)
	}
}

func mkdir(p string) error {
	return syscall.Mkdir(p, 0o700)
}
