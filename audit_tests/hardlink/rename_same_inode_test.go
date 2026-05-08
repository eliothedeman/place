package hardlink

import (
	"encoding/binary"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestRenameSameInodeLosesPath drives the real RenameTx body — the same code
// node.go:Rename runs inside its UpdateLocked tx — for the case where the
// source and destination are existing hardlinks of the same inode. POSIX
// rename(2): "If oldpath and newpath are existing hard links referring to
// the same file, then rename() does nothing, and returns a success status."
//
// Pre-fix this collapsed into:
//  1. paths={"a"→42, "b"→42}, inode 42 Nlink=2.
//  2. Rename "a" → "b":
//     - GetFileTx("b") returns inode 42, Nlink=2.
//     - DeleteFileTx("b") decrements Nlink to 1 (paths now {"a"→42}).
//     - movePathTx("a","b") moves the surviving path to "b".
//  3. Final: paths={"b"→42}, Nlink=1. The "a" path is gone — data is
//     still readable via "b" but a process holding "a" is broken.
//
// Post-fix RenameTx detects oldID == newID up front and returns nil; both
// paths remain, Nlink stays at 2, and ctime/version are not bumped.
func TestRenameSameInodeLosesPath(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	mkdir(hotPath)
	mkdir(filepath.Join(hotPath, ".place"))
	dbPath := filepath.Join(hotPath, ".place", "meta.db")

	meta, err := place.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "a",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	fmA, _ := meta.GetFile("a")
	id := fmA.InodeID

	// Add hardlink "b".
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		var idBytes [8]byte
		binary.LittleEndian.PutUint64(idBytes[:], id)
		if err := tx.Bucket([]byte("paths")).Put([]byte("/b"), idBytes[:]); err != nil {
			return err
		}
		fm, _ := place.GetFileTx(tx, "a")
		fm.Nlink = 2
		return place.PutFileTx(meta, tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	preCtime := fmA.Ctime
	preVersion := fmA.Version

	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.RenameTx(tx, "a", "b")
	}); err != nil {
		t.Fatal(err)
	}

	fmA2, _ := meta.GetFile("a")
	fmB2, _ := meta.GetFile("b")
	t.Logf("after rename: a=%v b=%v", fmA2, fmB2)

	if fmA2 == nil {
		t.Fatalf("POSIX violation: rename(a, b) lost path 'a' when both pointed at the same inode")
	}
	if fmB2 == nil {
		t.Fatalf("path 'b' should still exist")
	}
	if fmA2.Nlink != 2 || fmB2.Nlink != 2 {
		t.Errorf("Nlink not conserved: a.Nlink=%d b.Nlink=%d, want 2 each", fmA2.Nlink, fmB2.Nlink)
	}
	if fmA2.InodeID != id || fmB2.InodeID != id {
		t.Errorf("inode identity changed: a=%d b=%d, want %d each", fmA2.InodeID, fmB2.InodeID, id)
	}
	// True no-op: ctime / version must not advance for the POSIX same-inode
	// success path. (If we mutated the inode we'd bump these fields.)
	if fmB2.Ctime != preCtime {
		t.Errorf("ctime bumped on no-op rename: pre=%d post=%d", preCtime, fmB2.Ctime)
	}
	if fmB2.Version != preVersion {
		t.Errorf("version bumped on no-op rename: pre=%d post=%d", preVersion, fmB2.Version)
	}
}

// TestRenameSameInode_SamePath: rename("foo", "foo") trivially resolves to
// the same inodeID and must be a successful no-op — pre-fix this fell
// through DeleteFileTx + movePathTx and lost the only path.
func TestRenameSameInode_SamePath(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	mkdir(hotPath)
	mkdir(filepath.Join(hotPath, ".place"))
	dbPath := filepath.Join(hotPath, ".place", "meta.db")

	meta, err := place.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "foo",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	pre, _ := meta.GetFile("foo")

	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.RenameTx(tx, "foo", "foo")
	}); err != nil {
		t.Fatalf("rename(foo, foo): %v", err)
	}

	post, _ := meta.GetFile("foo")
	if post == nil {
		t.Fatalf("path 'foo' lost by rename(foo, foo)")
	}
	if post.Nlink != 1 {
		t.Errorf("Nlink desync after rename(foo, foo): got %d want 1", post.Nlink)
	}
	if post.InodeID != pre.InodeID {
		t.Errorf("inode changed across no-op rename: pre=%d post=%d", pre.InodeID, post.InodeID)
	}
	if post.Ctime != pre.Ctime {
		t.Errorf("ctime bumped on no-op rename: pre=%d post=%d", pre.Ctime, post.Ctime)
	}
	if post.Version != pre.Version {
		t.Errorf("version bumped on no-op rename: pre=%d post=%d", pre.Version, post.Version)
	}
}

// TestRenameDifferentInodes_StillWorks pins the regular rename path: when
// oldRel and newRel resolve to different inodes (or newRel doesn't exist),
// the move proceeds and ctime/version advance on the moved inode.
func TestRenameDifferentInodes_StillWorks(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	mkdir(hotPath)
	mkdir(filepath.Join(hotPath, ".place"))
	dbPath := filepath.Join(hotPath, ".place", "meta.db")

	meta, err := place.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "src",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	preSrc, _ := meta.GetFile("src")
	srcInode := preSrc.InodeID

	// rename src → dst (dst doesn't exist).
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.RenameTx(tx, "src", "dst")
	}); err != nil {
		t.Fatalf("rename(src, dst): %v", err)
	}
	if fm, _ := meta.GetFile("src"); fm != nil {
		t.Errorf("src should be gone after rename")
	}
	dst, _ := meta.GetFile("dst")
	if dst == nil {
		t.Fatalf("dst missing after rename")
	}
	if dst.InodeID != srcInode {
		t.Errorf("rename should preserve inodeID: got %d want %d", dst.InodeID, srcInode)
	}
	if dst.Version <= preSrc.Version {
		t.Errorf("version not bumped on real rename: pre=%d post=%d", preSrc.Version, dst.Version)
	}

	// Make a separate inode and overwrite dst with it — ensures the
	// overwrite-existing branch still runs.
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "src2",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	src2, _ := meta.GetFile("src2")
	src2Inode := src2.InodeID
	if src2Inode == dst.InodeID {
		t.Fatalf("test setup: src2 and dst share an inode (%d)", src2Inode)
	}
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.RenameTx(tx, "src2", "dst")
	}); err != nil {
		t.Fatalf("rename(src2, dst): %v", err)
	}
	post, _ := meta.GetFile("dst")
	if post == nil {
		t.Fatalf("dst missing after overwrite rename")
	}
	if post.InodeID != src2Inode {
		t.Errorf("dst should now be src2's inode: got %d want %d", post.InodeID, src2Inode)
	}
	if fm, _ := meta.GetFile("src2"); fm != nil {
		t.Errorf("src2 should be gone after overwrite rename")
	}
}
