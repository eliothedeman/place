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

// TestRenameSameInodeLosesPath demonstrates that node.go's Rename, when
// asked to rename one hardlink to another existing-hardlink-of-the-same-
// inode, deletes the source path. POSIX says rename should be a no-op
// when oldpath and newpath refer to the same inode.
//
// Sequence (mirrors node.go:Rename):
//  1. paths={"a"→42, "b"→42}, inode 42 Nlink=2.
//  2. Rename "a" → "b":
//     - GetFileTx("b") returns inode 42, Nlink=2.
//     - DeleteFileTx("b") decrements Nlink to 1 (paths now {"a"→42}).
//     - movePathTx("a","b") moves the surviving path to "b".
//  3. Final: paths={"b"→42}, Nlink=1. The "a" path is gone — data is
//     still readable via "b" but a process holding "a" is broken.
//
// This is also POSIX-violating (rename(2) of same-inode oldpath/newpath
// must be a no-op) and means link counts aren't conserved.
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
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate node.go's Rename "a" → "b" exactly.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "a")
		if err != nil {
			return err
		}
		if fm == nil {
			return syscall.ENOENT
		}
		// Existing dst.
		existing, err := place.GetFileTx(tx, "b")
		if err != nil {
			return err
		}
		if existing != nil {
			if existing.IsDir() {
				return syscall.EISDIR
			}
			// Nlink>1 → skip AddLiveBytesTx.
			if existing.Nlink <= 1 {
				if err := place.AddLiveBytesTx(tx, existing.HotFragments, -1); err != nil {
					return err
				}
				if err := place.AddLiveBytesTx(tx, existing.ColdFragments, -1); err != nil {
					return err
				}
			}
			if err := place.DeleteFileTx(tx, "b"); err != nil {
				return err
			}
		}
		// movePathTx is unexported — replicate it: read paths["a"], write
		// paths["b"], delete paths["a"], update fm.Rel if it was "a".
		pb := tx.Bucket([]byte("paths"))
		v := pb.Get([]byte("/a"))
		if v == nil {
			return syscall.ENOENT
		}
		idLocal := binary.LittleEndian.Uint64(v)
		var idBytes [8]byte
		binary.LittleEndian.PutUint64(idBytes[:], idLocal)
		if err := pb.Put([]byte("/b"), idBytes[:]); err != nil {
			return err
		}
		if err := pb.Delete([]byte("/a")); err != nil {
			return err
		}
		// fm.Rel update.
		fm2, _ := place.GetFileTx(tx, "b")
		if fm2 != nil && fm2.Rel == "a" {
			fm2.Rel = "b"
			fm2.Ctime = time.Now().UnixNano()
			fm2.Version++
			return place.PutFileTx(tx, fm2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// POSIX-correct outcome: both "a" and "b" still resolve to the same
	// inode (Nlink=2). place's actual outcome: only "b" survives, Nlink=1.
	fmA2, _ := meta.GetFile("a")
	fmB2, _ := meta.GetFile("b")
	t.Logf("after rename: a=%v b=%v", fmA2, fmB2)

	if fmA2 == nil {
		t.Errorf("POSIX violation: rename(a, b) lost path 'a' when both pointed at the same inode")
	}
	if fmB2 == nil {
		t.Errorf("path 'b' should still exist")
	}
	if fmA2 != nil && fmB2 != nil {
		if fmA2.Nlink != 2 || fmB2.Nlink != 2 {
			t.Errorf("Nlink not conserved: a.Nlink=%d b.Nlink=%d, want 2 each", fmA2.Nlink, fmB2.Nlink)
		}
	}
	if fmB2 != nil && fmB2.Nlink != 2 {
		t.Errorf("Nlink desync: rename of one hardlink to its sibling dropped Nlink to %d", fmB2.Nlink)
	}
}
