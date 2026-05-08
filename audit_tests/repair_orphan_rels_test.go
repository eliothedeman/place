package audit_tests

import (
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestRepairOrphanRels constructs the buggy state pre-fix DeleteFileTx
// would leave behind: an inode survives via a sibling hardlink but its
// fm.Rel still names the unlinked primary path. RepairOrphanRels must
// repoint Rel at the surviving path so subsequent compaction sees a
// consistent paths/inodes mapping.
func TestRepairOrphanRels(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	// Create "foo" then add hardlink "bar".
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "foo", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	fm, _ := meta.GetFile("foo")
	id := fm.InodeID

	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		var idBytes [8]byte
		binary.LittleEndian.PutUint64(idBytes[:], id)
		if err := tx.Bucket([]byte("paths")).Put([]byte("/bar"), idBytes[:]); err != nil {
			return err
		}
		fm2, _ := place.GetFileTx(tx, "foo")
		fm2.Nlink = 2
		return place.PutFileTx(meta, tx, fm2)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Construct the buggy post-unlink state: paths["foo"] gone, Nlink=1,
	// but fm.Rel still says "foo". Pre-fix DeleteFileTx left this exact
	// shape on disk for any DB that has run with the bug.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte("paths")).Delete([]byte("/foo")); err != nil {
			return err
		}
		fm2, _ := place.GetFileTx(tx, "bar")
		fm2.Nlink = 1
		fm2.Rel = "foo"
		return place.PutFileTx(meta, tx, fm2)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Pre-repair: GetFile("foo") returns nil, GetFile("bar") returns the
	// inode but with Rel="foo" stale.
	if got, _ := meta.GetFile("foo"); got != nil {
		t.Fatalf("setup: foo path should be gone, got %+v", got)
	}
	fmBar, _ := meta.GetFile("bar")
	if fmBar == nil || fmBar.Rel != "foo" {
		t.Fatalf("setup: expected bar with Rel=foo (stale), got %+v", fmBar)
	}

	if err := place.RepairOrphanRels(meta); err != nil {
		t.Fatalf("RepairOrphanRels: %v", err)
	}

	fmBar2, _ := meta.GetFile("bar")
	if fmBar2 == nil {
		t.Fatal("post-repair: bar gone")
	}
	if fmBar2.Rel != "bar" {
		t.Errorf("post-repair: fm.Rel=%q want %q", fmBar2.Rel, "bar")
	}
}
