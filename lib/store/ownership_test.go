package store

import "testing"

func TestCreateRecordsCallerCreds(t *testing.T) {
	s := newStore(t)
	const (
		uid uint32 = 1000
		gid uint32 = 1001
	)
	n, h, err := s.Create(RootInode, "owned", 0o644, uid, gid)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	if n.UID != uid || n.GID != gid {
		t.Errorf("Create returned UID=%d GID=%d, want %d/%d", n.UID, n.GID, uid, gid)
	}
	got, err := s.Stat(n.Inode)
	if err != nil {
		t.Fatal(err)
	}
	if got.UID != uid || got.GID != gid {
		t.Errorf("post-Stat UID=%d GID=%d, want %d/%d", got.UID, got.GID, uid, gid)
	}
}

func TestMkdirAndSymlinkRecordOwnership(t *testing.T) {
	s := newStore(t)
	d, err := s.Mkdir(RootInode, "d", 0o755, 42, 43)
	if err != nil {
		t.Fatal(err)
	}
	if d.UID != 42 || d.GID != 43 {
		t.Errorf("Mkdir UID=%d GID=%d, want 42/43", d.UID, d.GID)
	}
	l, err := s.Symlink(RootInode, "ptr", "/some/where", 7, 8)
	if err != nil {
		t.Fatal(err)
	}
	if l.UID != 7 || l.GID != 8 {
		t.Errorf("Symlink UID=%d GID=%d, want 7/8", l.UID, l.GID)
	}
}
