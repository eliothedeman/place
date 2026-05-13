package store

import (
	"bytes"
	"errors"
	"syscall"
	"testing"

	"github.com/eliothedeman/place/lib/index"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 20, SegmentMaxSize: 1 << 22})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestRootExistsAfterOpen(t *testing.T) {
	s := newStore(t)
	root, err := s.Stat(RootInode)
	if err != nil {
		t.Fatal(err)
	}
	if !root.Mode.IsDir() {
		t.Fatalf("root not a dir, mode=%o", root.Mode)
	}
}

func TestCreateAndReadBack(t *testing.T) {
	s := newStore(t)
	n, h, err := s.Create(RootInode, "hello.txt", 0o644, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !n.Mode.IsRegular() {
		t.Fatalf("expected regular file, mode=%o", n.Mode)
	}
	payload := []byte("hi from L4")
	wn, err := h.WriteAt(payload, 0)
	if err != nil || wn != len(payload) {
		t.Fatalf("write: %v n=%d", err, wn)
	}
	h.Close()

	// Reopen by inode.
	h2, err := s.OpenInode(n.Inode, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	rn, err := h2.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rn != len(payload) || !bytes.Equal(got, payload) {
		t.Fatalf("got %q (n=%d) want %q", got, rn, payload)
	}

	// File size should match the write.
	st, _ := s.Stat(n.Inode)
	if st.Size != int64(len(payload)) {
		t.Fatalf("size %d want %d", st.Size, len(payload))
	}
}

func TestReadPastEOFShortReturns(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "x", 0o644, 0, 0)
	h.WriteAt([]byte("abc"), 0)
	buf := make([]byte, 100)
	n, err := h.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("read past EOF n=%d want 3", n)
	}
	if !bytes.Equal(buf[:3], []byte("abc")) {
		t.Fatalf("garbled read: %q", buf[:3])
	}
}

func TestReadSparseGapIsZero(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "sparse", 0o644, 0, 0)
	h.WriteAt([]byte("END"), 100) // write 3 bytes at offset 100
	buf := make([]byte, 103)
	n, err := h.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 103 {
		t.Fatalf("n=%d want 103", n)
	}
	// First 100 bytes should be zero.
	for i := 0; i < 100; i++ {
		if buf[i] != 0 {
			t.Fatalf("buf[%d]=%d, want 0", i, buf[i])
		}
	}
	if !bytes.Equal(buf[100:], []byte("END")) {
		t.Fatalf("trailing bytes: %q", buf[100:])
	}
}

func TestMkdirReaddir(t *testing.T) {
	s := newStore(t)
	if _, err := s.Mkdir(RootInode, "sub", 0o755, 0, 0); err != nil {
		t.Fatal(err)
	}
	sub, err := s.Lookup(RootInode, "sub")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(sub.Inode, "a.txt", 0o644, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(sub.Inode, "b.txt", 0o644, 0, 0); err != nil {
		t.Fatal(err)
	}
	entries, err := s.Readdir(sub.Inode)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("readdir got %d entries: %v", len(entries), entries)
	}
}

func TestCreateExistingReturnsExist(t *testing.T) {
	s := newStore(t)
	if _, _, err := s.Create(RootInode, "dup", 0o644, 0, 0); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.Create(RootInode, "dup", 0o644, 0, 0)
	if !errors.Is(err, ErrExist) {
		t.Fatalf("second Create returned %v, want EEXIST", err)
	}
}

func TestUnlinkRemovesEntryAndFragments(t *testing.T) {
	s := newStore(t)
	n, h, _ := s.Create(RootInode, "gone", 0o644, 0, 0)
	h.WriteAt([]byte("bye"), 0)
	if err := s.Unlink(RootInode, "gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lookup(RootInode, "gone"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("lookup after unlink: %v", err)
	}
	if _, err := s.Stat(n.Inode); !errors.Is(err, ErrNotExist) {
		t.Fatalf("stat after unlink: %v", err)
	}
	stripes, _ := s.idx.StripesOf(n.Inode)
	if len(stripes) != 0 {
		t.Fatalf("fragments remain after unlink: %v", stripes)
	}
}

func TestRmdirNotEmpty(t *testing.T) {
	s := newStore(t)
	d, _ := s.Mkdir(RootInode, "d", 0o755, 0, 0)
	s.Create(d.Inode, "f", 0o644, 0, 0)
	err := s.Rmdir(RootInode, "d")
	if !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("rmdir non-empty: %v want ENOTEMPTY", err)
	}
}

func TestRenameOverwriteFile(t *testing.T) {
	s := newStore(t)
	_, h1, _ := s.Create(RootInode, "src", 0o644, 0, 0)
	h1.WriteAt([]byte("source"), 0)
	_, h2, _ := s.Create(RootInode, "dst", 0o644, 0, 0)
	h2.WriteAt([]byte("destination"), 0)

	if err := s.Rename(RootInode, "src", RootInode, "dst"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lookup(RootInode, "src"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("src still present: %v", err)
	}
	dst, err := s.Lookup(RootInode, "dst")
	if err != nil {
		t.Fatal(err)
	}
	h, _ := s.OpenInode(dst.Inode, 0)
	buf := make([]byte, 6)
	h.ReadAt(buf, 0)
	if !bytes.Equal(buf, []byte("source")) {
		t.Fatalf("after rename dst has %q, want %q", buf, "source")
	}
}

func TestRenameCrossDir(t *testing.T) {
	s := newStore(t)
	a, _ := s.Mkdir(RootInode, "A", 0o755, 0, 0)
	b, _ := s.Mkdir(RootInode, "B", 0o755, 0, 0)
	s.Create(a.Inode, "x", 0o644, 0, 0)
	if err := s.Rename(a.Inode, "x", b.Inode, "y"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lookup(a.Inode, "x"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("old still present")
	}
	if _, err := s.Lookup(b.Inode, "y"); err != nil {
		t.Fatalf("new missing: %v", err)
	}
}

func TestRenameDirIntoSelfRejected(t *testing.T) {
	s := newStore(t)
	a, _ := s.Mkdir(RootInode, "A", 0o755, 0, 0)
	b, _ := s.Mkdir(a.Inode, "B", 0o755, 0, 0)
	err := s.Rename(RootInode, "A", b.Inode, "moved")
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected EINVAL for cycle, got %v", err)
	}
}

func TestSymlinkReadlink(t *testing.T) {
	s := newStore(t)
	n, err := s.Symlink(RootInode, "ptr", "/some/where", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !n.Mode.IsSymlink() {
		t.Fatalf("not a symlink: %o", n.Mode)
	}
	tgt, err := s.Readlink(n.Inode)
	if err != nil {
		t.Fatal(err)
	}
	if tgt != "/some/where" {
		t.Fatalf("readlink %q", tgt)
	}
}

func TestHardlinkSharesData(t *testing.T) {
	s := newStore(t)
	n, h, _ := s.Create(RootInode, "orig", 0o644, 0, 0)
	h.WriteAt([]byte("shared"), 0)
	_, err := s.Link(n.Inode, RootInode, "alias")
	if err != nil {
		t.Fatal(err)
	}
	// Read through alias.
	a, _ := s.Lookup(RootInode, "alias")
	if a.Inode != n.Inode {
		t.Fatalf("alias inode %d != orig %d", a.Inode, n.Inode)
	}
	hh, _ := s.OpenInode(a.Inode, 0)
	buf := make([]byte, 6)
	hh.ReadAt(buf, 0)
	if !bytes.Equal(buf, []byte("shared")) {
		t.Fatalf("alias read mismatch: %q", buf)
	}
	// Unlink orig — data should still be reachable via alias.
	if err := s.Unlink(RootInode, "orig"); err != nil {
		t.Fatal(err)
	}
	hh, _ = s.OpenInode(a.Inode, 0)
	hh.ReadAt(buf, 0)
	if !bytes.Equal(buf, []byte("shared")) {
		t.Fatalf("after unlink alias read: %q", buf)
	}
}

func TestSetattrTruncate(t *testing.T) {
	s := newStore(t)
	n, h, _ := s.Create(RootInode, "trunc", 0o644, 0, 0)
	h.WriteAt(bytes.Repeat([]byte("x"), 1024), 0)
	zero := int64(10)
	if _, err := s.Setattr(n.Inode, SetAttr{Size: &zero}); err != nil {
		t.Fatal(err)
	}
	st, _ := s.Stat(n.Inode)
	if st.Size != 10 {
		t.Fatalf("after truncate size=%d", st.Size)
	}
	hh, _ := s.OpenInode(n.Inode, 0)
	buf := make([]byte, 1024)
	rn, _ := hh.ReadAt(buf, 0)
	if rn != 10 {
		t.Fatalf("after truncate read n=%d want 10", rn)
	}
}

func TestLookupPath(t *testing.T) {
	s := newStore(t)
	a, _ := s.Mkdir(RootInode, "a", 0o755, 0, 0)
	b, _ := s.Mkdir(a.Inode, "b", 0o755, 0, 0)
	s.Create(b.Inode, "c.txt", 0o644, 0, 0)

	n, err := s.LookupPath("/a/b/c.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !n.Mode.IsRegular() {
		t.Fatalf("LookupPath returned %o", n.Mode)
	}
	root, err := s.LookupPath("/")
	if err != nil {
		t.Fatal(err)
	}
	if root.Inode != RootInode {
		t.Fatalf("root path inode %d want %d", root.Inode, RootInode)
	}
}

func TestInvalidNamesRejected(t *testing.T) {
	s := newStore(t)
	cases := []string{"", ".", "..", "a/b", "a\x00b"}
	for _, name := range cases {
		_, _, err := s.Create(RootInode, name, 0o644, 0, 0)
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("Create(%q) = %v, want EINVAL", name, err)
		}
	}
}

func TestStatfsReturnsSomething(t *testing.T) {
	s := newStore(t)
	st, err := s.Statfs()
	if err != nil {
		t.Fatal(err)
	}
	if st.BlockSize == 0 {
		t.Fatalf("BlockSize zero in statfs")
	}
	// Just a sanity bound — most filesystems we'll be in have at least 4 KiB blocks.
	if st.BlockSize < 512 {
		t.Fatalf("BlockSize=%d looks bogus", st.BlockSize)
	}
	_ = syscall.Statfs_t{}
}
