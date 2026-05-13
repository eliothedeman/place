package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	oldplace "github.com/eliothedeman/place"
	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/migrate"
	"github.com/eliothedeman/place/lib/store"
)

// makeOldFixture builds a small old-format dataset on disk under root with
// the documented layout: {hot}/.place.db for the DB; {hot}/.place/segments/
// and {cold}/.place/segments/ for the segment files.
func makeOldFixture(t *testing.T) (hot, cold string, expected map[string][]byte) {
	t.Helper()
	root := t.TempDir()
	hot = filepath.Join(root, "old-hot")
	cold = filepath.Join(root, "old-cold")
	if err := os.MkdirAll(hot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cold, 0o700); err != nil {
		t.Fatal(err)
	}
	// cmd/place's convention: DB at {hot}/.place.db.
	dbPath := filepath.Join(hot, ".place.db")

	m, err := oldplace.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	hs, err := oldplace.NewSegmentSet(oldplace.TierHot, hot, 1<<30)
	if err != nil {
		m.Close()
		t.Fatal(err)
	}
	cs, err := oldplace.NewSegmentSet(oldplace.TierCold, cold, 1<<30)
	if err != nil {
		hs.CloseAll()
		m.Close()
		t.Fatal(err)
	}
	// AttachMeta wires the durable segment-id counter — required by the
	// writer path; harmless otherwise.
	if err := hs.AttachMeta(m); err != nil {
		t.Fatal(err)
	}
	if err := cs.AttachMeta(m); err != nil {
		t.Fatal(err)
	}
	w := oldplace.NewWriter(hs, m, nil)

	now := time.Now().UnixNano()
	mk := func(rel string, mode uint32) {
		t.Helper()
		if err := m.PutFile(&oldplace.FileMeta{
			Rel: rel, Mode: mode, Mtime: now, Ctime: now, Atime: now, Nlink: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Make some dirs.
	mk("a", syscall.S_IFDIR|0o755)
	mk("a/b", syscall.S_IFDIR|0o755)

	// Regular files with content.
	expected = map[string][]byte{
		"a/hello.txt":  []byte("hello from a"),
		"a/b/large.bin": bytes.Repeat([]byte("LARGE"), 4096),
		"top.txt":      []byte("top-level content"),
	}
	for rel, content := range expected {
		mk(rel, syscall.S_IFREG|0o644)
		if err := w.Submit(rel, 0, content); err != nil {
			t.Fatalf("submit %s: %v", rel, err)
		}
		// Set size so reads stop at the right place.
		fm, err := m.GetFile(rel)
		if err != nil || fm == nil {
			t.Fatalf("getfile %s: %v", rel, err)
		}
		fm.Size = int64(len(content))
		if err := m.PutFile(fm); err != nil {
			t.Fatal(err)
		}
	}

	// A symlink.
	if err := m.PutFile(&oldplace.FileMeta{
		Rel: "a/ptr", Mode: syscall.S_IFLNK | 0o777,
		LinkTarget: "/some/where", Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := hs.CloseAll(); err != nil {
		t.Fatal(err)
	}
	if err := cs.CloseAll(); err != nil {
		t.Fatal(err)
	}
	return hot, cold, expected
}

func TestLooksLikeOldFormat(t *testing.T) {
	hot, _, _ := makeOldFixture(t)
	if !migrate.LooksLikeOldFormat(hot) {
		t.Fatalf("LooksLikeOldFormat(%s) = false; want true", hot)
	}
	if migrate.LooksLikeOldFormat(t.TempDir()) {
		t.Fatalf("LooksLikeOldFormat on empty dir returned true")
	}
}

func TestMigratorCopiesEverything(t *testing.T) {
	oldHot, oldCold, expected := makeOldFixture(t)

	// Set up a fresh new-format store.
	newRoot := t.TempDir()
	idx, err := index.Open(index.Config{Root: newRoot})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	stats, err := migrate.Run(st, migrate.Options{
		OldHot:  oldHot,
		OldCold: oldCold,
		Logger:  func(string, ...any) {},
	})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if stats.Files != len(expected) {
		t.Errorf("migrated %d files, want %d", stats.Files, len(expected))
	}
	if stats.Symlinks != 1 {
		t.Errorf("migrated %d symlinks, want 1", stats.Symlinks)
	}
	if stats.Dirs != 2 {
		t.Errorf("migrated %d dirs, want 2", stats.Dirs)
	}

	// Verify each file's contents are reachable through the new Store.
	for rel, want := range expected {
		n, err := st.LookupPath("/" + rel)
		if err != nil {
			t.Errorf("lookup %s: %v", rel, err)
			continue
		}
		h, err := st.OpenInode(n.Inode, 0)
		if err != nil {
			t.Errorf("open %s: %v", rel, err)
			continue
		}
		got := make([]byte, len(want))
		rn, err := h.ReadAt(got, 0)
		h.Close()
		if err != nil {
			t.Errorf("read %s: %v", rel, err)
			continue
		}
		if rn != len(want) || !bytes.Equal(got, want) {
			t.Errorf("content for %s mismatched: got %q (n=%d) want %q", rel, got, rn, want)
		}
	}

	// Verify symlink.
	ln, err := st.LookupPath("/a/ptr")
	if err != nil {
		t.Fatalf("lookup symlink: %v", err)
	}
	if !ln.Mode.IsSymlink() {
		t.Fatalf("/a/ptr not a symlink: %o", ln.Mode)
	}
	if tgt, _ := st.Readlink(ln.Inode); tgt != "/some/where" {
		t.Fatalf("symlink target %q", tgt)
	}
}

// TestSameDirUpgradeFlow simulates the real upgrade path: hot and cold
// directories already contain old-format data, the user runs the new binary
// with the same -hot/-cold paths, the new index opens, the migrator runs,
// and afterward both layouts coexist on disk — old in .place/, new in
// .placefs/ — with the new store fully populated.
func TestSameDirUpgradeFlow(t *testing.T) {
	hot, cold, expected := makeOldFixture(t)

	// Open the new index pointing at the same hot/cold dirs.
	idx, err := index.Open(index.Config{HotDir: hot, ColdDir: cold})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if !migrate.LooksLikeOldFormat(hot) {
		t.Fatalf("LooksLikeOldFormat returned false on fixture")
	}
	stats, err := migrate.Run(st, migrate.Options{
		OldHot:  hot,
		OldCold: cold,
		Logger:  func(string, ...any) {},
	})
	if err != nil {
		t.Fatalf("migrate.Run: %v", err)
	}
	if stats.Files != len(expected) {
		t.Errorf("files migrated %d want %d", stats.Files, len(expected))
	}

	// Old subtree still present.
	if _, err := os.Stat(filepath.Join(hot, ".place.db")); err != nil {
		t.Errorf("old .place.db gone: %v", err)
	}
	if _, err := os.Stat(filepath.Join(hot, ".place", "segments")); err != nil {
		t.Errorf("old .place/segments gone: %v", err)
	}
	// New subtree present.
	if _, err := os.Stat(filepath.Join(hot, ".placefs", "db.bolt")); err != nil {
		t.Errorf("new db.bolt missing: %v", err)
	}

	// Bytes match through the new Store.
	for rel, want := range expected {
		n, err := st.LookupPath("/" + rel)
		if err != nil {
			t.Errorf("lookup %s: %v", rel, err)
			continue
		}
		h, _ := st.OpenInode(n.Inode, 0)
		got := make([]byte, len(want))
		h.ReadAt(got, 0)
		h.Close()
		if !bytes.Equal(got, want) {
			t.Errorf("content mismatch for %s", rel)
		}
	}
}

// TestMigratorWritesToCold verifies the default target tier puts migrated
// bytes in cold, leaving hot segments empty. This is what makes a bulk
// upgrade safe when the hot drive is much smaller than the dataset.
func TestMigratorWritesToCold(t *testing.T) {
	oldHot, oldCold, _ := makeOldFixture(t)
	newRoot := t.TempDir()
	idx, err := index.Open(index.Config{Root: newRoot})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := migrate.Run(st, migrate.Options{
		OldHot: oldHot, OldCold: oldCold,
		Logger: func(string, ...any) {},
	}); err != nil {
		t.Fatal(err)
	}

	// Walk every stripe; none should have hot bytes.
	err = idx.IterStripes(func(s index.StripeInfo) bool {
		if s.HotBytes > 0 {
			t.Errorf("inode=%d stripe=%d landed %d hot bytes; default migrate should write to cold",
				s.Inode, s.StripeID, s.HotBytes)
		}
		if s.ColdBytes == 0 {
			t.Errorf("inode=%d stripe=%d has no cold bytes; migrate didn't write to cold",
				s.Inode, s.StripeID)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestMigratorCleanupRemovesLegacyFiles verifies that --migrate-cleanup
// deletes the legacy DB and segments on a successful run, and that the
// new dataset is still readable after.
func TestMigratorCleanupRemovesLegacyFiles(t *testing.T) {
	oldHot, oldCold, expected := makeOldFixture(t)
	newRoot := t.TempDir()
	idx, err := index.Open(index.Config{Root: newRoot})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := migrate.Run(st, migrate.Options{
		OldHot: oldHot, OldCold: oldCold,
		Cleanup: true,
		Logger:  func(string, ...any) {},
	}); err != nil {
		t.Fatal(err)
	}

	// Legacy files should be gone.
	if _, err := os.Stat(filepath.Join(oldHot, ".place.db")); !os.IsNotExist(err) {
		t.Errorf(".place.db not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(oldHot, ".place")); !os.IsNotExist(err) {
		t.Errorf(".place dir not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(oldCold, ".place")); !os.IsNotExist(err) {
		t.Errorf("cold .place dir not removed: %v", err)
	}

	// New dataset still works.
	for rel, want := range expected {
		n, err := st.LookupPath("/" + rel)
		if err != nil {
			t.Errorf("post-cleanup lookup %s: %v", rel, err)
			continue
		}
		h, _ := st.OpenInode(n.Inode, 0)
		got := make([]byte, len(want))
		h.ReadAt(got, 0)
		h.Close()
		if !bytes.Equal(got, want) {
			t.Errorf("post-cleanup content mismatch for %s", rel)
		}
	}
}

func TestMigratorRejectsMissingOldData(t *testing.T) {
	newRoot := t.TempDir()
	idx, err := index.Open(index.Config{Root: newRoot})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_, err = migrate.Run(st, migrate.Options{
		OldHot:  filepath.Join(t.TempDir(), "no-such"),
		OldCold: filepath.Join(t.TempDir(), "no-such-cold"),
	})
	if err == nil {
		t.Fatalf("migrate.Run should have failed with no old data")
	}
}
