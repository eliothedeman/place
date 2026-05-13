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

// makeOldFixture builds a small legacy-format dataset on disk under separate
// hot/cold roots. Returns those roots and the expected file content map.
func makeOldFixture(t *testing.T) (hot, cold string, expected map[string][]byte) {
	t.Helper()
	root := t.TempDir()
	hot = filepath.Join(root, "hot")
	cold = filepath.Join(root, "cold")
	if err := os.MkdirAll(hot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cold, 0o700); err != nil {
		t.Fatal(err)
	}
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
	mk("a", syscall.S_IFDIR|0o755)
	mk("a/b", syscall.S_IFDIR|0o755)

	expected = map[string][]byte{
		"a/hello.txt":   []byte("hello from a"),
		"a/b/large.bin": bytes.Repeat([]byte("LARGE"), 4096),
		"top.txt":       []byte("top-level content"),
	}
	for rel, content := range expected {
		mk(rel, syscall.S_IFREG|0o644)
		if err := w.Submit(rel, 0, content); err != nil {
			t.Fatalf("submit %s: %v", rel, err)
		}
		fm, err := m.GetFile(rel)
		if err != nil || fm == nil {
			t.Fatalf("getfile %s: %v", rel, err)
		}
		fm.Size = int64(len(content))
		if err := m.PutFile(fm); err != nil {
			t.Fatal(err)
		}
	}
	// Symlink.
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

// openNew opens a store on the same hot/cold dirs as the legacy fixture.
func openNew(t *testing.T, hot, cold string) *store.Store {
	t.Helper()
	idx, err := index.Open(index.Config{HotDir: hot, ColdDir: cold})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(idx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func legacyExists(hot, cold string) bool {
	for _, p := range []string{
		filepath.Join(hot, ".place.db"),
		filepath.Join(hot, ".place"),
		filepath.Join(cold, ".place"),
	} {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

func TestRunNoLegacyDataNoOp(t *testing.T) {
	root := t.TempDir()
	st := openNew(t, filepath.Join(root, "hot"), filepath.Join(root, "cold"))
	stats, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Files != 0 || stats.Dirs != 0 {
		t.Fatalf("non-zero stats on fresh dir: %+v", stats)
	}
}

func TestRunMigratesEverythingAndCleansUp(t *testing.T) {
	hot, cold, expected := makeOldFixture(t)
	st := openNew(t, hot, cold)

	stats, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Files != len(expected) {
		t.Errorf("files=%d want %d", stats.Files, len(expected))
	}
	if stats.Symlinks != 1 {
		t.Errorf("symlinks=%d want 1", stats.Symlinks)
	}
	if stats.Dirs != 2 {
		t.Errorf("dirs=%d want 2", stats.Dirs)
	}

	// Legacy subtree is gone.
	if legacyExists(hot, cold) {
		t.Errorf("legacy subtree still present after Run")
	}

	// Bytes match through the new store.
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

// TestRunWritesToCold confirms the hardcoded target tier.
func TestRunWritesToCold(t *testing.T) {
	hot, cold, _ := makeOldFixture(t)
	st := openNew(t, hot, cold)

	if _, err := migrate.Run(st); err != nil {
		t.Fatal(err)
	}
	st.Index().IterStripes(func(s index.StripeInfo) bool {
		if s.HotBytes > 0 {
			t.Errorf("inode=%d stripe=%d landed %d hot bytes; should be cold-only",
				s.Inode, s.StripeID, s.HotBytes)
		}
		return true
	})
}

// TestRunIsIdempotent runs migrate twice in a row. Second call should be a
// no-op since the legacy tree is gone.
func TestRunIsIdempotent(t *testing.T) {
	hot, cold, _ := makeOldFixture(t)
	st := openNew(t, hot, cold)

	first, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	second, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	if second.Files != 0 || second.Bytes != 0 {
		t.Fatalf("second run did work: %+v (first was %+v)", second, first)
	}
}

// TestResumeFromPartialState mimics an interrupted migration: we
// pre-migrate a few entries by hand (via Mkdir/Create), then call Run and
// confirm it picks up the rest without erroring on the already-done ones.
func TestResumeFromPartialState(t *testing.T) {
	hot, cold, expected := makeOldFixture(t)
	st := openNew(t, hot, cold)

	// Manually create one of the expected entries before Run sees it.
	d, err := st.Mkdir(store.RootInode, "a", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Create(d.Inode, "hello.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	// Note: we don't populate bytes — the test verifies Run leaves
	// existing-path entries alone (it can't safely re-migrate them
	// without losing whatever's there). The other files do get migrated.

	stats, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	// Should have migrated only the entries that didn't exist yet.
	if stats.Files >= len(expected) {
		t.Errorf("resume migrated %d files; expected fewer than %d", stats.Files, len(expected))
	}
	if legacyExists(hot, cold) {
		t.Errorf("legacy subtree still present after resume run")
	}
	// Each expected path should exist in the new store.
	for rel := range expected {
		if _, err := st.LookupPath("/" + rel); err != nil {
			t.Errorf("path %q missing after resume: %v", rel, err)
		}
	}
}

// TestOrphanLegacySegmentDeleted creates a stray .seg file in the legacy
// segments dir with no FileMeta referencing it, and verifies Run cleans it
// up (principle 4).
func TestOrphanLegacySegmentDeleted(t *testing.T) {
	hot, cold, _ := makeOldFixture(t)

	// Drop a fake unreferenced .seg file into the legacy dir.
	orphanPath := filepath.Join(hot, ".place", "segments", "99999999.seg")
	if err := os.WriteFile(orphanPath, []byte("not a real record"), 0o600); err != nil {
		t.Fatal(err)
	}

	st := openNew(t, hot, cold)
	stats, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	if stats.LegacyOrphans == 0 {
		t.Errorf("LegacyOrphans=0; should have counted the planted orphan")
	}
}

// TestRunGCsNewFormatToo verifies principle 4 also applies to the new
// segments dir: drop an unreferenced .seg in .placefs/segments and confirm
// it's gone after Run.
func TestRunGCsNewFormatToo(t *testing.T) {
	root := t.TempDir()
	hot := filepath.Join(root, "hot")
	cold := filepath.Join(root, "cold")
	st := openNew(t, hot, cold)

	// Plant an orphan .seg in the new segments dir.
	newSegDir := filepath.Join(hot, ".placefs", "segments")
	if err := os.MkdirAll(newSegDir, 0o700); err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(newSegDir, "99999999.seg")
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := migrate.Run(st); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Errorf("new-format orphan not cleaned: %v", err)
	}
}

// TestMigratorSkipsCorruptSourceFiles simulates a corrupt legacy segment
// (truncated to zero) and verifies:
//   - migrate.Run does NOT return an error (per-file failures don't take
//     down the whole migration),
//   - the affected files are counted in Failures,
//   - they are NOT visible in the new store (rollback worked),
//   - the legacy subtree is preserved so the operator can investigate
//     and re-run.
func TestMigratorSkipsCorruptSourceFiles(t *testing.T) {
	hot, cold, expected := makeOldFixture(t)

	// Truncate the only hot segment so every file read fails.
	segFile := filepath.Join(hot, ".place", "segments", "00000000.seg")
	if _, err := os.Stat(segFile); err != nil {
		t.Fatalf("expected segment file: %v", err)
	}
	if err := os.Truncate(segFile, 0); err != nil {
		t.Fatal(err)
	}

	st := openNew(t, hot, cold)
	stats, err := migrate.Run(st)
	if err != nil {
		t.Fatalf("Run errored (should have soft-failed per file): %v", err)
	}

	// All regular files failed.
	if stats.Failures != len(expected) {
		t.Errorf("Failures=%d want %d", stats.Failures, len(expected))
	}
	if stats.Files != 0 {
		t.Errorf("Files=%d, expected 0 successful copies", stats.Files)
	}
	// But non-file entities migrated cleanly.
	if stats.Dirs == 0 || stats.Symlinks == 0 {
		t.Errorf("non-file entities didn't migrate: %+v", stats)
	}

	// None of the failed files should be reachable in the new store.
	for rel := range expected {
		if _, err := st.LookupPath("/" + rel); err == nil {
			t.Errorf("failed file %q still visible after rollback", rel)
		}
	}

	// Legacy subtree preserved for retry.
	if !legacyExists(hot, cold) {
		t.Errorf("legacy subtree removed despite failures — would prevent retry")
	}
}

// TestMigratorRetriesAfterFix simulates the operator fixing the corruption
// and re-running. The previously-failed files should now succeed and the
// legacy tree should finally be cleaned up.
func TestMigratorRetriesAfterFix(t *testing.T) {
	hot, cold, expected := makeOldFixture(t)
	segFile := filepath.Join(hot, ".place", "segments", "00000000.seg")
	originalBytes, err := os.ReadFile(segFile)
	if err != nil {
		t.Fatal(err)
	}
	// Break it.
	if err := os.Truncate(segFile, 0); err != nil {
		t.Fatal(err)
	}

	st := openNew(t, hot, cold)
	first, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	if first.Failures == 0 {
		t.Fatalf("expected failures from corrupted segment")
	}

	// "Operator fixes the underlying problem" — restore the segment bytes.
	if err := os.WriteFile(segFile, originalBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	second, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	if second.Failures != 0 {
		t.Errorf("retry had failures=%d", second.Failures)
	}
	if second.Files != len(expected) {
		t.Errorf("retry migrated %d files want %d", second.Files, len(expected))
	}
	// All files should now be reachable.
	for rel, want := range expected {
		n, err := st.LookupPath("/" + rel)
		if err != nil {
			t.Errorf("post-retry lookup %s: %v", rel, err)
			continue
		}
		h, _ := st.OpenInode(n.Inode, 0)
		got := make([]byte, len(want))
		h.ReadAt(got, 0)
		h.Close()
		if !bytes.Equal(got, want) {
			t.Errorf("post-retry content mismatch for %s", rel)
		}
	}
	// Legacy gone after the clean retry.
	if legacyExists(hot, cold) {
		t.Errorf("legacy subtree still present after clean retry")
	}
}

// TestIncrementalCleanup verifies that legacy .seg files are unlinked as
// their last referencer migrates, rather than all at the end. We approximate
// by checking that after Run completes there are no remaining files in
// {hot,cold}/.place/segments/ AND that segments-reaped count is > 0.
func TestIncrementalCleanup(t *testing.T) {
	hot, cold, _ := makeOldFixture(t)
	st := openNew(t, hot, cold)
	stats, err := migrate.Run(st)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsReaped == 0 && stats.LegacyOrphans == 0 {
		t.Errorf("no legacy segments removed; stats=%+v", stats)
	}
	// Final tree removal should have happened; .place is gone.
	if _, err := os.Stat(filepath.Join(hot, ".place")); !os.IsNotExist(err) {
		t.Errorf("legacy hot .place still present: %v", err)
	}
}
