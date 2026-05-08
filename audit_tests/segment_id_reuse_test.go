package audit_tests

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestSegmentIDReuseAfterCrash_NoReuse asserts that AttachMeta-backed id
// allocation prevents post-crash reuse of an id whose .seg file was
// unlinked but whose detaching bbolt tx was rolled back.
//
// Pre-fix bug:
//   SegmentSet.loadExisting derived nextID purely from the highest *.seg
//   file. If a crash dropped a higher .seg without persisting the
//   corresponding meta detach, the writer would reissue the same id and
//   future reads of the still-stale FileMeta fragments would return
//   garbage from the freshly-recycled segment.
//
// Post-fix:
//   AttachMeta seeds nextID from a bbolt-backed monotonic counter, and
//   AllocSegmentID bumps that counter under fsync — so even after the
//   highest .seg is unlinked and its detach rolled back, the in-memory
//   counter still picks a strictly larger id.
func TestSegmentIDReuseAfterCrash_NoReuse(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	if err := os.MkdirAll(filepath.Join(hotPath, ".place"), 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(hotPath, ".place", "meta.db")
	segDir := filepath.Join(hotPath, ".place", "segments")

	// Phase A: build a state where bbolt knows about segs 0..5 (counter
	// advanced past 5) AND a FileMeta references seg 5. This mirrors what
	// the writer would have produced before any compaction / GC.
	{
		meta, err := place.NewMeta(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		hotSegs, err := place.NewSegmentSet(place.TierHot, hotPath, 1<<10)
		if err != nil {
			t.Fatal(err)
		}
		// AttachMeta must be wired so AllocSegmentID persists the counter.
		if err := hotSegs.AttachMeta(meta); err != nil {
			t.Fatal(err)
		}
		// Force allocation of ids 0..5 via RotateIfFull (each rotation
		// advances the durable counter under fsync).
		for i := 0; i <= 5; i++ {
			if _, _, err := hotSegs.RotateIfFull(1 << 30); err != nil {
				t.Fatal(err)
			}
		}
		hotSegs.CloseAll()

		// Wipe the auto-generated seg files so we can plant deterministic
		// per-id markers — this lets us tell whether a later read is
		// hitting the original seg-5 bytes or the recycled file's bytes.
		os.RemoveAll(segDir)
		if err := os.MkdirAll(segDir, 0o700); err != nil {
			t.Fatal(err)
		}
		for i := 0; i <= 5; i++ {
			f, err := os.Create(filepath.Join(segDir, segName(uint32(i))))
			if err != nil {
				t.Fatal(err)
			}
			marker := []byte{byte('A' + i), byte('A' + i), byte('A' + i), byte('A' + i)}
			if _, err := f.Write(marker); err != nil {
				t.Fatal(err)
			}
			f.Close()
		}

		// Insert a FileMeta whose HotFragments point at seg 5.
		err = meta.UpdateLocked(func(tx *bolt.Tx) error {
			fm := &place.FileMeta{
				Rel:   "victim",
				Mode:  syscall.S_IFREG | 0o644,
				Size:  4,
				Mtime: time.Now().UnixNano(),
				Ctime: time.Now().UnixNano(),
				Atime: time.Now().UnixNano(),
				Nlink: 1,
				HotFragments: []place.Fragment{
					{LogicalOffset: 0, Length: 4, Tier: place.TierHot, SegmentID: 5, SegmentOffset: 0},
				},
			}
			return place.PutFileTx(meta, tx, fm)
		})
		if err != nil {
			t.Fatal(err)
		}
		// Flush the counter + filemeta to disk so the simulated crash
		// preserves them.
		if err := meta.Flush(); err != nil {
			t.Fatal(err)
		}
		meta.Close()
	}

	// Phase B: simulate the post-crash state. Compaction unlinked seg 5
	// but its detach tx was rolled back (we just delete the file directly,
	// leaving bbolt's view unchanged).
	if err := os.Remove(filepath.Join(segDir, segName(5))); err != nil {
		t.Fatal(err)
	}

	// Phase C: re-Mount-style startup.
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

	// Sanity: seg 5 is gone from disk.
	if _, err := os.Stat(filepath.Join(segDir, segName(5))); !os.IsNotExist(err) {
		t.Fatalf("expected seg 5 file missing, got stat err=%v", err)
	}

	// Phase D: writer allocates a new segment. With AttachMeta wired the
	// id MUST be > 5 — the persisted counter survived the crash even
	// though the disk_max regressed.
	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "fresh",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now,
		Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := hotSegs.RotateIfFull(1 << 30); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("fresh", 0, []byte("ZZZZZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Assert: no seg with id <= 5 was newly created. Specifically id=5
	// must NOT have been reissued.
	if _, err := os.Stat(filepath.Join(segDir, segName(5))); err == nil {
		t.Fatalf("seg 5 was reissued — counter not persistent, post-crash id reuse possible")
	}

	// Phase E: read "victim" — fragments still point at seg 5 which is
	// missing. The reader should error rather than silently serving
	// bytes from a recycled segment.
	r := place.NewReader(hotSegs, nil, meta)
	out := make([]byte, 4)
	if _, err := r.ReadAt("victim", out, 0); err == nil {
		t.Fatalf("expected read of victim to error (seg 5 missing), got %q", out)
	}
}

// TestNextSegmentIDPersistsAcrossRestart asserts the durable counter is
// monotonic across open / close cycles even without any crash simulation.
// Wipes the .seg files between restarts so loadExisting returns nextID=0;
// the assertion is that AttachMeta lifts nextID back up from the persisted
// counter.
func TestNextSegmentIDPersistsAcrossRestart(t *testing.T) {
	root := t.TempDir()
	hotPath := filepath.Join(root, "hot")
	if err := os.MkdirAll(filepath.Join(hotPath, ".place"), 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(hotPath, ".place", "meta.db")
	segDir := filepath.Join(hotPath, ".place", "segments")

	{
		meta, err := place.NewMeta(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		segs, err := place.NewSegmentSet(place.TierHot, hotPath, 1<<10)
		if err != nil {
			t.Fatal(err)
		}
		if err := segs.AttachMeta(meta); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if _, _, err := segs.RotateIfFull(1 << 30); err != nil {
				t.Fatal(err)
			}
		}
		// Pre-restart highest .seg id = 2 (three rotations created
		// 00000000.seg .. 00000002.seg). Counter should be persisted at 3.
		segs.CloseAll()
		if err := meta.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Wipe segments; counter persistence is the only thing keeping us
	// past id 2.
	os.RemoveAll(segDir)
	if err := os.MkdirAll(segDir, 0o700); err != nil {
		t.Fatal(err)
	}

	meta, err := place.NewMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	segs, err := place.NewSegmentSet(place.TierHot, hotPath, 1<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer segs.CloseAll()
	if err := segs.AttachMeta(meta); err != nil {
		t.Fatal(err)
	}
	if _, _, err := segs.RotateIfFull(1 << 30); err != nil {
		t.Fatal(err)
	}
	// New segment must NOT be 0/1/2 — those were used pre-restart.
	for i := 0; i <= 2; i++ {
		if _, err := os.Stat(filepath.Join(segDir, segName(uint32(i)))); err == nil {
			t.Fatalf("post-restart created seg %d — counter not persistent (pre-restart used 0..2)", i)
		}
	}
}
