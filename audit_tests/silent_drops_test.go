package audit_tests

import (
	"bytes"
	"log"
	"strings"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestPutFileTxSurfacesStaleInodeDrop pins the operator-visible surface for
// PutFileTx's stale-inode drop branch. Pre-fix the branch was silent: a
// FileMeta whose path was concurrently removed AND whose InodeID no longer
// resolved to an inode entry was dropped on the floor with no log line and
// no counter — so a writer-overlay flush after an unlink-and-collect cycle
// silently lost the user's most recent write, with no signal until a later
// audit (or, in practice, never).
//
// Post-fix:
//   - Meta.PutFileDropped() bumps once per drop so tests and operators can
//     assert (or alert on) drift.
//   - A log line per drop fires with (rel, inodeID) so the cause is
//     traceable in production logs.
//
// The drop semantic is unchanged — re-allocating a fresh inode would
// resurrect a deleted file under a new identity, which is worse than
// dropping. The fix is observability, not behaviour.
func TestPutFileTxSurfacesStaleInodeDrop(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	// Baseline: clean state has no drops.
	if got := meta.PutFileDropped(); got != 0 {
		t.Fatalf("baseline PutFileDropped: got %d want 0", got)
	}

	// Capture log output so we can assert the operator-visible message
	// fires alongside the counter bump. Restore on exit so we don't leak
	// a buffer-redirected logger to other tests in the package.
	var logBuf bytes.Buffer
	prevOut := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	}()

	// Build a FileMeta carrying an InodeID that does not exist in bbolt.
	// fm.Rel also doesn't exist as a path. PutFileTx's stale-inode drop
	// branch matches exactly this state.
	fm := &place.FileMeta{
		Rel:     "ghost",
		InodeID: 9999, // never allocated
		Mode:    syscall.S_IFREG | 0o644,
		Size:    16,
		Nlink:   1,
		HotFragments: []place.Fragment{{
			LogicalOffset: 0, Length: 16,
			Tier: place.TierHot, SegmentID: 1, SegmentOffset: 0,
		}},
	}
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.PutFileTx(meta, tx, fm)
	})
	if err != nil {
		t.Fatalf("PutFileTx returned error %v; expected nil (drop is OK, just must be surfaced)", err)
	}

	// Counter must bump exactly once on the drop.
	if got := meta.PutFileDropped(); got != 1 {
		t.Fatalf("PutFileDropped after one stale-inode call: got %d want 1 "+
			"— pre-fix this counter didn't exist; the drop was silent and the "+
			"writer's user-visible-success was divorced from durability.", got)
	}

	// Log line must fire so production logs name the dropped path/inode.
	logged := logBuf.String()
	if !strings.Contains(logged, `PutFileTx`) ||
		!strings.Contains(logged, `rel="ghost"`) ||
		!strings.Contains(logged, `inodeID=9999`) {
		t.Errorf("expected drop log line naming rel and inodeID; got %q", logged)
	}

	// Read-back: drop semantic is unchanged — nothing was written.
	got := readFile(t, meta, "ghost")
	if got != nil {
		t.Fatalf("expected ghost to be absent (drop), got %+v", got)
	}

	// A second identical call bumps the counter again — every drop is
	// observable, no rate-limiting / dedup.
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.PutFileTx(meta, tx, fm)
	}); err != nil {
		t.Fatal(err)
	}
	if got := meta.PutFileDropped(); got != 2 {
		t.Errorf("PutFileDropped after second drop call: got %d want 2", got)
	}
}

// TestPutFileTxNoDropsOnCleanFlow asserts the counter stays at zero across a
// normal create→write→flush→unlink flow. If clean operation produces any
// drops, the counter is useless as a signal — every legitimate run would
// already be noisy. The drop branch only fires under writer-overlay racing
// with concurrent unlink-and-collect, which a single-threaded test never
// reaches.
func TestPutFileTxNoDropsOnCleanFlow(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "f", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("f", 0, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "f")
		if err != nil {
			return err
		}
		if err := place.AddLiveBytesTx(meta, tx, fm.HotFragments, -1); err != nil {
			return err
		}
		return place.DeleteFileTx(tx, "f")
	}); err != nil {
		t.Fatal(err)
	}

	if got := meta.PutFileDropped(); got != 0 {
		t.Errorf("clean create+write+unlink flow produced %d drop(s); counter is supposed to stay at 0 in clean operation", got)
	}
}

// TestAddLiveBytesTxSurfacesMissingSegmentSkip pins the operator-visible
// surface for AddLiveBytesTx's missing-segment skip branch. Pre-fix the
// branch was silent: a fragment referencing a SegmentMeta that didn't
// exist had its Live credit dropped on the floor with no log line and
// no counter — so a chain like rebuildMetaFromCold → reconcile → gcPass
// could happily race a fragment into a Live=0 segment that GC would then
// unlink, surfacing as "segment N missing" EIO at read time. There was
// no signal until the EIO.
//
// Post-fix:
//   - Meta.AddLiveSkipped() bumps once per missing-segment skip so tests
//     and operators can assert (or alert on) drift.
//   - A log line per skip fires regardless of whether a *Meta is in scope
//     at the call site (some helpers like RenameTx pass nil).
//
// We don't assert log output here — that's environment-dependent and the
// counter is the contract — but the log line is the operator surface.
//
// Severity (post-fix): observable. The skip is still semantically correct
// in narrow cases (segment GC'd a moment earlier), but is no longer silent.
func TestAddLiveBytesTxSurfacesMissingSegmentSkip(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	// Baseline: clean state has no skips.
	if got := meta.AddLiveSkipped(); got != 0 {
		t.Fatalf("baseline AddLiveSkipped: got %d want 0", got)
	}

	// A fragment to a non-existent seg — the missing-segment branch.
	frags := []place.Fragment{{
		LogicalOffset: 0, Length: 100,
		Tier: place.TierCold, SegmentID: 12345, SegmentOffset: 0,
	}}
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.AddLiveBytesTx(meta, tx, frags, -1)
	})
	if err != nil {
		t.Fatalf("AddLiveBytesTx returned %v on missing segment; expected nil (skip is OK, just must be surfaced)", err)
	}
	if got := meta.AddLiveSkipped(); got != 1 {
		t.Fatalf("AddLiveSkipped after one missing-segment call: got %d want 1 "+
			"— pre-fix this counter didn't exist; the skip was silent and the "+
			"Live-byte credit was lost without trace.", got)
	}

	// Two more fragments aggregating to one missing segment + one missing
	// segment in the same call — the agg map collapses by (tier, id), so
	// two distinct missing segments produce two skips.
	frags2 := []place.Fragment{
		{LogicalOffset: 0, Length: 50, Tier: place.TierCold, SegmentID: 12345, SegmentOffset: 0},
		{LogicalOffset: 50, Length: 50, Tier: place.TierCold, SegmentID: 12345, SegmentOffset: 50},
		{LogicalOffset: 100, Length: 50, Tier: place.TierHot, SegmentID: 99999, SegmentOffset: 0},
	}
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.AddLiveBytesTx(meta, tx, frags2, -1)
	}); err != nil {
		t.Fatal(err)
	}
	if got := meta.AddLiveSkipped(); got != 3 {
		t.Errorf("AddLiveSkipped after second call (2 distinct missing segs): got %d want 3", got)
	}
}

// TestAddLiveBytesTxNoSkipsOnCleanFlow asserts the counter stays at zero
// across a normal write→flush→unlink flow. If clean operation produces
// any skips, the counter is useless as a signal — every legitimate run
// would already be noisy.
func TestAddLiveBytesTxNoSkipsOnCleanFlow(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "f", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("f", 0, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Unlink — releases the segment-live-bytes credit through AddLiveBytesTx.
	// All referenced segments still exist at credit time, so no skips.
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "f")
		if err != nil {
			return err
		}
		if err := place.AddLiveBytesTx(meta, tx, fm.HotFragments, -1); err != nil {
			return err
		}
		return place.DeleteFileTx(tx, "f")
	}); err != nil {
		t.Fatal(err)
	}

	if got := meta.AddLiveSkipped(); got != 0 {
		t.Errorf("clean write+unlink flow produced %d skip(s); counter is supposed to stay at 0 in clean operation", got)
	}
}

// TestRebuildMetaThenReconcileThenGCDataLoss exercises the full --rebuild-meta
// path end-to-end: rebuildMetaFromCold → reconcile → gcPass. Pre-fix, rebuild
// reconstructed FileMetas but never registered SegmentMeta for the cold
// segments it parsed. reconcile then created entries with Live=0, and the
// first GC tick (10s after Mount) saw "Live==0 && Total>0" and unlinked
// every cold .seg — wiping all the data the user just rebuilt against.
//
// Post-fix: rebuild credits live bytes per segment as it walks records, so
// reconcile sees a correct SegmentMeta and gcPass leaves the file alone.
func TestRebuildMetaThenReconcileThenGCDataLoss(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()
	coldSegs := newCold(t, cold, 1<<30)

	// Stage 1: produce a real cold segment by writing a file and replicating
	// it. We can't drive Compactor.replicateOne directly (private), but we
	// can write the same record framing into a cold segment via the public
	// Append API — that's the byte-format rebuild parses.
	w := place.NewWriter(hotSegs, meta, nil)
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "rebuilt", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("X"), 64)
	if err := w.Submit("rebuilt", 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Mirror the data into a real cold segment so rebuild has something to
	// parse. We write the framed record via the cold SegmentSet's Append
	// API — same code path replicate uses.
	coldSeg, err := coldSegs.Active()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coldSeg.AppendDataForTest("rebuilt", 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := coldSeg.Sync(); err != nil {
		t.Fatal(err)
	}
	coldSegs.CloseAll()
	hotSegs.CloseAll()

	// Wipe the bbolt FileMeta and any cold SegmentMeta to simulate the
	// "metadata is gone" state that --rebuild-meta is meant to recover from.
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		if err := place.DeleteFileTx(tx, "rebuilt"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Stage 2: invoke the production rebuild path on a freshly-opened cold set.
	coldSegs2, err := place.NewSegmentSet(place.TierCold, cold, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer coldSegs2.CloseAll()
	if err := place.RebuildMetaFromColdForTest(meta, coldSegs2); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	// Verify SegmentMeta got written with Live > 0 (the bug-#6 missing piece).
	rebuiltSegID := coldSegs2.All()[0]
	sm := getSegMeta(t, meta, place.TierCold, rebuiltSegID)
	if sm == nil {
		t.Fatalf("rebuild did not register SegmentMeta for cold seg %d", rebuiltSegID)
	}
	if sm.Live <= 0 {
		t.Fatalf("rebuild registered seg %d with Live=%d; GC will treat it as fully dead", rebuiltSegID, sm.Live)
	}
	if sm.Total <= 0 {
		t.Fatalf("rebuild registered seg %d with Total=%d", rebuiltSegID, sm.Total)
	}

	// Stage 3: run reconcile (bug pre-fix would zero Live here). With the
	// fix, reconcile sees an existing SegmentMeta and leaves it alone.
	reconcileForTest(t, meta, coldSegs2, place.TierCold)
	smAfterReconcile := getSegMeta(t, meta, place.TierCold, rebuiltSegID)
	if smAfterReconcile.Live != sm.Live {
		t.Errorf("reconcile clobbered Live: pre=%d post=%d", sm.Live, smAfterReconcile.Live)
	}

	// Stage 4: simulate gcPass — it should NOT remove the file because
	// Live > 0. Use the same Live==0 criterion as compact.go's gcPass.
	segs, err := meta.ListSegments(place.TierCold)
	if err != nil {
		t.Fatal(err)
	}
	var dead []uint32
	for _, smm := range segs {
		if smm.Total > 0 && smm.Live == 0 {
			dead = append(dead, smm.ID)
		}
	}
	if len(dead) > 0 {
		t.Fatalf("post-rebuild gcPass would unlink %v — data loss reproduced", dead)
	}

	// Stage 5: read the file. It must come back intact.
	fm := readFile(t, meta, "rebuilt")
	if fm == nil {
		t.Fatal("FileMeta missing post-rebuild")
	}
	if int64(len(payload)) != fm.Size {
		t.Errorf("rebuilt size: got %d want %d", fm.Size, len(payload))
	}
	reader := place.NewReader(newHot(t, hot, 1<<30), coldSegs2, meta)
	buf := make([]byte, len(payload))
	if _, err := reader.ReadAt("rebuilt", buf, 0); err != nil {
		t.Fatalf("post-rebuild read: %v", err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatalf("post-rebuild read: got %q want %q", buf, payload)
	}
}

// TestRebuildPreservesLiveAcrossOverwrites checks that rebuild's per-segment
// Live accounting subtracts the byte ranges shadowed by later records. A
// segment full of records that are all superseded by later records correctly
// drops to Live=0 (and is then a candidate for GC, which is the right
// outcome — those bytes really are dead).
func TestRebuildPreservesLiveAcrossOverwrites(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()

	// Pre-create the file so rebuild has metadata to anchor against — not
	// strictly necessary, but mirrors a partial-loss scenario.
	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "f", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Write two records into the same cold segment: the first covers
	// [0, 64), the second overwrites [16, 48) of it. After rebuild,
	// mergeFragment will identify the middle 32 bytes of the first record
	// as superseded; Live should be 64 + 32 - 32 = 64 (32 bytes of the
	// first record's payload remain referenced; all 32 bytes of the second
	// record are referenced; one 32-byte range from the first record is
	// dead).
	seg, err := coldSegs.Active()
	if err != nil {
		t.Fatal(err)
	}
	first := bytes.Repeat([]byte("A"), 64)
	second := bytes.Repeat([]byte("B"), 32)
	if _, err := seg.AppendDataForTest("f", 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.AppendDataForTest("f", 16, second); err != nil {
		t.Fatal(err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatal(err)
	}

	// Wipe the FileMeta so rebuild repopulates from cold.
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.DeleteFileTx(tx, "f")
	}); err != nil {
		t.Fatal(err)
	}

	if err := place.RebuildMetaFromColdForTest(meta, coldSegs); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	// Verify Live equals the sum of currently-live fragment lengths.
	fm := readFile(t, meta, "f")
	if fm == nil {
		t.Fatal("FileMeta missing post-rebuild")
	}
	var liveByFrag int64
	for _, f := range fm.ColdFragments {
		liveByFrag += f.Length
	}

	id := coldSegs.All()[0]
	sm := getSegMeta(t, meta, place.TierCold, id)
	if sm == nil {
		t.Fatalf("no SegmentMeta for seg %d", id)
	}
	if sm.Live != liveByFrag {
		t.Errorf("Live mismatch: SegmentMeta.Live=%d but live fragments sum to %d",
			sm.Live, liveByFrag)
	}
	// Concrete expectation given the overwrite layout above.
	const expectedLive = 64
	if liveByFrag != expectedLive {
		t.Errorf("live fragment sum: got %d want %d (overwrite math)", liveByFrag, expectedLive)
	}
}

// TestReaderReturnsSurvivorBytesOnPartialReadError pins the post-fix
// behaviour for reader.go's parallel execute. When a read spans multiple
// segments and one fails, ReadAt now returns (maxN, err) where maxN is the
// DstOffset of the earliest-failing slice — i.e. the byte length of the
// contiguous prefix that no failed slice intersected. POSIX read(2) permits
// short returns; the FUSE layer surfaces those bytes as a successful short
// read so a subsequent read at off+maxN retries the bad range.
//
// Pre-fix: ReadAt returned (0, err) and file.Read truncated dest[:0],
// magnifying a single-fragment fault into a whole-buffer EIO.
func TestReaderReturnsSurvivorBytesOnPartialReadError(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	// Tiny segment so a second write rotates into a new seg.
	hotSegs := newHot(t, hot, 64)
	defer hotSegs.CloseAll()
	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "f", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Two writes; the small max segment forces them into different segs.
	// First write nearly fills the seg, second triggers rotation.
	if err := w.Submit("f", 0, bytes.Repeat([]byte("A"), 30)); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("f", 100, bytes.Repeat([]byte("B"), 30)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	fm := readFile(t, meta, "f")
	if len(fm.HotFragments) != 2 {
		t.Fatalf("setup: expected 2 fragments, got %v", fm.HotFragments)
	}
	if fm.HotFragments[0].SegmentID == fm.HotFragments[1].SegmentID {
		t.Fatalf("setup: expected different segs, got both in seg %d", fm.HotFragments[0].SegmentID)
	}
	doomed := fm.HotFragments[1].SegmentID
	doomedDstOff := fm.HotFragments[1].LogicalOffset // read starts at off=0

	// Take down the second fragment's segment to force a read failure on
	// it while the first fragment remains readable.
	if err := hotSegs.Remove(doomed); err != nil {
		t.Fatal(err)
	}

	reader := place.NewReader(hotSegs, coldSegs, meta)
	dst := make([]byte, 200) // covers offsets 0..200, intersects both fragments
	n, err := reader.ReadAt("f", dst, 0)
	t.Logf("read after killing seg %d: n=%d err=%v", doomed, n, err)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// maxN must be at least the survivor fragment's end (no failed slice
	// intersects [0, doomedDstOff)). The exact value is the DstOffset of
	// the earliest-failing slice — for this layout, that's the doomed
	// fragment's LogicalOffset.
	if int64(n) != doomedDstOff {
		t.Errorf("expected n=%d (doomed fragment's DstOffset), got %d", doomedDstOff, n)
	}
	// Survivor bytes must be present in dst[:n].
	survivorRange := dst[fm.HotFragments[0].LogicalOffset : fm.HotFragments[0].LogicalOffset+fm.HotFragments[0].Length]
	if !bytes.Equal(survivorRange, bytes.Repeat([]byte("A"), 30)) {
		t.Errorf("survivor bytes missing from dst: got %q", survivorRange)
	}
}

// TestReaderReturnsZeroWhenFirstSliceFails pins the no-survivor edge case.
// When the earliest-failing slice is at DstOffset 0, there's no contiguous
// survivor prefix and ReadAt must return (0, err) so the caller sees a real
// EIO — not a phantom successful short read of zero bytes.
func TestReaderReturnsZeroWhenFirstSliceFails(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 64)
	defer hotSegs.CloseAll()
	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "f", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Two writes that rotate into separate segs (same setup as the
	// survivor test). We then kill the FIRST fragment's segment so the
	// earliest-failing DstOffset is 0.
	if err := w.Submit("f", 0, bytes.Repeat([]byte("A"), 30)); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("f", 100, bytes.Repeat([]byte("B"), 30)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	fm := readFile(t, meta, "f")
	if len(fm.HotFragments) != 2 {
		t.Fatalf("setup: expected 2 fragments, got %v", fm.HotFragments)
	}
	if fm.HotFragments[0].SegmentID == fm.HotFragments[1].SegmentID {
		t.Fatalf("setup: expected different segs, got both in seg %d", fm.HotFragments[0].SegmentID)
	}
	if fm.HotFragments[0].LogicalOffset != 0 {
		t.Fatalf("setup: expected first fragment at offset 0, got %d", fm.HotFragments[0].LogicalOffset)
	}
	doomed := fm.HotFragments[0].SegmentID

	if err := hotSegs.Remove(doomed); err != nil {
		t.Fatal(err)
	}

	reader := place.NewReader(hotSegs, coldSegs, meta)
	dst := make([]byte, 200)
	n, err := reader.ReadAt("f", dst, 0)
	t.Logf("read after killing seg %d (covering offset 0): n=%d err=%v", doomed, n, err)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if n != 0 {
		t.Errorf("expected n=0 (no survivor prefix), got %d", n)
	}
}

// TestRebuildDefaultsToMode0644 is a contract test: --rebuild-meta cannot
// recover Mode bits because the cold record format doesn't encode them. Every
// rebuilt file lands as a regular 0o100644 file. Exec/setuid/setgid are lost.
//
// This is a documented limitation, not a bug — preserving Mode would require
// extending the cold record format with a mode field (or a separate metadata
// record type). Until that change ships, the contract is "rebuild defaults
// to 0o100644 for all regular files". The test pins the contract so it can't
// drift silently if someone changes the default without also adding format
// support.
func TestRebuildDefaultsToMode0644(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()

	// Forge a cold record for "exec" and run rebuild against an empty bbolt.
	seg, err := coldSegs.Active()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seg.AppendDataForTest("exec", 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatal(err)
	}

	if err := place.RebuildMetaFromColdForTest(meta, coldSegs); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	fm := readFile(t, meta, "exec")
	if fm == nil {
		t.Fatal("rebuild produced no FileMeta for exec")
	}
	const want = uint32(0o100644)
	if fm.Mode != want {
		t.Errorf("rebuild Mode contract violated: got 0o%o want 0o%o", fm.Mode, want)
	}
	t.Logf("contract: rebuildMetaFromCold defaults Mode to 0o%o; exec/setuid/setgid "+
		"are lost until the cold record format gains a Mode field.", want)
}
