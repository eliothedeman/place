package audit_tests

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestPutFileTxSilentlyDropsStaleInodeUpdate exercises the silent-drop
// branch in meta.go:619-625. PutFileTx is invoked with a FileMeta whose
// path has been removed from bbolt and whose InodeID points at an inode
// that no longer exists. The function returns nil — the caller has no way
// to tell the data was dropped.
//
// In production this branch fires whenever the writer's overlay flushes
// after a concurrent unlink-and-collect cycle (e.g. last-link unlink,
// then GC of the inode entry). The Writer's commitBatch logged success
// to the user before the flush, so the user's perception of "write
// landed" is divorced from durability.
//
// Severity: latent / data-loss-detection-failure.
func TestPutFileTxSilentlyDropsStaleInodeUpdate(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	// Build a FileMeta carrying an InodeID that does not exist in bbolt.
	// fm.Rel also doesn't exist as a path. PutFileTx's silent-drop branch
	// matches exactly this state.
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
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatalf("PutFileTx returned error %v; expected nil (silent drop)", err)
	}

	// Read-back: nothing was written. Caller has no signal.
	got := readFile(t, meta, "ghost")
	if got != nil {
		t.Fatalf("expected ghost to be absent (drop), got %+v", got)
	}
	t.Logf("BUG CONFIRMED: PutFileTx returned nil but stored nothing; "+
		"a writer flushing this fm would silently lose the user's data. "+
		"Fragments=%v Size=%d", fm.HotFragments, fm.Size)
}

// TestAddLiveBytesTxSilentlySkipsMissingSegment exercises meta.go:868.
// When a fragment references a SegmentMeta that doesn't exist in bbolt,
// AddLiveBytesTx silently skips it — the per-segment Live counter is
// never updated, and the caller has no way to know the fragment was
// orphaned.
//
// This combines with rebuildMetaFromCold (recover.go:81) which creates
// fragments to cold seg IDs but never registers SegmentMeta entries.
// Then reconcile (recover.go:38-60) registers them with Live=0. After
// that, gcPass (compact.go:430) sees Live==0 + Total>0 and unlinks
// the cold segment file. The FileMeta still references it — reads
// surface "segment N missing" EIO.
//
// This test demonstrates the silent-skip primitive directly and the
// full rebuild→reconcile→GC sequence that triggers the data loss.
//
// Severity: data-loss (when --rebuild-meta is used).
func TestAddLiveBytesTxSilentlySkipsMissingSegment(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()

	// Direct demonstration: a fragment to a non-existent seg.
	frags := []place.Fragment{{
		LogicalOffset: 0, Length: 100,
		Tier: place.TierCold, SegmentID: 12345, SegmentOffset: 0,
	}}
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.AddLiveBytesTx(tx, frags, -1)
	})
	if err != nil {
		t.Fatalf("AddLiveBytesTx returned %v on missing segment; expected nil (silent skip)", err)
	}
	t.Logf("AddLiveBytesTx silently returned nil for fragment pointing at non-existent seg 12345.")
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

// TestReaderDiscardsPartialReadOnError exercises reader.go:178-183. When a
// read spans multiple segments and one fails, the parallel execute returns
// the first worker's error and ReadAt returns (0, err). Bytes that other
// workers successfully wrote into dst are discarded — and file.go:65 uses
// `dest[:n]` where n is forced to 0 on error, so the kernel sees an EIO
// with no bytes and the application discards everything.
//
// The "right" behaviour for POSIX read is "return whatever was read up to
// the first hole/error" (short read with partial data). The current
// behaviour means a single bad fragment in a 16 MB read kills the whole
// buffer.
//
// Severity: latent — magnifies a small fault into a much larger one.
func TestReaderDiscardsPartialReadOnError(t *testing.T) {
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
	// Each record carries headerFixedSize(19) + len(rel) + payload + trailer(4)
	// of framing. With maxSize=64, an 8-byte payload (32-byte record) plus a
	// second 8-byte payload fits in one seg. We need the FIRST write to
	// nearly fill the seg, then the second triggers rotation.
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
	survivor := fm.HotFragments[0].SegmentID
	doomed := fm.HotFragments[1].SegmentID

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
	if n != 0 {
		t.Logf("returned partial bytes — bug may be partly fixed (n=%d)", n)
		return
	}
	// Inspect dst for the survivor's bytes — they were written, then
	// discarded by the (n=0, err) return convention.
	survivorRange := dst[fm.HotFragments[0].LogicalOffset : fm.HotFragments[0].LogicalOffset+fm.HotFragments[0].Length]
	if bytes.Equal(survivorRange, bytes.Repeat([]byte("A"), 30)) {
		t.Logf("BUG CONFIRMED: survivor seg %d's bytes WERE written into dst, "+
			"but ReadAt returned n=0 so file.Read truncates to dest[:0]. "+
			"The kernel/app sees EIO with zero bytes — single-fragment failure poisons whole read. "+
			"survivor bytes in dst=%q",
			survivor, survivorRange)
	} else {
		// Workers may not have completed before the error short-circuited.
		// Either way the API surface drops everything.
		t.Logf("survivor bytes not in dst (worker scheduled after error short-circuit). "+
			"Either way: caller has no way to receive partial data. survivor=%v doomed=%v",
			survivor, doomed)
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
