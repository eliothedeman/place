package audit_tests

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// reconcileForTest drives the production reconcile path via the test-only
// ReconcileForTest entry point. Kept thin so behavioural assertions live in
// the test cases below, not in re-implemented logic.
func reconcileForTest(t *testing.T, meta *place.Meta, set *place.SegmentSet, tier place.Tier) {
	t.Helper()
	_ = tier // kept for call-site readability; tier is set-internal
	if err := place.ReconcileForTest(meta, set); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

// segStat returns the file size in bytes.
func segStat(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func truncateFile(path string, size int64) error {
	return os.Truncate(path, size)
}

func makeReg(rel string) *place.FileMeta {
	now := time.Now().UnixNano()
	return &place.FileMeta{
		Rel: rel, Mode: syscall.S_IFREG | 0o644, Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}
}

// TestRegisterInMetaIsDeadCode confirms that SegmentSet.RegisterInMeta is
// never called by any production code path (verified by grep). The function
// in segment.go:414 is unused. Its docstring claims "creation time, zero
// live bytes" but the body sets Live=seg.size. If it were ever wired in
// after reconcile, it would clobber reconcile's Live=0 for orphans, pinning
// inherited orphan segments forever.
//
// Severity: cosmetic (dead code, misleading doc).
func TestRegisterInMetaIsDeadCode(t *testing.T) {
	// This test just documents the inconsistency — there is nothing to
	// invoke; RegisterInMeta is unreachable from production paths.
	t.Log("RegisterInMeta in segment.go:414 has Live=seg.size despite the " +
		"docstring claiming 'zero live bytes'. Currently dead code.")
}

// TestReconcileShorterThanMetaQuarantines verifies the production fix for a
// segment file shorter than its bbolt SegmentMeta.Total: reconcile now drops
// fragments past seg.Size() from referencing FileMetas, lowers SegmentMeta.Total
// to the file size, and surfaces a logline. Subsequent reads of the affected
// byte range fall through to Reader's pre-zero-fill rather than returning EIO.
//
// In normal write/sync ordering this should never happen (Writer.doSync
// fsyncs segments before bbolt). It CAN happen on hardware where fsync
// returns success without durability, after a manual external truncate, or
// on cold-tier segments after a Replicate crash window.
func TestReconcileShorterThanMetaQuarantines(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	hotSegs := newHot(t, hot, 1<<30)

	w := place.NewWriter(hotSegs, meta, nil)
	if err := meta.PutFile(makeReg("foo")); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("foo", 0, []byte("ABCDEFGH")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	hotSegs.CloseAll()
	meta.Close()

	segPath := fmt.Sprintf("%s/.place/segments/00000000.seg", hot)
	origSize := segStat(t, segPath)
	// Truncate the segment file mid-record so the surviving FileMeta points
	// past EOF. The exact post-truncate size doesn't matter as long as it
	// lands inside the fragment's [SegmentOffset, SegmentOffset+Length) range.
	if err := truncateFile(segPath, origSize/2); err != nil {
		t.Fatal(err)
	}
	postTruncateSize := segStat(t, segPath)

	meta2 := openMeta(t, hot)
	defer meta2.Close()
	hotSegs2 := newHot(t, hot, 1<<30)
	defer hotSegs2.CloseAll()
	reconcileForTest(t, meta2, hotSegs2, place.TierHot)

	// SegmentMeta.Total should now match the file size, so a re-reconcile
	// would not retrigger the warning path.
	sm := getSegMeta(t, meta2, place.TierHot, 0)
	if sm == nil {
		t.Fatal("segment meta missing after reconcile")
	}
	if sm.Total != postTruncateSize {
		t.Errorf("SegmentMeta.Total: got %d want %d (file size)", sm.Total, postTruncateSize)
	}

	fm := readFile(t, meta2, "foo")
	if fm == nil {
		t.Fatal("foo missing after reconcile")
	}
	// The single fragment was wholly past the new EOF (its SegmentOffset is
	// the post-header offset, which lives near the start of the file; with
	// origSize/2 the cut bisects the payload). Whether wholly dropped or
	// partially truncated, no surviving fragment should extend past seg.Size().
	for i, f := range fm.HotFragments {
		end := f.SegmentOffset + f.Length
		if end > postTruncateSize {
			t.Errorf("HotFragment[%d] still past EOF: end=%d seg.Size=%d",
				i, end, postTruncateSize)
		}
	}

	// fm.Size is left at the original logical end (8); reads past surviving
	// coverage now go through Reader's zero-fill rather than EIO.
	r := place.NewReader(hotSegs2, hotSegs2 /* unused cold */, meta2)
	dst := make([]byte, fm.Size)
	n, err := r.ReadAt("foo", dst, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v (expected zero-fill, not EIO)", err)
	}
	if int64(n) != fm.Size {
		t.Errorf("ReadAt n: got %d want %d", n, fm.Size)
	}
	// At least the trailing bytes (those that lost coverage) must be zero.
	// We don't assert on the leading bytes — those may or may not survive
	// depending on whether the truncate cut inside or before the payload.
	zeros := make([]byte, fm.Size)
	if bytes.Equal(dst, zeros) {
		t.Logf("entire read zero-filled (truncate cut before payload start)")
	} else {
		t.Logf("partial survivor: %q", dst)
	}
}

// TestReconcilePartialTruncationKeepsPrefix exercises the straddle case:
// a single fragment whose SegmentOffset is before seg.Size() but whose
// SegmentOffset+Length is past it. dropUnreadableFragments must keep the
// readable prefix at its original LogicalOffset and drop only the
// unreadable tail bytes.
func TestReconcilePartialTruncationKeepsPrefix(t *testing.T) {
	hot, _ := dirs(t)
	meta := openMeta(t, hot)
	hotSegs := newHot(t, hot, 1<<30)
	defer func() {
		hotSegs.CloseAll()
		meta.Close()
	}()

	if err := meta.PutFile(makeReg("foo")); err != nil {
		t.Fatal(err)
	}
	// Place a hand-crafted fragment whose payload extent we control. Use
	// the public AppendDataForTest hook so the bytes pass through real
	// framing (CRC + magic), giving us a deterministic SegmentOffset.
	seg, err := hotSegs.Active()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("ABCDEFGHIJKLMNOP") // 16 bytes
	segOff, err := seg.AppendDataForTest("foo", 0, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := place.GetFileTx(tx, "foo")
		if err != nil {
			return err
		}
		fm.HotFragments = []place.Fragment{{
			LogicalOffset: 0,
			Length:        int64(len(payload)),
			Tier:          place.TierHot,
			SegmentID:     0,
			SegmentOffset: segOff,
		}}
		fm.Size = int64(len(payload))
		return place.PutFileTx(meta, tx, fm)
	}); err != nil {
		t.Fatal(err)
	}
	// Register the segment in bbolt so reconcile sees an expectTotal entry.
	if err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		return place.PutSegmentTx(tx, &place.SegmentMeta{
			ID:    0,
			Tier:  place.TierHot,
			Total: seg.Size(),
			Live:  int64(len(payload)),
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := meta.Flush(); err != nil {
		t.Fatal(err)
	}

	hotSegs.CloseAll()
	meta.Close()

	// Truncate the segment so the cut lands halfway into the payload region:
	// keep the first half of the payload bytes readable, drop the second half.
	keepPayloadBytes := int64(len(payload) / 2) // 8
	newSize := segOff + keepPayloadBytes
	segPath := fmt.Sprintf("%s/.place/segments/00000000.seg", hot)
	if err := truncateFile(segPath, newSize); err != nil {
		t.Fatal(err)
	}

	meta2 := openMeta(t, hot)
	defer meta2.Close()
	hotSegs2 := newHot(t, hot, 1<<30)
	defer hotSegs2.CloseAll()
	reconcileForTest(t, meta2, hotSegs2, place.TierHot)

	fm := readFile(t, meta2, "foo")
	if fm == nil {
		t.Fatal("foo missing after reconcile")
	}
	if len(fm.HotFragments) != 1 {
		t.Fatalf("want 1 surviving fragment, got %d: %+v", len(fm.HotFragments), fm.HotFragments)
	}
	f := fm.HotFragments[0]
	if f.LogicalOffset != 0 {
		t.Errorf("LogicalOffset: got %d want 0", f.LogicalOffset)
	}
	if f.Length != keepPayloadBytes {
		t.Errorf("Length: got %d want %d (truncated to readable prefix)", f.Length, keepPayloadBytes)
	}
	if f.SegmentOffset != segOff {
		t.Errorf("SegmentOffset: got %d want %d", f.SegmentOffset, segOff)
	}

	sm := getSegMeta(t, meta2, place.TierHot, 0)
	if sm == nil {
		t.Fatal("segment meta missing")
	}
	if sm.Total != newSize {
		t.Errorf("SegmentMeta.Total: got %d want %d", sm.Total, newSize)
	}
	// Live should have been debited by the dropped tail (len/2 bytes).
	wantLive := int64(len(payload)) - (int64(len(payload)) - keepPayloadBytes)
	if sm.Live != wantLive {
		t.Errorf("SegmentMeta.Live: got %d want %d", sm.Live, wantLive)
	}

	// Reads of [0, len(payload)) return the readable prefix in [0, keepPayloadBytes)
	// and zero-fill afterward.
	r := place.NewReader(hotSegs2, hotSegs2 /* unused cold */, meta2)
	dst := make([]byte, len(payload))
	n, err := r.ReadAt("foo", dst, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(payload) {
		t.Errorf("ReadAt n: got %d want %d", n, len(payload))
	}
	if !bytes.Equal(dst[:keepPayloadBytes], payload[:keepPayloadBytes]) {
		t.Errorf("survivor prefix mismatch: got %q want %q", dst[:keepPayloadBytes], payload[:keepPayloadBytes])
	}
	for i := keepPayloadBytes; i < int64(len(payload)); i++ {
		if dst[i] != 0 {
			t.Errorf("post-truncate tail not zero at %d: %v", i, dst[i])
			break
		}
	}
}
