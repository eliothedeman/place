package audit_tests

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestVersionChangedReplicateOrphanBytesAccounted exercises the real
// replicateOne path through a version-change race and asserts that the
// orphan framed bytes are accounted in cold SegmentMeta.Total.
//
// Pre-fix: replicateOne Append'd chunks to the active cold segment, then
// the swap-in tx aborted with errVersionChanged. The appended bytes stayed
// on disk and seg.size advanced, but sm.Total (bumped only inside the
// aborted tx) did not. A subsequent successful replicate to the same
// segment Append'd past the orphan region and credited only its own
// recLen. On restart, recover.go:reconcile saw seg.Size() > sm.Total and
// truncated the file back to sm.Total — taking the legitimate post-orphan
// records with it.
//
// Post-fix: on errVersionChanged, replicateOne runs a follow-up
// UpdateLocked that bumps each touched cold segment's Total by the framed
// recLen of the bytes we wrote (Live unchanged — the bytes have no
// fragment refs). Reconcile sees Total == seg.Size and leaves the file
// alone; legitimate post-orphan records survive the restart.
func TestVersionChangedReplicateOrphanBytesAccounted(t *testing.T) {
	hot, cold := dirs(t)

	// Plumb the same stack mount.go assembles, minus FUSE.
	meta := openMeta(t, hot)
	hotSegs := newHot(t, hot, 1<<30)
	coldSegs := newCold(t, cold, 1<<30)
	if err := hotSegs.AttachMeta(meta); err != nil {
		t.Fatal(err)
	}
	if err := coldSegs.AttachMeta(meta); err != nil {
		t.Fatal(err)
	}

	hotStorage, err := place.NewStorage(hot)
	if err != nil {
		t.Fatal(err)
	}
	ev := place.NewEvictorForTest(hotStorage, meta, hotSegs, coldSegs, 0.95, 0.85)
	w := place.NewWriter(hotSegs, meta, ev)
	defer w.Close()
	reader := place.NewReader(hotSegs, coldSegs, meta)
	c := place.NewCompactorForTest(reader, hotSegs, coldSegs, meta, ev)

	// Create + write file "racey": this is the inode whose replicate will
	// race with a concurrent version bump.
	now := time.Now().UnixNano()
	const raceyN = 1024
	raceyData := bytes.Repeat([]byte("R"), raceyN)
	if err := meta.PutFile(&place.FileMeta{
		Rel: "racey", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("racey", 0, raceyData); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	raceyFM, _ := meta.GetFile("racey")
	if raceyFM == nil {
		t.Fatal("racey FileMeta missing after write")
	}
	raceyID := raceyFM.InodeID

	// Install a pre-swap hook that bumps fm.Version on the inode mid-flight,
	// so the swap-in tx aborts via errVersionChanged. This deterministically
	// reproduces the race window the bug originally hit only under
	// concurrent write/truncate/etc. We drive a synchronous Writer.Submit so
	// commitBatch bumps Version durably before replicateOne's swap-in runs.
	c.SetPreSwapHookForTest(func(id uint64) {
		if id != raceyID {
			return
		}
		if err := w.Submit("racey", int64(raceyN), []byte("Z")); err != nil {
			t.Errorf("hook submit: %v", err)
		}
		if err := w.Flush(); err != nil {
			t.Errorf("hook flush: %v", err)
		}
	})

	// Drive replicateOne. The hook bumps Version after Append but before
	// the swap-in tx, so the swap aborts with errVersionChanged.
	if err := c.ReplicateOneForTest(raceyID); err != nil {
		t.Fatalf("replicateOne: %v", err)
	}

	// Expect: cold seg 0 exists, its sm.Total matches seg.Size() exactly
	// (orphan bytes accounted for). sm.Live should be 0 since no fragment
	// references those bytes.
	c.SetPreSwapHookForTest(nil)

	coldSegPath := filepath.Join(cold, ".place", "segments", segFileBaseName(0))
	st, err := os.Stat(coldSegPath)
	if err != nil {
		t.Fatalf("cold seg 0 stat: %v", err)
	}
	diskSize := st.Size()
	if diskSize == 0 {
		t.Fatalf("expected orphan bytes on disk, got empty cold seg")
	}
	sm := getSegMeta(t, meta, place.TierCold, 0)
	if sm == nil {
		t.Fatalf("cold seg 0 has no SegmentMeta after orphan accounting")
	}
	if sm.Total != diskSize {
		t.Errorf("orphan accounting mismatch: sm.Total=%d disk=%d (post-fix should be equal)",
			sm.Total, diskSize)
	}
	if sm.Live != 0 {
		t.Errorf("orphan bytes credited as Live (should be 0): sm.Live=%d", sm.Live)
	}
	t.Logf("post-orphan: sm.Total=%d sm.Live=%d disk=%d", sm.Total, sm.Live, diskSize)

	// Now replicate a different file ("kept") — its bytes will be Append'd
	// past the orphan region. Its Total contribution + the orphan accounting
	// should leave reconcile satisfied.
	const keptN = 64
	keptData := bytes.Repeat([]byte("G"), keptN)
	if err := meta.PutFile(&place.FileMeta{
		Rel: "kept", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Submit("kept", 0, keptData); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	keptFM, _ := meta.GetFile("kept")
	if err := c.ReplicateOneForTest(keptFM.InodeID); err != nil {
		t.Fatalf("replicateOne kept: %v", err)
	}

	// Pre-restart read should return "G".
	preBuf := make([]byte, keptN)
	if _, err := reader.ReadAt("kept", preBuf, 0); err != nil {
		t.Fatalf("pre-restart kept read: %v", err)
	}
	if !bytes.Equal(preBuf, keptData) {
		t.Fatalf("pre-restart kept read: got %q want %q", preBuf, keptData)
	}

	// Capture the cold seg size before close — should equal sm.Total now.
	preStat, _ := os.Stat(coldSegPath)
	preSize := preStat.Size()
	preMeta := getSegMeta(t, meta, place.TierCold, 0)
	if preMeta == nil || preMeta.Total != preSize {
		t.Fatalf("pre-restart sm.Total=%v disk=%d — expected equal", preMeta, preSize)
	}

	// Close + restart. Reconcile would have truncated the seg pre-fix.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_ = hotSegs.CloseAll()
	_ = coldSegs.CloseAll()
	_ = meta.Close()

	meta2 := openMeta(t, hot)
	defer meta2.Close()
	hotSegs2 := newHot(t, hot, 1<<30)
	defer hotSegs2.CloseAll()
	coldSegs2 := newCold(t, cold, 1<<30)
	defer coldSegs2.CloseAll()
	reconcileForTest(t, meta2, coldSegs2, place.TierCold)

	postStat, _ := os.Stat(coldSegPath)
	postSize := postStat.Size()
	if postSize != preSize {
		t.Errorf("reconcile truncated cold seg: pre=%d post=%d (post-fix should be unchanged)",
			preSize, postSize)
	}

	// "kept" must read back intact post-restart.
	reader2 := place.NewReader(hotSegs2, coldSegs2, meta2)
	postBuf := make([]byte, keptN)
	if _, err := reader2.ReadAt("kept", postBuf, 0); err != nil {
		t.Fatalf("post-restart kept read: %v", err)
	}
	if !bytes.Equal(postBuf, keptData) {
		t.Fatalf("post-restart kept read: got %q want %q (data lost in reconcile)",
			postBuf, keptData)
	}
	t.Logf("post-restart: cold seg %d bytes preserved, kept reads back intact", postSize)
}

// TestVersionChangedReplicateOrphanCorruptsRecovery_PreFixDemo is the
// pre-fix structural reproduction kept as a regression-history witness.
// It manually mimics the corrupt state (orphan bytes on disk + sm.Total
// reflecting only the post-orphan good record) and confirms reconcile
// truncates the data away. Post-fix, replicateOne never produces this
// state — see TestVersionChangedReplicateOrphanBytesAccounted for the
// real-path test.
func TestVersionChangedReplicateOrphanCorruptsRecovery_PreFixDemo(t *testing.T) {
	hot, cold := dirs(t)
	const segID uint32 = 0

	const orphanLen = 1024
	orphan := bytes.Repeat([]byte("X"), orphanLen)
	_ = stampCold(t, cold, segID, "aborted-rel", 0, orphan)

	const N = 64
	good := bytes.Repeat([]byte("G"), N)
	goodPayloadOff := stampCold(t, cold, segID, "kept", 0, good)

	meta := openMeta(t, hot)
	hotSegs := newHot(t, hot, 1<<30)
	coldSegs := newCold(t, cold, 1<<30)

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "kept",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
		Size: int64(N),
		ColdFragments: []place.Fragment{{
			LogicalOffset: 0,
			Length:        int64(N),
			Tier:          place.TierCold,
			SegmentID:     segID,
			SegmentOffset: goodPayloadOff,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// Mimic pre-fix bookkeeping: only the good record's recLen is in
	// sm.Total. Orphan framed bytes are unaccounted.
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		sm := &place.SegmentMeta{
			ID:        segID,
			Tier:      place.TierCold,
			Total:     recLenFor("kept", N),
			Live:      int64(N),
			CreatedAt: now,
		}
		return place.PutSegmentTx(tx, sm)
	})
	if err != nil {
		t.Fatal(err)
	}

	reader := place.NewReader(hotSegs, coldSegs, meta)
	preBuf := make([]byte, N)
	if _, err := reader.ReadAt("kept", preBuf, 0); err != nil {
		t.Fatalf("pre-recover read: %v", err)
	}
	if !bytes.Equal(preBuf, good) {
		t.Fatalf("pre-recover read: got %q want %q", preBuf, good)
	}

	coldSegPath := filepath.Join(cold, ".place", "segments", segFileBaseName(segID))

	_ = hotSegs.CloseAll()
	_ = coldSegs.CloseAll()
	_ = meta.Close()

	meta2 := openMeta(t, hot)
	defer meta2.Close()
	coldSegs2 := newCold(t, cold, 1<<30)
	defer coldSegs2.CloseAll()
	reconcileForTest(t, meta2, coldSegs2, place.TierCold)

	hotSegs2 := newHot(t, hot, 1<<30)
	defer hotSegs2.CloseAll()
	reader2 := place.NewReader(hotSegs2, coldSegs2, meta2)
	postBuf := make([]byte, N)
	_, readErr := reader2.ReadAt("kept", postBuf, 0)

	if readErr == nil && bytes.Equal(postBuf, good) {
		t.Fatalf("UNEXPECTED PASS: synthetic pre-fix reproduction did not corrupt — "+
			"reconcileForTest at %s may have drifted from recover.go", coldSegPath)
	}
	t.Logf("synthetic pre-fix state confirms data loss: post-reconcile read err=%v "+
		"bytes=%q (expected %q)", readErr, postBuf, good)
}
