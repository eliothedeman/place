package audit_tests

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestSetattrGrowThenWriteThenReplicateThenOverwriteThenEvict exercises the
// full lifecycle around a grown file:
//  1. Create a small file, write+replicate it (cold clean).
//  2. Setattr-grow to a larger Size.
//  3. Write into the grown region (extending hot past previous Size).
//  4. Simulate a fresh replicate that snapshots the post-grow state and
//     clears ColdDirty.
//  5. Overwrite a byte range inside the freshly-replicated grown region.
//  6. Eviction must skip — HasColdCopy must report false because the
//     overwrite in step 5 left hot newer than cold.
//  7. Read returns the live post-overwrite bytes.
func TestSetattrGrowThenWriteThenReplicateThenOverwriteThenEvict(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()

	const initialSize = 16
	const grownSize = 64
	originalA := bytes.Repeat([]byte("A"), initialSize)

	// Pre-stamp two cold records so the simulated replicate steps below
	// have somewhere to point. cold0 covers [0, initialSize); cold1 will
	// cover [0, grownSize) for the post-grow snapshot.
	const coldSegID uint32 = 0
	cold0Off := stampCold(t, cold, coldSegID, "f", 0, originalA)
	postGrow := make([]byte, grownSize)
	copy(postGrow, originalA)
	for i := initialSize; i < grownSize; i++ {
		postGrow[i] = 'C'
	}
	cold1Off := stampCold(t, cold, coldSegID, "f", 0, postGrow)

	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()
	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()
	reader := place.NewReader(hotSegs, coldSegs, meta)

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel:   "f",
		Mode:  syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Step 1: write + replicate (cold clean) for the initial small file.
	if err := w.Submit("f", 0, originalA); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		fm.ColdFragments = []place.Fragment{{
			LogicalOffset: 0, Length: initialSize, Tier: place.TierCold,
			SegmentID: coldSegID, SegmentOffset: cold0Off,
		}}
		fm.ColdDirty = false
		sm, _ := place.GetSegmentTx(tx, place.TierCold, coldSegID)
		if sm == nil {
			sm = &place.SegmentMeta{ID: coldSegID, Tier: place.TierCold, CreatedAt: time.Now().UnixNano()}
		}
		sm.Total += recLenFor("f", initialSize)
		sm.Live += int64(initialSize)
		_ = place.PutSegmentTx(tx, sm)
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Sanity: cold is in sync — HasColdCopy true.
	if fm := readFile(t, meta, "f"); !fm.HasColdCopy() {
		t.Fatalf("after replicate: HasColdCopy=false, ColdDirty=%v ColdFragments=%v",
			fm.ColdDirty, fm.ColdFragments)
	}

	// Step 2: Setattr-grow to grownSize. node.go:Setattr's grow path sets
	// ColdDirty=true (cold doesn't cover the new Size).
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		fm.Size = grownSize
		fm.ColdDirty = true
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}
	if fm := readFile(t, meta, "f"); fm.HasColdCopy() {
		t.Fatalf("after grow: HasColdCopy=true unexpectedly (cold doesn't cover grown range "+
			"and ColdDirty must be set); ColdFragments=%v ColdDirty=%v Size=%d",
			fm.ColdFragments, fm.ColdDirty, fm.Size)
	}

	// Step 3: write "C"s into the grown hole [initialSize, grownSize).
	holeWrite := bytes.Repeat([]byte("C"), grownSize-initialSize)
	if err := w.Submit("f", initialSize, holeWrite); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if fm := readFile(t, meta, "f"); fm.HasColdCopy() {
		t.Fatalf("after hole write: HasColdCopy=true unexpectedly; cold doesn't cover [0, %d)",
			grownSize)
	}

	// Step 4: simulated full re-replicate captures the post-grow state
	// and clears ColdDirty. (cold1Off is the pre-stamped record holding
	// the same A's + C's payload.)
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		fm.ColdFragments = []place.Fragment{{
			LogicalOffset: 0, Length: grownSize, Tier: place.TierCold,
			SegmentID: coldSegID, SegmentOffset: cold1Off,
		}}
		fm.ColdDirty = false
		sm, _ := place.GetSegmentTx(tx, place.TierCold, coldSegID)
		sm.Total += recLenFor("f", grownSize)
		sm.Live += int64(grownSize)
		_ = place.PutSegmentTx(tx, sm)
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}
	if fm := readFile(t, meta, "f"); !fm.HasColdCopy() {
		t.Fatalf("after re-replicate: HasColdCopy=false; ColdFragments=%v ColdDirty=%v",
			fm.ColdFragments, fm.ColdDirty)
	}

	// Step 5: overwrite 4 bytes inside the grown region (offset 32).
	overwrite := []byte("BBBB")
	const overwriteOff = 32
	if err := w.Submit("f", overwriteOff, overwrite); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Step 6: HasColdCopy must report false — the overwrite is hot-only.
	fm := readFile(t, meta, "f")
	if fm.HasColdCopy() {
		t.Fatalf("HasColdCopy=true after overwrite-in-grown-area — eviction would drop "+
			"the live bytes; ColdFragments=%v ColdDirty=%v Size=%d",
			fm.ColdFragments, fm.ColdDirty, fm.Size)
	}

	want := make([]byte, grownSize)
	copy(want, originalA)
	for i := initialSize; i < grownSize; i++ {
		want[i] = 'C'
	}
	copy(want[overwriteOff:], overwrite)

	got := make([]byte, grownSize)
	if _, err := reader.ReadAt("f", got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("pre-evict read: got %q want %q", got, want)
	}

	// Mirror evict.go:dropCached — gated on HasColdCopy.
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		if !fm.HasColdCopy() {
			return nil
		}
		dead := fm.HotFragments
		_ = place.AddLiveBytesTx(tx, dead, -1)
		fm.HotFragments = nil
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Step 7: read returns live bytes.
	got2 := make([]byte, grownSize)
	if _, err := reader.ReadAt("f", got2, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, want) {
		t.Fatalf("read after evict: got %q want live %q (eviction dropped newer hot bytes)",
			got2, want)
	}
}
