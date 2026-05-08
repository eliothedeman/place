package audit_tests

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/eliothedeman/place"
)

// TestSetattrTruncateShrinkThenOverwriteThenEvict guards the HasColdCopy
// regression for truncate-shrink + overwrite-in-hot-only: even though
// truncated cold still spans [0, newSize), eviction must NOT drop hot
// because the overwrite lives only in hot.
//
// Sequence:
//  1. File written. Size=N=32. Hot [0,N). Cold (simulated) [0,N) clean.
//  2. Setattr shrink to Size=16 (sets ColdDirty=true).
//  3. User writes "BBBB" at offset 4 (sets ColdDirty=true again).
//  4. HasColdCopy must report false; eviction skips. Read returns live bytes.
func TestSetattrTruncateShrinkThenOverwriteThenEvict(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()

	const coldSegID uint32 = 0
	const N = 32
	originalA := bytes.Repeat([]byte("A"), N)
	coldPayloadOff := stampCold(t, cold, coldSegID, "f", 0, originalA)

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
	if err := w.Submit("f", 0, originalA); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Simulate replicate: cold = [0, N), ColdDirty cleared (mirrors
	// replicateOne's atomic swap-in).
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		fm.ColdFragments = []place.Fragment{{
			LogicalOffset: 0,
			Length:        int64(N),
			Tier:          place.TierCold,
			SegmentID:     coldSegID,
			SegmentOffset: coldPayloadOff,
		}}
		fm.ColdDirty = false
		sm, _ := place.GetSegmentTx(tx, place.TierCold, coldSegID)
		if sm == nil {
			sm = &place.SegmentMeta{ID: coldSegID, Tier: place.TierCold, CreatedAt: time.Now().UnixNano()}
		}
		sm.Total += recLenFor("f", N)
		sm.Live += int64(N)
		_ = place.PutSegmentTx(tx, sm)
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Setattr shrink to 16 (mirrors node.go Setattr's truncate branch).
	const NewSize = 16
	err = meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, _ := place.GetFileTx(tx, "f")
		// truncateFragments-equivalent: hot/cold trimmed to [0, NewSize).
		// We use the public APIs by calling truncateFragments via Setattr's
		// path. There's no exported truncateFragments, so we reconstruct.
		var hotKeep, hotDead, coldKeep, coldDead []place.Fragment
		for _, f := range fm.HotFragments {
			if f.LogicalOffset >= NewSize {
				hotDead = append(hotDead, f)
				continue
			}
			if f.LogicalOffset+f.Length <= NewSize {
				hotKeep = append(hotKeep, f)
				continue
			}
			keepLen := int64(NewSize) - f.LogicalOffset
			hotKeep = append(hotKeep, place.Fragment{
				LogicalOffset: f.LogicalOffset, Length: keepLen,
				Tier: f.Tier, SegmentID: f.SegmentID, SegmentOffset: f.SegmentOffset,
			})
			hotDead = append(hotDead, place.Fragment{
				LogicalOffset: int64(NewSize), Length: f.LogicalOffset + f.Length - int64(NewSize),
				Tier: f.Tier, SegmentID: f.SegmentID, SegmentOffset: f.SegmentOffset + keepLen,
			})
		}
		for _, f := range fm.ColdFragments {
			if f.LogicalOffset >= NewSize {
				coldDead = append(coldDead, f)
				continue
			}
			if f.LogicalOffset+f.Length <= NewSize {
				coldKeep = append(coldKeep, f)
				continue
			}
			keepLen := int64(NewSize) - f.LogicalOffset
			coldKeep = append(coldKeep, place.Fragment{
				LogicalOffset: f.LogicalOffset, Length: keepLen,
				Tier: f.Tier, SegmentID: f.SegmentID, SegmentOffset: f.SegmentOffset,
			})
			coldDead = append(coldDead, place.Fragment{
				LogicalOffset: int64(NewSize), Length: f.LogicalOffset + f.Length - int64(NewSize),
				Tier: f.Tier, SegmentID: f.SegmentID, SegmentOffset: f.SegmentOffset + keepLen,
			})
		}
		fm.HotFragments = hotKeep
		fm.ColdFragments = coldKeep
		_ = place.AddLiveBytesTx(tx, hotDead, -1)
		_ = place.AddLiveBytesTx(tx, coldDead, -1)
		fm.Size = int64(NewSize)
		fm.ColdDirty = true // mirrors node.go:Setattr truncate path
		fm.Version++
		return place.PutFileTx(tx, fm)
	})
	if err != nil {
		t.Fatal(err)
	}

	// User writes "BBBB" at offset 4 (overwrite within [0, NewSize)).
	overwrite := []byte("BBBB")
	if err := w.Submit("f", 4, overwrite); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	want := bytes.Repeat([]byte("A"), NewSize)
	copy(want[4:], overwrite)

	fm := readFile(t, meta, "f")
	if fm.HasColdCopy() {
		t.Fatalf("HasColdCopy=true after truncate+overwrite — eviction would drop the live "+
			"bytes; ColdFragments=%v ColdDirty=%v Size=%d", fm.ColdFragments, fm.ColdDirty, fm.Size)
	}

	got := make([]byte, NewSize)
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

	got2 := make([]byte, NewSize)
	if _, err := reader.ReadAt("f", got2, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, want) {
		t.Fatalf("read after evict: got %q want live %q (eviction dropped newer hot bytes)",
			got2, want)
	}
}
