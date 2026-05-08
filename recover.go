package place

import (
	"fmt"
	"log"
	"time"

	bolt "go.etcd.io/bbolt"
)

// reconcile aligns segment files on disk with bbolt's view of them.
//
// For each segment in the set:
//   - If bbolt has a SegmentMeta, truncate the file to SegmentMeta.Total
//     (any tail beyond that is orphaned data from a write whose metadata
//     tx never committed).
//   - If bbolt has no SegmentMeta, register one based on the file's size.
//     This covers a crash that left an empty freshly-created segment.
//   - If the file is shorter than SegmentMeta.Total, drop fragments past
//     seg.Size() from every referencing FileMeta and update Total down
//     to seg.Size(). The lost bytes are unreadable either way; surfacing
//     them as zero-fill (per Reader's pre-zero behavior) beats permanent
//     EIO on reads of the affected range.
func reconcile(meta *Meta, set *SegmentSet) error {
	for _, id := range set.All() {
		seg := set.Get(id)
		if seg == nil {
			continue
		}
		var expectTotal int64 = -1
		err := meta.db.View(func(tx *bolt.Tx) error {
			sm, err := GetSegmentTx(tx, set.tier, id)
			if err != nil {
				return err
			}
			if sm != nil {
				expectTotal = sm.Total
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("reconcile segment %d: %w", id, err)
		}
		if expectTotal == -1 {
			// No meta entry — could be an orphan segment or an unrecorded
			// segment. If the file is empty, drop it; otherwise register it
			// with its current size (best effort; will be GC'd if no live
			// fragments reference it).
			if seg.Size() == 0 {
				if err := set.Remove(id); err != nil {
					log.Printf("place: reconcile drop orphan %d: %v", id, err)
				}
				continue
			}
			err := meta.db.Update(func(tx *bolt.Tx) error {
				return PutSegmentTx(tx, &SegmentMeta{
					ID:    id,
					Tier:  set.tier,
					Total: seg.Size(),
					Live:  0, // unknown; GC will reclaim if nothing references it
				})
			})
			if err != nil {
				log.Printf("place: reconcile register %d: %v", id, err)
			}
			continue
		}
		if seg.Size() > expectTotal {
			log.Printf("place: reconcile truncate segment %d (tier=%d): %d -> %d",
				id, set.tier, seg.Size(), expectTotal)
			if err := seg.Truncate(expectTotal); err != nil {
				return fmt.Errorf("truncate %d: %w", id, err)
			}
		}
		if seg.Size() < expectTotal {
			// File is shorter than bbolt claims — fsync ordering normally
			// prevents this, but it can surface after a manual truncate, a
			// half-failed sync, or a hardware fault. Drop fragments past
			// seg.Size() from every referencing FileMeta and lower Total
			// to match the file. Reads of the affected byte ranges then
			// fall through to Reader's pre-zero-fill instead of EIO.
			if err := quarantineShortSegment(meta, set, id, seg); err != nil {
				return fmt.Errorf("quarantine %d: %w", id, err)
			}
		}
	}
	return nil
}

// quarantineShortSegment is reconcile's repair path for a segment whose
// file is shorter than its bbolt SegmentMeta.Total. It walks every FileMeta
// that references the segment, drops (or partially truncates) fragments
// whose extent exceeds seg.Size(), debits the dropped bytes from the
// segment's Live counter, and lowers Total to seg.Size() so subsequent
// reconciles don't re-trigger.
//
// fm.Size is intentionally not adjusted: the file's logical end may still
// be valid via cold/other-segment fragments, and even if it isn't, the
// Reader's pre-zero behavior turns reads past coverage into zero-fill —
// strictly better than the EIO that the caller would otherwise see.
//
// For partial truncation (a fragment straddling seg.Size()), the readable
// prefix is retained via fragment.go's truncateFragments helper.
func quarantineShortSegment(meta *Meta, set *SegmentSet, segID uint32, seg *Segment) error {
	segSize := seg.Size()
	var (
		droppedFiles int
		droppedBytes int64
	)
	err := meta.UpdateLocked(func(tx *bolt.Tx) error {
		// Collect inode IDs first; we mutate inodes as we go and don't
		// want to invalidate the cursor mid-walk.
		var refs []uint64
		if err := iterateInodesTx(tx, func(id uint64, fm *FileMeta) error {
			if referencesSegment(fm, set.tier, segID) {
				refs = append(refs, id)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, id := range refs {
			fm, err := getInodeTx(tx, id)
			if err != nil {
				return err
			}
			if fm == nil {
				continue
			}
			changed, deadBytes := dropUnreadableFragments(fm, segID, set.tier, segSize)
			if !changed {
				continue
			}
			fm.Version++
			if err := putInodeTx(tx, id, fm); err != nil {
				return err
			}
			droppedFiles++
			droppedBytes += deadBytes
		}
		// Adjust SegmentMeta: Total down to seg.Size(), Live debited by
		// the bytes we just dropped (clamped at 0 by the segment-meta
		// arithmetic — a corrupt Live underflow would be visible as a
		// negative pre-clamp value, but we don't flag it specially here).
		sm, err := GetSegmentTx(tx, set.tier, segID)
		if err != nil {
			return err
		}
		if sm == nil {
			// No meta to update; nothing else to do.
			return nil
		}
		sm.Total = segSize
		sm.Live -= droppedBytes
		if sm.Live < 0 {
			sm.Live = 0
		}
		return PutSegmentTx(tx, sm)
	})
	if err != nil {
		return err
	}
	log.Printf("place: reconcile %d (tier=%d) shorter than meta — "+
		"dropped %d unreadable fragment bytes across %d files, lowered Total to %d",
		segID, set.tier, droppedBytes, droppedFiles, segSize)
	return nil
}

// dropUnreadableFragments removes (or partially truncates) fragments in
// fm.HotFragments / fm.ColdFragments that point at segID with a SegmentOffset
// extent past segSize. Returns whether any change was made and the number of
// payload bytes dropped (so the caller can debit segment.Live).
//
// A fragment is "unreadable" when SegmentOffset+Length > segSize. The prefix
// [SegmentOffset, segSize) is still on disk, so we keep the corresponding
// logical-byte prefix as a shorter fragment and drop only the past-EOF tail.
// Fragments wholly past segSize are dropped entirely.
func dropUnreadableFragments(fm *FileMeta, segID uint32, tier Tier, segSize int64) (bool, int64) {
	walk := func(list []Fragment) (out []Fragment, dropped int64, changed bool) {
		for _, f := range list {
			if f.Tier != tier || f.SegmentID != segID {
				out = append(out, f)
				continue
			}
			fragEnd := f.SegmentOffset + f.Length
			if fragEnd <= segSize {
				out = append(out, f)
				continue
			}
			changed = true
			if f.SegmentOffset >= segSize {
				// Fully past EOF — drop the whole fragment.
				dropped += f.Length
				continue
			}
			// Straddles the boundary: keep the readable prefix.
			keepLen := segSize - f.SegmentOffset
			out = append(out, Fragment{
				LogicalOffset: f.LogicalOffset,
				Length:        keepLen,
				Tier:          f.Tier,
				SegmentID:     f.SegmentID,
				SegmentOffset: f.SegmentOffset,
			})
			dropped += f.Length - keepLen
		}
		return out, dropped, changed
	}
	hot, hotDead, hotChanged := walk(fm.HotFragments)
	cold, coldDead, coldChanged := walk(fm.ColdFragments)
	if !hotChanged && !coldChanged {
		return false, 0
	}
	fm.HotFragments = hot
	fm.ColdFragments = cold
	return true, hotDead + coldDead
}

// rebuildMetaFromCold walks cold segment records and reconstructs the files
// bucket. Use when bbolt has been lost. Hot segments are ignored; hot
// coverage will be empty until the user re-writes files.
//
// Limitations of the cold record format that this rebuild cannot recover:
//   - Mode bits — every restored file gets 0o100644 (regular 0644). Exec,
//     setuid, setgid bits are lost. TODO(rebuild-meta): add a record-format
//     extension that carries Mode so binaries restore executable.
//   - Hardlinks — each rel becomes its own inode, so two paths that
//     pre-rebuild shared an inode resurface as independent files with their
//     own copies. TODO(rebuild-meta): hardlinks require an inodeID in the
//     record header.
//   - Deletions — a path that was deleted but whose cold records are still
//     in segments will resurrect on rebuild. Rebuild has no signal that the
//     records correspond to a since-removed path. TODO(rebuild-meta): a
//     tombstone record is written today (recordTombstone) but rebuild
//     doesn't honour it as "drop coverage" yet.
func rebuildMetaFromCold(meta *Meta, coldSegs *SegmentSet) error {
	// Per-segment live byte counter. Live starts at 0 and is bumped by every
	// merged record's payload length, then debited for any range that a
	// later record supersedes (mergeFragment's `dead` return). Without this
	// pass, reconcile registers each cold seg with Live=0 and the first GC
	// tick unlinks every cold .seg — catastrophic data loss on rebuild.
	liveBySeg := map[uint32]int64{}
	totalBySeg := map[uint32]int64{}
	for _, id := range coldSegs.All() {
		seg := coldSegs.Get(id)
		if seg == nil {
			continue
		}
		size := seg.Size()
		if size == 0 {
			continue
		}
		totalBySeg[id] = size
		buf := make([]byte, size)
		if _, err := seg.ReadAt(buf, 0); err != nil {
			return fmt.Errorf("read segment %d: %w", id, err)
		}
		var off int64
		for off < size {
			_, rel, logOff, payloadStart, payloadLen, next, perr := parseRecord(buf, off)
			if perr != nil {
				log.Printf("place: rebuild: stopping at bad record in segment %d offset %d: %v", id, off, perr)
				break
			}
			err := meta.db.Update(func(tx *bolt.Tx) error {
				fm, err := GetFileTx(tx, rel)
				if err != nil {
					return err
				}
				if fm == nil {
					fm = &FileMeta{
						Rel:  rel,
						Mode: 0o100644, // regular file 0644 — see func doc for limitations
					}
				}
				frag := Fragment{
					LogicalOffset: logOff,
					Length:        int64(payloadLen),
					Tier:          TierCold,
					SegmentID:     id,
					SegmentOffset: payloadStart,
				}
				merged, dead := mergeFragment(fm.ColdFragments, frag)
				fm.ColdFragments = merged
				// Credit this record's payload as live in its segment, then
				// debit any earlier coverage it just superseded. Both legs
				// are required: a segment whose every record is fully
				// shadowed by a later record correctly drops to Live=0.
				liveBySeg[id] += int64(payloadLen)
				for _, d := range dead {
					liveBySeg[d.SegmentID] -= d.Length
				}
				if logOff+int64(payloadLen) > fm.Size {
					fm.Size = logOff + int64(payloadLen)
				}
				fm.Version++
				return PutFileTx(meta, tx, fm)
			})
			if err != nil {
				return fmt.Errorf("rebuild put %q: %w", rel, err)
			}
			off = next
		}
	}
	// Persist a SegmentMeta for every cold segment we walked. Without this,
	// reconcile (recover.go:reconcile) sees missing entries and registers
	// Live=0, and gcPass deletes the file at the next tick — see bug #6.
	now := time.Now().UnixNano()
	activeID := uint32(0)
	if a, _ := coldSegs.Active(); a != nil {
		activeID = a.id
	}
	if err := meta.db.Update(func(tx *bolt.Tx) error {
		for id, total := range totalBySeg {
			live := liveBySeg[id]
			if live < 0 {
				live = 0
			}
			sm := &SegmentMeta{
				ID:        id,
				Tier:      TierCold,
				Total:     total,
				Live:      live,
				Sealed:    id != activeID,
				CreatedAt: now,
			}
			if err := PutSegmentTx(tx, sm); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("rebuild persist segment meta: %w", err)
	}
	// Walk directory paths and ensure parents exist.
	return meta.db.Update(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bucketPaths).Cursor()
		var rels []string
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			rels = append(rels, keyToRel(k))
		}
		for _, rel := range rels {
			parent := ParentOf(rel)
			for parent != "" {
				p, err := GetFileTx(tx, parent)
				if err != nil {
					return err
				}
				if p == nil {
					if err := PutFileTx(meta, tx, &FileMeta{Rel: parent, Mode: 0o40755}); err != nil {
						return err
					}
				}
				parent = ParentOf(parent)
			}
		}
		return nil
	})
}

// RebuildMetaFromColdForTest is a test-only entry point that runs the
// production rebuild logic. The audit suite uses it to exercise the real
// recovery path end-to-end without standing up a full Mount.
func RebuildMetaFromColdForTest(meta *Meta, coldSegs *SegmentSet) error {
	return rebuildMetaFromCold(meta, coldSegs)
}

// ReconcileForTest is a test-only entry point that runs the production
// reconcile logic. Mirrors mount.go's startup ordering for one tier.
func ReconcileForTest(meta *Meta, set *SegmentSet) error {
	return reconcile(meta, set)
}
