package place

import (
	"log"

	bolt "go.etcd.io/bbolt"
)

// RepairOrphanRels scans the inodes bucket and updates fm.Rel for any
// inode whose stored Rel doesn't map back to itself in the paths bucket.
// Typical cause: pre-fix DeleteFileTx left fm.Rel pointing at an unlinked
// hardlink while the inode itself survived via another path.
//
// Inodes with no surviving paths at all are logged and left alone — we
// can't safely delete them since their fragments may still be live in
// segments, and the paths bucket may simply be incomplete (this pass is
// defensive, not authoritative).
//
// Returns the number of inodes whose Rel was repaired. Quiet (no log) when
// zero.
func RepairOrphanRels(meta *Meta) error {
	type entry struct {
		id     uint64
		oldRel string
		newRel string
	}
	var todo []entry
	var orphans []uint64

	err := meta.ViewLocked(func(tx *bolt.Tx) error {
		return iterateInodesTx(tx, func(id uint64, fm *FileMeta) error {
			// Validate fm.Rel resolves back to this inode. Mismatch means
			// either a stale primary name (hardlink unlink) or no surviving
			// path at all.
			ownerID, err := inodeForPathTx(tx, fm.Rel)
			if err != nil {
				return err
			}
			if ownerID == id {
				return nil // healthy
			}
			survivor, err := findPathForInodeTx(tx, id)
			if err != nil {
				return err
			}
			if survivor == "" {
				orphans = append(orphans, id)
				return nil
			}
			todo = append(todo, entry{id: id, oldRel: fm.Rel, newRel: survivor})
			return nil
		})
	})
	if err != nil {
		return err
	}

	if len(todo) > 0 {
		err = meta.UpdateLocked(func(tx *bolt.Tx) error {
			for _, e := range todo {
				fm, err := getInodeTx(tx, e.id)
				if err != nil {
					return err
				}
				if fm == nil {
					continue
				}
				// Re-check that the captured survivor still resolves to
				// this inode (UpdateLocked flushes the overlay between
				// View and Update; in single-threaded startup nothing else
				// races, but defend anyway).
				ownerID, err := inodeForPathTx(tx, e.newRel)
				if err != nil {
					return err
				}
				if ownerID != e.id {
					continue
				}
				fm.Rel = e.newRel
				if err := putInodeTx(tx, e.id, fm); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		log.Printf("place: repaired stale fm.Rel for %d inode(s)", len(todo))
	}
	if len(orphans) > 0 {
		log.Printf("place: %d inode(s) have no surviving path; leaving in place "+
			"(fragments may still be live in segments)", len(orphans))
	}
	return nil
}
