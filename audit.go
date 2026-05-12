package place

import (
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// AuditKind classifies an audit finding.
type AuditKind int

const (
	// AuditMissingSegment: the Fragment references a segment that is not
	// present in the SegmentSet (no in-memory entry, no on-disk file).
	AuditMissingSegment AuditKind = iota
	// AuditPastEOF: the Fragment's byte range extends past the segment file's
	// current on-disk size. Reads in this range will short-read.
	AuditPastEOF
)

func (k AuditKind) String() string {
	switch k {
	case AuditMissingSegment:
		return "missing-segment"
	case AuditPastEOF:
		return "past-eof"
	default:
		return fmt.Sprintf("audit-kind-%d", int(k))
	}
}

// AuditFinding is one dangling Fragment discovered by Audit.
type AuditFinding struct {
	Kind       AuditKind
	InodeID    uint64
	Rel        string // fm.Rel at audit time
	Tier       Tier
	SegmentID  uint32
	Frag       Fragment // the offending fragment
	SegSize    int64    // on-disk segment size at audit time; 0 if missing
	FileSize   int64    // fm.Size
}

func (f AuditFinding) String() string {
	tierStr := "hot"
	if f.Tier == TierCold {
		tierStr = "cold"
	}
	switch f.Kind {
	case AuditMissingSegment:
		return fmt.Sprintf("missing-segment inode=%d rel=%q %s/seg=%d frag=[%d,%d) len=%d",
			f.InodeID, f.Rel, tierStr, f.SegmentID,
			f.Frag.LogicalOffset, f.Frag.End(), f.Frag.Length)
	case AuditPastEOF:
		return fmt.Sprintf("past-eof inode=%d rel=%q %s/seg=%d segOff=%d+len=%d=%d > segSize=%d (file %s, size=%d)",
			f.InodeID, f.Rel, tierStr, f.SegmentID,
			f.Frag.SegmentOffset, f.Frag.Length,
			f.Frag.SegmentOffset+f.Frag.Length, f.SegSize,
			f.Rel, f.FileSize)
	default:
		return fmt.Sprintf("%s inode=%d rel=%q %s/seg=%d frag=[%d,%d)",
			f.Kind, f.InodeID, f.Rel, tierStr, f.SegmentID,
			f.Frag.LogicalOffset, f.Frag.End())
	}
}

// Audit walks every FileMeta and verifies that each Fragment lies within its
// referenced segment file on disk. Catches:
//
//   - dangling Fragments whose Segment was unlinked / GC'd while a FileMeta
//     still pointed at it
//   - Fragments whose [SegmentOffset, SegmentOffset+Length) extends past the
//     segment file's current size — symptoms of a reconcile truncation that
//     didn't trim fragments, or an Append/Total accounting drift.
//
// Read-only: opens nothing it doesn't already have, takes a meta view tx,
// stat()s segment files via SegmentSet.Get().Size(). Safe to run live, but
// findings are a point-in-time snapshot.
func Audit(meta *Meta, hotSegs, coldSegs *SegmentSet) ([]AuditFinding, error) {
	var findings []AuditFinding

	check := func(id uint64, rel string, fileSize int64, frags []Fragment, tier Tier, set *SegmentSet) {
		for _, f := range frags {
			seg := set.Get(f.SegmentID)
			if seg == nil {
				findings = append(findings, AuditFinding{
					Kind: AuditMissingSegment, InodeID: id, Rel: rel,
					Tier: tier, SegmentID: f.SegmentID, Frag: f, FileSize: fileSize,
				})
				continue
			}
			segSize := seg.Size()
			if f.SegmentOffset+f.Length > segSize {
				findings = append(findings, AuditFinding{
					Kind: AuditPastEOF, InodeID: id, Rel: rel,
					Tier: tier, SegmentID: f.SegmentID, Frag: f,
					SegSize: segSize, FileSize: fileSize,
				})
			}
		}
	}

	err := meta.ViewLocked(func(tx *bolt.Tx) error {
		return iterateInodesTx(tx, func(id uint64, fm *FileMeta) error {
			if !fm.IsRegular() {
				return nil
			}
			check(id, fm.Rel, fm.Size, fm.HotFragments, TierHot, hotSegs)
			check(id, fm.Rel, fm.Size, fm.ColdFragments, TierCold, coldSegs)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Stable order: by tier, segID, inode, fragment offset — makes diffs
	// across runs meaningful when chasing a regression.
	sort.Slice(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.Tier != b.Tier {
			return a.Tier < b.Tier
		}
		if a.SegmentID != b.SegmentID {
			return a.SegmentID < b.SegmentID
		}
		if a.InodeID != b.InodeID {
			return a.InodeID < b.InodeID
		}
		return a.Frag.LogicalOffset < b.Frag.LogicalOffset
	})
	return findings, nil
}
