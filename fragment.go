package place

import "sort"

// mergeFragment splices `n` into a sorted, non-overlapping fragment list.
// Existing fragments that overlap with `n` are truncated or removed, with
// the displaced ranges returned as `dead`. The returned `merged` list is
// sorted by LogicalOffset and non-overlapping.
func mergeFragment(existing []Fragment, n Fragment) (merged, dead []Fragment) {
	newStart := n.LogicalOffset
	newEnd := n.End()

	// Fast path: new fragment starts at or after the end of the list. This
	// is the hot case for sequential appends (dd, rsync, log writes). O(1)
	// amortized and mutates `existing`'s backing array when it has capacity.
	if len(existing) == 0 || existing[len(existing)-1].End() <= newStart {
		return append(existing, n), nil
	}

	var kept []Fragment
	for _, f := range existing {
		fStart := f.LogicalOffset
		fEnd := f.End()

		// No overlap — keep as-is.
		if fEnd <= newStart || fStart >= newEnd {
			kept = append(kept, f)
			continue
		}
		// Entirely within new — fully dead.
		if fStart >= newStart && fEnd <= newEnd {
			dead = append(dead, f)
			continue
		}
		// Straddles new — split into left/dead/right.
		if fStart < newStart && fEnd > newEnd {
			kept = append(kept, Fragment{
				LogicalOffset: fStart,
				Length:        newStart - fStart,
				Tier:          f.Tier,
				SegmentID:     f.SegmentID,
				SegmentOffset: f.SegmentOffset,
			})
			dead = append(dead, Fragment{
				LogicalOffset: newStart,
				Length:        newEnd - newStart,
				Tier:          f.Tier,
				SegmentID:     f.SegmentID,
				SegmentOffset: f.SegmentOffset + (newStart - fStart),
			})
			kept = append(kept, Fragment{
				LogicalOffset: newEnd,
				Length:        fEnd - newEnd,
				Tier:          f.Tier,
				SegmentID:     f.SegmentID,
				SegmentOffset: f.SegmentOffset + (newEnd - fStart),
			})
			continue
		}
		// Overlap at start — f begins before new but ends within it.
		if fStart < newStart {
			kept = append(kept, Fragment{
				LogicalOffset: fStart,
				Length:        newStart - fStart,
				Tier:          f.Tier,
				SegmentID:     f.SegmentID,
				SegmentOffset: f.SegmentOffset,
			})
			dead = append(dead, Fragment{
				LogicalOffset: newStart,
				Length:        fEnd - newStart,
				Tier:          f.Tier,
				SegmentID:     f.SegmentID,
				SegmentOffset: f.SegmentOffset + (newStart - fStart),
			})
			continue
		}
		// Overlap at end — f begins within new but ends beyond it.
		dead = append(dead, Fragment{
			LogicalOffset: fStart,
			Length:        newEnd - fStart,
			Tier:          f.Tier,
			SegmentID:     f.SegmentID,
			SegmentOffset: f.SegmentOffset,
		})
		kept = append(kept, Fragment{
			LogicalOffset: newEnd,
			Length:        fEnd - newEnd,
			Tier:          f.Tier,
			SegmentID:     f.SegmentID,
			SegmentOffset: f.SegmentOffset + (newEnd - fStart),
		})
	}

	// Sorted insertion of n into kept.
	inserted := false
	for _, k := range kept {
		if !inserted && k.LogicalOffset >= newEnd {
			merged = append(merged, n)
			inserted = true
		}
		merged = append(merged, k)
	}
	if !inserted {
		merged = append(merged, n)
	}
	return merged, dead
}

// readSlice is one scatter-gather entry for a read.
type readSlice struct {
	DstOffset  int64 // offset in destination buffer
	Frag       Fragment
	FragOffset int64 // offset within the fragment's payload
	Length     int64
}

// planRead returns the list of scatter-gather reads needed to fill
// [readOff, readOff+readLen). Bytes not covered by any fragment are "holes"
// — callers should pre-zero their buffer to get implicit zero-fill.
//
// Fragments are sorted by LogicalOffset and non-overlapping (per
// mergeFragment), so we binary-search for the first fragment whose end is
// past readOff and walk forward until we pass readEnd. O(log n + k) for k
// overlapping fragments — the linear-scan version was a real cost on
// large files with many small writes (rsync of a 35 GB movie ends up
// with ~140K fragments).
func planRead(frags []Fragment, readOff, readLen int64) []readSlice {
	if readLen <= 0 || len(frags) == 0 {
		return nil
	}
	readEnd := readOff + readLen
	// First fragment whose End() > readOff — i.e. could overlap [readOff, ...).
	i := sort.Search(len(frags), func(i int) bool {
		return frags[i].LogicalOffset+frags[i].Length > readOff
	})
	var out []readSlice
	for ; i < len(frags); i++ {
		f := frags[i]
		fStart := f.LogicalOffset
		if fStart >= readEnd {
			break
		}
		fEnd := f.End()
		start := fStart
		if readOff > start {
			start = readOff
		}
		end := fEnd
		if readEnd < end {
			end = readEnd
		}
		out = append(out, readSlice{
			DstOffset:  start - readOff,
			Frag:       f,
			FragOffset: start - fStart,
			Length:     end - start,
		})
	}
	return out
}

// truncateFragments returns the sublist whose logical range lies in [0, size),
// with any straddling fragment truncated and the truncated-away range added
// to `dead`.
func truncateFragments(frags []Fragment, size int64) (kept, dead []Fragment) {
	for _, f := range frags {
		if f.LogicalOffset >= size {
			dead = append(dead, f)
			continue
		}
		if f.End() <= size {
			kept = append(kept, f)
			continue
		}
		// Straddles the truncation boundary.
		keepLen := size - f.LogicalOffset
		kept = append(kept, Fragment{
			LogicalOffset: f.LogicalOffset,
			Length:        keepLen,
			Tier:          f.Tier,
			SegmentID:     f.SegmentID,
			SegmentOffset: f.SegmentOffset,
		})
		dead = append(dead, Fragment{
			LogicalOffset: size,
			Length:        f.End() - size,
			Tier:          f.Tier,
			SegmentID:     f.SegmentID,
			SegmentOffset: f.SegmentOffset + keepLen,
		})
	}
	return kept, dead
}
