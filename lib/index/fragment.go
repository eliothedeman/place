package index

import (
	"encoding/binary"
	"sort"

	"github.com/eliothedeman/place/lib/segment"
)

// A Fragment names a contiguous run of bytes in a file's logical range, with
// a locator describing where those bytes physically live and a Seq that
// orders writes within the fragment's stripe. The stripe a fragment belongs
// to is implicit in the bbolt key it's stored under, not in the struct.
//
// Fragments are immutable once written. The index appends new fragments;
// it never edits existing ones in place. Shadowing (newer-wins) happens at
// read time via planRead.
type Fragment struct {
	// Seq is monotonic within the owning stripe. Higher seq means newer.
	Seq uint64
	// LogicalOff is the byte offset within the file (not within the stripe).
	LogicalOff int64
	// Length is the payload length.
	Length int64
	// Tier of the segment carrying the payload.
	Tier segment.Tier
	// SegmentID within its tier's Set.
	SegmentID uint32
	// SegmentOffset is the absolute byte offset of the payload in the segment.
	SegmentOffset int64
}

const fragmentEncodedSize = 40

func encodeFragmentInto(buf []byte, f Fragment) {
	binary.LittleEndian.PutUint64(buf[0:], f.Seq)
	binary.LittleEndian.PutUint64(buf[8:], uint64(f.LogicalOff))
	binary.LittleEndian.PutUint64(buf[16:], uint64(f.Length))
	buf[24] = byte(f.Tier)
	buf[25] = 0
	buf[26] = 0
	buf[27] = 0
	binary.LittleEndian.PutUint32(buf[28:], f.SegmentID)
	binary.LittleEndian.PutUint64(buf[32:], uint64(f.SegmentOffset))
}

func decodeFragment(buf []byte) Fragment {
	return Fragment{
		Seq:           binary.LittleEndian.Uint64(buf[0:]),
		LogicalOff:    int64(binary.LittleEndian.Uint64(buf[8:])),
		Length:        int64(binary.LittleEndian.Uint64(buf[16:])),
		Tier:          segment.Tier(buf[24]),
		SegmentID:     binary.LittleEndian.Uint32(buf[28:]),
		SegmentOffset: int64(binary.LittleEndian.Uint64(buf[32:])),
	}
}

func encodeFragments(frags []Fragment) []byte {
	out := make([]byte, len(frags)*fragmentEncodedSize)
	for i := range frags {
		encodeFragmentInto(out[i*fragmentEncodedSize:], frags[i])
	}
	return out
}

func decodeFragments(buf []byte) []Fragment {
	n := len(buf) / fragmentEncodedSize
	out := make([]Fragment, n)
	for i := 0; i < n; i++ {
		out[i] = decodeFragment(buf[i*fragmentEncodedSize:])
	}
	return out
}

// ReadSlice describes one piece of a read plan: either a Locator pointing
// at physical bytes, or a sparse gap (no fragment covers this range).
type ReadSlice struct {
	LogicalOff int64
	Length     int64
	Sparse     bool
	Locator    segment.Locator
}

// planRead computes the byte-for-byte plan to satisfy a read of
// [readOff, readOff+readLen) given a set of fragments. Newer-seq fragments
// shadow older-seq fragments where they overlap. Bytes not covered by any
// fragment become Sparse slices. Output is sorted by logical offset.
//
// The algorithm: keep a sorted, disjoint list of "uncovered" intervals
// starting with the whole read range. For each fragment in descending-seq
// order, subtract its range from uncovered; the parts that were uncovered-
// and-now-aren't become Locator slices. After all fragments, what remains
// in uncovered becomes Sparse slices.
func planRead(frags []Fragment, readOff, readLen int64) []ReadSlice {
	if readLen <= 0 {
		return nil
	}
	// Local copy so we can sort without surprising the caller.
	sorted := make([]Fragment, len(frags))
	copy(sorted, frags)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq > sorted[j].Seq })

	type ivl struct{ start, end int64 }
	uncovered := []ivl{{readOff, readOff + readLen}}
	var slices []ReadSlice

	for _, f := range sorted {
		if len(uncovered) == 0 {
			break
		}
		fStart := f.LogicalOff
		fEnd := f.LogicalOff + f.Length
		var next []ivl
		for _, u := range uncovered {
			// No overlap?
			if fEnd <= u.start || fStart >= u.end {
				next = append(next, u)
				continue
			}
			ovStart := fStart
			if u.start > ovStart {
				ovStart = u.start
			}
			ovEnd := fEnd
			if u.end < ovEnd {
				ovEnd = u.end
			}
			slices = append(slices, ReadSlice{
				LogicalOff: ovStart,
				Length:     ovEnd - ovStart,
				Locator: segment.Locator{
					Tier:      f.Tier,
					SegmentID: f.SegmentID,
					Offset:    f.SegmentOffset + (ovStart - fStart),
					Length:    ovEnd - ovStart,
				},
			})
			if u.start < ovStart {
				next = append(next, ivl{u.start, ovStart})
			}
			if ovEnd < u.end {
				next = append(next, ivl{ovEnd, u.end})
			}
		}
		uncovered = next
	}
	for _, u := range uncovered {
		slices = append(slices, ReadSlice{
			LogicalOff: u.start,
			Length:     u.end - u.start,
			Sparse:     true,
		})
	}
	sort.Slice(slices, func(i, j int) bool { return slices[i].LogicalOff < slices[j].LogicalOff })
	return slices
}

// canonicalView walks fragments in descending seq order and returns the
// non-overlapping list of "fragment-derived slices" representing the
// current contents of the stripe — what bytes are live, and which fragment
// they come from. Output is sorted by logical offset.
//
// This is the building block for Move: each output entry is one contiguous
// run of bytes the mover needs to either keep or copy to a new tier.
func canonicalView(frags []Fragment) []Fragment {
	if len(frags) == 0 {
		return nil
	}
	sorted := make([]Fragment, len(frags))
	copy(sorted, frags)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq > sorted[j].Seq })

	type ivl struct{ start, end int64 }
	// Initial "needs covering" range = union of all fragment extents.
	minOff := sorted[0].LogicalOff
	maxEnd := sorted[0].LogicalOff + sorted[0].Length
	for _, f := range sorted[1:] {
		if f.LogicalOff < minOff {
			minOff = f.LogicalOff
		}
		if e := f.LogicalOff + f.Length; e > maxEnd {
			maxEnd = e
		}
	}
	uncovered := []ivl{{minOff, maxEnd}}
	var out []Fragment

	for _, f := range sorted {
		if len(uncovered) == 0 {
			break
		}
		fStart := f.LogicalOff
		fEnd := f.LogicalOff + f.Length
		var next []ivl
		for _, u := range uncovered {
			if fEnd <= u.start || fStart >= u.end {
				next = append(next, u)
				continue
			}
			ovStart := fStart
			if u.start > ovStart {
				ovStart = u.start
			}
			ovEnd := fEnd
			if u.end < ovEnd {
				ovEnd = u.end
			}
			out = append(out, Fragment{
				Seq:           f.Seq,
				LogicalOff:    ovStart,
				Length:        ovEnd - ovStart,
				Tier:          f.Tier,
				SegmentID:     f.SegmentID,
				SegmentOffset: f.SegmentOffset + (ovStart - fStart),
			})
			if u.start < ovStart {
				next = append(next, ivl{u.start, ovStart})
			}
			if ovEnd < u.end {
				next = append(next, ivl{ovEnd, u.end})
			}
		}
		uncovered = next
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LogicalOff < out[j].LogicalOff })
	return out
}
