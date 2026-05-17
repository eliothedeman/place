package index

import (
	"reflect"
	"testing"

	"github.com/eliothedeman/place/segment"
)

func TestPlanReadDisjoint(t *testing.T) {
	// Two non-overlapping fragments. Read covers part of each plus a gap.
	frags := []Fragment{
		{Seq: 1, LogicalOff: 0, Length: 10, Tier: segment.TierHot, SegmentID: 1, SegmentOffset: 100},
		{Seq: 2, LogicalOff: 30, Length: 10, Tier: segment.TierHot, SegmentID: 1, SegmentOffset: 200},
	}
	got := planRead(frags, 5, 30) // [5, 35)
	want := []ReadSlice{
		{LogicalOff: 5, Length: 5, Locator: segment.Locator{Tier: segment.TierHot, SegmentID: 1, Offset: 105, Length: 5}},
		{LogicalOff: 10, Length: 20, Sparse: true},
		{LogicalOff: 30, Length: 5, Locator: segment.Locator{Tier: segment.TierHot, SegmentID: 1, Offset: 200, Length: 5}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestPlanReadShadowing(t *testing.T) {
	// Older fragment [0, 20). Newer fragment [5, 15) shadows the middle.
	frags := []Fragment{
		{Seq: 1, LogicalOff: 0, Length: 20, Tier: segment.TierHot, SegmentID: 1, SegmentOffset: 100},
		{Seq: 2, LogicalOff: 5, Length: 10, Tier: segment.TierHot, SegmentID: 1, SegmentOffset: 500},
	}
	got := planRead(frags, 0, 20)
	want := []ReadSlice{
		{LogicalOff: 0, Length: 5, Locator: segment.Locator{Tier: segment.TierHot, SegmentID: 1, Offset: 100, Length: 5}},
		{LogicalOff: 5, Length: 10, Locator: segment.Locator{Tier: segment.TierHot, SegmentID: 1, Offset: 500, Length: 10}},
		{LogicalOff: 15, Length: 5, Locator: segment.Locator{Tier: segment.TierHot, SegmentID: 1, Offset: 115, Length: 5}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestPlanReadSparse(t *testing.T) {
	got := planRead(nil, 0, 100)
	want := []ReadSlice{{LogicalOff: 0, Length: 100, Sparse: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestCanonicalViewShadows(t *testing.T) {
	// Older covers [0, 20). Newer covers [5, 15). View should be three runs:
	// [0,5) old; [5,15) new; [15,20) old.
	frags := []Fragment{
		{Seq: 1, LogicalOff: 0, Length: 20, Tier: segment.TierHot, SegmentID: 7, SegmentOffset: 100},
		{Seq: 2, LogicalOff: 5, Length: 10, Tier: segment.TierCold, SegmentID: 9, SegmentOffset: 500},
	}
	got := canonicalView(frags)
	want := []Fragment{
		{Seq: 1, LogicalOff: 0, Length: 5, Tier: segment.TierHot, SegmentID: 7, SegmentOffset: 100},
		{Seq: 2, LogicalOff: 5, Length: 10, Tier: segment.TierCold, SegmentID: 9, SegmentOffset: 500},
		{Seq: 1, LogicalOff: 15, Length: 5, Tier: segment.TierHot, SegmentID: 7, SegmentOffset: 115},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestFragmentEncode(t *testing.T) {
	in := []Fragment{
		{Seq: 1, LogicalOff: 0, Length: 1024, Tier: segment.TierHot, SegmentID: 5, SegmentOffset: 4096},
		{Seq: 2, LogicalOff: 1024, Length: 2048, Tier: segment.TierCold, SegmentID: 8, SegmentOffset: 8192},
	}
	out := decodeFragments(encodeFragments(in))
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch:\n%v\n%v", in, out)
	}
}
