package mover

import (
	"bytes"
	"testing"

	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/segment"
)

func TestEvictMovesHotStripesToCold(t *testing.T) {
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Write some bytes across multiple stripes.
	payload := bytes.Repeat([]byte("M"), 16<<10)
	for s := int64(0); s < 4; s++ {
		if err := idx.Append(1, s*(1<<16), payload); err != nil {
			t.Fatal(err)
		}
	}
	initialHot := idx.HotUsedBytes()
	if initialHot == 0 {
		t.Fatalf("no hot bytes after appends")
	}

	mv := Start(Config{
		Index:          idx,
		HotMaxBytes:    1,
		HotTargetBytes: 0,
		Tick:           1 << 30, // huge — we'll trigger manually
		Logger:         func(string, ...any) {},
	})
	defer mv.Stop()
	if err := mv.RunOnce(); err != nil {
		t.Fatal(err)
	}

	// All stripes should now be cold.
	err = idx.IterStripes(func(s index.StripeInfo) bool {
		if s.HotBytes > 0 {
			t.Errorf("inode=%d stripe=%d still has %d hot bytes", s.Inode, s.StripeID, s.HotBytes)
		}
		if s.ColdBytes == 0 {
			t.Errorf("inode=%d stripe=%d has no cold bytes after evict", s.Inode, s.StripeID)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// Reads should still work.
	got := make([]byte, len(payload))
	plan, err := idx.PlanRead(1, 0, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	for _, sl := range plan {
		if sl.Sparse {
			t.Fatalf("unexpected sparse slice")
		}
		dst := got[sl.LogicalOff : sl.LogicalOff+sl.Length]
		if _, err := idx.ReadAt(sl.Locator, dst); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("post-evict read mismatch")
	}
}

func TestGCSkipsActiveSegment(t *testing.T) {
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Write a single tiny record; active hot segment exists with that one frag.
	if err := idx.Append(1, 0, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	pre := len(idx.SegmentIDs(segment.TierHot))
	if pre == 0 {
		t.Fatalf("no hot segments after one append")
	}

	mv := Start(Config{Index: idx, Logger: func(string, ...any) {}})
	defer mv.Stop()
	if err := mv.RunOnce(); err != nil {
		t.Fatal(err)
	}

	if got := len(idx.SegmentIDs(segment.TierHot)); got != pre {
		t.Fatalf("GC dropped a segment we still need: pre=%d post=%d", pre, got)
	}
}
