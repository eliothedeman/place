package place

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"syscall"
)

// firstColdOverlap returns the index of the first cold fragment whose
// end is past `off`, using a binary search on the sorted, non-overlapping
// cold fragment list.
func firstColdOverlap(cold []Fragment, off int64) int {
	return sort.Search(len(cold), func(i int) bool {
		return cold[i].LogicalOffset+cold[i].Length > off
	})
}

// Reader serves random reads over a file's fragments, preferring hot
// segments and falling back to cold for any gaps.
type Reader struct {
	hot  *SegmentSet
	cold *SegmentSet
	meta *Meta

	// Parallelism for a single ReadAt. Small by default — for most FUSE
	// reads there's only one or two fragments to fetch.
	parallel int
}

func NewReader(hot, cold *SegmentSet, meta *Meta) *Reader {
	return &Reader{hot: hot, cold: cold, meta: meta, parallel: 4}
}

// ReadAt fills dst from rel starting at `off`. Returns the number of bytes
// filled (may be < len(dst) at EOF). Missing ranges within [0, Size) are
// zero-filled.
func (r *Reader) ReadAt(rel string, dst []byte, off int64) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	fm, err := r.meta.GetFile(rel)
	if err != nil {
		return 0, err
	}
	if fm == nil {
		return 0, syscall.ENOENT
	}
	if !fm.IsRegular() {
		return 0, syscall.EISDIR
	}
	if off >= fm.Size {
		return 0, io.EOF
	}
	readEnd := off + int64(len(dst))
	if readEnd > fm.Size {
		readEnd = fm.Size
	}
	readLen := readEnd - off
	// Pre-zero the window we'll fill.
	for i := int64(0); i < readLen; i++ {
		dst[i] = 0
	}

	hotSlices := planRead(fm.HotFragments, off, readLen)
	gaps := uncoveredRanges(hotSlices, 0, readLen)
	coldSlices := planColdForGaps(fm.ColdFragments, gaps, off)
	all := append(hotSlices, coldSlices...)
	if err := r.execute(all, dst); err != nil {
		return 0, err
	}
	return int(readLen), nil
}

type span struct{ start, end int64 }

// planColdForGaps emits cold-fragment slices that cover the given gaps in the
// caller's buffer (gaps are coordinates relative to `off`). Cold fragments
// are sorted by LogicalOffset; each gap is served by binary-searching to
// the first overlapping fragment and walking until past the gap.
func planColdForGaps(cold []Fragment, gaps []span, off int64) []readSlice {
	if len(cold) == 0 {
		return nil
	}
	var out []readSlice
	for _, g := range gaps {
		gapAbsStart := off + g.start
		gapAbsEnd := off + g.end
		i := firstColdOverlap(cold, gapAbsStart)
		for ; i < len(cold); i++ {
			f := cold[i]
			if f.LogicalOffset >= gapAbsEnd {
				break
			}
			start := f.LogicalOffset
			if start < gapAbsStart {
				start = gapAbsStart
			}
			end := f.End()
			if end > gapAbsEnd {
				end = gapAbsEnd
			}
			out = append(out, readSlice{
				DstOffset:  start - off,
				Frag:       f,
				FragOffset: start - f.LogicalOffset,
				Length:     end - start,
			})
		}
	}
	return out
}

// uncoveredRanges returns the subranges of [base, end) not covered by any
// of the given slices. Slices' DstOffsets are assumed relative to `base`.
func uncoveredRanges(slices []readSlice, base, end int64) []span {
	// Sort by DstOffset.
	sorted := make([]readSlice, len(slices))
	copy(sorted, slices)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].DstOffset < sorted[j].DstOffset })
	var out []span
	pos := base
	for _, s := range sorted {
		if s.DstOffset > pos {
			out = append(out, span{pos, s.DstOffset})
		}
		after := s.DstOffset + s.Length
		if after > pos {
			pos = after
		}
	}
	if pos < end {
		out = append(out, span{pos, end})
	}
	return out
}

func (r *Reader) execute(slices []readSlice, dst []byte) error {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return r.readSlice(slices[0], dst)
	}
	// Bounded parallel execution.
	workers := r.parallel
	if workers > len(slices) {
		workers = len(slices)
	}
	jobs := make(chan readSlice)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				if err := r.readSlice(s, dst); err != nil {
					// Surface the first error per worker, but keep
					// draining `jobs` so the sender can't deadlock if
					// every worker errors on its first slice (e.g.
					// segments removed by GC mid-replicate).
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}()
	}
	for _, s := range slices {
		jobs <- s
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (r *Reader) readSlice(s readSlice, dst []byte) error {
	var seg *Segment
	switch s.Frag.Tier {
	case TierHot:
		seg = r.hot.Get(s.Frag.SegmentID)
	case TierCold:
		seg = r.cold.Get(s.Frag.SegmentID)
	default:
		return fmt.Errorf("unknown tier %d", s.Frag.Tier)
	}
	if seg == nil {
		return fmt.Errorf("segment %d/%d missing", s.Frag.Tier, s.Frag.SegmentID)
	}
	buf := dst[s.DstOffset : s.DstOffset+s.Length]
	n, err := seg.ReadAt(buf, s.Frag.SegmentOffset+s.FragOffset)
	if err != nil && err != io.EOF {
		return err
	}
	if int64(n) != s.Length {
		return fmt.Errorf("short read: got %d want %d", n, s.Length)
	}
	return nil
}

