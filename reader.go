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
//
// On a worker error, returns (n, err) where n is the byte length of the
// contiguous prefix not affected by any failed slice — POSIX read(2) permits
// short returns and the FUSE layer surfaces the survivor bytes to the caller.
// If the very first slice failed (no survivor prefix), returns (0, err).
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
	// Pre-zero the window we'll fill (holes in the fragment coverage are
	// implicit zeros). `clear` lowers to runtime.memclrNoHeapPointers,
	// which is SIMD-vectorized — the byte-at-a-time loop this replaced
	// was 23% flat of all CPU under replicate's 16 MiB chunk reads.
	clear(dst[:readLen])

	hotSlices := planRead(fm.HotFragments, off, readLen)
	gaps := uncoveredRanges(hotSlices, 0, readLen)
	coldSlices := planColdForGaps(fm.ColdFragments, gaps, off)
	all := append(hotSlices, coldSlices...)
	maxN, execErr := r.execute(all, dst, readLen)
	if execErr != nil {
		return int(maxN), execErr
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

// execute runs the planned scatter-gather reads. Returns (maxN, err) where
// maxN is the byte length of the contiguous prefix of dst that no failed
// slice intersected — i.e. the DstOffset of the earliest-failing slice, or
// readLen if every slice succeeded. Holes (no slice) are implicit zero-fill
// from ReadAt's pre-clear, so they don't break contiguity. err is nil if
// every slice succeeded.
//
// readSlice's strict length check means a partial pread returns an error
// (no partial fill of the slice's dst window), so an errored slice's
// DstOffset is a safe upper bound for "bytes the caller can trust".
func (r *Reader) execute(slices []readSlice, dst []byte, readLen int64) (int64, error) {
	if len(slices) == 0 {
		return readLen, nil
	}
	if len(slices) == 1 {
		if err := r.readSlice(slices[0], dst); err != nil {
			return slices[0].DstOffset, err
		}
		return readLen, nil
	}
	// Bounded parallel execution.
	workers := r.parallel
	if workers > len(slices) {
		workers = len(slices)
	}
	type sliceErr struct {
		dstOff int64
		err    error
	}
	jobs := make(chan readSlice)
	errCh := make(chan sliceErr, len(slices))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				if err := r.readSlice(s, dst); err != nil {
					// Record (dstOffset, err) for every failure — caller
					// picks the earliest dstOffset to compute the survivor
					// prefix. Buffer is sized to len(slices) so we never
					// drop and never block.
					errCh <- sliceErr{dstOff: s.DstOffset, err: err}
				}
			}
		}()
	}
	for _, s := range slices {
		jobs <- s
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	firstErrAt := readLen
	var firstErr error
	for se := range errCh {
		if se.dstOff < firstErrAt {
			firstErrAt = se.dstOff
			firstErr = se.err
		}
	}
	if firstErr != nil {
		return firstErrAt, firstErr
	}
	return readLen, nil
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

