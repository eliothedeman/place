package place

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// writeReq is one user Write request — a chunk of bytes at a logical offset
// in a file. Pooled to avoid per-Submit allocations.
type writeReq struct {
	rel        string
	logicalOff int64
	data       []byte
	reply      chan writeResult
}

type writeResult struct {
	err error
}

// writeReqPool reuses writeReq structs (and their reply channels) across
// Submit calls. Since Submit is synchronous, each caller gets its own req
// for the duration of the call and returns it on exit.
var writeReqPool = sync.Pool{
	New: func() any {
		return &writeReq{reply: make(chan writeResult, 1)}
	},
}

// Writer group-commits file writes into the active hot segment + bbolt. The
// commit path does NOT fsync — writes land in page cache (segment) and in
// bbolt's unsync'd state. A background flusher makes them durable (segment
// fsync first, then bbolt fsync). Explicit Flush() waits for durability.
type Writer struct {
	hot     *SegmentSet
	meta    *Meta
	ev      *Evictor
	compact *Compactor // optional; notified of bytes written for replicate triggers

	ch chan *writeReq

	maxBatch int

	mu     sync.Mutex
	closed bool
	done   chan struct{}

	// Durability watermarks.
	commitSeq  atomic.Uint64 // bumped after each successful commit batch
	durableSeq atomic.Uint64 // bumped after each successful flush cycle

	flushMu   sync.Mutex
	dirtySegs map[uint32]*Segment
	flushedCh chan struct{} // broadcast: closed and replaced on each flush cycle
	flushErr  error         // most recent flush error

	flushKick chan struct{}
	flushDone chan struct{} // closed to signal flusher to exit

	flushInterval time.Duration // overlay drain → bbolt (no fsync)
	syncInterval  time.Duration // full fsync (segments + bbolt)
}

func NewWriter(hot *SegmentSet, meta *Meta, ev *Evictor) *Writer {
	w := &Writer{
		hot:           hot,
		meta:          meta,
		ev:            ev,
		ch:            make(chan *writeReq, 1024),
		maxBatch:      256,
		done:          make(chan struct{}),
		dirtySegs:     make(map[uint32]*Segment),
		flushedCh:     make(chan struct{}),
		flushKick:     make(chan struct{}, 1),
		flushDone:     make(chan struct{}),
		flushInterval: 10 * time.Millisecond,
		syncInterval:  1 * time.Second,
	}
	go w.loop()
	go w.flushLoop()
	return w
}

// SetCompactor wires a Compactor so the writer can report bytes written to
// hot — used to drive the size-threshold replicate trigger. Call once
// before any Submit.
func (w *Writer) SetCompactor(c *Compactor) {
	w.compact = c
}

// Submit sends a write request; blocks until the commit batch replies. The
// reply means "visible to subsequent reads"; durability requires Flush.
func (w *Writer) Submit(rel string, logicalOff int64, data []byte) error {
	if w.ev != nil {
		if err := w.ev.Admit(int64(len(data))); err != nil {
			return err
		}
	}
	req := writeReqPool.Get().(*writeReq)
	req.rel = rel
	req.logicalOff = logicalOff
	req.data = data
	// Drain any stale reply (shouldn't happen with a cap-1 channel and a
	// single consumer, but defend against misuse).
	select {
	case <-req.reply:
	default:
	}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		req.rel = ""
		req.data = nil
		writeReqPool.Put(req)
		return syscall.ESHUTDOWN
	}
	w.mu.Unlock()
	w.ch <- req
	res := <-req.reply
	req.rel = ""
	req.data = nil
	writeReqPool.Put(req)
	return res.err
}

// Close drains in-flight writes, does a final flush, and stops the flusher.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.ch)
	w.mu.Unlock()
	<-w.done
	err := w.Flush()
	close(w.flushDone)
	return err
}

func (w *Writer) loop() {
	defer close(w.done)
	batch := make([]*writeReq, 0, w.maxBatch)
	for {
		req, ok := <-w.ch
		if !ok {
			return
		}
		batch = append(batch, req)
	drain:
		for len(batch) < w.maxBatch {
			select {
			case req, ok := <-w.ch:
				if !ok {
					break drain
				}
				batch = append(batch, req)
			default:
				break drain
			}
		}
		w.commitBatch(batch)
		for i := range batch {
			batch[i] = nil
		}
		batch = batch[:0]
	}
}

// commitBatch appends each request to a hot segment, then mutates the Meta
// overlay in-place (no bbolt writes on the hot path). Touched segments are
// marked dirty for the background flusher, which fsyncs them and then
// commits the overlay to bbolt.
func (w *Writer) commitBatch(batch []*writeReq) {
	type applied struct {
		req    *writeReq
		segID  uint32
		segOff int64
	}

	var appliedReqs []applied
	touchedSegs := map[uint32]*Segment{}
	segDelta := map[uint32]int64{}

	for _, r := range batch {
		recordSize := int64(headerFixedSize + len(r.rel) + len(r.data) + trailerSize)
		_, _, err := w.hot.RotateIfFull(recordSize)
		if err != nil {
			r.reply <- writeResult{err: err}
			continue
		}
		seg, err := w.hot.Active()
		if err != nil {
			r.reply <- writeResult{err: err}
			continue
		}
		segOff, err := seg.Append(recordData, r.rel, r.logicalOff, r.data)
		if err != nil {
			r.reply <- writeResult{err: err}
			continue
		}
		appliedReqs = append(appliedReqs, applied{r, seg.id, segOff})
		touchedSegs[seg.id] = seg
		segDelta[seg.id] += recordSize
	}

	err := w.meta.WithOverlay(func() error {
		now := time.Now().UnixNano()
		for _, a := range appliedReqs {
			fm, err := w.meta.getFileLocked(a.req.rel)
			if err != nil {
				return err
			}
			if fm == nil {
				return fmt.Errorf("write to missing file %q", a.req.rel)
			}
			newFrag := Fragment{
				LogicalOffset: a.req.logicalOff,
				Length:        int64(len(a.req.data)),
				Tier:          TierHot,
				SegmentID:     a.segID,
				SegmentOffset: a.segOff,
			}
			merged, dead := mergeFragment(fm.HotFragments, newFrag)
			fm.HotFragments = merged
			for _, f := range dead {
				sm, err := w.meta.getSegLocked(f.Tier, f.SegmentID)
				if err != nil {
					return err
				}
				if sm != nil {
					sm.Live -= f.Length
					if sm.Live < 0 {
						sm.Live = 0
					}
				}
			}
			fm.Mtime = now
			fm.Ctime = now
			if a.req.logicalOff+int64(len(a.req.data)) > fm.Size {
				fm.Size = a.req.logicalOff + int64(len(a.req.data))
			}
			fm.Version++
		}
		for id, delta := range segDelta {
			sm, err := w.meta.getSegLocked(TierHot, id)
			if err != nil {
				return err
			}
			if sm == nil {
				sm = &SegmentMeta{ID: id, Tier: TierHot, CreatedAt: now}
				w.meta.putSegLocked(sm)
			}
			sm.Total += delta
			sm.Live += delta
		}
		return nil
	})

	if err == nil {
		w.flushMu.Lock()
		for id, seg := range touchedSegs {
			w.dirtySegs[id] = seg
		}
		w.flushMu.Unlock()
		w.commitSeq.Add(1)
		// Don't kick the flusher here — writes aren't required to be
		// durable on return. Durability comes from syncInterval or explicit
		// Fsync.
		if w.compact != nil {
			var total int64
			for _, a := range appliedReqs {
				total += int64(len(a.req.data))
			}
			w.compact.AddWriteBytes(total)
		}
	}

	for _, a := range appliedReqs {
		select {
		case a.req.reply <- writeResult{err: err}:
		default:
		}
	}
}

func (w *Writer) flushLoop() {
	commitTick := time.NewTicker(w.flushInterval)
	defer commitTick.Stop()
	syncTick := time.NewTicker(w.syncInterval)
	defer syncTick.Stop()
	for {
		select {
		case <-w.flushDone:
			return
		case <-w.flushKick:
			w.doSync()
		case <-syncTick.C:
			w.doSync()
		case <-commitTick.C:
			w.doFlush()
		}
	}
}

// doFlush drains the overlay into bbolt without fsyncing. This is the
// high-frequency path (every flushInterval) — it moves bytes out of the
// in-memory overlay so read paths see them via bbolt cursors (Readdir etc.)
// and cap memory growth. Durability is achieved by doSync below, which
// runs less often or on explicit Fsync.
func (w *Writer) doFlush() {
	if err := w.meta.FlushNoSync(); err != nil {
		w.flushMu.Lock()
		w.flushErr = err
		log.Printf("place: commit: %v", err)
		w.flushMu.Unlock()
	}
}

// doSync makes all preceding writes durable: fsync every dirty hot segment,
// flush the meta overlay (no-op if already drained), then fsync bbolt.
// Ordering matters: segment fsync MUST precede bbolt's sync, otherwise a
// crash can leave bbolt fragments pointing at un-fsync'd segment bytes.
func (w *Writer) doSync() {
	w.flushMu.Lock()
	snapshot := w.dirtySegs
	w.dirtySegs = make(map[uint32]*Segment)
	seq := w.commitSeq.Load()
	w.flushMu.Unlock()

	var firstErr error
	for _, seg := range snapshot {
		if err := seg.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		if err := w.meta.Flush(); err != nil {
			firstErr = err
		}
	}

	w.flushMu.Lock()
	w.flushErr = firstErr
	if firstErr == nil {
		if seq > w.durableSeq.Load() {
			w.durableSeq.Store(seq)
		}
	} else {
		for id, seg := range snapshot {
			if _, ok := w.dirtySegs[id]; !ok {
				w.dirtySegs[id] = seg
			}
		}
		log.Printf("place: sync: %v", firstErr)
	}
	close(w.flushedCh)
	w.flushedCh = make(chan struct{})
	w.flushMu.Unlock()
}

// Flush blocks until all previously-committed writes are durable on disk.
// Returns the flush error if one occurred.
func (w *Writer) Flush() error {
	target := w.commitSeq.Load()
	for {
		w.flushMu.Lock()
		if w.durableSeq.Load() >= target {
			err := w.flushErr
			w.flushMu.Unlock()
			return err
		}
		if w.flushErr != nil {
			err := w.flushErr
			w.flushMu.Unlock()
			return err
		}
		ch := w.flushedCh
		w.flushMu.Unlock()

		select {
		case w.flushKick <- struct{}{}:
		default:
		}
		<-ch
	}
}
