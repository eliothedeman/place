// Write-back buffer for the store. Each Handle.Write copies its bytes
// into a per-inode in-memory buffer and returns immediately; a worker
// pool drains buffers into the index in batches (one segment fsync per
// touched segment, one pebble commit per batch). fsync(2) forces a
// drain for the inode and waits for it to land.
//
// Why this is here, and not e.g. a separate package: every read+write
// the coalescer mediates is keyed by inode. The two consumers that
// need to see the buffer are store.Handle (Read/Write/Sync) and
// store.Store (Stat for the size hint, Setattr for truncate
// invalidation, Unlink for drop). Both already live in this package.
// Pulling the coalescer out would either re-export private codecs
// (encodeNode, etc.) or hand back internals through public wrappers.
// Keeping it unexported here lets us reach for whatever we need.
//
// Durability contract: write(2) returns when bytes are in process
// memory. fsync(2) returns only after every preceding write for that
// inode has hit segment fsync + pebble WAL fsync. Same as the Linux
// page cache; no lie about an fsync.

package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eliothedeman/place/index"
	"github.com/eliothedeman/place/kv"
	"github.com/eliothedeman/place/obs"
	"github.com/eliothedeman/place/segment"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
)

// Default buffer sizing. Match the user-approved plan:
//   - 64 MiB per inode: roughly one BT piece for typical clients.
//   - 4 GiB global: comfortable headroom on TrueNAS-class boxes.
//   - 100 ms flush interval: bounds the crash window to ~one frame.
//   - 4 flusher workers: more than enough for sequential per-inode
//     flush; cross-inode parallelism is the dominant axis.
const (
	defaultPerInodeCap    = 64 << 20
	defaultGlobalCap      = 4 << 30
	defaultFlushInterval  = 100 * time.Millisecond
	defaultFlushWorkers   = 4
	defaultFlushQueueSize = 1024
)

// coalescerOptions captures tunables. Tests can override via
// newCoalescerWithOptions; production uses the defaults.
type coalescerOptions struct {
	perInodeCap   int64
	globalCap     int64
	flushInterval time.Duration
	workers       int
	queueSize     int
}

func defaultCoalescerOptions() coalescerOptions {
	return coalescerOptions{
		perInodeCap:   defaultPerInodeCap,
		globalCap:     defaultGlobalCap,
		flushInterval: defaultFlushInterval,
		workers:       defaultFlushWorkers,
		queueSize:     defaultFlushQueueSize,
	}
}

// coalescer is the per-Store write-back buffer.
type coalescer struct {
	store *Store

	opts      coalescerOptions
	globalSem *semaphore.Weighted

	mu      sync.Mutex
	buffers map[uint64]*inodeBuf

	flushQ   chan flushReq
	workers  sync.WaitGroup
	stop     chan struct{}    // closed by Close(); workers exit on drain
	closing  atomic.Bool      // true once Close was called; writes start failing
	tickStop chan struct{}    // closes when the time-tick goroutine exits
}

type flushReq struct {
	inode uint64
	// fired (if non-nil) is closed when the resulting flush completes.
	// Used by Fsync to know when a forced flush is done; in the
	// time-tick + size-threshold paths it is nil.
	fired chan struct{}
}

// inodeBuf is the per-inode state.
type inodeBuf struct {
	inode uint64
	tier  index.Tier

	mu       sync.Mutex
	current  *bufBatch // accumulating writes
	flushing *bufBatch // currently being drained to disk
	// onDiskSize / onDiskMtime cache the durable Node fields the last
	// time we observed them. The coalescer's LiveAttrs combines these
	// with current+flushing.maxEnd to advertise a never-going-backward
	// Size to Stat.
	onDiskSize  int64
	onDiskMtime int64
	// liveSize/Mtime are the values the outside world sees via Stat.
	// Maintained on every Write, on flush start (move current.maxEnd
	// to flushing), on flush success (move flushing.maxEnd to disk),
	// and on InvalidatePastSize / Drop. Stored separately from
	// current/flushing so a brief inodeBuf.mu hold gives Stat a fast
	// answer.
	liveSize  int64
	liveMtime int64

	// poisoned: a sticky error from a failed flush. Surfaces on the
	// next Write/Fsync so callers learn the buffer is broken and stop.
	poisoned error
}

// bufBatch is one chunk-of-time worth of buffered writes — all of the
// writes that have arrived since the previous flush started, plus the
// running maxEnd/mtime for the same window.
type bufBatch struct {
	extents []bufExtent
	bytes   int64
	maxEnd  int64
	mtime   int64
	// done closes when this batch is durable (or has been abandoned by
	// Drop). Fsync waits on this. Multiple Fsyncs may share one channel.
	done chan struct{}
	// err is set by the flusher on failure. Readers of done check err
	// to learn whether the flush succeeded. Nil on a clean flush or an
	// in-progress one.
	err error
}

// bufExtent: one Write call's bytes plus the seq we'll stamp on the
// fragment when this batch flushes.
type bufExtent struct {
	seq  uint64
	off  int64
	data []byte // owned by the coalescer; never aliased to caller's slice
}

// newCoalescer is the production constructor.
func newCoalescer(s *Store) *coalescer {
	return newCoalescerWithOptions(s, defaultCoalescerOptions())
}

func newCoalescerWithOptions(s *Store, opts coalescerOptions) *coalescer {
	if opts.perInodeCap <= 0 {
		opts.perInodeCap = defaultPerInodeCap
	}
	if opts.globalCap <= 0 {
		opts.globalCap = defaultGlobalCap
	}
	if opts.flushInterval <= 0 {
		opts.flushInterval = defaultFlushInterval
	}
	if opts.workers <= 0 {
		opts.workers = defaultFlushWorkers
	}
	if opts.queueSize <= 0 {
		opts.queueSize = defaultFlushQueueSize
	}
	c := &coalescer{
		store:     s,
		opts:      opts,
		globalSem: semaphore.NewWeighted(opts.globalCap),
		buffers:   make(map[uint64]*inodeBuf),
		flushQ:    make(chan flushReq, opts.queueSize),
		stop:      make(chan struct{}),
		tickStop:  make(chan struct{}),
	}
	for i := 0; i < opts.workers; i++ {
		c.workers.Add(1)
		go c.flusherLoop()
	}
	go c.tickLoop()
	return c
}

// --- public-to-store API ------------------------------------------------

// Write copies data into the per-inode buffer, blocking on the global
// semaphore if memory pressure demands it. Returns when the bytes are
// committed to process memory.
func (c *coalescer) Write(ctx context.Context, inode uint64, off int64, data []byte, tier index.Tier) error {
	if c.closing.Load() {
		return errors.New("store: write to closing coalescer")
	}
	if len(data) == 0 {
		return nil
	}

	ctx, span := obs.Tracer().Start(ctx, "coalescer.write")
	span.SetAttributes(
		attribute.Int64("inode", int64(inode)),
		attribute.Int64("off", off),
		attribute.Int("bytes", len(data)),
		attribute.String("tier", tier.String()),
	)
	defer span.End()

	// Backpressure: bound the global memory footprint.
	if err := c.globalSem.Acquire(ctx, int64(len(data))); err != nil {
		recordCoalescerError(span, err)
		return err
	}

	buf := c.getOrCreateBuf(inode, tier)
	buf.mu.Lock()
	if buf.poisoned != nil {
		err := buf.poisoned
		buf.mu.Unlock()
		c.globalSem.Release(int64(len(data)))
		recordCoalescerError(span, err)
		return err
	}
	if buf.current == nil {
		buf.current = newBufBatch()
	}
	// Each FUSE write may straddle a stripe boundary. We split here so
	// every bufExtent fits within one stripe, which is the invariant
	// AppendBatchCtx enforces at flush time. Each split chunk gets its
	// own seq; readers merge with on-disk fragments via the unified
	// seq order, so split chunks compose correctly with concurrent
	// same-inode writers.
	stripeSize := c.store.idx.StripeSize()
	rem := data
	curOff := off
	for len(rem) > 0 {
		stripeID := curOff / stripeSize
		stripeEnd := (stripeID + 1) * stripeSize
		take := int64(len(rem))
		if curOff+take > stripeEnd {
			take = stripeEnd - curOff
		}
		chunk := make([]byte, take)
		copy(chunk, rem[:take])
		buf.current.extents = append(buf.current.extents, bufExtent{
			seq:  c.store.idx.NextSeq(),
			off:  curOff,
			data: chunk,
		})
		buf.current.bytes += take
		if e := curOff + take; e > buf.current.maxEnd {
			buf.current.maxEnd = e
		}
		rem = rem[take:]
		curOff += take
	}
	now := time.Now().UnixNano()
	buf.current.mtime = now
	if e := off + int64(len(data)); e > buf.liveSize {
		buf.liveSize = e
	}
	buf.liveMtime = now
	overCap := buf.current.bytes >= c.opts.perInodeCap
	buf.mu.Unlock()

	if overCap {
		// Non-blocking enqueue: the worker pool will pick it up.
		// If the queue is full, the ticker will catch this inode
		// shortly — no need to block the writer.
		select {
		case c.flushQ <- flushReq{inode: inode}:
		default:
		}
	}
	return nil
}

// LiveAttrs reports the size/mtime hints the outside world should see.
// ok=false means "the coalescer holds nothing for this inode; defer to
// on-disk values." Cheap (one map lookup + one short mutex).
func (c *coalescer) LiveAttrs(inode uint64) (size, mtime int64, ok bool) {
	c.mu.Lock()
	buf := c.buffers[inode]
	c.mu.Unlock()
	if buf == nil {
		return 0, 0, false
	}
	buf.mu.Lock()
	defer buf.mu.Unlock()
	return buf.liveSize, buf.liveMtime, true
}

// FragmentsFor returns the buffered extents covering [off, off+length)
// as in-memory Fragments (Tier=segment.TierMem). The Locator on each
// Fragment encodes (SegmentID=0, Offset=seq) so the caller can look
// the data back up via CopyOut. Fragments returned here sort against
// on-disk Fragments by Seq in the unified planRead.
func (c *coalescer) FragmentsFor(inode uint64, off, length int64) []index.Fragment {
	if length <= 0 {
		return nil
	}
	c.mu.Lock()
	buf := c.buffers[inode]
	c.mu.Unlock()
	if buf == nil {
		return nil
	}
	rangeStart := off
	rangeEnd := off + length

	buf.mu.Lock()
	defer buf.mu.Unlock()
	// Walk current + flushing; both are "buffered" from the reader's
	// perspective. Even bytes being written to disk right now should
	// be readable through the buffer until the commit lands and the
	// matching on-disk fragment becomes visible.
	var out []index.Fragment
	collect := func(batch *bufBatch) {
		if batch == nil {
			return
		}
		for _, ext := range batch.extents {
			extEnd := ext.off + int64(len(ext.data))
			if extEnd <= rangeStart || ext.off >= rangeEnd {
				continue
			}
			out = append(out, index.Fragment{
				Seq:        ext.seq,
				LogicalOff: ext.off,
				Length:     int64(len(ext.data)),
				Tier:       segment.TierMem,
				// SegmentID is unused for TierMem; encode the seq in
				// Offset so CopyOut can look up the right bufExtent.
				SegmentID:     0,
				SegmentOffset: int64(ext.seq),
			})
		}
	}
	collect(buf.current)
	collect(buf.flushing)
	return out
}

// CopyOut fills dst with bytes from the in-memory extent identified by
// loc. The caller obtained loc from a Fragment returned by FragmentsFor
// (Tier=TierMem). loc.Offset is the seq; loc.Length tells us how many
// bytes are valid in this slice. The logical-offset adjustment is the
// caller's responsibility — they pass us a slice into the read buffer
// already positioned at the right start.
//
// extentBaseOff is the LogicalOff of the buffered extent (so we can
// compute the in-extent slice that satisfies the locator's
// (extentBaseOff + delta, length) window).
func (c *coalescer) CopyOut(inode uint64, loc segment.Locator, extentBaseOff int64, dst []byte) (int, error) {
	c.mu.Lock()
	buf := c.buffers[inode]
	c.mu.Unlock()
	if buf == nil {
		return 0, fmt.Errorf("store: coalescer.CopyOut: no buffer for inode %d", inode)
	}
	wantSeq := uint64(loc.Offset)
	buf.mu.Lock()
	defer buf.mu.Unlock()
	find := func(batch *bufBatch) *bufExtent {
		if batch == nil {
			return nil
		}
		for i := range batch.extents {
			if batch.extents[i].seq == wantSeq {
				return &batch.extents[i]
			}
		}
		return nil
	}
	ext := find(buf.current)
	if ext == nil {
		ext = find(buf.flushing)
	}
	if ext == nil {
		return 0, fmt.Errorf("store: coalescer.CopyOut: extent seq=%d gone for inode %d", wantSeq, inode)
	}
	// The locator's LogicalOff (which the caller knows as extentBaseOff)
	// might not equal ext.off if planRead clipped the locator. The
	// caller passes extentBaseOff so we can find the right slice
	// within ext.data without re-decoding the locator.
	delta := extentBaseOff - ext.off
	if delta < 0 || delta+loc.Length > int64(len(ext.data)) {
		return 0, fmt.Errorf("store: coalescer.CopyOut: bounds out of range (delta=%d locLen=%d extLen=%d)", delta, loc.Length, len(ext.data))
	}
	n := copy(dst, ext.data[delta:delta+loc.Length])
	return n, nil
}

// Fsync forces every buffered byte for inode to disk and waits for it.
func (c *coalescer) Fsync(ctx context.Context, inode uint64) error {
	ctx, span := obs.Tracer().Start(ctx, "coalescer.fsync")
	span.SetAttributes(attribute.Int64("inode", int64(inode)))
	defer span.End()

	c.mu.Lock()
	buf := c.buffers[inode]
	c.mu.Unlock()
	if buf == nil {
		return nil
	}

	// Snapshot anything in-flight or queued. We wait for these, plus
	// any forced flush we trigger here. Writes that arrive after the
	// snapshot don't need to be durable by this fsync.
	var waits []chan struct{}
	buf.mu.Lock()
	if buf.poisoned != nil {
		err := buf.poisoned
		buf.mu.Unlock()
		recordCoalescerError(span, err)
		return err
	}
	if buf.flushing != nil {
		waits = append(waits, buf.flushing.done)
	}
	needTrigger := buf.current != nil && buf.current.bytes > 0
	if needTrigger {
		waits = append(waits, buf.current.done)
	}
	buf.mu.Unlock()

	if needTrigger {
		fired := make(chan struct{})
		select {
		case c.flushQ <- flushReq{inode: inode, fired: fired}:
		case <-ctx.Done():
			recordCoalescerError(span, ctx.Err())
			return ctx.Err()
		}
		// Wait for our enqueue to land in the worker. Pure synchro:
		// once fired closes the flusher has begun, which means our
		// buf.current snapshot has been swapped into buf.flushing.
		select {
		case <-fired:
		case <-ctx.Done():
			recordCoalescerError(span, ctx.Err())
			return ctx.Err()
		}
	}

	for _, w := range waits {
		select {
		case <-w:
		case <-ctx.Done():
			recordCoalescerError(span, ctx.Err())
			return ctx.Err()
		}
	}

	buf.mu.Lock()
	err := buf.poisoned
	buf.mu.Unlock()
	if err != nil {
		recordCoalescerError(span, err)
	}
	return err
}

// InvalidatePastSize drops or clips buffered extents past size. Used by
// Setattr-truncate to honor "shrink takes effect immediately." Returns
// the new liveSize so callers can compose with on-disk truncate.
func (c *coalescer) InvalidatePastSize(inode uint64, size int64) {
	c.mu.Lock()
	buf := c.buffers[inode]
	c.mu.Unlock()
	if buf == nil {
		return
	}
	buf.mu.Lock()
	defer buf.mu.Unlock()
	var freed int64
	clip := func(batch *bufBatch) {
		if batch == nil {
			return
		}
		kept := batch.extents[:0]
		var newMax int64
		for _, ext := range batch.extents {
			extEnd := ext.off + int64(len(ext.data))
			switch {
			case ext.off >= size:
				freed += int64(len(ext.data))
			case extEnd > size:
				keepLen := size - ext.off
				freed += int64(len(ext.data)) - keepLen
				ext.data = ext.data[:keepLen]
				kept = append(kept, ext)
				if e := ext.off + keepLen; e > newMax {
					newMax = e
				}
			default:
				kept = append(kept, ext)
				if extEnd > newMax {
					newMax = extEnd
				}
			}
		}
		batch.extents = kept
		batch.bytes -= freed
		if batch.maxEnd > newMax {
			batch.maxEnd = newMax
		}
	}
	clip(buf.current)
	clip(buf.flushing)
	if buf.liveSize > size {
		buf.liveSize = size
	}
	if freed > 0 {
		c.globalSem.Release(freed)
	}
}

// Drop abandons every buffered extent for inode. Used when nlink hits
// zero: the inode is going away, so flushing the buffer would just
// produce orphan segments that GC reclaims anyway. If a flush is
// already in flight we wait for it to settle before dropping (so we
// don't free bytes while the flusher is still reading them).
func (c *coalescer) Drop(inode uint64) {
	c.mu.Lock()
	buf := c.buffers[inode]
	c.mu.Unlock()
	if buf == nil {
		return
	}
	// Wait out an in-flight flush. The flusher closes flushing.done;
	// holding inodeBuf.mu briefly to peek is enough.
	for {
		buf.mu.Lock()
		flush := buf.flushing
		buf.mu.Unlock()
		if flush == nil {
			break
		}
		<-flush.done
	}
	buf.mu.Lock()
	var freed int64
	if buf.current != nil {
		freed += buf.current.bytes
		close(buf.current.done)
		buf.current = nil
	}
	buf.liveSize = 0
	buf.liveMtime = 0
	buf.mu.Unlock()
	if freed > 0 {
		c.globalSem.Release(freed)
	}
	c.mu.Lock()
	delete(c.buffers, inode)
	c.mu.Unlock()
}

// Drain flushes every inode buffer and waits for completion. Used by
// Store.Close. Idempotent: a second Drain returns immediately.
func (c *coalescer) Drain(ctx context.Context) error {
	c.closing.Store(true)
	// Snapshot the list of inodes with non-empty buffers.
	c.mu.Lock()
	inodes := make([]uint64, 0, len(c.buffers))
	for ino := range c.buffers {
		inodes = append(inodes, ino)
	}
	c.mu.Unlock()
	// Force-flush each; reuse Fsync so we share the wait machinery.
	var firstErr error
	for _, ino := range inodes {
		if err := c.Fsync(ctx, ino); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close stops the time-tick and flusher workers. Drain should be
// called first to make sure no buffered data is lost. Close itself
// does not flush.
func (c *coalescer) Close() {
	c.closing.Store(true)
	close(c.stop)
	close(c.tickStop)
	close(c.flushQ)
	c.workers.Wait()
}

// --- internal -----------------------------------------------------------

func newBufBatch() *bufBatch {
	return &bufBatch{done: make(chan struct{})}
}

func (c *coalescer) getOrCreateBuf(inode uint64, tier index.Tier) *inodeBuf {
	c.mu.Lock()
	defer c.mu.Unlock()
	if buf, ok := c.buffers[inode]; ok {
		return buf
	}
	buf := &inodeBuf{inode: inode, tier: tier}
	c.buffers[inode] = buf
	return buf
}

// flusherLoop is one worker. Pulls flushReqs off the queue and runs
// one flush per request. Multiple workers can be in flight on
// different inodes concurrently; each inode is single-threaded
// because flushInode takes inodeBuf.mu around the current↔flushing
// swap.
func (c *coalescer) flusherLoop() {
	defer c.workers.Done()
	for req := range c.flushQ {
		c.flushInode(req)
	}
}

// tickLoop periodically scans the buffer map for inodes whose current
// batch has bytes and enqueues them. Catches inodes that never crossed
// the per-inode size threshold but have been idle too long.
func (c *coalescer) tickLoop() {
	t := time.NewTicker(c.opts.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-c.tickStop:
			return
		case <-t.C:
		}
		c.mu.Lock()
		victims := make([]uint64, 0)
		for ino, buf := range c.buffers {
			buf.mu.Lock()
			if buf.current != nil && buf.current.bytes > 0 && buf.flushing == nil {
				victims = append(victims, ino)
			}
			buf.mu.Unlock()
		}
		c.mu.Unlock()
		for _, ino := range victims {
			select {
			case c.flushQ <- flushReq{inode: ino}:
			default:
				// Queue full — another worker will pick it up on the
				// next tick. No need to block tickLoop.
			}
		}
	}
}

// flushInode drains the current batch for one inode to disk. Returns
// any error that the flush encountered; if non-nil, the inode's
// buffer is marked poisoned and future Writes/Fsyncs surface the
// error until the buffer is dropped.
func (c *coalescer) flushInode(req flushReq) {
	c.mu.Lock()
	buf := c.buffers[req.inode]
	c.mu.Unlock()
	if buf == nil {
		if req.fired != nil {
			close(req.fired)
		}
		return
	}

	// Move current → flushing. If a flush is already running on this
	// inode (shouldn't happen normally; tickLoop checks; size-trigger
	// path is racy but rare), bail — the in-flight flush will cover
	// whatever was in current.
	buf.mu.Lock()
	if buf.flushing != nil || buf.current == nil || buf.current.bytes == 0 {
		buf.mu.Unlock()
		if req.fired != nil {
			close(req.fired)
		}
		return
	}
	batch := buf.current
	buf.flushing = batch
	buf.current = newBufBatch()
	tier := buf.tier
	buf.mu.Unlock()

	if req.fired != nil {
		// Signal Fsync that the swap is done. Fsync now knows the
		// buf.flushing it snapshotted is the one we're working on.
		close(req.fired)
	}

	// Build AppendExtents from buffered extents.
	ctx := context.Background()
	ctx, span := obs.Tracer().Start(ctx, "coalescer.flush")
	span.SetAttributes(
		attribute.Int64("inode", int64(req.inode)),
		attribute.Int("extents", len(batch.extents)),
		attribute.Int64("bytes", batch.bytes),
	)
	ae := make([]index.AppendExtent, len(batch.extents))
	for i, ext := range batch.extents {
		ae[i] = index.AppendExtent{
			Seq:  ext.seq,
			Off:  ext.off,
			Data: ext.data,
			Tier: tier,
		}
	}

	// txHook bumps node Size/Mtime in the same pebble batch that
	// commits the fragments. Reads existing node, computes the new
	// Size = max(node.Size, batch.maxEnd), writes back.
	end := batch.maxEnd
	mtime := batch.mtime
	hook := func(b *kv.Batch) error {
		v, err := b.Get(kv.NodeKey(req.inode))
		if err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("store: coalescer flush: no node entry for inode %d", req.inode)
		}
		n, err := decodeNode(v)
		if err != nil {
			return err
		}
		if end > n.Size {
			n.Size = end
		}
		n.Mtime = mtime
		n.Ctime = mtime
		return b.Set(kv.NodeKey(req.inode), encodeNode(n))
	}

	err := c.store.idx.AppendBatchCtx(ctx, req.inode, ae, hook)
	span.End()

	// Settle the batch.
	buf.mu.Lock()
	batch.err = err
	if err != nil {
		buf.poisoned = err
	} else {
		// Promote the just-flushed maxEnd/mtime to on-disk view.
		if end > buf.onDiskSize {
			buf.onDiskSize = end
		}
		if mtime > buf.onDiskMtime {
			buf.onDiskMtime = mtime
		}
	}
	buf.flushing = nil
	buf.mu.Unlock()
	close(batch.done)
	// Release the bytes back to the global semaphore — unblocks
	// writers waiting on backpressure.
	c.globalSem.Release(batch.bytes)
}

// recordCoalescerError stamps the span and is the single place that
// formats the error → span mapping for this file.
func recordCoalescerError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
