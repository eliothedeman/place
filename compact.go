package place

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Compactor runs background replication (hot → cold) and segment GC.
// Replication fires on three triggers:
//   - Hot-disk pressure from Evictor (via TriggerReplicate).
//   - Time threshold: at most replicateAfter between passes in steady state.
//   - Size threshold: once bytesSince exceeds replicateMaxBytes.
type Compactor struct {
	reader   *Reader
	hotSegs  *SegmentSet
	coldSegs *SegmentSet
	meta     *Meta
	ev       *Evictor
	dbg      dbg

	replicateAfter    time.Duration
	replicateMaxBytes int64

	// bytesSince is the count of user bytes written to hot since the last
	// replicate pass. Bumped by the Writer via AddWriteBytes; reset on pass.
	bytesSince atomic.Int64

	mu              sync.Mutex
	lastReplicateAt time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// preSwapHookForTest fires inside replicateOne after all chunk Appends
	// have flushed to disk but before the metadata swap-in tx runs. The
	// audit suite uses it to mutate fm.Version mid-flight so the swap aborts
	// with errVersionChanged, exercising the orphan-bytes bookkeeping path.
	// nil in production.
	preSwapHookForTest func(id uint64)
}

func NewCompactor(reader *Reader, hotSegs, coldSegs *SegmentSet, meta *Meta, ev *Evictor, replicateAfter time.Duration, replicateMaxBytes int64, dbg dbg) *Compactor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Compactor{
		reader:            reader,
		hotSegs:           hotSegs,
		coldSegs:          coldSegs,
		meta:              meta,
		ev:                ev,
		dbg:               dbg,
		replicateAfter:    replicateAfter,
		replicateMaxBytes: replicateMaxBytes,
		lastReplicateAt:   time.Now(),
		ctx:               ctx,
		cancel:            cancel,
	}
}

// AddWriteBytes is called by the Writer after each commit with the total
// user-data bytes written. Used to drive the size-based replicate trigger.
func (c *Compactor) AddWriteBytes(n int64) {
	if n <= 0 {
		return
	}
	c.bytesSince.Add(n)
}

func (c *Compactor) Start() {
	c.wg.Add(2)
	go c.replicateLoop()
	go c.gcLoop()
}

func (c *Compactor) Stop() {
	c.cancel()
	c.wg.Wait()
}

// replicateLoop handles triggered and threshold-based replication.
// Polls at a fraction of replicateAfter so the time threshold fires
// promptly without burning CPU.
func (c *Compactor) replicateLoop() {
	defer c.wg.Done()
	interval := c.replicateAfter / 10
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.ev.TriggerReplicate():
			c.doReplicatePass("pressure")
		case <-ticker.C:
			if reason := c.shouldReplicate(); reason != "" {
				c.doReplicatePass(reason)
			}
		}
	}
}

// shouldReplicate returns a non-empty reason string if a pass should run.
func (c *Compactor) shouldReplicate() string {
	c.mu.Lock()
	last := c.lastReplicateAt
	c.mu.Unlock()
	if time.Since(last) >= c.replicateAfter {
		return "time"
	}
	if c.bytesSince.Load() >= c.replicateMaxBytes {
		return "bytes"
	}
	return ""
}

// doReplicatePass runs a pass and resets the triggers.
func (c *Compactor) doReplicatePass(reason string) {
	c.mu.Lock()
	since := time.Since(c.lastReplicateAt)
	c.mu.Unlock()
	c.dbg.log("compact: replicate pass (reason=%s, bytes=%s, since=%v)",
		reason, humanBytes(c.bytesSince.Load()), since.Round(time.Second))
	c.replicatePass()
	c.mu.Lock()
	c.lastReplicateAt = time.Now()
	c.mu.Unlock()
	c.bytesSince.Store(0)
}

// replicatePass picks files needing replication and copies them to cold.
// Stops when no more candidates or ctx is cancelled.
//
// Operates on inodeID rather than fm.Rel — between scan and copy, fm.Rel
// can be invalidated by hardlink unlink or rename. replicateOne re-fetches
// the FileMeta via inodeID and uses the freshly-loaded fm.Rel for record
// framing, which is guaranteed by DeleteFileTx/movePathTx invariants to
// resolve back to this inode.
func (c *Compactor) replicatePass() {
	var candidates []uint64
	err := c.meta.ViewLocked(func(tx *bolt.Tx) error {
		return iterateInodesTx(tx, func(id uint64, fm *FileMeta) error {
			if !fm.IsRegular() || len(fm.HotFragments) == 0 {
				return nil
			}
			if fm.HasColdCopy() {
				return nil
			}
			candidates = append(candidates, id)
			return nil
		})
	})
	if err != nil {
		log.Printf("place: replicatePass scan: %v", err)
		return
	}
	for _, id := range candidates {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if err := c.replicateOne(id); err != nil {
			log.Printf("place: replicate inode %d: %v", id, err)
		}
	}
}

// replicateChunkSize is the max bytes read per chunk during replication.
// Replication streams a large file as a series of chunks so memory use is
// bounded regardless of file size, and no single cold record is huge.
const replicateChunkSize = 16 << 20 // 16 MiB

// coldRecord is one chunk written to cold during a replication pass.
type coldRecord struct {
	logOff int64
	length int64
	segID  uint32
	segOff int64
	recLen int64 // framed record size (for SegmentMeta.Total bookkeeping)
}

// replicateOne copies the current contents of inodeID into cold as one or
// more chunked fragments and atomically updates metadata. No-op if the
// file's version changed mid-flight or if the inode was deleted.
//
// Reads the current fm via getInodeTx (not paths[fm.Rel]) so it's robust
// to hardlink unlink and rename happening between scan and replicate.
// reader.ReadAt and seg.Append still need a rel — fm.Rel is used because
// it's guaranteed by DeleteFileTx/movePathTx invariants to resolve back to
// this inode.
func (c *Compactor) replicateOne(id uint64) error {
	var fm1 *FileMeta
	err := c.meta.ViewLocked(func(tx *bolt.Tx) error {
		got, err := getInodeTx(tx, id)
		fm1 = got
		return err
	})
	if err != nil {
		return err
	}
	if fm1 == nil || !fm1.IsRegular() {
		return nil
	}
	rel := fm1.Rel
	done := c.dbg.op("Replicate", rel)
	if fm1.HasColdCopy() {
		done(0, "already-complete")
		return nil
	}

	size := fm1.Size
	if size == 0 {
		// Nothing to copy; still need to advance state? A zero-byte file
		// has no fragments, so HasColdCopy would have returned true above.
		done(0, "empty")
		return nil
	}

	bufSize := int64(replicateChunkSize)
	if size < bufSize {
		bufSize = size
	}
	buf := make([]byte, bufSize)
	var records []coldRecord
	touchedSegs := map[uint32]*Segment{}

	// Log progress every replicateProgressEvery bytes for files large
	// enough to benefit. Keeps small-file replicate quiet while making
	// long copies (UHD movies, etc.) visible during the minutes they
	// spend streaming hot → cold.
	const replicateProgressEvery = 256 << 20 // 256 MiB
	var nextProgress int64 = replicateProgressEvery
	if size >= replicateProgressEvery*2 {
		c.dbg.log("replicate: %s starting (size=%s)", rel, humanBytes(size))
	}

	for off := int64(0); off < size; {
		remaining := size - off
		n := int64(replicateChunkSize)
		if n > remaining {
			n = remaining
		}
		chunk := buf[:n]
		if _, err := c.reader.ReadAt(rel, chunk, off); err != nil {
			// Eviction (which only fires for files that already have a
			// full cold copy) wipes HotFragments and can leave GC to
			// remove the now-dead hot segments. If our read failed and
			// the file is now fully cold, another path completed the
			// work for us — treat as success and move on.
			var fmNow *FileMeta
			if gerr := c.meta.ViewLocked(func(tx *bolt.Tx) error {
				got, e := getInodeTx(tx, id)
				fmNow = got
				return e
			}); gerr == nil && fmNow != nil && fmNow.HasColdCopy() {
				done(0, "evicted-during-replicate")
				return nil
			}
			done(fs_errno(err))
			return fmt.Errorf("read %q @%d: %w", rel, off, err)
		}

		recLen := int64(headerFixedSize + len(rel) + int(n) + trailerSize)
		if _, _, err := c.coldSegs.RotateIfFull(recLen); err != nil {
			done(fs_errno(err))
			return err
		}
		seg, err := c.coldSegs.Active()
		if err != nil {
			done(fs_errno(err))
			return err
		}
		segOff, err := seg.Append(recordData, rel, off, chunk)
		if err != nil {
			done(fs_errno(err))
			return err
		}
		records = append(records, coldRecord{
			logOff: off, length: n, segID: seg.id, segOff: segOff, recLen: recLen,
		})
		touchedSegs[seg.id] = seg
		off += n

		if size >= replicateProgressEvery*2 && off >= nextProgress {
			c.dbg.log("replicate: %s %s/%s (%.0f%%)",
				rel, humanBytes(off), humanBytes(size), float64(off)/float64(size)*100)
			nextProgress += replicateProgressEvery
		}

		// Yield between chunks for responsiveness.
		select {
		case <-c.ctx.Done():
			done(0, "cancelled")
			return nil
		default:
		}
	}

	// Fsync once per touched cold segment — cheaper than per-chunk.
	for _, seg := range touchedSegs {
		if err := seg.Sync(); err != nil {
			done(fs_errno(err))
			return err
		}
	}

	if c.preSwapHookForTest != nil {
		c.preSwapHookForTest(id)
	}

	// Atomically swap in the new cold fragments. Look up by inodeID so we
	// don't get tripped up by a path-bucket mutation that happened
	// mid-flight (rename, unlink-of-hardlink).
	err = c.meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm2, err := getInodeTx(tx, id)
		if err != nil {
			return err
		}
		if fm2 == nil {
			// Inode was deleted (last hardlink unlinked); leave the cold
			// bytes as dead space.
			return nil
		}
		if fm2.Version != fm1.Version {
			return errVersionChanged
		}
		if err := AddLiveBytesTx(tx, fm2.ColdFragments, -1); err != nil {
			return err
		}
		newCold := make([]Fragment, 0, len(records))
		for _, r := range records {
			newCold = append(newCold, Fragment{
				LogicalOffset: r.logOff,
				Length:        r.length,
				Tier:          TierCold,
				SegmentID:     r.segID,
				SegmentOffset: r.segOff,
			})
		}
		fm2.ColdFragments = newCold
		// We just rewrote cold to match the snapshot at fm1.Version; the
		// version-changed check above guarantees no hot mutation has
		// landed between then and now, so cold is in sync.
		fm2.ColdDirty = false
		// Account new cold segment bytes per touched segment.
		perSeg := map[uint32]struct{ total, live int64 }{}
		for _, r := range records {
			v := perSeg[r.segID]
			v.total += r.recLen
			v.live += r.length
			perSeg[r.segID] = v
		}
		for sid, v := range perSeg {
			sm, err := GetSegmentTx(tx, TierCold, sid)
			if err != nil {
				return err
			}
			if sm == nil {
				sm = &SegmentMeta{ID: sid, Tier: TierCold, CreatedAt: time.Now().UnixNano()}
			}
			sm.Total += v.total
			sm.Live += v.live
			if err := PutSegmentTx(tx, sm); err != nil {
				return err
			}
		}
		fm2.Version++
		return putInodeTx(tx, id, fm2)
	})
	if err == errVersionChanged {
		// The swap-in tx aborted, but seg.Append already wrote our chunks to
		// the active cold segment(s). Without bookkeeping, sm.Total stays
		// behind seg.size by exactly the orphan framed bytes; subsequent
		// successful replicates append past that gap and credit only their
		// own recLen, leaving Total perpetually short. On restart, reconcile
		// would truncate the seg back to sm.Total — lopping off the
		// legitimate post-orphan records along with our orphan tail.
		//
		// Run a follow-up tx that bumps Total (not Live — these bytes are
		// referenced by no Fragment) so reconcile sees the real on-disk size.
		// The orphan ranges are walked over harmlessly by forwardCompact (no
		// inode references them) and reclaimed when the seg eventually
		// crosses gcDeadRatio and is forward-compacted out.
		var orphanBytes int64
		perSeg := map[uint32]int64{}
		for _, r := range records {
			perSeg[r.segID] += r.recLen
			orphanBytes += r.recLen
		}
		if err2 := c.meta.UpdateLocked(func(tx *bolt.Tx) error {
			for sid, total := range perSeg {
				sm, err := GetSegmentTx(tx, TierCold, sid)
				if err != nil {
					return err
				}
				if sm == nil {
					sm = &SegmentMeta{ID: sid, Tier: TierCold, CreatedAt: time.Now().UnixNano()}
				}
				sm.Total += total
				// sm.Live unchanged — orphan bytes have no fragment refs.
				if err := PutSegmentTx(tx, sm); err != nil {
					return err
				}
			}
			return nil
		}); err2 != nil {
			done(fs_errno(err2))
			return fmt.Errorf("account orphan bytes after version-change on %q: %w", rel, err2)
		}
		c.dbg.log("Replicate %q: version changed mid-flight, %s orphan bytes accounted across %d cold seg(s)",
			rel, humanBytes(orphanBytes), len(perSeg))
		done(0, "raced")
		return nil
	}
	if err != nil {
		done(fs_errno(err))
		return err
	}
	done(0, "size=%s chunks=%d", humanBytes(size), len(records))
	c.ev.Freed()
	return nil
}

var errVersionChanged = fmt.Errorf("version changed")

// gcLoop periodically reclaims dead and mostly-dead segments. Tick interval
// is short and per-pass work is bounded so reclaim throughput is bursty in
// the small but adds up — without this the only thing freeing segments is
// "wait for every fragment in a segment to be evicted", which never
// happens for segments shared across an active replicate pass.
func (c *Compactor) gcLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.gcPass(TierHot)
			c.gcPass(TierCold)
		}
	}
}

const (
	gcInterval         = 10 * time.Second
	gcDeadRatio        = 0.3 // forward-compact a segment once it's >30% dead
	gcMaxFwdPerPass    = 8   // cap on expensive forward-compacts per tick
)

// RunGCPass triggers one synchronous gc sweep for tier. Test-only hook so
// the audit suite can exercise the real fsync-before-unlink ordering path
// instead of structurally re-enacting it.
func (c *Compactor) RunGCPass(tier Tier) {
	c.gcPass(tier)
}

// NewCompactorForTest builds a Compactor with default debug-off settings.
// Tests use this when they need to drive gc/replicate paths without
// constructing the full Mount stack.
func NewCompactorForTest(reader *Reader, hotSegs, coldSegs *SegmentSet, meta *Meta, ev *Evictor) *Compactor {
	return NewCompactor(reader, hotSegs, coldSegs, meta, ev, time.Hour, 1<<30, dbg{})
}

// ReplicateOneForTest drives the production replicateOne path for a single
// inode synchronously. The audit suite uses this (paired with
// SetPreSwapHookForTest) to deterministically exercise the version-change
// race window without standing up a full background loop.
func (c *Compactor) ReplicateOneForTest(id uint64) error {
	return c.replicateOne(id)
}

// SetPreSwapHookForTest installs a callback that fires inside replicateOne
// after the cold appends have flushed but before the metadata swap-in tx.
// Tests use it to bump fm.Version (simulating a concurrent write) so the
// swap aborts with errVersionChanged.
func (c *Compactor) SetPreSwapHookForTest(hook func(id uint64)) {
	c.preSwapHookForTest = hook
}

// gcPass reclaims segments in the given tier:
//   - drops every fully-dead (Live==0) segment outright
//   - forward-compacts up to gcMaxFwdPerPass partially-dead ones whose
//     dead ratio exceeds gcDeadRatio, deadest first
func (c *Compactor) gcPass(tier Tier) {
	segs, err := c.meta.ListSegments(tier)
	if err != nil {
		log.Printf("place: gc list segments: %v", err)
		return
	}
	set := c.hotSegs
	if tier == TierCold {
		set = c.coldSegs
	}
	activeID := uint32(0)
	if a, _ := set.Active(); a != nil {
		activeID = a.id
	}

	type candidate struct {
		sm   *SegmentMeta
		dead float64
	}
	var partials []candidate
	dropped := 0

	var deadIDs []uint32
	for _, sm := range segs {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if sm.ID == activeID {
			continue
		}
		if sm.Total == 0 {
			continue
		}
		if sm.Live == 0 {
			deadIDs = append(deadIDs, sm.ID)
			continue
		}
		dead := float64(sm.Total-sm.Live) / float64(sm.Total)
		if dead > gcDeadRatio {
			partials = append(partials, candidate{sm: sm, dead: dead})
		}
	}

	// Drop fully-dead segments. Crash-safe ordering matters: bbolt is
	// NoSync, so the writer txs that drove these segs to Live=0 may sit
	// unsynced in mmap. If we unlink the .seg file before fsyncing those
	// txs, a power loss rolls bbolt back to a state with stale live
	// fragments pointing at the now-deleted file — bytes lost.
	//   1. Flush so every pre-existing Live=0 transition is durable.
	//   2. Delete the SegmentMeta entries in one batched tx.
	//   3. Flush again to make the deletions durable.
	//   4. Only THEN unlink the .seg files.
	if len(deadIDs) > 0 {
		if err := c.meta.Flush(); err != nil {
			log.Printf("place: gc pre-delete flush: %v", err)
			return
		}
		if err := c.meta.UpdateLocked(func(tx *bolt.Tx) error {
			for _, id := range deadIDs {
				if err := DeleteSegmentTx(tx, tier, id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Printf("place: gc delete meta (batch %d): %v", len(deadIDs), err)
			return
		}
		if err := c.meta.Flush(); err != nil {
			log.Printf("place: gc post-delete flush: %v", err)
			return
		}
		for _, id := range deadIDs {
			if err := set.Remove(id); err != nil {
				log.Printf("place: gc remove %d: %v", id, err)
				// Continue — meta is already detached, so a residual
				// .seg file is harmless dead space; a later reconcile
				// or restart will drop it.
			}
		}
		dropped = len(deadIDs)
		if tier == TierHot {
			c.ev.Freed()
		}
	}

	if dropped > 0 {
		c.dbg.log("gc: dropped %d fully-dead segments (tier=%d)", dropped, tier)
	}

	// Forward-compact the deadest partials, up to the per-pass cap.
	sort.Slice(partials, func(i, j int) bool {
		return partials[i].dead > partials[j].dead
	})
	limit := len(partials)
	if limit > gcMaxFwdPerPass {
		limit = gcMaxFwdPerPass
	}
	for i := 0; i < limit; i++ {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		c.forwardCompact(tier, partials[i].sm)
	}
}

// forwardCompact walks all FileMetas whose fragments point at segID, copies
// each live fragment's bytes into the active segment of the same tier, and
// updates the FileMeta to point at the new location. Old segment is then
// deleted.
func (c *Compactor) forwardCompact(tier Tier, target *SegmentMeta) {
	c.dbg.log("gc: forward-compact segment %d tier=%d (live=%s total=%s)",
		target.ID, tier, humanBytes(target.Live), humanBytes(target.Total))

	set := c.hotSegs
	if tier == TierCold {
		set = c.coldSegs
	}
	oldSeg := set.Get(target.ID)
	if oldSeg == nil {
		log.Printf("place: gc: segment %d missing on disk", target.ID)
		return
	}

	// Find all inodes referencing this segment. Walk by inodeID — fm.Rel
	// can become stale across hardlink unlink and rename, so re-resolving
	// via paths[fm.Rel] later would silently skip the inode and the source
	// segment would be unlinked without relocating its fragments.
	var refs []uint64
	err := c.meta.ViewLocked(func(tx *bolt.Tx) error {
		return iterateInodesTx(tx, func(id uint64, fm *FileMeta) error {
			if referencesSegment(fm, tier, target.ID) {
				refs = append(refs, id)
			}
			return nil
		})
	})
	if err != nil {
		log.Printf("place: gc: scan refs: %v", err)
		return
	}

	// For each ref, read its live fragments on this segment, rewrite to
	// active, update meta.
	for _, id := range refs {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if err := c.rewriteRefs(id, tier, target.ID, oldSeg, set); err != nil {
			log.Printf("place: gc rewrite inode %d: %v", id, err)
			return
		}
	}

	// Crash-safe teardown of the source segment. bbolt is NoSync, so the
	// rewriteRefs txs that pointed FileMetas at the new locations are still
	// unsynced. We MUST fsync them (and the upcoming SegmentMeta delete)
	// before unlinking the .seg file — otherwise a crash rolls bbolt back
	// to FileMetas that still reference target.ID while the file is gone.
	//   1. Flush so the relocations land in the on-disk db.
	//   2. Delete the source SegmentMeta in its own tx.
	//   3. Flush so the delete is durable.
	//   4. Only THEN unlink the .seg file.
	if err := c.meta.Flush(); err != nil {
		log.Printf("place: gc pre-delete flush %d: %v", target.ID, err)
		return
	}
	if err := c.meta.UpdateLocked(func(tx *bolt.Tx) error {
		return DeleteSegmentTx(tx, tier, target.ID)
	}); err != nil {
		log.Printf("place: gc delete meta %d: %v", target.ID, err)
		return
	}
	if err := c.meta.Flush(); err != nil {
		log.Printf("place: gc post-delete flush %d: %v", target.ID, err)
		return
	}
	if err := set.Remove(target.ID); err != nil {
		log.Printf("place: gc remove %d: %v", target.ID, err)
		// Don't bail — bbolt no longer references this segment, so the
		// stray file is dead space; reconcile will drop it on restart.
	}
	if tier == TierHot {
		c.ev.Freed()
	}
}

func referencesSegment(fm *FileMeta, tier Tier, segID uint32) bool {
	list := fm.HotFragments
	if tier == TierCold {
		list = fm.ColdFragments
	}
	for _, f := range list {
		if f.SegmentID == segID {
			return true
		}
	}
	return false
}

func (c *Compactor) rewriteRefs(id uint64, tier Tier, segID uint32, oldSeg *Segment, set *SegmentSet) error {
	var fm *FileMeta
	err := c.meta.ViewLocked(func(tx *bolt.Tx) error {
		got, e := getInodeTx(tx, id)
		fm = got
		return e
	})
	if err != nil {
		return err
	}
	if fm == nil {
		// Inode was deleted between scan and rewrite. The unlink path
		// already credited the segment's Live count downward, so the
		// segment is safe to remove without us relocating anything for
		// this inode.
		return nil
	}
	rel := fm.Rel
	list := fm.HotFragments
	if tier == TierCold {
		list = fm.ColdFragments
	}
	// Read each live fragment into memory. Fragments whose data is no
	// longer on disk (segment file shorter than bbolt's view, typically
	// from a prior unclean shutdown) are dropped: keeping them in fm
	// pins the old segment forever and GC loops on it. The lost bytes
	// were already irretrievable; surfacing as zero-fill on read is
	// strictly better than infinite retry.
	type newLoc struct {
		old     Fragment
		newID   uint32
		newOff  int64
		recSize int64 // framed size of the record we wrote at the new location
		lost    bool
	}
	var news []newLoc
	for _, f := range list {
		if f.SegmentID != segID {
			news = append(news, newLoc{old: f, newID: f.SegmentID, newOff: f.SegmentOffset})
			continue
		}
		buf := make([]byte, f.Length)
		if _, err := oldSeg.ReadAt(buf, f.SegmentOffset); err != nil {
			log.Printf("place: gc rewrite %q: dropping unreadable fragment "+
				"(logical=%d len=%d, segment %d offset=%d): %v — data lost; "+
				"reads of this byte range will return zero-fill",
				rel, f.LogicalOffset, f.Length, segID, f.SegmentOffset, err)
			news = append(news, newLoc{old: f, lost: true})
			continue
		}
		recSize := int64(headerFixedSize + len(rel) + len(buf) + trailerSize)
		if _, _, err := set.RotateIfFull(recSize); err != nil {
			return err
		}
		seg, err := set.Active()
		if err != nil {
			return err
		}
		segOff, err := seg.Append(recordData, rel, f.LogicalOffset, buf)
		if err != nil {
			return err
		}
		if err := seg.Sync(); err != nil {
			return err
		}
		news = append(news, newLoc{old: f, newID: seg.id, newOff: segOff, recSize: recSize})
	}
	// Swap in new locations under a transaction; abort if Version changed.
	// Look up by inodeID — a path mutation between scan and now (rename,
	// hardlink unlink) must not be misread as "file gone".
	return c.meta.UpdateLocked(func(tx *bolt.Tx) error {
		cur, err := getInodeTx(tx, id)
		if err != nil {
			return err
		}
		if cur == nil {
			return nil
		}
		if cur.Version != fm.Version {
			return errVersionChanged
		}
		var updated []Fragment
		var lostFrags []Fragment
		// Per-destination-segment Total/Live deltas: framed size for Total,
		// payload size for Live (matches the writer's split accounting and
		// what AddLiveBytesTx subtracts on eviction).
		addPerSeg := map[uint32]struct{ total, live int64 }{}
		for i, f := range list {
			if news[i].lost {
				lostFrags = append(lostFrags, f)
				continue
			}
			nf := f
			nf.SegmentID = news[i].newID
			nf.SegmentOffset = news[i].newOff
			updated = append(updated, nf)
			// Only count fragments we actually relocated; passthrough
			// fragments (newID == old SegmentID) were already accounted
			// when originally written.
			if news[i].newID != f.SegmentID {
				v := addPerSeg[news[i].newID]
				v.total += news[i].recSize
				v.live += f.Length
				addPerSeg[news[i].newID] = v
			}
		}
		if tier == TierHot {
			cur.HotFragments = updated
		} else {
			cur.ColdFragments = updated
		}
		// Drop lost fragments from the source segment's Live count so it
		// can become fully dead and GC can finally remove the file.
		if len(lostFrags) > 0 {
			if err := AddLiveBytesTx(tx, lostFrags, -1); err != nil {
				return err
			}
		}
		// Bump destination segments' Total/Live to reflect the bytes we
		// just appended. Without this, reconcile-at-restart could see the
		// active segment file longer than its SegmentMeta.Total and
		// truncate it, dropping the relocated bytes.
		for sid, v := range addPerSeg {
			sm, err := GetSegmentTx(tx, tier, sid)
			if err != nil {
				return err
			}
			if sm == nil {
				sm = &SegmentMeta{ID: sid, Tier: tier, CreatedAt: time.Now().UnixNano()}
			}
			sm.Total += v.total
			sm.Live += v.live
			if err := PutSegmentTx(tx, sm); err != nil {
				return err
			}
		}
		cur.Version++
		return putInodeTx(tx, id, cur)
	})
}
