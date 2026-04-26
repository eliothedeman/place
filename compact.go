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
func (c *Compactor) replicatePass() {
	var candidates []string
	err := c.meta.ViewLocked(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bucketInodes).Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			fm, err := decodeFileMeta(v)
			if err != nil {
				return err
			}
			if !fm.IsRegular() || len(fm.HotFragments) == 0 {
				continue
			}
			if fm.HasColdCopy() {
				continue
			}
			candidates = append(candidates, fm.Rel)
		}
		return nil
	})
	if err != nil {
		log.Printf("place: replicatePass scan: %v", err)
		return
	}
	for _, rel := range candidates {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if err := c.replicateOne(rel); err != nil {
			log.Printf("place: replicate %q: %v", rel, err)
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

// replicateOne copies the current contents of rel into cold as one or more
// chunked fragments and atomically updates metadata. No-op if the file's
// version changed mid-flight.
func (c *Compactor) replicateOne(rel string) error {
	done := c.dbg.op("Replicate", rel)
	fm1, err := c.meta.GetFile(rel)
	if err != nil {
		done(fs_errno(err))
		return err
	}
	if fm1 == nil || !fm1.IsRegular() {
		done(0, "gone")
		return nil
	}
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
			if fmNow, gerr := c.meta.GetFile(rel); gerr == nil && fmNow != nil && fmNow.HasColdCopy() {
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

	// Atomically swap in the new cold fragments.
	err = c.meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm2, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if fm2 == nil {
			// File vanished; leave the cold bytes as dead space.
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
		// Account new cold segment bytes per touched segment.
		perSeg := map[uint32]struct{ total, live int64 }{}
		for _, r := range records {
			v := perSeg[r.segID]
			v.total += r.recLen
			v.live += r.length
			perSeg[r.segID] = v
		}
		for id, v := range perSeg {
			sm, err := GetSegmentTx(tx, TierCold, id)
			if err != nil {
				return err
			}
			if sm == nil {
				sm = &SegmentMeta{ID: id, Tier: TierCold, CreatedAt: time.Now().UnixNano()}
			}
			sm.Total += v.total
			sm.Live += v.live
			if err := PutSegmentTx(tx, sm); err != nil {
				return err
			}
		}
		fm2.Version++
		return PutFileTx(tx, fm2)
	})
	if err == errVersionChanged {
		c.dbg.log("Replicate %q: version changed mid-flight, aborting", rel)
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
			// Dead segment — drop outright.
			if err := set.Remove(sm.ID); err != nil {
				log.Printf("place: gc remove %d: %v", sm.ID, err)
				continue
			}
			if err := c.meta.UpdateLocked(func(tx *bolt.Tx) error {
				return DeleteSegmentTx(tx, tier, sm.ID)
			}); err != nil {
				log.Printf("place: gc delete meta %d: %v", sm.ID, err)
			}
			dropped++
			if tier == TierHot {
				c.ev.Freed()
			}
			continue
		}
		dead := float64(sm.Total-sm.Live) / float64(sm.Total)
		if dead > gcDeadRatio {
			partials = append(partials, candidate{sm: sm, dead: dead})
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

	// Find all rels referencing this segment.
	var refs []string
	err := c.meta.ViewLocked(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bucketInodes).Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			fm, err := decodeFileMeta(v)
			if err != nil {
				return err
			}
			if referencesSegment(fm, tier, target.ID) {
				refs = append(refs, fm.Rel)
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("place: gc: scan refs: %v", err)
		return
	}

	// For each ref, read its live fragments on this segment, rewrite to
	// active, update meta.
	for _, rel := range refs {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if err := c.rewriteRefs(rel, tier, target.ID, oldSeg, set); err != nil {
			log.Printf("place: gc rewrite %q: %v", rel, err)
			return
		}
	}

	// Remove the old segment.
	if err := set.Remove(target.ID); err != nil {
		log.Printf("place: gc remove %d: %v", target.ID, err)
		return
	}
	if err := c.meta.UpdateLocked(func(tx *bolt.Tx) error {
		return DeleteSegmentTx(tx, tier, target.ID)
	}); err != nil {
		log.Printf("place: gc delete meta %d: %v", target.ID, err)
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

func (c *Compactor) rewriteRefs(rel string, tier Tier, segID uint32, oldSeg *Segment, set *SegmentSet) error {
	fm, err := c.meta.GetFile(rel)
	if err != nil {
		return err
	}
	if fm == nil {
		return nil
	}
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
		old    Fragment
		newID  uint32
		newOff int64
		lost   bool
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
		news = append(news, newLoc{old: f, newID: seg.id, newOff: segOff})
	}
	// Swap in new locations under a transaction; abort if Version changed.
	return c.meta.UpdateLocked(func(tx *bolt.Tx) error {
		cur, err := GetFileTx(tx, rel)
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
		for i, f := range list {
			if news[i].lost {
				lostFrags = append(lostFrags, f)
				continue
			}
			nf := f
			nf.SegmentID = news[i].newID
			nf.SegmentOffset = news[i].newOff
			updated = append(updated, nf)
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
		cur.Version++
		return PutFileTx(tx, cur)
	})
}
