package place

import (
	"context"
	"log"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	tier2Start = 0.95 // block writes above this usage
	tier3Start = 0.99 // ENOSPC above this usage
	admitWait  = 5 * time.Second
)

// Evictor monitors hot storage usage and applies backpressure + reclamation.
type Evictor struct {
	hot     *Storage
	meta    *Meta
	hotSegs *SegmentSet
	coldSegs *SegmentSet

	evictAt float64
	evictTo float64

	// Signaled when bytes are freed. Admit() waits on it.
	mu      sync.Mutex
	freedCh chan struct{}

	// Hint to the Compactor that replication is needed.
	triggerReplicate chan struct{}

	dbg dbg

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewEvictor(hot *Storage, meta *Meta, hotSegs, coldSegs *SegmentSet, evictAt, evictTo float64, dbg dbg) *Evictor {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Evictor{
		hot:              hot,
		meta:             meta,
		hotSegs:          hotSegs,
		coldSegs:         coldSegs,
		evictAt:          evictAt,
		evictTo:          evictTo,
		freedCh:          make(chan struct{}),
		triggerReplicate: make(chan struct{}, 1),
		dbg:              dbg,
		ctx:              ctx,
		cancel:           cancel,
	}
	return e
}

// NewEvictorForTest builds an Evictor with debug off. Test-only.
func NewEvictorForTest(hot *Storage, meta *Meta, hotSegs, coldSegs *SegmentSet, evictAt, evictTo float64) *Evictor {
	return NewEvictor(hot, meta, hotSegs, coldSegs, evictAt, evictTo, dbg{})
}

func (e *Evictor) Start() {
	e.wg.Add(1)
	go e.loop()
}

func (e *Evictor) Stop() {
	e.cancel()
	e.wg.Wait()
}

// TriggerReplicate returns the channel the Compactor reads from to learn
// that replication is needed.
func (e *Evictor) TriggerReplicate() <-chan struct{} { return e.triggerReplicate }

// Admit blocks until hot has room for `size` bytes, or returns ENOSPC.
func (e *Evictor) Admit(size int64) error {
	usage := e.hot.UsedFraction()
	if usage < tier2Start {
		return nil
	}
	if usage >= tier3Start {
		e.kick()
		return syscall.ENOSPC
	}
	// Tier 2: wait on progress up to admitWait.
	e.kick()
	deadline := time.NewTimer(admitWait)
	defer deadline.Stop()
	for {
		e.mu.Lock()
		ch := e.freedCh
		e.mu.Unlock()

		usage := e.hot.UsedFraction()
		if usage < tier2Start {
			return nil
		}
		if usage >= tier3Start {
			return syscall.ENOSPC
		}
		select {
		case <-ch:
			continue
		case <-deadline.C:
			if e.hot.UsedFraction() < tier2Start {
				return nil
			}
			return syscall.ENOSPC
		}
	}
}

// Freed is called when bytes are released on hot (replication, eviction, GC).
func (e *Evictor) Freed() {
	e.mu.Lock()
	close(e.freedCh)
	e.freedCh = make(chan struct{})
	e.mu.Unlock()
}

// kick signals the Compactor to run replication if it isn't already.
func (e *Evictor) kick() {
	select {
	case e.triggerReplicate <- struct{}{}:
	default:
	}
}

func (e *Evictor) loop() {
	defer e.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.tick()
		}
	}
}

func (e *Evictor) tick() {
	usage := e.hot.UsedFraction()
	if usage < e.evictAt {
		return
	}
	e.dbg.log("evict tick: usage=%.2f%% (evictAt=%.0f%%, target=%.0f%%)",
		usage*100, e.evictAt*100, e.evictTo*100)
	e.kick()
	e.dropCached()
}

// dropCached drops hot fragments for files whose cold coverage is complete.
// Returns bytes freed.
func (e *Evictor) dropCached() int64 {
	if e.hot.UsedFraction() < e.evictAt {
		return 0
	}
	// Collect candidate rel paths first (read-only scan), then mutate in a
	// second transaction to avoid cursor-during-mutation pitfalls.
	var candidates []string
	err := e.meta.ViewLocked(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketInodes).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fm, err := decodeFileMeta(v)
			if err != nil {
				return err
			}
			if !fm.IsRegular() || len(fm.HotFragments) == 0 {
				continue
			}
			if !fm.HasColdCopy() {
				continue
			}
			_ = k
			candidates = append(candidates, fm.Rel)
		}
		return nil
	})
	if err != nil {
		log.Printf("place: dropCached scan: %v", err)
		return 0
	}
	var freed int64
	err = e.meta.UpdateLocked(func(tx *bolt.Tx) error {
		for _, rel := range candidates {
			if e.hot.UsedFraction() < e.evictTo {
				return nil
			}
			fm, err := GetFileTx(tx, rel)
			if err != nil {
				return err
			}
			if fm == nil || !fm.IsRegular() || len(fm.HotFragments) == 0 {
				continue
			}
			if !fm.HasColdCopy() {
				continue
			}
			dead := fm.HotFragments
			if err := AddLiveBytesTx(tx, dead, -1); err != nil {
				return err
			}
			for _, f := range dead {
				freed += f.Length
			}
			fm.HotFragments = nil
			fm.Version++
			if err := PutFileTx(tx, fm); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("place: dropCached apply: %v", err)
	}
	if freed > 0 {
		e.Freed()
		e.dbg.log("evict: freed %s of hot by dropping cached fragments", humanBytes(freed))
	} else {
		e.dbg.log("evict: nothing to drop yet (%d candidates scanned, none with full cold copy)", len(candidates))
	}
	return freed
}
