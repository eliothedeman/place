// Package mover is L3: background policy that decides which stripes to
// rewrite, in what tier, and when to GC. It talks only to L2 (the index).
// L3 never reaches into L1 segments directly — its vocabulary is
// (inode, stripe_id) and (tier).
//
// The current policy set is intentionally small:
//
//   - EvictPolicy: when hot disk usage exceeds HotMaxBytes, walk stripes
//     still on hot in (inode, stripe_id) order and Move them to cold until
//     hot drops below HotTargetBytes.
//   - GCPolicy: periodically run index.GC() to reclaim segments that no
//     fragment references.
//
// Add more policies by adding more selector functions; they all funnel into
// idx.Move and idx.GC. There's no second mover engine.
package mover

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/segment"
)

// Config controls the background mover.
type Config struct {
	Index *index.Index

	// HotMaxBytes is the soft upper bound on hot tier byte usage. When
	// exceeded, EvictPolicy runs until usage drops to HotTargetBytes.
	HotMaxBytes int64
	// HotTargetBytes is the post-eviction target. Should be < HotMaxBytes.
	HotTargetBytes int64

	// Tick is the polling interval for both eviction and GC. Default 30s.
	Tick time.Duration

	// Logger is optional; defaults to log.Printf-with-prefix. Use a no-op
	// for quiet tests.
	Logger func(format string, args ...any)
}

// Mover is the running background engine. Call Stop to shut it down.
type Mover struct {
	cfg    Config
	cancel context.CancelFunc
	wg     sync.WaitGroup
	log    func(format string, args ...any)
}

// Start launches the mover loop. Returns immediately.
func Start(cfg Config) *Mover {
	if cfg.Tick == 0 {
		cfg.Tick = 30 * time.Second
	}
	if cfg.HotTargetBytes == 0 && cfg.HotMaxBytes > 0 {
		cfg.HotTargetBytes = int64(float64(cfg.HotMaxBytes) * 0.8)
	}
	logf := cfg.Logger
	if logf == nil {
		logf = func(format string, args ...any) {
			log.Printf("mover: "+format, args...)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Mover{cfg: cfg, cancel: cancel, log: logf}
	m.wg.Add(1)
	go m.loop(ctx)
	return m
}

// Stop signals the loop to exit and waits for it.
func (m *Mover) Stop() {
	m.cancel()
	m.wg.Wait()
}

// RunOnce executes one pass of every policy synchronously. Tests call this
// directly to avoid needing a real ticker.
func (m *Mover) RunOnce() error {
	if err := m.evictOnce(); err != nil {
		return err
	}
	return m.gcOnce()
}

func (m *Mover) loop(ctx context.Context) {
	defer m.wg.Done()
	t := time.NewTicker(m.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := m.evictOnce(); err != nil {
				m.log("evict: %v", err)
			}
			if err := m.gcOnce(); err != nil {
				m.log("gc: %v", err)
			}
		}
	}
}

// evictOnce runs the hot-pressure policy: if hot usage is over the cap,
// move stripes from hot to cold (in iter order) until under the target.
func (m *Mover) evictOnce() error {
	idx := m.cfg.Index
	if m.cfg.HotMaxBytes <= 0 {
		return nil
	}
	used := idx.HotUsedBytes()
	if used <= m.cfg.HotMaxBytes {
		return nil
	}
	m.log("hot pressure: used=%d cap=%d target=%d", used, m.cfg.HotMaxBytes, m.cfg.HotTargetBytes)

	// Collect candidates first so we can release the iteration tx before
	// doing the slow Move IO. Stop collecting once we have enough bytes
	// queued to plausibly hit the target.
	type cand struct {
		inode    uint64
		stripeID uint32
		hotBytes int64
	}
	var cands []cand
	var queued int64
	want := used - m.cfg.HotTargetBytes
	err := idx.IterStripes(func(s index.StripeInfo) bool {
		if s.HotBytes == 0 {
			return true
		}
		cands = append(cands, cand{s.Inode, s.StripeID, s.HotBytes})
		queued += s.HotBytes
		return queued < want*2 // 2x headroom for shadowed overlap
	})
	if err != nil {
		return err
	}

	for _, c := range cands {
		if idx.HotUsedBytes() <= m.cfg.HotTargetBytes {
			break
		}
		if err := idx.Move(c.inode, c.stripeID, segment.TierCold); err != nil {
			m.log("move(inode=%d stripe=%d): %v", c.inode, c.stripeID, err)
			continue
		}
		m.log("moved inode=%d stripe=%d ~%d bytes", c.inode, c.stripeID, c.hotBytes)
	}
	return nil
}

// gcOnce drops segments with zero live references.
func (m *Mover) gcOnce() error {
	return m.cfg.Index.GC()
}
