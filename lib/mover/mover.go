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
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
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

	// MaxBackoff is the longest the loop will sleep between attempts when
	// errors are coming back consecutively. Default 5 minutes. A run of
	// errors doubles the sleep up to this cap; a clean tick resets it.
	MaxBackoff time.Duration

	// Logger is the structured logger. Nil disables mover logging entirely
	// (useful in tests). All mover lines carry a "component=mover" attr,
	// added automatically.
	Logger *slog.Logger
}

// Mover is the running background engine. Call Stop to shut it down.
type Mover struct {
	cfg    Config
	cancel context.CancelFunc
	wg     sync.WaitGroup
	log    *slog.Logger

	// metrics — read with the matching accessor methods so callers don't
	// have to know the layout. Atomic so they're safe to read off-thread
	// from a /metrics handler.
	evictRuns        atomic.Int64
	gcRuns           atomic.Int64
	movesOK          atomic.Int64
	moveFailures     atomic.Int64
	bytesMoved       atomic.Int64
	consecErrs       atomic.Int64
	lastTickUnix     atomic.Int64
	lastErrUnix      atomic.Int64
	moveDurationNanos atomic.Int64
	gcDurationNanos   atomic.Int64
}

// Start launches the mover loop. Returns immediately.
func Start(cfg Config) *Mover {
	if cfg.Tick == 0 {
		cfg.Tick = 30 * time.Second
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 5 * time.Minute
	}
	if cfg.HotTargetBytes == 0 && cfg.HotMaxBytes > 0 {
		cfg.HotTargetBytes = int64(float64(cfg.HotMaxBytes) * 0.8)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	logger = logger.With("component", "mover")
	ctx, cancel := context.WithCancel(context.Background())
	m := &Mover{cfg: cfg, cancel: cancel, log: logger}
	m.wg.Add(1)
	go m.loop(ctx)
	return m
}

// Stop signals the loop to exit and waits for it. Eviction stops after the
// current candidate's Move completes — Moves are bounded by stripe size, so
// a Stop after SIGTERM normally returns in seconds, not the full Move time.
func (m *Mover) Stop() {
	m.cancel()
	m.wg.Wait()
}

// RunOnce executes one pass of every policy synchronously. Tests call this
// directly to avoid needing a real ticker.
func (m *Mover) RunOnce() error {
	if err := m.evictOnce(context.Background()); err != nil {
		return err
	}
	return m.gcOnce()
}

// Stats is a snapshot of mover counters. Cheap; safe to call frequently.
type Stats struct {
	EvictRuns         int64
	GCRuns            int64
	MovesOK           int64
	MoveFailures      int64
	BytesMoved        int64
	ConsecutiveErrs   int64
	LastTickUnix      int64 // 0 if the loop has never ticked
	LastErrorUnix     int64 // 0 if no errors observed yet
	MoveDurationNanos int64 // total wall-time spent inside successful Moves
	GCDurationNanos   int64 // total wall-time spent inside GC
}

// Stats returns a snapshot of the mover's lifetime counters.
func (m *Mover) Stats() Stats {
	return Stats{
		EvictRuns:         m.evictRuns.Load(),
		GCRuns:            m.gcRuns.Load(),
		MovesOK:           m.movesOK.Load(),
		MoveFailures:      m.moveFailures.Load(),
		BytesMoved:        m.bytesMoved.Load(),
		ConsecutiveErrs:   m.consecErrs.Load(),
		LastTickUnix:      m.lastTickUnix.Load(),
		LastErrorUnix:     m.lastErrUnix.Load(),
		MoveDurationNanos: m.moveDurationNanos.Load(),
		GCDurationNanos:   m.gcDurationNanos.Load(),
	}
}

func (m *Mover) loop(ctx context.Context) {
	defer m.wg.Done()
	delay := m.cfg.Tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		hadErr := false
		if err := m.evictOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			m.log.Error("evict failed", "err", err)
			hadErr = true
		}
		if err := m.gcOnce(); err != nil {
			m.log.Error("gc failed", "err", err)
			hadErr = true
		}
		m.lastTickUnix.Store(time.Now().Unix())
		if hadErr {
			c := m.consecErrs.Add(1)
			m.lastErrUnix.Store(time.Now().Unix())
			delay = backoff(m.cfg.Tick, m.cfg.MaxBackoff, int(c))
		} else {
			m.consecErrs.Store(0)
			delay = m.cfg.Tick
		}
	}
}

// backoff doubles tick until cap, capped by MaxBackoff. Conservative shape:
// a wedged loop sleeps progressively longer so it stops drowning the logs
// and storming the index with retries, without ever fully giving up.
func backoff(base, max time.Duration, consec int) time.Duration {
	if consec <= 0 {
		return base
	}
	d := base
	for i := 0; i < consec && d < max; i++ {
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

// evictOnce runs the hot-pressure policy: if hot usage is over the cap,
// move stripes from hot to cold (in iter order) until under the target.
// ctx-aware so a Stop() during a long candidate list breaks out promptly.
func (m *Mover) evictOnce(ctx context.Context) error {
	m.evictRuns.Add(1)
	idx := m.cfg.Index
	if m.cfg.HotMaxBytes <= 0 {
		return nil
	}
	used := idx.HotUsedBytes()
	if used <= m.cfg.HotMaxBytes {
		return nil
	}
	m.log.Info("hot pressure", "used", used, "cap", m.cfg.HotMaxBytes, "target", m.cfg.HotTargetBytes)

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
		return queued < want*2
	})
	if err != nil {
		return err
	}

	for _, c := range cands {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if idx.HotUsedBytes() <= m.cfg.HotTargetBytes {
			break
		}
		start := time.Now()
		if err := idx.Move(c.inode, c.stripeID, segment.TierCold); err != nil {
			m.moveFailures.Add(1)
			m.log.Error("move failed", "inode", c.inode, "stripe", c.stripeID, "err", err)
			continue
		}
		m.moveDurationNanos.Add(int64(time.Since(start)))
		m.movesOK.Add(1)
		m.bytesMoved.Add(c.hotBytes)
		m.log.Debug("stripe moved", "inode", c.inode, "stripe", c.stripeID, "bytes", c.hotBytes, "duration", time.Since(start))
	}
	return nil
}

// gcOnce drops segments with zero live references.
func (m *Mover) gcOnce() error {
	m.gcRuns.Add(1)
	start := time.Now()
	err := m.cfg.Index.GC()
	m.gcDurationNanos.Add(int64(time.Since(start)))
	return err
}
