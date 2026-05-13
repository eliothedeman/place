package fuselayer

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is the FUSE-layer observability bag: per-op counts, per-op
// error counts, and per-op total-duration (nanoseconds). Methods are
// safe to call from many goroutines; per-op rows are created lazily on
// first reference. Zero value is unusable — call NewMetrics.
type Metrics struct {
	mu  sync.RWMutex
	ops map[string]*opRow
}

type opRow struct {
	calls    atomic.Int64
	errors   atomic.Int64
	duration atomic.Int64 // nanoseconds
}

// NewMetrics returns an empty Metrics.
func NewMetrics() *Metrics {
	return &Metrics{ops: map[string]*opRow{}}
}

func (m *Metrics) row(op string) *opRow {
	m.mu.RLock()
	r, ok := m.ops[op]
	m.mu.RUnlock()
	if ok {
		return r
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok = m.ops[op]; ok {
		return r
	}
	r = &opRow{}
	m.ops[op] = r
	return r
}

// Observe records one finished op. If isErr is true, the error counter
// is bumped too. d is the wall-clock duration of the op. Public so
// admin tests (and any out-of-package middleware) can stamp synthetic
// samples; the FUSE adapter calls it after each entrypoint returns.
func (m *Metrics) Observe(op string, d time.Duration, isErr bool) {
	r := m.row(op)
	r.calls.Add(1)
	r.duration.Add(int64(d))
	if isErr {
		r.errors.Add(1)
	}
}

// OpStat is a single op's snapshot.
type OpStat struct {
	Op             string
	Calls          int64
	Errors         int64
	DurationNanos  int64
}

// Snapshot returns the current per-op state sorted by op name. Cheap to
// call; copies counters only.
func (m *Metrics) Snapshot() []OpStat {
	m.mu.RLock()
	out := make([]OpStat, 0, len(m.ops))
	for name, r := range m.ops {
		out = append(out, OpStat{
			Op:            name,
			Calls:         r.calls.Load(),
			Errors:        r.errors.Load(),
			DurationNanos: r.duration.Load(),
		})
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Op < out[j].Op })
	return out
}
