// Package admin exposes a tiny HTTP surface for observability:
//
//   - /healthz : 200 if the mover loop is ticking and the index can be
//     touched cheaply; 503 otherwise.
//   - /metrics : Prometheus text-exposition of mover + index counters.
//     Hand-rolled (no client-library dependency) — the format is stable
//     enough to be a half-page of formatting code.
//
// Mount in cmd/placefs with admin.Serve(addr, deps). Set addr=""
// to disable. The server runs until ctx is canceled.
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/mover"
	"github.com/eliothedeman/place/lib/segment"
)

// Deps is the dependency bundle the admin server reads from.
type Deps struct {
	Index *index.Index
	Mover *mover.Mover
	// HealthMaxTickGap is the largest gap (now - lastTick) we'll tolerate
	// from the mover before reporting unhealthy. 0 disables this check.
	HealthMaxTickGap time.Duration
	// StartTime, used to report process_uptime_seconds and to suppress
	// the mover-tick check before the first tick has had a chance to fire.
	StartTime time.Time
}

// Serve starts an HTTP server on addr (e.g. ":9090") and returns
// immediately. Cancel ctx to shut it down gracefully.
func Serve(ctx context.Context, addr string, deps Deps) (*http.Server, error) {
	if addr == "" {
		return nil, errors.New("admin: empty listen addr")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(deps))
	mux.HandleFunc("/metrics", metricsHandler(deps))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			io.WriteString(w, "placefs admin: /healthz /metrics\n")
			return
		}
		http.NotFound(w, r)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return srv, nil
}

// readinessFlag is set true once main signals "we're ready to serve" —
// stops /healthz from reporting OK during boot. Process-global because
// there's exactly one process.
var readinessFlag atomic.Bool

// MarkReady flips /healthz to 200. Call from main after FUSE mount + any
// long migrations complete.
func MarkReady() { readinessFlag.Store(true) }

func healthHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !readinessFlag.Load() {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, "not ready\n")
			return
		}
		// Cheap liveness check: bbolt write tx open/close is the heaviest
		// thing we'd want this handler to do (and even that is excessive
		// for a polling probe). Stick to a read tx via Stat-style call:
		// SegmentIDs is a fast in-memory lookup.
		_ = deps.Index.SegmentIDs(segment.TierHot)

		// Mover-tick freshness check — only if the mover is configured and
		// we've been running long enough for the first tick to have fired.
		if deps.Mover != nil && deps.HealthMaxTickGap > 0 {
			s := deps.Mover.Stats()
			gracePeriod := deps.HealthMaxTickGap + 2*time.Second
			if !deps.StartTime.IsZero() && time.Since(deps.StartTime) > gracePeriod {
				if s.LastTickUnix == 0 {
					w.Header().Set("Content-Type", "text/plain; charset=utf-8")
					w.WriteHeader(http.StatusServiceUnavailable)
					io.WriteString(w, "mover not ticking\n")
					return
				}
				last := time.Unix(s.LastTickUnix, 0)
				if time.Since(last) > deps.HealthMaxTickGap {
					w.Header().Set("Content-Type", "text/plain; charset=utf-8")
					w.WriteHeader(http.StatusServiceUnavailable)
					fmt.Fprintf(w, "mover last tick %s ago (limit %s)\n",
						time.Since(last).Round(time.Second), deps.HealthMaxTickGap)
					return
				}
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok\n")
	}
}

func metricsHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder
		uptime := float64(0)
		if !deps.StartTime.IsZero() {
			uptime = time.Since(deps.StartTime).Seconds()
		}
		writeOne(&b, "gauge", "placefs_process_uptime_seconds",
			"Seconds since the placefs process started.",
			[]sample{{value: uptime}})
		writeOne(&b, "gauge", "placefs_hot_segment_bytes",
			"Total bytes across all hot tier segment files.",
			[]sample{{value: float64(deps.Index.HotUsedBytes())}})
		writeOne(&b, "gauge", "placefs_segment_count",
			"Number of open segment files per tier.",
			[]sample{
				{labels: `tier="hot"`, value: float64(len(deps.Index.SegmentIDs(segment.TierHot)))},
				{labels: `tier="cold"`, value: float64(len(deps.Index.SegmentIDs(segment.TierCold)))},
			})
		if deps.Mover != nil {
			s := deps.Mover.Stats()
			writeOne(&b, "counter", "placefs_mover_evict_runs_total",
				"Eviction policy invocations.",
				[]sample{{value: float64(s.EvictRuns)}})
			writeOne(&b, "counter", "placefs_mover_gc_runs_total",
				"GC policy invocations.",
				[]sample{{value: float64(s.GCRuns)}})
			writeOne(&b, "counter", "placefs_mover_moves_ok_total",
				"Successful stripe moves.",
				[]sample{{value: float64(s.MovesOK)}})
			writeOne(&b, "counter", "placefs_mover_moves_failed_total",
				"Failed stripe moves.",
				[]sample{{value: float64(s.MoveFailures)}})
			writeOne(&b, "counter", "placefs_mover_bytes_moved_total",
				"Bytes moved hot→cold.",
				[]sample{{value: float64(s.BytesMoved)}})
			writeOne(&b, "gauge", "placefs_mover_consecutive_errors",
				"Number of consecutive failing ticks (resets to 0 on a clean tick).",
				[]sample{{value: float64(s.ConsecutiveErrs)}})
			writeOne(&b, "gauge", "placefs_mover_last_tick_unix_seconds",
				"Unix timestamp of the last mover tick.",
				[]sample{{value: float64(s.LastTickUnix)}})
		}
		io.WriteString(w, b.String())
	}
}

type sample struct {
	labels string // e.g. `tier="hot"`; empty for unlabeled
	value  float64
}

// writeOne emits a single HELP/TYPE preamble followed by one line per
// sample. Prometheus rejects exposition that repeats HELP/TYPE for the
// same metric name, so all label variants of a metric go through one
// call.
func writeOne(b *strings.Builder, kind, name, help string, samples []sample) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s %s\n", name, kind)
	for _, s := range samples {
		if s.labels == "" {
			fmt.Fprintf(b, "%s %g\n", name, s.value)
		} else {
			fmt.Fprintf(b, "%s{%s} %g\n", name, s.labels, s.value)
		}
	}
}
