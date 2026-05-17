// Package admin exposes the operator-facing HTTP surface:
//
//   - /healthz : 200 once FUSE is mounted and the mover loop is alive;
//     503 otherwise.
//   - /metrics : Prometheus text exposition of mover + index + fuse +
//     storage counters. Hand-rolled (no client-library dependency).
//   - /debug/pprof/* : standard Go runtime profiles, enabled only when
//     EnablePprof is set in Deps.
//
// Mount in cmd/placefs with admin.Serve(ctx, addr, deps). Set addr=""
// to disable. The server runs until ctx is canceled.
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eliothedeman/place/fuselayer"
	"github.com/eliothedeman/place/index"
	"github.com/eliothedeman/place/mover"
	"github.com/eliothedeman/place/segment"
)

// Deps is the dependency bundle the admin server reads from.
type Deps struct {
	Index *index.Index
	Mover *mover.Mover
	// Fuse is optional; if nil, fuse op metrics are simply omitted.
	Fuse *fuselayer.Metrics

	// HealthMaxTickGap: largest tolerated (now - lastMoverTick) before
	// /healthz reports unhealthy. 0 disables this check.
	HealthMaxTickGap time.Duration
	// StartTime: used for process_uptime_seconds and to suppress the
	// mover-tick check before the first tick has had a chance to fire.
	StartTime time.Time

	// Logger receives admin-server lifecycle and request-handling logs.
	// If nil, a discard logger is used.
	Logger *slog.Logger

	// EnablePprof mounts the net/http/pprof handlers under /debug/pprof.
	// Off by default — pprof leaks goroutine names + heap data, so leave
	// it off in production unless you're actively debugging.
	EnablePprof bool
}

// Serve starts an HTTP server on addr (e.g. ":9090") and returns
// immediately. Cancel ctx to shut it down gracefully.
func Serve(ctx context.Context, addr string, deps Deps) (*http.Server, error) {
	if addr == "" {
		return nil, errors.New("admin: empty listen addr")
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(deps))
	mux.HandleFunc("/metrics", metricsHandler(deps))
	if deps.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			io.WriteString(w, "placefs admin: /healthz /metrics")
			if deps.EnablePprof {
				io.WriteString(w, " /debug/pprof/")
			}
			io.WriteString(w, "\n")
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
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			deps.Logger.Error("admin server exited", "err", err)
		}
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
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !readinessFlag.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, "not ready\n")
			return
		}
		// Cheap liveness check: SegmentIDs is a fast in-memory snapshot,
		// which exercises the index handle without touching disk.
		_ = deps.Index.SegmentIDs(segment.TierHot)

		// Mover-tick freshness check.
		if deps.Mover != nil && deps.HealthMaxTickGap > 0 {
			s := deps.Mover.Stats()
			gracePeriod := deps.HealthMaxTickGap + 2*time.Second
			if !deps.StartTime.IsZero() && time.Since(deps.StartTime) > gracePeriod {
				if s.LastTickUnix == 0 {
					w.WriteHeader(http.StatusServiceUnavailable)
					io.WriteString(w, "mover not ticking\n")
					return
				}
				last := time.Unix(s.LastTickUnix, 0)
				if time.Since(last) > deps.HealthMaxTickGap {
					w.WriteHeader(http.StatusServiceUnavailable)
					fmt.Fprintf(w, "mover last tick %s ago (limit %s)\n",
						time.Since(last).Round(time.Second), deps.HealthMaxTickGap)
					return
				}
			}
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok\n")
	}
}

func metricsHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder

		// --- process ---
		if !deps.StartTime.IsZero() {
			writeOne(&b, "gauge", "placefs_process_uptime_seconds",
				"Seconds since the placefs process started.",
				[]sample{{value: time.Since(deps.StartTime).Seconds()}})
		}

		// --- segments ---
		writeOne(&b, "gauge", "placefs_hot_segment_bytes",
			"Total bytes across all hot tier segment files.",
			[]sample{{value: float64(deps.Index.HotUsedBytes())}})
		writeOne(&b, "gauge", "placefs_segment_count",
			"Number of open segment files per tier.",
			[]sample{
				{labels: `tier="hot"`, value: float64(len(deps.Index.SegmentIDs(segment.TierHot)))},
				{labels: `tier="cold"`, value: float64(len(deps.Index.SegmentIDs(segment.TierCold)))},
			})

		// --- disk space ---
		writeDiskGauges(&b, deps.Index.HotDir(), deps.Index.ColdDir())

		// --- pebble ---
		writePebbleStats(&b, deps.Index)

		// --- mover ---
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
			writeOne(&b, "counter", "placefs_mover_move_duration_seconds_total",
				"Total wall-time spent inside successful Moves. Divide by moves_ok for avg.",
				[]sample{{value: float64(s.MoveDurationNanos) / 1e9}})
			writeOne(&b, "counter", "placefs_mover_gc_duration_seconds_total",
				"Total wall-time spent inside GC. Divide by gc_runs for avg.",
				[]sample{{value: float64(s.GCDurationNanos) / 1e9}})
			writeOne(&b, "gauge", "placefs_mover_consecutive_errors",
				"Number of consecutive failing ticks (resets to 0 on a clean tick).",
				[]sample{{value: float64(s.ConsecutiveErrs)}})
			writeOne(&b, "gauge", "placefs_mover_last_tick_unix_seconds",
				"Unix timestamp of the last mover tick.",
				[]sample{{value: float64(s.LastTickUnix)}})
		}

		// --- FUSE ops ---
		if deps.Fuse != nil {
			ops := deps.Fuse.Snapshot()
			if len(ops) > 0 {
				callsSamples := make([]sample, 0, len(ops))
				errSamples := make([]sample, 0, len(ops))
				durSamples := make([]sample, 0, len(ops))
				for _, op := range ops {
					lbl := fmt.Sprintf(`op=%q`, op.Op)
					callsSamples = append(callsSamples, sample{labels: lbl, value: float64(op.Calls)})
					errSamples = append(errSamples, sample{labels: lbl, value: float64(op.Errors)})
					durSamples = append(durSamples, sample{labels: lbl, value: float64(op.DurationNanos) / 1e9})
				}
				writeOne(&b, "counter", "placefs_fuse_op_calls_total",
					"FUSE entrypoint invocations by op name.",
					callsSamples)
				writeOne(&b, "counter", "placefs_fuse_op_errors_total",
					"FUSE entrypoint errors by op name.",
					errSamples)
				writeOne(&b, "counter", "placefs_fuse_op_duration_seconds_total",
					"FUSE entrypoint total wall-time by op name.",
					durSamples)
			}
		}

		io.WriteString(w, b.String())
	}
}

// writeDiskGauges emits per-tier filesystem free/total bytes using statfs.
func writeDiskGauges(b *strings.Builder, hotDir, coldDir string) {
	type entry struct {
		label string
		dir   string
	}
	var totalSamples, availSamples []sample
	for _, e := range []entry{{"hot", hotDir}, {"cold", coldDir}} {
		var st syscall.Statfs_t
		if err := syscall.Statfs(e.dir, &st); err != nil {
			continue
		}
		total := float64(st.Blocks) * float64(st.Bsize)
		avail := float64(st.Bavail) * float64(st.Bsize)
		totalSamples = append(totalSamples, sample{labels: fmt.Sprintf(`tier=%q`, e.label), value: total})
		availSamples = append(availSamples, sample{labels: fmt.Sprintf(`tier=%q`, e.label), value: avail})
	}
	if len(totalSamples) > 0 {
		writeOne(b, "gauge", "placefs_disk_total_bytes",
			"Total capacity of the underlying filesystem hosting each tier.",
			totalSamples)
		writeOne(b, "gauge", "placefs_disk_avail_bytes",
			"Available (unprivileged-writable) bytes on the underlying filesystem.",
			availSamples)
	}
}

// writePebbleStats emits pebble DB metrics: WAL byte counters, sstable
// counts per level, compaction activity, block-cache hits/misses. Pebble
// surfaces all of this via DB.Metrics(); we cherry-pick the fields most
// useful for "is the LSM keeping up with the write rate."
func writePebbleStats(b *strings.Builder, idx *index.Index) {
	m := idx.DB().Pebble().Metrics()

	// WAL — single sequential file, fsync'd on every Sync commit. Most
	// of the placefs write-path cost lands here.
	writeOne(b, "counter", "placefs_pebble_wal_bytes_written_total",
		"Total bytes written to the pebble WAL.",
		[]sample{{value: float64(m.WAL.BytesWritten)}})
	writeOne(b, "gauge", "placefs_pebble_wal_files",
		"Number of WAL files on disk (active + obsolete).",
		[]sample{{value: float64(m.WAL.Files + m.WAL.ObsoleteFiles)}})
	writeOne(b, "gauge", "placefs_pebble_wal_size_bytes",
		"On-disk size of the active WAL file.",
		[]sample{{value: float64(m.WAL.Size)}})

	// Compaction — if Count is climbing while EstimatedDebt grows, the
	// LSM is falling behind and writes will start stalling.
	writeOne(b, "counter", "placefs_pebble_compactions_total",
		"Total compactions run.",
		[]sample{{value: float64(m.Compact.Count)}})
	writeOne(b, "gauge", "placefs_pebble_compact_debt_bytes",
		"Pebble's estimate of compaction work remaining.",
		[]sample{{value: float64(m.Compact.EstimatedDebt)}})
	writeOne(b, "gauge", "placefs_pebble_compact_in_progress",
		"Compactions currently running.",
		[]sample{{value: float64(m.Compact.NumInProgress)}})

	// Flush — memtable → L0. Slow flushes cause back-pressure on writes.
	writeOne(b, "counter", "placefs_pebble_flushes_total",
		"Total memtable flushes.",
		[]sample{{value: float64(m.Flush.Count)}})

	// Per-level table counts. Healthy LSM keeps L0 shallow (≤4 files).
	levelSizeSamples := make([]sample, 0, 7)
	levelFilesSamples := make([]sample, 0, 7)
	for i, l := range m.Levels {
		lbl := fmt.Sprintf(`level="%d"`, i)
		levelSizeSamples = append(levelSizeSamples, sample{labels: lbl, value: float64(l.Size)})
		levelFilesSamples = append(levelFilesSamples, sample{labels: lbl, value: float64(l.NumFiles)})
	}
	writeOne(b, "gauge", "placefs_pebble_level_bytes",
		"Bytes per LSM level.",
		levelSizeSamples)
	writeOne(b, "gauge", "placefs_pebble_level_files",
		"sstable files per LSM level.",
		levelFilesSamples)

	// Block cache — cheap to surface and tells you how often metadata
	// reads actually hit disk.
	writeOne(b, "counter", "placefs_pebble_block_cache_hits_total",
		"Block-cache hits.",
		[]sample{{value: float64(m.BlockCache.Hits)}})
	writeOne(b, "counter", "placefs_pebble_block_cache_misses_total",
		"Block-cache misses.",
		[]sample{{value: float64(m.BlockCache.Misses)}})
	writeOne(b, "gauge", "placefs_pebble_block_cache_size_bytes",
		"Bytes currently held in the block cache.",
		[]sample{{value: float64(m.BlockCache.Size)}})
}

type sample struct {
	labels string // e.g. `tier="hot"`; empty for unlabeled
	value  float64
}

// writeOne emits one HELP/TYPE preamble plus one line per sample.
// Prometheus rejects exposition that repeats HELP/TYPE for the same
// metric name, so all label variants must go through one call.
func writeOne(b *strings.Builder, kind, name, help string, samples []sample) {
	if len(samples) == 0 {
		return
	}
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
