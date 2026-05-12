package place

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every Prometheus collector exposed by a Mount. One per
// place.Server; constructed in Mount() and threaded into Writer / Compactor /
// Evictor / placeRoot so hot paths can bump counters without going through a
// global.
//
// All collectors live in a private registry — we don't touch
// prometheus.DefaultRegisterer, so a process that mounts more than one place
// (or that also exports its own /metrics) can't collide with us.
type Metrics struct {
	reg *prometheus.Registry

	// Reader counters. ReadPartial is the signal that catches silent
	// short-reads (the file.go:64 path) — the kernel sees success but a
	// fragment failed underneath. Spike => corruption.
	ReadBytes   prometheus.Counter
	ReadPartial prometheus.Counter
	ReadErrors  prometheus.Counter

	// Writer counters. WriteBytes is the input side of the amplification
	// equation; ReplicateBytes is the output side.
	WriteBytes  prometheus.Counter
	WriteErrors prometheus.Counter

	// Compactor / replicate counters. ReplicatePasses is labelled by reason
	// (pressure | time | bytes) so you can tell what's driving cold I/O.
	ReplicatePasses      *prometheus.CounterVec
	ReplicateFiles       prometheus.Counter
	ReplicateBytes       prometheus.Counter
	ReplicateOrphanBytes prometheus.Counter
	ReplicateErrors      prometheus.Counter

	// GC counters per tier ("hot" | "cold").
	GCPasses           *prometheus.CounterVec
	GCSegmentsRemoved  *prometheus.CounterVec
	GCForwardCompacts  *prometheus.CounterVec
	GCBytesReclaimed   *prometheus.CounterVec

	// Evictor counters.
	EvictBytesDropped prometheus.Counter
	EvictAdmitBlocked prometheus.Counter

	// Storage gauges (sampled by the gauge loop).
	HotUsedFraction  prometheus.Gauge
	Segments         *prometheus.GaugeVec // {tier}
	SegmentTotalBytes *prometheus.GaugeVec // {tier}
	SegmentLiveBytes  *prometheus.GaugeVec // {tier}
	Inodes           prometheus.Gauge

	// Audit gauge — populated by the periodic in-mount audit when enabled.
	AuditFindings *prometheus.GaugeVec // {kind}
}

// NewMetrics constructs and registers a fresh set of collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	// Include go runtime + process collectors so a single /metrics scrape
	// gives you GC pauses, goroutine count, RSS, fd count — the obvious
	// things you'd want when debugging "why is place slow / leaking".
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &Metrics{reg: reg}

	c := func(name, help string) prometheus.Counter {
		x := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		reg.MustRegister(x)
		return x
	}
	cv := func(name, help string, labels ...string) *prometheus.CounterVec {
		x := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		reg.MustRegister(x)
		return x
	}
	g := func(name, help string) prometheus.Gauge {
		x := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		reg.MustRegister(x)
		return x
	}
	gv := func(name, help string, labels ...string) *prometheus.GaugeVec {
		x := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		reg.MustRegister(x)
		return x
	}

	m.ReadBytes = c("place_read_bytes_total", "User bytes returned to FUSE Read.")
	m.ReadPartial = c("place_read_partial_total", "FUSE reads that returned a short result because at least one underlying slice failed. The kernel sees success and may retry at off+n; spikes signal silent fragment-vs-segment corruption.")
	m.ReadErrors = c("place_read_errors_total", "FUSE reads that returned a hard error to the kernel (n=0).")

	m.WriteBytes = c("place_write_bytes_total", "User bytes accepted by FUSE Write.")
	m.WriteErrors = c("place_write_errors_total", "FUSE writes that returned an error.")

	m.ReplicatePasses = cv("place_replicate_passes_total", "Replicate passes by trigger reason.", "reason")
	m.ReplicateFiles = c("place_replicate_files_total", "Files successfully replicated hot->cold.")
	m.ReplicateBytes = c("place_replicate_bytes_total", "Bytes written to cold during replication (includes zero-fill of sparse holes — this is the amplification metric).")
	m.ReplicateOrphanBytes = c("place_replicate_orphan_bytes_total", "Cold bytes appended during a version-changed-mid-flight race; left as dead space until GC reclaims them.")
	m.ReplicateErrors = c("place_replicate_errors_total", "Replicate passes that failed for an inode.")

	m.GCPasses = cv("place_gc_passes_total", "GC passes run.", "tier")
	m.GCSegmentsRemoved = cv("place_gc_segments_removed_total", "Segments unlinked by GC (Live==0).", "tier")
	m.GCForwardCompacts = cv("place_gc_forward_compacts_total", "Forward-compaction operations (partially-dead segment merge).", "tier")
	m.GCBytesReclaimed = cv("place_gc_bytes_reclaimed_total", "Bytes reclaimed by GC (segments removed + forward-compacted).", "tier")

	m.EvictBytesDropped = c("place_evict_bytes_dropped_total", "Bytes of hot Fragments dropped because a complete cold copy exists.")
	m.EvictAdmitBlocked = c("place_evict_admit_blocked_total", "Writes blocked at Admit because hot is over EvictAt and full.")

	m.HotUsedFraction = g("place_hot_used_fraction", "Hot storage used fraction in [0,1].")
	m.Segments = gv("place_segments", "Segment count per tier.", "tier")
	m.SegmentTotalBytes = gv("place_segment_total_bytes", "Sum of SegmentMeta.Total per tier (framed bytes on disk).", "tier")
	m.SegmentLiveBytes = gv("place_segment_live_bytes", "Sum of SegmentMeta.Live per tier (referenced payload bytes).", "tier")
	m.Inodes = g("place_inodes", "Total inodes in the meta DB.")

	m.AuditFindings = gv("place_audit_findings", "Findings from the most recent periodic audit, by kind. Zero is healthy.", "kind")
	// Pre-zero so scrapes have all label series visible from boot.
	m.AuditFindings.WithLabelValues("missing-segment").Set(0)
	m.AuditFindings.WithLabelValues("past-eof").Set(0)

	return m
}

// Handler returns the http.Handler that serves the Prometheus exposition
// format for this Metrics' registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// tierLabel maps a Tier to the prometheus label value used on every
// {tier}-labelled metric.
func tierLabel(t Tier) string {
	if t == TierCold {
		return "cold"
	}
	return "hot"
}
