package admin

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/mover"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func newIndex(t *testing.T) *index.Index {
	t.Helper()
	dir := t.TempDir()
	idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 16, SegmentMaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })
	return idx
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func waitServerUp(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("admin server didn't come up at %s", url)
}

func TestHealthzReadinessFlow(t *testing.T) {
	idx := newIndex(t)
	addr := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readinessFlag.Store(false) // reset for this test
	_, err := Serve(ctx, addr, Deps{Index: idx, StartTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	waitServerUp(t, "http://"+addr+"/healthz")

	code, body := get(t, "http://"+addr+"/healthz")
	if code != http.StatusServiceUnavailable {
		t.Errorf("pre-Ready code=%d body=%q (want 503)", code, body)
	}
	MarkReady()
	code, _ = get(t, "http://"+addr+"/healthz")
	if code != http.StatusOK {
		t.Errorf("post-Ready code=%d (want 200)", code)
	}
}

func TestHealthzFailsIfMoverStale(t *testing.T) {
	idx := newIndex(t)
	mv := mover.Start(mover.Config{Index: idx, Tick: time.Hour, Logger: func(string, ...any) {}})
	defer mv.Stop()

	addr := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readinessFlag.Store(true)
	// HealthMaxTickGap is tiny; StartTime far in the past so the
	// grace-period guard doesn't suppress the check. We never call
	// mv.RunOnce, so LastTickUnix stays 0 → 503.
	_, err := Serve(ctx, addr, Deps{
		Index:            idx,
		Mover:            mv,
		HealthMaxTickGap: 1 * time.Millisecond,
		StartTime:        time.Now().Add(-1 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitServerUp(t, "http://"+addr+"/healthz")
	code, body := get(t, "http://"+addr+"/healthz")
	if code != http.StatusServiceUnavailable {
		t.Errorf("stale mover: code=%d body=%q (want 503)", code, body)
	}
	if !strings.Contains(body, "mover") {
		t.Errorf("expected body to mention mover, got %q", body)
	}
}

func TestMetricsExposition(t *testing.T) {
	idx := newIndex(t)
	mv := mover.Start(mover.Config{Index: idx, Tick: time.Hour, Logger: func(string, ...any) {}})
	defer mv.Stop()

	addr := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readinessFlag.Store(true)
	_, err := Serve(ctx, addr, Deps{
		Index:     idx,
		Mover:     mv,
		StartTime: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitServerUp(t, "http://"+addr+"/healthz")

	code, body := get(t, "http://"+addr+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics code=%d", code)
	}
	wantHints := []string{
		"placefs_process_uptime_seconds",
		"placefs_hot_segment_bytes",
		`placefs_segment_count{tier="hot"}`,
		`placefs_segment_count{tier="cold"}`,
		"placefs_mover_evict_runs_total",
		"placefs_mover_moves_ok_total",
		"# TYPE placefs_segment_count gauge",
	}
	for _, w := range wantHints {
		if !strings.Contains(body, w) {
			t.Errorf("metrics body missing %q\n--- body ---\n%s", w, body)
		}
	}

	// Sanity: HELP/TYPE lines aren't doubled-up for the labelled metric.
	if strings.Count(body, "# TYPE placefs_segment_count") != 1 {
		t.Errorf("expected exactly one TYPE line for placefs_segment_count, got:\n%s", body)
	}
}
