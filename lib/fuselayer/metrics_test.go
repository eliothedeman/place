package fuselayer

import (
	"testing"
	"time"
)

func TestMetricsCountsAndDurations(t *testing.T) {
	m := NewMetrics()
	m.Observe("read", 100*time.Microsecond, false)
	m.Observe("read", 200*time.Microsecond, false)
	m.Observe("read", 50*time.Microsecond, true)
	m.Observe("write", 1*time.Millisecond, false)

	got := map[string]OpStat{}
	for _, s := range m.Snapshot() {
		got[s.Op] = s
	}
	if got["read"].Calls != 3 {
		t.Errorf("read calls = %d, want 3", got["read"].Calls)
	}
	if got["read"].Errors != 1 {
		t.Errorf("read errors = %d, want 1", got["read"].Errors)
	}
	wantReadDur := int64(350 * time.Microsecond)
	if got["read"].DurationNanos != wantReadDur {
		t.Errorf("read duration = %d ns, want %d ns", got["read"].DurationNanos, wantReadDur)
	}
	if got["write"].Calls != 1 {
		t.Errorf("write calls = %d, want 1", got["write"].Calls)
	}
}

func TestSnapshotIsSortedByOpName(t *testing.T) {
	m := NewMetrics()
	for _, op := range []string{"zeta", "alpha", "mid"} {
		m.Observe(op, 0, false)
	}
	snap := m.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snap len %d", len(snap))
	}
	if snap[0].Op != "alpha" || snap[1].Op != "mid" || snap[2].Op != "zeta" {
		t.Errorf("snapshot not sorted: %+v", snap)
	}
}
