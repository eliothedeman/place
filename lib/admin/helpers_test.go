package admin

import (
	"time"

	"github.com/eliothedeman/place/lib/fuselayer"
)

// stampOps records a small canned set of op observations so /metrics
// assertions can run without spinning up a real FUSE mount.
func stampOps(m *fuselayer.Metrics) {
	m.Observe("read", 250*time.Microsecond, false)
	m.Observe("read", 1*time.Millisecond, true)
	m.Observe("write", 5*time.Millisecond, false)
	m.Observe("lookup", 10*time.Microsecond, false)
}
