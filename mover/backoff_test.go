package mover

import (
	"testing"
	"time"
)

func TestBackoffDoublesUpToCap(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second
	cases := []struct {
		consec int
		want   time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 30 * time.Second}, // capped
		{20, 30 * time.Second},
	}
	for _, c := range cases {
		if got := backoff(base, max, c.consec); got != c.want {
			t.Errorf("backoff(consec=%d) = %s, want %s", c.consec, got, c.want)
		}
	}
}
