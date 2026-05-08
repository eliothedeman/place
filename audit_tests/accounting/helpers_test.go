package audit_tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eliothedeman/place"
)

// dirs returns ready-to-use {hot, cold} dirs under t.TempDir(). The DB lives
// at hot/.place/meta.db; segments at hot/.place/segments and cold/.place/segments.
func dirs(t *testing.T) (hot, cold string) {
	t.Helper()
	root := t.TempDir()
	hot = filepath.Join(root, "hot")
	cold = filepath.Join(root, "cold")
	if err := os.MkdirAll(filepath.Join(hot, ".place"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cold, 0700); err != nil {
		t.Fatal(err)
	}
	return hot, cold
}

func dbPath(hot string) string { return filepath.Join(hot, ".place", "meta.db") }

func openMeta(t *testing.T, hot string) *place.Meta {
	t.Helper()
	m, err := place.NewMeta(dbPath(hot))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func newHot(t *testing.T, hot string, segSize int64) *place.SegmentSet {
	t.Helper()
	if segSize == 0 {
		segSize = 256 << 20
	}
	s, err := place.NewSegmentSet(place.TierHot, hot, segSize)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newCold(t *testing.T, cold string, segSize int64) *place.SegmentSet {
	t.Helper()
	if segSize == 0 {
		segSize = 256 << 20
	}
	s, err := place.NewSegmentSet(place.TierCold, cold, segSize)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
