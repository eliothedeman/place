// Package audit_tests holds crash-recovery tests for `place`. Each test
// simulates a hostile shutdown by avoiding Close()/Flush() at the chosen
// moment, then re-opening the on-disk state to observe the result.
//
// These tests intentionally use the public API surface of `place` only.
package audit_tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eliothedeman/place"
	bolt "go.etcd.io/bbolt"
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

// listSegFiles returns the *.seg basenames sorted for stable comparison.
func listSegFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, ".place", "segments"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// readSegMeta opens the bbolt db and returns the SegmentMeta for (tier, id),
// or nil if absent. Used to peek at recovery state without going through
// the overlay.
func readSegMeta(t *testing.T, hot string, tier place.Tier, id uint32) *place.SegmentMeta {
	t.Helper()
	db, err := bolt.Open(dbPath(hot), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var sm *place.SegmentMeta
	err = db.View(func(tx *bolt.Tx) error {
		got, e := place.GetSegmentTx(tx, tier, id)
		sm = got
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

// readFile returns the FileMeta for rel, or nil.
func readFile(t *testing.T, m *place.Meta, rel string) *place.FileMeta {
	t.Helper()
	fm, err := m.GetFile(rel)
	if err != nil {
		t.Fatal(err)
	}
	return fm
}

// snapshotDB copies the bbolt file as it currently exists on disk.
// Use AFTER Meta.Flush() to capture a known-durable point. Restore via
// restoreDB to model "power loss after this point" — bbolt unsynced
// writes are effectively discarded.
func snapshotDB(t *testing.T, hot string) string {
	t.Helper()
	src := dbPath(hot)
	dst := src + ".snap"
	in, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, in, 0600); err != nil {
		t.Fatal(err)
	}
	return dst
}

// restoreDB overwrites the live db file with the snapshot. Caller must
// ensure no live bbolt handle is open against the live db.
func restoreDB(t *testing.T, snap, hot string) {
	t.Helper()
	in, err := os.ReadFile(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath(hot), in, 0600); err != nil {
		t.Fatal(err)
	}
}


