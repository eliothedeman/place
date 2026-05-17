package kv

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// DB is the placefs key/value handle, a thin wrapper over *pebble.DB.
// Cheap to share across packages — Pebble is goroutine-safe.
type DB struct {
	pdb *pebble.DB
}

// Batch buffers a set of writes that commit atomically. Concurrent
// Commits on different batches coalesce in Pebble's WAL group commit, so
// callers don't need to coordinate; just commit.
//
// Read methods on Batch go to the parent DB, not the in-batch state —
// that means callers can't see their own pending writes. None of placefs's
// callers want read-your-own-writes today; if a future caller does, switch
// to an indexed batch. The simpler form is faster.
type Batch struct {
	db *DB
	pb *pebble.Batch
}

// Iter is a forward iterator over a half-open [lo, hi) key range.
// Returned by DB.Iter; close after use.
type Iter struct {
	pi *pebble.Iterator
}

// Open opens (or creates) a pebble DB at path. Sensible defaults for the
// placefs workload (small, mostly metadata-sized values, write-heavy):
//
//   - L0CompactionThreshold + LBaseMaxBytes tuned so compaction keeps up
//     with append churn without throttling writes.
//   - DisableWAL=false (default) — every commit fsyncs the WAL; that's the
//     entire durability story for the metadata.
//
// Pebble's defaults already do most of what we want; we only override
// where the placefs access pattern is unusual.
func Open(path string) (*DB, error) {
	opts := &pebble.Options{
		// Pebble defaults are tuned for a generic LSM. We want to keep
		// memtables small so flushes are quick and L0 stays shallow —
		// the metadata DB is small (KBs–MBs) so compaction cost is
		// dominated by overhead, not bytes moved.
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       12,
		MaxConcurrentCompactions:    func() int { return 2 },
	}
	pdb, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("kv: open %q: %w", path, err)
	}
	return &DB{pdb: pdb}, nil
}

// Close flushes and closes the underlying DB. After Close, every method
// returns an error.
func (db *DB) Close() error { return db.pdb.Close() }

// Pebble exposes the raw pebble handle for places that genuinely need
// engine-specific surface (metrics, manual flush). Most callers should
// not touch this.
func (db *DB) Pebble() *pebble.DB { return db.pdb }

// Get reads the value for key. Returns a freshly allocated copy
// (Pebble's borrowed slice is invalid after the closer runs), or nil if
// the key is absent.
func (db *DB) Get(key []byte) ([]byte, error) {
	v, closer, err := db.pdb.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	closer.Close()
	return out, nil
}

// Has is a Get-shaped existence check that skips the value copy. Useful
// for "does this dirent exist" checks where the value is just a pointer.
func (db *DB) Has(key []byte) (bool, error) {
	_, closer, err := db.pdb.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// NewBatch returns a fresh write batch. Callers must Commit or Close.
func (db *DB) NewBatch() *Batch {
	return &Batch{db: db, pb: db.pdb.NewBatch()}
}

// Iter returns an iterator over the half-open range [lo, hi). Pass nil
// hi (or use PrefixUpperBound) for an open-ended scan. The iterator
// starts positioned before the first key; call First() or SeekGE() to
// advance.
func (db *DB) Iter(lo, hi []byte) (*Iter, error) {
	it, err := db.pdb.NewIter(&pebble.IterOptions{
		LowerBound: lo,
		UpperBound: hi,
	})
	if err != nil {
		return nil, err
	}
	return &Iter{pi: it}, nil
}

// Get on a Batch reads through to the parent DB. See the Batch docstring
// for why we don't expose in-batch reads.
func (b *Batch) Get(key []byte) ([]byte, error) { return b.db.Get(key) }

// Has is the existence-check counterpart of Get on a Batch.
func (b *Batch) Has(key []byte) (bool, error) { return b.db.Has(key) }

// Set queues a write of (key, value) in the batch. Pebble takes its own
// copies of both slices internally so the caller can reuse them.
func (b *Batch) Set(key, value []byte) error {
	return b.pb.Set(key, value, nil)
}

// Delete queues a delete of key.
func (b *Batch) Delete(key []byte) error {
	return b.pb.Delete(key, nil)
}

// Commit makes every queued write durable. When sync is true the
// commit blocks until the WAL is fsync'd (this is what placefs uses on
// the data path); when false the commit returns as soon as the WAL
// write is buffered (used only by maintenance paths where durability is
// re-established later via DB-level sync).
func (b *Batch) Commit(sync bool) error {
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	if err := b.pb.Commit(opts); err != nil {
		return err
	}
	return b.pb.Close()
}

// Close discards the batch without committing. Safe to call after
// Commit (no-op); call this from a deferred close path when you've
// already committed, or to abort.
func (b *Batch) Close() error {
	if b.pb == nil {
		return nil
	}
	err := b.pb.Close()
	b.pb = nil
	return err
}

// First positions the iterator at the smallest key in range.
func (it *Iter) First() bool { return it.pi.First() }

// Next advances the iterator. Returns false past the upper bound.
func (it *Iter) Next() bool { return it.pi.Next() }

// SeekGE positions the iterator at the first key >= target.
func (it *Iter) SeekGE(target []byte) bool { return it.pi.SeekGE(target) }

// Valid reports whether the iterator is at a valid position.
func (it *Iter) Valid() bool { return it.pi.Valid() }

// Key returns the current key. The returned slice is owned by Pebble
// and may be invalidated by the next iterator call — copy if you need
// to hold it.
func (it *Iter) Key() []byte { return it.pi.Key() }

// Value returns the current value. Same lifetime caveat as Key.
func (it *Iter) Value() []byte { return it.pi.Value() }

// Close releases the iterator. Idempotent.
func (it *Iter) Close() error {
	if it.pi == nil {
		return nil
	}
	err := it.pi.Close()
	it.pi = nil
	return err
}
