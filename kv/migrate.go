package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

// MigrateFromBolt copies every bucket/key/value in the legacy bbolt DB at
// boltPath into the pebble DB at peblePath. After a successful migration
// the bbolt file is renamed to boltPath + ".migrated" so the next start
// no-ops. If pebblePath already contains a non-empty DB the call returns
// ErrAlreadyMigrated without touching anything.
//
// The migration is one-shot, single-threaded, and runs before any normal
// writes — no locking concerns. Spans are not emitted; we log progress
// to stderr via the caller's logger if they pass one.
//
// Mapping (legacy bbolt → kv keyspace):
//
//	bucket "_l4_meta" key "next_inode" → StoreNextInodeKey
//	bucket "nodes"   key u64(inode)    → NodeKey(inode)
//	bucket "dirents" key u64(parent)+name → DirentKey(parent, name)
//	bucket "stripes" key u64(inode)+u32(stripe) → StripeKey(inode, stripe)
//	bucket "_meta"   (legacy, always empty)    → skipped
//
// Callers should invoke MigrateFromBolt before kv.Open on the pebble path.
func MigrateFromBolt(boltPath, pebblePath string, progress func(bucket string, n int)) error {
	if _, err := os.Stat(boltPath); errors.Is(err, os.ErrNotExist) {
		return nil // nothing to do
	} else if err != nil {
		return err
	}
	// If a populated pebble DB already exists at pebblePath, refuse to
	// migrate on top of it. Detection: any entry in the directory other
	// than the empty-DB marker. Pebble creates a non-empty dir on Open,
	// so we use os.ReadDir.
	if entries, err := os.ReadDir(pebblePath); err == nil && len(entries) > 0 {
		// Sanity check: is this an empty-shell directory (mkdirall but
		// never opened) or a real pebble DB? An open pebble has at least
		// MANIFEST-* + a CURRENT file. The simplest signal is "CURRENT
		// exists" — that's the file Pebble writes on first open.
		if _, err := os.Stat(filepath.Join(pebblePath, "CURRENT")); err == nil {
			return ErrAlreadyMigrated
		}
	}

	src, err := bolt.Open(boltPath, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("kv migrate: open legacy %q: %w", boltPath, err)
	}
	defer src.Close()

	dst, err := Open(pebblePath)
	if err != nil {
		return fmt.Errorf("kv migrate: open pebble %q: %w", pebblePath, err)
	}
	defer dst.Close()

	if err := src.View(func(tx *bolt.Tx) error {
		// _l4_meta/next_inode → StoreNextInodeKey
		if b := tx.Bucket([]byte("_l4_meta")); b != nil {
			if v := b.Get([]byte("next_inode")); v != nil {
				batch := dst.NewBatch()
				if err := batch.Set(StoreNextInodeKey, v); err != nil {
					batch.Close()
					return err
				}
				if err := batch.Commit(true); err != nil {
					return err
				}
				if progress != nil {
					progress("_l4_meta", 1)
				}
			}
		}
		// nodes/<inode>
		if err := copyBucket(tx, []byte("nodes"), dst, progress, func(k []byte) ([]byte, error) {
			if len(k) != 8 {
				return nil, fmt.Errorf("kv migrate: nodes key length %d", len(k))
			}
			return NodeKey(binary.BigEndian.Uint64(k)), nil
		}); err != nil {
			return err
		}
		// dirents/<parent><name>
		if err := copyBucket(tx, []byte("dirents"), dst, progress, func(k []byte) ([]byte, error) {
			if len(k) < 8 {
				return nil, fmt.Errorf("kv migrate: dirents key length %d", len(k))
			}
			return DirentKey(binary.BigEndian.Uint64(k[:8]), string(k[8:])), nil
		}); err != nil {
			return err
		}
		// stripes/<inode><stripe>
		if err := copyBucket(tx, []byte("stripes"), dst, progress, func(k []byte) ([]byte, error) {
			if len(k) != 12 {
				return nil, fmt.Errorf("kv migrate: stripes key length %d", len(k))
			}
			// Land the legacy blob under the same legacy key shape;
			// the per-fragment migration in index.Open will split it
			// out into per-fragment keys on first start.
			return LegacyStripeKey(
				binary.BigEndian.Uint64(k[:8]),
				binary.BigEndian.Uint32(k[8:]),
			), nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Close the legacy DB before renaming so Windows-clients work too,
	// though placefs runs on Linux.
	if err := src.Close(); err != nil {
		return err
	}
	if err := os.Rename(boltPath, boltPath+".migrated"); err != nil {
		return fmt.Errorf("kv migrate: rename legacy db: %w", err)
	}
	return nil
}

// ErrAlreadyMigrated signals that the destination pebble DB is non-empty,
// so the migration would clobber existing data. Treat as "nothing to do."
var ErrAlreadyMigrated = errors.New("kv migrate: destination pebble DB already populated")

// copyBucket walks every (k, v) in bucketName and writes (mapKey(k), v) into
// dst. Done in batches of batchSize keys so memory stays bounded on multi-GB
// metadata stores.
func copyBucket(tx *bolt.Tx, bucketName []byte, dst *DB, progress func(string, int), mapKey func([]byte) ([]byte, error)) error {
	b := tx.Bucket(bucketName)
	if b == nil {
		return nil
	}
	const batchSize = 1024
	batch := dst.NewBatch()
	count := 0
	flush := func() error {
		if err := batch.Commit(true); err != nil {
			return err
		}
		batch = dst.NewBatch()
		return nil
	}
	err := b.ForEach(func(k, v []byte) error {
		dk, err := mapKey(k)
		if err != nil {
			return err
		}
		// Pebble's batch buffers slices internally, but the bbolt k/v
		// are scoped to the iteration so we copy v explicitly. Pebble's
		// docs say it copies in Set, but being defensive here is cheap.
		vc := make([]byte, len(v))
		copy(vc, v)
		if err := batch.Set(dk, vc); err != nil {
			return err
		}
		count++
		if count%batchSize == 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(true); err != nil {
		return err
	}
	if progress != nil {
		progress(string(bucketName), count)
	}
	return nil
}
