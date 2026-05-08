package place

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// currentSchemaVersion is the version this binary writes and expects to read.
//
//	v0 — original layout: bucketFiles only, FileMeta encoding v1
//	v1 — paths/inodes split: bucketPaths + bucketInodes + bucketMeta,
//	     FileMeta encoding v2 (adds Nlink, defaulted to 1 on migrated rows)
const currentSchemaVersion uint32 = 1

// migration is one ordered upgrade step. apply runs inside a single
// db.Update tx — the user's design choice was "everything in one
// transaction, even for many thousands of files." Bbolt commits the whole
// tx atomically with one fsync at the end; if we crash mid-way, bbolt
// rolls back and the next start sees the prior schema version, so the
// migration is retried from scratch.
type migration struct {
	to    uint32
	name  string
	apply func(tx *bolt.Tx) error

	// dryRun, if non-nil, prints a one-line summary of what apply would do.
	// Cheap walk that doesn't mutate.
	dryRun func(tx *bolt.Tx) (string, error)
}

var migrations = []migration{
	{to: 1, name: "split-files-into-paths-and-inodes", apply: migrateV0ToV1, dryRun: dryRunV0ToV1},
}

// readSchemaVersion returns the on-disk schema version. Missing key = 0.
func readSchemaVersion(db *bolt.DB) (uint32, error) {
	var v uint32
	err := db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket(bucketMeta)
		if mb == nil {
			return nil
		}
		raw := mb.Get(metaKeySchemaVersion)
		if raw == nil {
			return nil
		}
		if len(raw) != 4 {
			return fmt.Errorf("schema_version: bad encoding (%d bytes)", len(raw))
		}
		v = binary.LittleEndian.Uint32(raw)
		return nil
	})
	return v, err
}

func writeSchemaVersionTx(tx *bolt.Tx, v uint32) error {
	mb, err := tx.CreateBucketIfNotExists(bucketMeta)
	if err != nil {
		return err
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return mb.Put(metaKeySchemaVersion, buf[:])
}

// runMigrations runs every applicable upgrade step in order. dbPath is used
// for the backup file. Called from NewMeta after the DB is opened and the
// new buckets have been ensured.
func runMigrations(db *bolt.DB, dbPath string) error {
	on, err := readSchemaVersion(db)
	if err != nil {
		return err
	}
	if on == currentSchemaVersion {
		return nil
	}
	if on > currentSchemaVersion {
		return fmt.Errorf("place: db schema is v%d but this binary only knows up to v%d — upgrade the binary or restore from a backup in %s",
			on, currentSchemaVersion, filepath.Join(filepath.Dir(dbPath), ".migrations"))
	}

	// Pending migrations.
	var pending []migration
	for _, m := range migrations {
		if m.to > on && m.to <= currentSchemaVersion {
			pending = append(pending, m)
		}
	}
	if len(pending) == 0 {
		return nil
	}

	log.Printf("place: schema migration v%d → v%d (%d step%s)",
		on, currentSchemaVersion, len(pending), pluralS(len(pending)))

	backupPath, err := backupDB(db, dbPath, on)
	if err != nil {
		return fmt.Errorf("backup before migrate: %w", err)
	}
	log.Printf("place: backup written to %s (downgrades require manual restore from this file)", backupPath)

	err = db.Update(func(tx *bolt.Tx) error {
		for _, m := range pending {
			t0 := time.Now()
			log.Printf("place: applying migration v%d: %s", m.to, m.name)
			if err := m.apply(tx); err != nil {
				return fmt.Errorf("v%d (%s): %w", m.to, m.name, err)
			}
			if err := writeSchemaVersionTx(tx, m.to); err != nil {
				return fmt.Errorf("v%d (%s): write version: %w", m.to, m.name, err)
			}
			log.Printf("place: migration v%d done in %v", m.to, time.Since(t0).Round(time.Millisecond))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("migrate failed (backup at %s): %w", backupPath, err)
	}
	log.Printf("place: schema is now v%d", currentSchemaVersion)
	return nil
}

// dryRunMigrations prints what runMigrations would do without writing to the
// db. Used by `place migrate --dry-run`.
func dryRunMigrations(db *bolt.DB, dbPath string) error {
	on, err := readSchemaVersion(db)
	if err != nil {
		return err
	}
	fmt.Printf("place: db at %s\n", dbPath)
	fmt.Printf("place: schema version: v%d (binary knows up to v%d)\n", on, currentSchemaVersion)
	if on == currentSchemaVersion {
		fmt.Println("place: no migrations pending")
		return nil
	}
	if on > currentSchemaVersion {
		fmt.Printf("place: REFUSE — db is newer than binary\n")
		return nil
	}
	for _, m := range migrations {
		if m.to <= on || m.to > currentSchemaVersion {
			continue
		}
		summary := "(no dry-run summary)"
		if m.dryRun != nil {
			err := db.View(func(tx *bolt.Tx) error {
				s, err := m.dryRun(tx)
				if err != nil {
					return err
				}
				summary = s
				return nil
			})
			if err != nil {
				summary = fmt.Sprintf("(dry-run error: %v)", err)
			}
		}
		fmt.Printf("place: pending v%d %q — %s\n", m.to, m.name, summary)
	}
	fmt.Printf("place: backup will be written to %s before applying\n",
		filepath.Join(filepath.Dir(dbPath), ".migrations"))
	return nil
}

// backupDB writes a transactionally-consistent copy of the db file to
// .migrations/meta.db.pre-v<from>.<UTC-timestamp>.bak alongside the db.
// Uses bbolt's tx.CopyFile under a read tx.
func backupDB(db *bolt.DB, dbPath string, from uint32) (string, error) {
	dir := filepath.Join(filepath.Dir(dbPath), ".migrations")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	backupName := fmt.Sprintf("%s.pre-v%d.%s.bak", filepath.Base(dbPath), from, stamp)
	backupPath := filepath.Join(dir, backupName)

	err := db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(backupPath, 0600)
	})
	if err != nil {
		_ = os.Remove(backupPath)
		return "", err
	}
	return backupPath, nil
}

// migrateV0ToV1 walks the legacy bucketFiles and rewrites each entry into
// (paths[rel] → inodeID, inodes[inodeID] → encoded FileMeta-with-Nlink=1).
// The legacy bucket is deleted on completion.
func migrateV0ToV1(tx *bolt.Tx) error {
	old := tx.Bucket(bucketFiles)
	if old == nil {
		// Fresh database — nothing to migrate. The version-bump alone is
		// what matters.
		return nil
	}
	if _, err := tx.CreateBucketIfNotExists(bucketPaths); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(bucketInodes); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(bucketMeta); err != nil {
		return err
	}

	moved := 0
	cur := old.Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		rel := keyToRel(k)
		fm, err := decodeFileMeta(v)
		if err != nil {
			return fmt.Errorf("decode legacy %q: %w", rel, err)
		}
		// PutFileTx allocates a fresh inodeID via _meta/next_inode_id and
		// writes both buckets. fm.Nlink defaults to 1 when not set; v1
		// records had no Nlink so decodeFileMeta already filled it in.
		// nil Meta: this is a one-shot tx-body migration with no Meta in
		// scope, and the stale-inode drop branch can't fire on legacy
		// records (decodeFileMeta doesn't populate InodeID from v0 keys).
		if err := PutFileTx(nil, tx, fm); err != nil {
			return fmt.Errorf("write inode for %q: %w", rel, err)
		}
		moved++
	}

	if err := tx.DeleteBucket(bucketFiles); err != nil {
		return fmt.Errorf("delete legacy bucket: %w", err)
	}
	log.Printf("place: V0→V1 migrated %d file entries", moved)
	return nil
}

func dryRunV0ToV1(tx *bolt.Tx) (string, error) {
	old := tx.Bucket(bucketFiles)
	if old == nil {
		return "0 file entries to migrate (fresh db)", nil
	}
	stats := old.Stats()
	return fmt.Sprintf("%d file entries to migrate", stats.KeyN), nil
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// RunMigrations is the exported entry point used by the `place migrate`
// CLI subcommand. Opens the bbolt file, runs every pending migration,
// closes the db. Safe to call against a db that's already at currentVersion
// (no-op).
func RunMigrations(dbPath string) error {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	// Ensure the v1 schema buckets exist so a fresh-DB call doesn't fail
	// looking up _meta.
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketPaths, bucketInodes, bucketMeta, bucketSegments, bucketConfig} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return runMigrations(db, dbPath)
}

// DryRunMigrations is the read-only counterpart to RunMigrations.
func DryRunMigrations(dbPath string) error {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second, ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()
	return dryRunMigrations(db, dbPath)
}
