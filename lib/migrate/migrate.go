// Package migrate copies a legacy-format place dataset into the new format
// living in the same hot/cold directories. The migration is:
//
//   - Self-configuring. No settings; no knobs to forget about. Migrated
//     bytes always land in cold (the new format's hot fills via writes
//     after mount, not from a bulk replay of cached old data). Progress
//     prints every 10 s.
//
//   - Resumable. A bbolt bucket ("migration_manifest") in the new DB
//     records, for each legacy inode, the new inode it now lives at.
//     Restart picks up exactly where it left off; hardlinks land as
//     hardlinks on resume too.
//
//   - Incrementally cleanup. Each legacy segment is reference-counted
//     against the still-un-migrated set; the moment its count hits zero
//     the .seg file is unlinked. Disk usage during migration is at most
//     `untouched bytes of legacy + bytes migrated so far` rather than
//     `2 × dataset`.
//
//   - Orphan-cleanup. At the start of every run, segment files in the
//     legacy directories that no one references are deleted before any
//     migration begins. At the end, the entire .place/ subtree and the
//     legacy DB are removed. The new-format index's own GC is also run
//     so unreferenced new segments are reaped.
//
// Run is safe to invoke on every Store.Open: if no legacy data is present,
// it still does the new-format orphan sweep and returns.
package migrate

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	oldplace "github.com/eliothedeman/place"
	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/store"
	bolt "go.etcd.io/bbolt"
)

const (
	// chunkSize is the per-read/per-write byte size during copyFile. Larger
	// means fewer iterations + smaller per-fragment overhead in bbolt; we
	// pick 16 MiB as a reasonable balance against migrator RSS.
	chunkSize          = 16 << 20
	progressInterval   = 10 * time.Second
	legacyDirName      = ".place"
	legacySegmentsName = "segments"
	// targetTier is where migrated bytes land. Hardcoded to cold so a bulk
	// migration of legacy data never fills up the hot drive.
	targetTier = index.TierCold
)

var manifestBucket = []byte("migration_manifest")

// Stats reports what one Run did.
type Stats struct {
	Dirs           int
	Files          int
	Symlinks       int
	Hardlinks      int
	Bytes          int64
	LegacyOrphans  int // legacy .seg files deleted because no FileMeta referenced them
	SegmentsReaped int // legacy .seg files deleted incrementally because their last referencer migrated
}

// candidateLegacyDBs returns the conventional legacy DB paths in priority
// order. The first one that exists wins.
func candidateLegacyDBs(oldHot string) []string {
	return []string{
		filepath.Join(oldHot, ".place.db"),
		filepath.Join(oldHot, ".place", "meta.db"),
	}
}

// FindLegacyDB returns the path of the recognised legacy DB under oldHot,
// or ("", false).
func FindLegacyDB(oldHot string) (string, bool) {
	if oldHot == "" {
		return "", false
	}
	for _, p := range candidateLegacyDBs(oldHot) {
		if _, err := os.Stat(p); err == nil {
			return p, true
		}
	}
	return "", false
}

// LegacyDBCandidates exposes the search list so callers can log it.
func LegacyDBCandidates(oldHot string) []string { return candidateLegacyDBs(oldHot) }

// LooksLikeOldFormat reports whether oldHot has both a recognised legacy
// DB and a .place/segments dir.
func LooksLikeOldFormat(oldHot string) bool {
	if _, ok := FindLegacyDB(oldHot); !ok {
		return false
	}
	st, err := os.Stat(filepath.Join(oldHot, legacyDirName, legacySegmentsName))
	return err == nil && st.IsDir()
}

// Run performs an in-place, resumable migration of any legacy data found
// in the store's hot/cold directories. It also reaps unreferenced segments
// in both the legacy and new layouts.
func Run(st *store.Store) (Stats, error) {
	hot := st.Index().HotDir()
	cold := st.Index().ColdDir()

	// Always reap new-format orphans first. Even if there's no legacy data,
	// principle 4 (delete any storage file without a DB reference) applies.
	if err := st.Index().GC(); err != nil {
		return Stats{}, fmt.Errorf("migrate: pre new-format GC: %w", err)
	}

	dbPath, ok := FindLegacyDB(hot)
	if !ok {
		log.Printf("migrate: no legacy data at %s; nothing to migrate", hot)
		return Stats{}, nil
	}

	stats, err := runLegacy(st, hot, cold, dbPath)
	if err != nil {
		return stats, err
	}

	// Final new-format orphan sweep too — Move-style operations can leave
	// segments behind that GC reclaims.
	if err := st.Index().GC(); err != nil {
		return stats, fmt.Errorf("migrate: post new-format GC: %w", err)
	}
	return stats, nil
}

func runLegacy(st *store.Store, hot, cold, dbPath string) (Stats, error) {
	log.Printf("migrate: legacy data found (db=%s); starting", dbPath)
	if err := st.Index().DB().Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(manifestBucket)
		return err
	}); err != nil {
		return Stats{}, err
	}

	oldMeta, err := oldplace.NewMeta(dbPath)
	if err != nil {
		return Stats{}, fmt.Errorf("migrate: open old meta: %w", err)
	}
	hotSet, err := oldplace.NewSegmentSet(oldplace.TierHot, hot, 1<<30)
	if err != nil {
		oldMeta.Close()
		return Stats{}, fmt.Errorf("migrate: open old hot segments: %w", err)
	}
	coldSet, err := oldplace.NewSegmentSet(oldplace.TierCold, cold, 1<<30)
	if err != nil {
		hotSet.CloseAll()
		oldMeta.Close()
		return Stats{}, fmt.Errorf("migrate: open old cold segments: %w", err)
	}
	reader := oldplace.NewReader(hotSet, coldSet, oldMeta)

	m := &migrator{
		st:           st,
		db:           st.Index().DB(),
		old:          oldMeta,
		reader:       reader,
		oldHot:       hot,
		oldCold:      cold,
		startedAt:    time.Now(),
		lastProgress: time.Now(),
		refs:         map[segKey]int{},
		inodeMap:     map[uint64]uint64{},
	}

	if err := m.loadManifest(); err != nil {
		m.closeOldHandles(hotSet, coldSet)
		return m.stats, err
	}
	log.Printf("migrate: manifest holds %d previously-migrated inodes", len(m.inodeMap))

	if err := m.buildRefsFromUnmigrated(); err != nil {
		m.closeOldHandles(hotSet, coldSet)
		return m.stats, err
	}
	if err := m.sweepUnreferencedLegacySegments(); err != nil {
		m.closeOldHandles(hotSet, coldSet)
		return m.stats, err
	}

	if err := m.walk("", store.RootInode); err != nil {
		m.closeOldHandles(hotSet, coldSet)
		return m.stats, err
	}

	// Migration completed. Release old handles before deleting the files.
	m.closeOldHandles(hotSet, coldSet)

	if err := m.removeLegacyTree(dbPath); err != nil {
		return m.stats, fmt.Errorf("migrate: remove legacy tree: %w", err)
	}

	// Drop the manifest now that no one needs it. Keeping it would just
	// be persistent dead weight.
	if err := m.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(manifestBucket)
	}); err != nil && err != bolt.ErrBucketNotFound {
		log.Printf("migrate: dropping manifest bucket: %v", err)
	}

	m.emitProgress("done")
	return m.stats, nil
}

// segKey identifies one legacy segment file.
type segKey struct {
	tier oldplace.Tier
	id   uint32
}

type migrator struct {
	st           *store.Store
	db           *bolt.DB
	old          *oldplace.Meta
	reader       *oldplace.Reader
	oldHot       string
	oldCold      string
	startedAt    time.Time
	lastProgress time.Time
	stats        Stats
	// refs[k] counts how many still-un-migrated FileMetas reference segment k.
	// When it hits zero (during walk's dec-ref or at startup's initial sweep),
	// k's .seg file is unlinked.
	refs map[segKey]int
	// inodeMap is the in-memory mirror of the manifest. It maps a legacy
	// inode id to the new inode it lives at. Used both for hardlink fan-out
	// during a single Run and for resume across Runs.
	inodeMap map[uint64]uint64
}

func (m *migrator) closeOldHandles(hotSet, coldSet *oldplace.SegmentSet) {
	hotSet.CloseAll()
	coldSet.CloseAll()
	m.old.Close()
}

func (m *migrator) loadManifest() error {
	return m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(manifestBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if len(k) != 8 || len(v) != 8 {
				return nil
			}
			m.inodeMap[binary.BigEndian.Uint64(k)] = binary.BigEndian.Uint64(v)
			return nil
		})
	})
}

// buildRefsFromUnmigrated walks every FileMeta in the old DB and, for the
// ones NOT in the manifest, counts each segment-id reference. The result is
// the basis for both the initial orphan sweep and the per-file decrement
// during migration.
func (m *migrator) buildRefsFromUnmigrated() error {
	return m.recurseOld("", func(fm *oldplace.FileMeta) error {
		if !fm.IsRegular() {
			return nil
		}
		if _, done := m.inodeMap[fm.InodeID]; done && fm.InodeID != 0 {
			return nil
		}
		for _, f := range fm.HotFragments {
			m.refs[segKey{oldplace.TierHot, f.SegmentID}]++
		}
		for _, f := range fm.ColdFragments {
			m.refs[segKey{oldplace.TierCold, f.SegmentID}]++
		}
		return nil
	})
}

// sweepUnreferencedLegacySegments deletes any .seg file in the legacy
// directories that no un-migrated FileMeta references. This catches:
//   - Orphans left behind by the legacy binary (segments that lost all
//     refs but were never GC'd before shutdown).
//   - Remnants of a prior interrupted migration (segments whose only
//     remaining refs are from manifest-recorded files).
func (m *migrator) sweepUnreferencedLegacySegments() error {
	for _, e := range []struct {
		tier oldplace.Tier
		root string
	}{
		{oldplace.TierHot, m.oldHot},
		{oldplace.TierCold, m.oldCold},
	} {
		segDir := filepath.Join(e.root, legacyDirName, legacySegmentsName)
		entries, err := os.ReadDir(segDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, ent := range entries {
			if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".seg") {
				continue
			}
			id, ok := parseSegID(ent.Name())
			if !ok {
				continue
			}
			if m.refs[segKey{e.tier, id}] == 0 {
				p := filepath.Join(segDir, ent.Name())
				if err := os.Remove(p); err != nil {
					return fmt.Errorf("remove orphan %s: %w", p, err)
				}
				m.stats.LegacyOrphans++
			}
		}
	}
	if m.stats.LegacyOrphans > 0 {
		log.Printf("migrate: removed %d unreferenced legacy segments before migrating", m.stats.LegacyOrphans)
	}
	return nil
}

// recurseOld walks the old directory tree depth-first, calling fn for every
// FileMeta encountered (including directories themselves; for those, fn is
// called before the recursion into their children).
func (m *migrator) recurseOld(parentRel string, fn func(*oldplace.FileMeta) error) error {
	children, err := m.old.DirChildren(parentRel)
	if err != nil {
		return fmt.Errorf("DirChildren(%q): %w", parentRel, err)
	}
	for _, fm := range children {
		if err := fn(fm); err != nil {
			return err
		}
		if fm.IsDir() {
			if err := m.recurseOld(fm.Rel, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// walk migrates one directory's contents into newParent, recursing.
func (m *migrator) walk(oldParentRel string, newParent uint64) error {
	children, err := m.old.DirChildren(oldParentRel)
	if err != nil {
		return fmt.Errorf("DirChildren(%q): %w", oldParentRel, err)
	}
	for _, fm := range children {
		if err := m.handle(fm, newParent); err != nil {
			return err
		}
		m.maybeProgress()
	}
	return nil
}

func (m *migrator) handle(fm *oldplace.FileMeta, newParent uint64) error {
	name := path.Base(fm.Rel)

	// Resume short-circuit: this path already exists in the new store. For
	// dirs we still recurse; for everything else we're done.
	if existing, err := m.st.Lookup(newParent, name); err == nil {
		if fm.InodeID != 0 {
			if _, known := m.inodeMap[fm.InodeID]; !known {
				m.inodeMap[fm.InodeID] = existing.Inode
				if err := m.markMigratedDurable(fm.InodeID, existing.Inode); err != nil {
					return err
				}
			}
		}
		if existing.Mode.IsDir() {
			return m.walk(fm.Rel, existing.Inode)
		}
		return nil
	}

	// Hardlink fan-out: legacy inode already migrated under another path.
	if existing, ok := m.inodeMap[fm.InodeID]; ok && fm.InodeID != 0 && !fm.IsDir() {
		if _, err := m.st.Link(existing, newParent, name); err != nil {
			return fmt.Errorf("Link(%q): %w", fm.Rel, err)
		}
		m.stats.Hardlinks++
		return nil
	}

	switch {
	case fm.IsDir():
		nn, err := m.st.Mkdir(newParent, name, store.Mode(fm.Mode))
		if err != nil {
			return fmt.Errorf("Mkdir(%q): %w", fm.Rel, err)
		}
		if err := m.recordInode(fm.InodeID, nn.Inode); err != nil {
			return err
		}
		m.stats.Dirs++
		return m.walk(fm.Rel, nn.Inode)
	case fm.IsLink():
		nn, err := m.st.Symlink(newParent, name, fm.LinkTarget)
		if err != nil {
			return fmt.Errorf("Symlink(%q): %w", fm.Rel, err)
		}
		if err := m.recordInode(fm.InodeID, nn.Inode); err != nil {
			return err
		}
		m.stats.Symlinks++
		return nil
	case fm.IsRegular():
		newN, h, err := m.st.Create(newParent, name, store.Mode(fm.Mode))
		if err != nil {
			return fmt.Errorf("Create(%q): %w", fm.Rel, err)
		}
		n, copyErr := m.copyFileBulk(h, fm)
		closeErr := h.Close()
		if copyErr != nil {
			return fmt.Errorf("copy(%q): %w", fm.Rel, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close(%q): %w", fm.Rel, closeErr)
		}
		if err := m.recordInode(fm.InodeID, newN.Inode); err != nil {
			return err
		}
		// Dec-ref this file's fragments; segments may get unlinked here.
		for _, f := range fm.HotFragments {
			m.decRef(segKey{oldplace.TierHot, f.SegmentID})
		}
		for _, f := range fm.ColdFragments {
			m.decRef(segKey{oldplace.TierCold, f.SegmentID})
		}
		m.stats.Files++
		m.stats.Bytes += n
		return nil
	default:
		log.Printf("migrate: skipping unsupported mode %o at %q", fm.Mode, fm.Rel)
		return nil
	}
}

// copyFileBulk streams bytes from the old reader into the new handle via
// the bulk-writer path: every chunk-sized piece goes to the target tier's
// active segment without a per-chunk fsync, and one Commit at the end
// fsyncs the touched segments and commits all fragments in a single bbolt
// tx. For multi-GB files this is the difference between hours and minutes
// — the regular WriteAt path issues three fsyncs per chunk.
//
// On a partial-copy failure the bulk writer is Abort'd, leaving orphan
// segment bytes that GC reclaims on the next pass. The legacy file is not
// marked migrated, so a future Run redoes it.
func (m *migrator) copyFileBulk(h *store.Handle, fm *oldplace.FileMeta) (int64, error) {
	if fm.Size == 0 {
		return 0, nil
	}
	bw := h.NewBulkWriter(targetTier)
	buf := make([]byte, chunkSize)
	var total int64
	for off := int64(0); off < fm.Size; {
		n := int64(chunkSize)
		if off+n > fm.Size {
			n = fm.Size - off
		}
		rn, err := m.reader.ReadAt(fm.Rel, buf[:n], off)
		if err != nil && err != io.EOF {
			bw.Abort()
			return total, err
		}
		if rn == 0 {
			break
		}
		if werr := bw.Write(off, buf[:rn]); werr != nil {
			bw.Abort()
			return total, werr
		}
		off += int64(rn)
		total += int64(rn)
		// Mid-file progress for multi-GB files: we'd otherwise look stuck.
		m.maybeProgress()
		if err == io.EOF {
			break
		}
	}
	if err := bw.Commit(); err != nil {
		return total, err
	}
	return total, nil
}

func (m *migrator) recordInode(oldInode, newInode uint64) error {
	if oldInode == 0 {
		return nil
	}
	m.inodeMap[oldInode] = newInode
	return m.markMigratedDurable(oldInode, newInode)
}

func (m *migrator) markMigratedDurable(oldInode, newInode uint64) error {
	var k, v [8]byte
	binary.BigEndian.PutUint64(k[:], oldInode)
	binary.BigEndian.PutUint64(v[:], newInode)
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(manifestBucket)
		if b == nil {
			return fmt.Errorf("manifest bucket missing")
		}
		return b.Put(k[:], v[:])
	})
}

// decRef decrements the per-segment ref count and, when it hits zero,
// unlinks the .seg file. Best-effort — a failed unlink is logged but not
// fatal; the next Run's startup sweep will pick it up.
func (m *migrator) decRef(k segKey) {
	cur := m.refs[k]
	if cur <= 0 {
		return
	}
	m.refs[k] = cur - 1
	if m.refs[k] != 0 {
		return
	}
	root := m.oldHot
	if k.tier == oldplace.TierCold {
		root = m.oldCold
	}
	p := filepath.Join(root, legacyDirName, legacySegmentsName, fmt.Sprintf("%08d.seg", k.id))
	if err := os.Remove(p); err == nil {
		m.stats.SegmentsReaped++
	} else if !os.IsNotExist(err) {
		log.Printf("migrate: unlink %s: %v", p, err)
	}
}

// removeLegacyTree drops the legacy DB and the .place/ subtrees. The
// migrator must have closed its old handles before calling this.
func (m *migrator) removeLegacyTree(dbPath string) error {
	for _, p := range []string{
		dbPath,
		filepath.Join(m.oldHot, legacyDirName),
		filepath.Join(m.oldCold, legacyDirName),
	} {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("remove %s: %w", p, err)
		}
	}
	return nil
}

func (m *migrator) emitProgress(status string) {
	dt := time.Since(m.startedAt).Seconds()
	if dt <= 0 {
		dt = 0.001
	}
	mbs := float64(m.stats.Bytes) / 1024 / 1024 / dt
	log.Printf("migrate progress (%s): %d files, %d dirs, %d symlinks, %d hardlinks, %d bytes, %d legacy segs reaped (%.1f MB/s avg)",
		status, m.stats.Files, m.stats.Dirs, m.stats.Symlinks, m.stats.Hardlinks,
		m.stats.Bytes, m.stats.SegmentsReaped+m.stats.LegacyOrphans, mbs)
	m.lastProgress = time.Now()
}

func (m *migrator) maybeProgress() {
	if time.Since(m.lastProgress) >= progressInterval {
		m.emitProgress("running")
	}
}

func parseSegID(name string) (uint32, bool) {
	base := strings.TrimSuffix(name, ".seg")
	if base == name {
		return 0, false
	}
	id, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}
