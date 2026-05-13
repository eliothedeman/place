// Package migrate copies an old-format place dataset into a freshly-opened
// new-format store. It uses the old (top-level) `place` package's public
// reading APIs (Meta + Reader) as the source, and writes everything through
// lib/store.Store so that the new on-disk format is produced by exactly the
// same code path as any other write.
//
// The migrator does not try to be clever about deduplication or in-place
// reuse of segment bytes. It rereads every regular-file byte and replays
// the directory tree, which is simple, correct, and works regardless of
// how messy the old data is. Hardlinks are preserved by tracking
// old_inode → new_inode and using Store.Link for second-and-later paths.
package migrate

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	oldplace "github.com/eliothedeman/place"
	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/store"
)

// candidateLegacyDBs returns the conventional DB locations the legacy
// binary or its test suite has used. The first one that exists wins.
func candidateLegacyDBs(oldHot string) []string {
	return []string{
		filepath.Join(oldHot, ".place.db"),         // cmd/place default
		filepath.Join(oldHot, ".place", "meta.db"), // audit_tests layout
	}
}

// FindLegacyDB returns the path of an existing legacy DB under oldHot if
// one is recognised, plus its found-or-not status. Useful for callers that
// want to log "looked here and here, didn't find one."
func FindLegacyDB(oldHot string) (path string, found bool) {
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

// LegacyDBCandidates returns the paths that FindLegacyDB checks, in order.
// Exposed so the placefs binary can log what it looked at when no legacy
// DB was found.
func LegacyDBCandidates(oldHot string) []string {
	return candidateLegacyDBs(oldHot)
}

// LooksLikeOldFormat reports whether oldHot appears to be an old-format
// place hot root: a recognised legacy DB plus the .place/segments dir.
// Returns false for empty / unrelated directories.
func LooksLikeOldFormat(oldHot string) bool {
	if _, ok := FindLegacyDB(oldHot); !ok {
		return false
	}
	if st, err := os.Stat(filepath.Join(oldHot, ".place", "segments")); err != nil || !st.IsDir() {
		return false
	}
	return true
}

// Options controls a migration run.
type Options struct {
	// OldHot / OldCold are the old hot and cold storage roots (the same
	// values the legacy binary's --hot and --cold flags pointed at).
	OldHot, OldCold string
	// OldDB overrides the default DB path of {OldHot}/.place.db. Leave empty
	// to use the default.
	OldDB string
	// ChunkSize is the byte size of each per-file read/write chunk during
	// copy. Defaults to 4 MiB.
	ChunkSize int
	// SegmentMaxSize is the old segment rotation size — only used so the
	// old SegmentSet objects can be constructed; reads ignore it. Defaults
	// to 1 GiB which is fine even if the real value was different.
	OldSegmentMaxSize int64
	// TargetTier is the tier migrated bytes land in. Defaults to TierCold
	// so a bulk migration doesn't fill up the (typically smaller) hot
	// drive. Operators that want migrated data hot-cached can set this to
	// TierHot, but it requires hot to have room for the full dataset.
	TargetTier index.Tier
	// ProgressInterval is how often the migrator emits a "progress: X files,
	// Y bytes, Z MB/s" line. Defaults to 10 seconds. Set to a very large
	// value (or use a no-op Logger) to silence.
	ProgressInterval time.Duration
	// Cleanup, if true, deletes the legacy DB and .place/segments tree
	// after a successful migration. Defaults to false so an operator can
	// verify the new dataset before discarding the source.
	Cleanup bool
	// Logger receives one line per migrated file/dir/symlink/hardlink as
	// well as periodic progress lines. Defaults to log.Printf with a
	// "migrate: " prefix. Pass a no-op for silent runs.
	Logger func(format string, args ...any)
	// Verbose, when true, also logs every individual file/dir/symlink as it
	// migrates. When false (default), only progress lines + start/finish
	// messages are emitted.
	Verbose bool
}

// Run executes the migration. dst must be an already-open Store; ideally
// freshly created (root only, no other entries). Existing entries in dst
// at the same paths return ErrExist from the underlying Store calls.
func Run(dst *store.Store, opts Options) (Stats, error) {
	if opts.OldHot == "" || opts.OldCold == "" {
		return Stats{}, errors.New("migrate: OldHot and OldCold are required")
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 4 << 20
	}
	if opts.OldSegmentMaxSize <= 0 {
		opts.OldSegmentMaxSize = 1 << 30
	}
	if opts.TargetTier == 0 {
		opts.TargetTier = index.TierCold
	}
	if opts.ProgressInterval == 0 {
		opts.ProgressInterval = 10 * time.Second
	}
	logf := opts.Logger
	if logf == nil {
		logf = func(format string, args ...any) {
			log.Printf("migrate: "+format, args...)
		}
	}
	dbPath := opts.OldDB
	if dbPath == "" {
		if p, ok := FindLegacyDB(opts.OldHot); ok {
			dbPath = p
		} else {
			dbPath = filepath.Join(opts.OldHot, ".place.db") // for the error message
		}
	}
	if _, err := os.Stat(dbPath); err != nil {
		return Stats{}, fmt.Errorf("migrate: old DB %s: %w", dbPath, err)
	}
	logf("using old DB at %s", dbPath)

	oldMeta, err := oldplace.NewMeta(dbPath)
	if err != nil {
		return Stats{}, fmt.Errorf("migrate: open old meta: %w", err)
	}
	defer oldMeta.Close()

	hot, err := oldplace.NewSegmentSet(oldplace.TierHot, opts.OldHot, opts.OldSegmentMaxSize)
	if err != nil {
		return Stats{}, fmt.Errorf("migrate: open old hot segments: %w", err)
	}
	defer hot.CloseAll()
	cold, err := oldplace.NewSegmentSet(oldplace.TierCold, opts.OldCold, opts.OldSegmentMaxSize)
	if err != nil {
		return Stats{}, fmt.Errorf("migrate: open old cold segments: %w", err)
	}
	defer cold.CloseAll()

	reader := oldplace.NewReader(hot, cold, oldMeta)

	logf("target tier: %s", opts.TargetTier)
	m := &migrator{
		dst:              dst,
		oldMeta:          oldMeta,
		reader:           reader,
		chunkSize:        opts.ChunkSize,
		targetTier:       opts.TargetTier,
		progressInterval: opts.ProgressInterval,
		startedAt:        time.Now(),
		lastProgress:     time.Now(),
		inodeMap:         map[uint64]uint64{},
		logf:             logf,
		verbose:          opts.Verbose,
	}
	if err := m.walk("", store.RootInode); err != nil {
		return m.stats, err
	}
	m.emitProgress("done")

	if opts.Cleanup {
		if err := cleanupLegacy(opts.OldHot, opts.OldCold, dbPath, logf); err != nil {
			return m.stats, fmt.Errorf("migrate: cleanup: %w", err)
		}
	}
	return m.stats, nil
}

// cleanupLegacy removes the legacy DB and the .place/ subtree under hot
// and cold. Run only after a successful migration when opts.Cleanup is set.
func cleanupLegacy(hot, cold, dbPath string, logf func(string, ...any)) error {
	for _, p := range []string{
		dbPath,
		filepath.Join(hot, ".place"),
		filepath.Join(cold, ".place"),
	} {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("remove %s: %w", p, err)
		}
		logf("cleanup: removed %s", p)
	}
	return nil
}

// Stats reports what was migrated.
type Stats struct {
	Dirs     int
	Files    int
	Symlinks int
	Hardlinks int
	Bytes    int64
}

type migrator struct {
	dst              *store.Store
	oldMeta          *oldplace.Meta
	reader           *oldplace.Reader
	chunkSize        int
	targetTier       index.Tier
	progressInterval time.Duration
	startedAt        time.Time
	lastProgress     time.Time
	inodeMap         map[uint64]uint64 // old inode id → new inode id
	stats            Stats
	logf             func(format string, args ...any)
	verbose          bool
}

// emitProgress prints "progress: X files, Y bytes (rate=Z MB/s)" using the
// running totals. The status arg goes on the same line ("done" at the
// end). Always emits regardless of progressInterval — callers throttle via
// maybeProgress.
func (m *migrator) emitProgress(status string) {
	dt := time.Since(m.startedAt).Seconds()
	if dt <= 0 {
		dt = 0.001
	}
	mbs := float64(m.stats.Bytes) / 1024 / 1024 / dt
	m.logf("progress (%s): %d files, %d dirs, %d symlinks, %d hardlinks, %d bytes (%.1f MB/s avg)",
		status, m.stats.Files, m.stats.Dirs, m.stats.Symlinks, m.stats.Hardlinks, m.stats.Bytes, mbs)
	m.lastProgress = time.Now()
}

func (m *migrator) maybeProgress() {
	if m.progressInterval > 0 && time.Since(m.lastProgress) >= m.progressInterval {
		m.emitProgress("running")
	}
}

func (m *migrator) walk(oldParentRel string, newParent uint64) error {
	children, err := m.oldMeta.DirChildren(oldParentRel)
	if err != nil {
		return fmt.Errorf("migrate: DirChildren(%q): %w", oldParentRel, err)
	}
	for _, fm := range children {
		name := path.Base(fm.Rel)
		switch {
		case fm.IsDir():
			n, err := m.dst.Mkdir(newParent, name, store.Mode(fm.Mode))
			if err != nil {
				return fmt.Errorf("migrate: Mkdir(%q): %w", fm.Rel, err)
			}
			m.stats.Dirs++
			if m.verbose {
				m.logf("dir   %s", "/"+fm.Rel)
			}
			if err := m.walk(fm.Rel, n.Inode); err != nil {
				return err
			}
		case fm.IsLink():
			if _, err := m.dst.Symlink(newParent, name, fm.LinkTarget); err != nil {
				return fmt.Errorf("migrate: Symlink(%q): %w", fm.Rel, err)
			}
			m.stats.Symlinks++
			if m.verbose {
				m.logf("link  %s → %s", "/"+fm.Rel, fm.LinkTarget)
			}
		case fm.IsRegular():
			if existing, ok := m.inodeMap[fm.InodeID]; ok && fm.InodeID != 0 {
				if _, err := m.dst.Link(existing, newParent, name); err != nil {
					return fmt.Errorf("migrate: Link(%q): %w", fm.Rel, err)
				}
				m.stats.Hardlinks++
				if m.verbose {
					m.logf("hard  %s → inode %d", "/"+fm.Rel, existing)
				}
				continue
			}
			newNode, h, err := m.dst.Create(newParent, name, store.Mode(fm.Mode))
			if err != nil {
				return fmt.Errorf("migrate: Create(%q): %w", fm.Rel, err)
			}
			n, err := m.copyFile(h, fm)
			closeErr := h.Close()
			if err != nil {
				return fmt.Errorf("migrate: copy(%q): %w", fm.Rel, err)
			}
			if closeErr != nil {
				return fmt.Errorf("migrate: close(%q): %w", fm.Rel, closeErr)
			}
			m.inodeMap[fm.InodeID] = newNode.Inode
			m.stats.Files++
			m.stats.Bytes += n
			if m.verbose {
				m.logf("file  %s (%d bytes)", "/"+fm.Rel, n)
			}
		default:
			// Unknown mode — skip with a log so the migration completes.
			m.logf("skip  %s (mode=%o)", "/"+fm.Rel, fm.Mode)
		}
		m.maybeProgress()
	}
	return nil
}

func (m *migrator) copyFile(h *store.Handle, fm *oldplace.FileMeta) (int64, error) {
	if fm.Size == 0 {
		return 0, nil
	}
	buf := make([]byte, m.chunkSize)
	var total int64
	for off := int64(0); off < fm.Size; {
		n := int64(m.chunkSize)
		if off+n > fm.Size {
			n = fm.Size - off
		}
		rn, err := m.reader.ReadAt(fm.Rel, buf[:n], off)
		if err != nil && err != io.EOF {
			return total, err
		}
		if rn == 0 {
			break
		}
		if _, werr := h.WriteAtTier(buf[:rn], off, m.targetTier); werr != nil {
			return total, werr
		}
		off += int64(rn)
		total += int64(rn)
		m.stats.Bytes += int64(rn)
		m.maybeProgress()
		if err == io.EOF {
			break
		}
	}
	// We bumped m.stats.Bytes inside the loop for periodic progress; subtract
	// it back out here so walk's "m.stats.Bytes += n" doesn't double-count.
	m.stats.Bytes -= total
	return total, h.Sync()
}
