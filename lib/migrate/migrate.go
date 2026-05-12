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

	oldplace "github.com/eliothedeman/place"
	"github.com/eliothedeman/place/lib/store"
)

// LooksLikeOldFormat reports whether oldHot appears to be an old-format
// place hot root (a `.place.db` file plus a `.place/segments` directory).
// Returns false for empty / unrelated directories.
func LooksLikeOldFormat(oldHot string) bool {
	if oldHot == "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(oldHot, ".place.db")); err != nil {
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
	// Logger receives one line per migrated file/dir/symlink. Defaults to
	// log.Printf with a "migrate: " prefix.
	Logger func(format string, args ...any)
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
	logf := opts.Logger
	if logf == nil {
		logf = func(format string, args ...any) {
			log.Printf("migrate: "+format, args...)
		}
	}
	dbPath := opts.OldDB
	if dbPath == "" {
		dbPath = filepath.Join(opts.OldHot, ".place.db")
	}
	if _, err := os.Stat(dbPath); err != nil {
		return Stats{}, fmt.Errorf("migrate: old DB %s: %w", dbPath, err)
	}

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

	m := &migrator{
		dst:       dst,
		oldMeta:   oldMeta,
		reader:    reader,
		chunkSize: opts.ChunkSize,
		inodeMap:  map[uint64]uint64{},
		logf:      logf,
	}
	if err := m.walk("", store.RootInode); err != nil {
		return m.stats, err
	}
	return m.stats, nil
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
	dst       *store.Store
	oldMeta   *oldplace.Meta
	reader    *oldplace.Reader
	chunkSize int
	inodeMap  map[uint64]uint64 // old inode id → new inode id
	stats     Stats
	logf      func(format string, args ...any)
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
			m.logf("dir   %s", "/"+fm.Rel)
			if err := m.walk(fm.Rel, n.Inode); err != nil {
				return err
			}
		case fm.IsLink():
			if _, err := m.dst.Symlink(newParent, name, fm.LinkTarget); err != nil {
				return fmt.Errorf("migrate: Symlink(%q): %w", fm.Rel, err)
			}
			m.stats.Symlinks++
			m.logf("link  %s → %s", "/"+fm.Rel, fm.LinkTarget)
		case fm.IsRegular():
			if existing, ok := m.inodeMap[fm.InodeID]; ok && fm.InodeID != 0 {
				if _, err := m.dst.Link(existing, newParent, name); err != nil {
					return fmt.Errorf("migrate: Link(%q): %w", fm.Rel, err)
				}
				m.stats.Hardlinks++
				m.logf("hard  %s → inode %d", "/"+fm.Rel, existing)
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
			m.logf("file  %s (%d bytes)", "/"+fm.Rel, n)
		default:
			// Unknown mode — skip with a log so the migration completes.
			m.logf("skip  %s (mode=%o)", "/"+fm.Rel, fm.Mode)
		}
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
		if _, werr := h.WriteAt(buf[:rn], off); werr != nil {
			return total, werr
		}
		off += int64(rn)
		total += int64(rn)
		if err == io.EOF {
			break
		}
	}
	return total, h.Sync()
}
