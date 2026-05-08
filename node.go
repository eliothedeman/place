package place

import (
	"context"
	"path"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	bolt "go.etcd.io/bbolt"
)

type placeRoot struct {
	placeNode
	hot      *Storage
	cold     *Storage
	hotSegs  *SegmentSet
	coldSegs *SegmentSet
	meta     *Meta
	writer   *Writer
	reader   *Reader
	compact  *Compactor
	evict    *Evictor
	dbg      dbg
}

type placeNode struct {
	fs.Inode
}

func (n *placeNode) root() *placeRoot {
	return n.Root().Operations().(*placeRoot)
}

func (n *placeNode) relPath() string {
	return n.Path(n.Root())
}

func (pr *placeRoot) stableAttr(fm *FileMeta) fs.StableAttr {
	return fs.StableAttr{
		Mode: fm.Mode,
		Gen:  1,
		Ino:  fm.InodeID, // hardlinks: all paths to the same inode share Ino
	}
}

func attrFromMeta(fm *FileMeta, out *fuse.Attr) {
	out.Mode = fm.Mode
	out.Size = uint64(fm.Size)
	out.Uid = fm.Uid
	out.Gid = fm.Gid
	out.Mtime = uint64(fm.Mtime / 1e9)
	out.Mtimensec = uint32(fm.Mtime % 1e9)
	out.Ctime = uint64(fm.Ctime / 1e9)
	out.Ctimensec = uint32(fm.Ctime % 1e9)
	out.Atime = uint64(fm.Atime / 1e9)
	out.Atimensec = uint32(fm.Atime % 1e9)
	// Nlink: directories report 2 (POSIX convention — "." and "..");
	// regular files and symlinks expose the actual hardlink count.
	if fm.Mode&syscall.S_IFMT == syscall.S_IFDIR {
		out.Nlink = 2
	} else if fm.Nlink > 0 {
		out.Nlink = fm.Nlink
	} else {
		out.Nlink = 1
	}
	// Rough block count for `du` — not critical.
	out.Blksize = 4096
	out.Blocks = (out.Size + 511) / 512
}

// --- Lookup / Getattr / Setattr ---

var _ = (fs.NodeLookuper)((*placeNode)(nil))

func (n *placeNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	r := n.root()
	rel := joinRel(n.relPath(), name)
	done := r.dbg.op("Lookup", rel)
	fm, err := r.meta.GetFile(rel)
	if err != nil {
		done(fs_errno(err))
		return nil, fs_errno(err)
	}
	if fm == nil {
		done(syscall.ENOENT)
		return nil, syscall.ENOENT
	}
	attrFromMeta(fm, &out.Attr)
	child := &placeNode{}
	ino := n.NewInode(ctx, child, r.stableAttr(fm))
	done(0)
	return ino, 0
}

var _ = (fs.NodeGetattrer)((*placeNode)(nil))

func (n *placeNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	r := n.root()
	rel := n.relPath()
	fm, err := r.meta.GetFile(rel)
	if err != nil {
		return fs_errno(err)
	}
	if fm == nil {
		return syscall.ENOENT
	}
	attrFromMeta(fm, &out.Attr)
	return 0
}

var _ = (fs.NodeSetattrer)((*placeNode)(nil))

func (n *placeNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	r := n.root()
	rel := n.relPath()
	err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		fm, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if fm == nil {
			return syscall.ENOENT
		}
		if m, ok := in.GetMode(); ok {
			fm.Mode = (fm.Mode &^ 0o7777) | (m & 0o7777)
		}
		if u, ok := in.GetUID(); ok {
			fm.Uid = u
		}
		if g, ok := in.GetGID(); ok {
			fm.Gid = g
		}
		now := time.Now().UnixNano()
		if t, ok := in.GetMTime(); ok {
			fm.Mtime = t.UnixNano()
		}
		if t, ok := in.GetATime(); ok {
			fm.Atime = t.UnixNano()
		}
		fm.Ctime = now
		if sz, ok := in.GetSize(); ok {
			newSize := int64(sz)
			if newSize != fm.Size {
				// Both shrink and grow desync cold from the file: shrink
				// can leave hot newer than cold within [0, newSize) (e.g.
				// when the trimmed range still contains an unreplicated
				// hot write); grow extends Size past cold's coverage.
				fm.ColdDirty = true
			}
			if newSize < fm.Size {
				hotKeep, hotDead := truncateFragments(fm.HotFragments, newSize)
				coldKeep, coldDead := truncateFragments(fm.ColdFragments, newSize)
				fm.HotFragments = hotKeep
				fm.ColdFragments = coldKeep
				if err := AddLiveBytesTx(r.meta, tx, hotDead, -1); err != nil {
					return err
				}
				if err := AddLiveBytesTx(r.meta, tx, coldDead, -1); err != nil {
					return err
				}
			}
			fm.Size = newSize
		}
		fm.Version++
		attrFromMeta(fm, &out.Attr)
		return PutFileTx(r.meta, tx, fm)
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return errno
		}
		return fs_errno(err)
	}
	return 0
}

// --- Readdir ---

var _ = (fs.NodeReaddirer)((*placeNode)(nil))

func (n *placeNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	r := n.root()
	rel := n.relPath()
	done := r.dbg.op("Readdir", rel)
	children, err := r.meta.DirChildren(rel)
	if err != nil {
		done(fs_errno(err))
		return nil, fs_errno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(children))
	for _, fm := range children {
		entries = append(entries, fuse.DirEntry{
			Name: BaseName(fm.Rel),
			Mode: fm.Mode,
			Ino:  fm.InodeID,
		})
	}
	done(0, "entries=%d", len(entries))
	return fs.NewListDirStream(entries), 0
}

// --- Create / Open ---

var _ = (fs.NodeCreater)((*placeNode)(nil))

func (n *placeNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	r := n.root()
	rel := joinRel(n.relPath(), name)
	done := r.dbg.op("Create", rel)
	now := time.Now().UnixNano()
	caller, _ := fuse.FromContext(ctx)
	var fm *FileMeta
	err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		existing, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if existing != nil {
			// Truncate existing file if O_TRUNC.
			if flags&syscall.O_TRUNC != 0 {
				if err := AddLiveBytesTx(r.meta, tx, existing.HotFragments, -1); err != nil {
					return err
				}
				if err := AddLiveBytesTx(r.meta, tx, existing.ColdFragments, -1); err != nil {
					return err
				}
				existing.HotFragments = nil
				existing.ColdFragments = nil
				existing.Size = 0
				existing.Mtime = now
				existing.Ctime = now
				// Size==0 short-circuits HasColdCopy regardless, but
				// flag the inode so any in-flight replicate aborts on
				// the version-changed check rather than racing past us.
				existing.ColdDirty = true
				existing.Version++
				if err := PutFileTx(r.meta, tx, existing); err != nil {
					return err
				}
			}
			fm = existing
			return nil
		}
		fm = &FileMeta{
			Rel:   rel,
			Mode:  syscall.S_IFREG | (mode & 0o7777),
			Size:  0,
			Mtime: now,
			Ctime: now,
			Atime: now,
		}
		if caller != nil {
			fm.Uid = caller.Uid
			fm.Gid = caller.Gid
		}
		return PutFileTx(r.meta, tx, fm)
	})
	if err != nil {
		done(fs_errno(err))
		return nil, nil, 0, fs_errno(err)
	}
	attrFromMeta(fm, &out.Attr)
	child := &placeNode{}
	ino := n.NewInode(ctx, child, r.stableAttr(fm))
	pf := &placeFile{rel: rel, root: r, writable: true}
	done(0)
	return ino, pf, 0, 0
}

var _ = (fs.NodeOpener)((*placeNode)(nil))

func (n *placeNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	r := n.root()
	rel := n.relPath()
	done := r.dbg.op("Open", rel, "flags=0x%x", flags)
	fm, err := r.meta.GetFile(rel)
	if err != nil {
		done(fs_errno(err))
		return nil, 0, fs_errno(err)
	}
	if fm == nil {
		done(syscall.ENOENT)
		return nil, 0, syscall.ENOENT
	}
	if fm.IsDir() {
		done(syscall.EISDIR)
		return nil, 0, syscall.EISDIR
	}
	isWrite := flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0
	if isWrite && flags&syscall.O_TRUNC != 0 {
		err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
			cur, err := GetFileTx(tx, rel)
			if err != nil {
				return err
			}
			if cur == nil {
				return syscall.ENOENT
			}
			if err := AddLiveBytesTx(r.meta, tx, cur.HotFragments, -1); err != nil {
				return err
			}
			if err := AddLiveBytesTx(r.meta, tx, cur.ColdFragments, -1); err != nil {
				return err
			}
			cur.HotFragments = nil
			cur.ColdFragments = nil
			cur.Size = 0
			cur.Mtime = time.Now().UnixNano()
			cur.Ctime = cur.Mtime
			cur.ColdDirty = true
			cur.Version++
			return PutFileTx(r.meta, tx, cur)
		})
		if err != nil {
			if errno, ok := err.(syscall.Errno); ok {
				done(errno)
				return nil, 0, errno
			}
			done(fs_errno(err))
			return nil, 0, fs_errno(err)
		}
	}
	pf := &placeFile{rel: rel, root: r, writable: isWrite}
	done(0)
	return pf, 0, 0
}

// --- Mkdir / Unlink / Rmdir ---

var _ = (fs.NodeMkdirer)((*placeNode)(nil))

func (n *placeNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	r := n.root()
	rel := joinRel(n.relPath(), name)
	now := time.Now().UnixNano()
	caller, _ := fuse.FromContext(ctx)
	fm := &FileMeta{
		Rel:   rel,
		Mode:  syscall.S_IFDIR | (mode & 0o7777),
		Mtime: now,
		Ctime: now,
		Atime: now,
	}
	if caller != nil {
		fm.Uid = caller.Uid
		fm.Gid = caller.Gid
	}
	err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		existing, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if existing != nil {
			return syscall.EEXIST
		}
		return PutFileTx(r.meta, tx, fm)
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return nil, errno
		}
		return nil, fs_errno(err)
	}
	attrFromMeta(fm, &out.Attr)
	child := &placeNode{}
	ino := n.NewInode(ctx, child, r.stableAttr(fm))
	return ino, 0
}

var _ = (fs.NodeUnlinker)((*placeNode)(nil))

func (n *placeNode) Unlink(ctx context.Context, name string) syscall.Errno {
	r := n.root()
	rel := joinRel(n.relPath(), name)
	done := r.dbg.op("Unlink", rel)
	err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		fm, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if fm == nil {
			return syscall.ENOENT
		}
		if fm.IsDir() {
			return syscall.EISDIR
		}
		// Only release segment-live-bytes credit when the last path is
		// going away — otherwise the data is still referenced by another
		// hardlink and segments must keep their fragments live.
		if fm.Nlink <= 1 {
			if err := AddLiveBytesTx(r.meta, tx, fm.HotFragments, -1); err != nil {
				return err
			}
			if err := AddLiveBytesTx(r.meta, tx, fm.ColdFragments, -1); err != nil {
				return err
			}
		}
		return DeleteFileTx(tx, rel)
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			done(errno)
			return errno
		}
		done(fs_errno(err))
		return fs_errno(err)
	}
	done(0)
	return 0
}

var _ = (fs.NodeRmdirer)((*placeNode)(nil))

func (n *placeNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	r := n.root()
	rel := joinRel(n.relPath(), name)
	has, err := r.meta.HasChildren(rel)
	if err != nil {
		return fs_errno(err)
	}
	if has {
		return syscall.ENOTEMPTY
	}
	err = r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		fm, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if fm == nil {
			return syscall.ENOENT
		}
		if !fm.IsDir() {
			return syscall.ENOTDIR
		}
		return DeleteFileTx(tx, rel)
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return errno
		}
		return fs_errno(err)
	}
	return 0
}

// --- Rename ---

var _ = (fs.NodeRenamer)((*placeNode)(nil))

func (n *placeNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	r := n.root()
	oldRel := joinRel(n.relPath(), name)
	// newParent's concrete type may be *placeNode (a regular dir) or
	// *placeRoot (when renaming into the root dir). Both embed fs.Inode
	// and satisfy InodeEmbedder, so resolve through the embedded inode
	// rather than asserting on a single concrete type — the latter
	// returns EXDEV for any rename whose destination is the mount root.
	newParentRel := newParent.EmbeddedInode().Path(n.Root())
	newRel := joinRel(newParentRel, newName)
	done := r.dbg.op("Rename", oldRel, "-> %q", newRel)
	// All the metadata work lives in RenameTx so the same-inode no-op (and
	// any future invariants) are exercisable directly from tests without
	// spinning up a FUSE mount. flags is currently ignored — same as before
	// — but RENAME_EXCHANGE / RENAME_NOREPLACE would need to be wired
	// through and the same-inode early return there guarded accordingly.
	err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		return RenameTx(tx, oldRel, newRel)
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			done(errno)
			return errno
		}
		done(fs_errno(err))
		return fs_errno(err)
	}
	done(0)
	return 0
}

// --- Symlink / Readlink ---

var _ = (fs.NodeSymlinker)((*placeNode)(nil))

func (n *placeNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	r := n.root()
	rel := joinRel(n.relPath(), name)
	now := time.Now().UnixNano()
	caller, _ := fuse.FromContext(ctx)
	fm := &FileMeta{
		Rel:        rel,
		Mode:       syscall.S_IFLNK | 0o777,
		LinkTarget: target,
		Mtime:      now,
		Ctime:      now,
		Atime:      now,
	}
	if caller != nil {
		fm.Uid = caller.Uid
		fm.Gid = caller.Gid
	}
	err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		existing, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if existing != nil {
			return syscall.EEXIST
		}
		return PutFileTx(r.meta, tx, fm)
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return nil, errno
		}
		return nil, fs_errno(err)
	}
	attrFromMeta(fm, &out.Attr)
	child := &placeNode{}
	ino := n.NewInode(ctx, child, r.stableAttr(fm))
	return ino, 0
}

var _ = (fs.NodeReadlinker)((*placeNode)(nil))

func (n *placeNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	r := n.root()
	rel := n.relPath()
	fm, err := r.meta.GetFile(rel)
	if err != nil {
		return nil, fs_errno(err)
	}
	if fm == nil {
		return nil, syscall.ENOENT
	}
	if !fm.IsLink() {
		return nil, syscall.EINVAL
	}
	return []byte(fm.LinkTarget), 0
}

// --- Link (hardlinks) ---

var _ = (fs.NodeLinker)((*placeNode)(nil))

// Link creates a new path entry under n (the destination directory) named
// `name` that points at the same inode as `target`. POSIX semantics:
//
//   - target must be a regular file or symlink (no hardlinks to directories)
//   - if a path called `name` already exists in n, return EEXIST
//   - the new entry shares all attributes (size, mode, mtime, fragments…)
//     with target — they are literally the same inode
//   - Nlink on the inode is incremented
//
// This is the runtime counterpart to the V1 schema's paths/inodes split:
// hardlinks were architecturally impossible before because each path owned
// its own FileMeta.
func (n *placeNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	r := n.root()
	tn, ok := target.(*placeNode)
	if !ok {
		return nil, syscall.EXDEV
	}
	targetRel := tn.relPath()
	newRel := joinRel(n.relPath(), name)
	done := r.dbg.op("Link", newRel, "→ %q", targetRel)

	var fm *FileMeta
	err := r.meta.UpdateLockedSync(func(tx *bolt.Tx) error {
		// Resolve target's inodeID.
		targetID, err := inodeForPathTx(tx, targetRel)
		if err != nil {
			return err
		}
		if targetID == 0 {
			return syscall.ENOENT
		}
		// Reject hardlinks to directories.
		targetFM, err := getInodeTx(tx, targetID)
		if err != nil {
			return err
		}
		if targetFM == nil {
			return syscall.ENOENT
		}
		if targetFM.IsDir() {
			return syscall.EPERM
		}
		// Reject if newRel already exists.
		existingID, err := inodeForPathTx(tx, newRel)
		if err != nil {
			return err
		}
		if existingID != 0 {
			return syscall.EEXIST
		}
		// Bump Nlink on the shared inode.
		targetFM.Nlink++
		targetFM.Ctime = time.Now().UnixNano()
		if err := putInodeTx(tx, targetID, targetFM); err != nil {
			return err
		}
		// Add the new path entry pointing at the same inode.
		if err := putPathTx(tx, newRel, targetID); err != nil {
			return err
		}
		fm = targetFM
		fm.InodeID = targetID
		return nil
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			done(errno)
			return nil, errno
		}
		done(fs_errno(err))
		return nil, fs_errno(err)
	}

	attrFromMeta(fm, &out.Attr)
	child := &placeNode{}
	ino := n.NewInode(ctx, child, r.stableAttr(fm))
	done(0, "nlink=%d", fm.Nlink)
	return ino, 0
}

// --- Statfs ---

var _ = (fs.NodeStatfser)((*placeNode)(nil))

func (n *placeNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	r := n.root()
	st, err := r.hot.Statfs()
	if err != nil {
		return fs_errno(err)
	}
	out.FromStatfsT(&st)
	return 0
}

// --- helpers ---

func joinRel(parent, name string) string {
	if parent == "" {
		return name
	}
	return path.Join(parent, name)
}
