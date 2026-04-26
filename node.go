package place

import (
	"context"
	"hash/fnv"
	"path"
	"sync"
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

	inos inodeMap
}

type inodeMap struct {
	mu   sync.RWMutex
	m    map[string]uint64
	next uint64
}

func (im *inodeMap) get(rel string) uint64 {
	im.mu.RLock()
	ino, ok := im.m[rel]
	im.mu.RUnlock()
	if ok {
		return ino
	}
	im.mu.Lock()
	defer im.mu.Unlock()
	if im.m == nil {
		im.m = map[string]uint64{}
	}
	if ino, ok := im.m[rel]; ok {
		return ino
	}
	// Start at 2 — 1 is typically the root.
	if im.next < 2 {
		im.next = 2
	}
	im.next++
	im.m[rel] = im.next
	return im.next
}

func (im *inodeMap) forget(rel string) {
	im.mu.Lock()
	delete(im.m, rel)
	im.mu.Unlock()
}

// rename the map entry from old to new.
func (im *inodeMap) rename(oldRel, newRel string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if ino, ok := im.m[oldRel]; ok {
		delete(im.m, oldRel)
		im.m[newRel] = ino
	}
}

// relHash is a fallback inode number based on FNV64 of the rel path.
// Used when inos map isn't in scope (rare).
func relHash(rel string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(rel))
	return h.Sum64()
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
		Ino:  pr.inos.get(fm.Rel),
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
	// Nlink: directories typically have at least 2; files have 1.
	if fm.Mode&syscall.S_IFMT == syscall.S_IFDIR {
		out.Nlink = 2
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
	err := r.meta.UpdateLocked(func(tx *bolt.Tx) error {
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
			if newSize < fm.Size {
				hotKeep, hotDead := truncateFragments(fm.HotFragments, newSize)
				coldKeep, coldDead := truncateFragments(fm.ColdFragments, newSize)
				fm.HotFragments = hotKeep
				fm.ColdFragments = coldKeep
				if err := AddLiveBytesTx(tx, hotDead, -1); err != nil {
					return err
				}
				if err := AddLiveBytesTx(tx, coldDead, -1); err != nil {
					return err
				}
			}
			fm.Size = newSize
		}
		fm.Version++
		attrFromMeta(fm, &out.Attr)
		return PutFileTx(tx, fm)
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
			Ino:  r.inos.get(fm.Rel),
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
	err := r.meta.UpdateLocked(func(tx *bolt.Tx) error {
		existing, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if existing != nil {
			// Truncate existing file if O_TRUNC.
			if flags&syscall.O_TRUNC != 0 {
				if err := AddLiveBytesTx(tx, existing.HotFragments, -1); err != nil {
					return err
				}
				if err := AddLiveBytesTx(tx, existing.ColdFragments, -1); err != nil {
					return err
				}
				existing.HotFragments = nil
				existing.ColdFragments = nil
				existing.Size = 0
				existing.Mtime = now
				existing.Ctime = now
				existing.Version++
				if err := PutFileTx(tx, existing); err != nil {
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
		return PutFileTx(tx, fm)
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
		err := r.meta.UpdateLocked(func(tx *bolt.Tx) error {
			cur, err := GetFileTx(tx, rel)
			if err != nil {
				return err
			}
			if cur == nil {
				return syscall.ENOENT
			}
			if err := AddLiveBytesTx(tx, cur.HotFragments, -1); err != nil {
				return err
			}
			if err := AddLiveBytesTx(tx, cur.ColdFragments, -1); err != nil {
				return err
			}
			cur.HotFragments = nil
			cur.ColdFragments = nil
			cur.Size = 0
			cur.Mtime = time.Now().UnixNano()
			cur.Ctime = cur.Mtime
			cur.Version++
			return PutFileTx(tx, cur)
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
	err := r.meta.UpdateLocked(func(tx *bolt.Tx) error {
		existing, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if existing != nil {
			return syscall.EEXIST
		}
		return PutFileTx(tx, fm)
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
	err := r.meta.UpdateLocked(func(tx *bolt.Tx) error {
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
		if err := AddLiveBytesTx(tx, fm.HotFragments, -1); err != nil {
			return err
		}
		if err := AddLiveBytesTx(tx, fm.ColdFragments, -1); err != nil {
			return err
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
	r.inos.forget(rel)
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
	err = r.meta.UpdateLocked(func(tx *bolt.Tx) error {
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
	r.inos.forget(rel)
	return 0
}

// --- Rename ---

var _ = (fs.NodeRenamer)((*placeNode)(nil))

func (n *placeNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	r := n.root()
	oldRel := joinRel(n.relPath(), name)
	np, ok := newParent.(*placeNode)
	if !ok {
		return syscall.EXDEV
	}
	newRel := joinRel(np.relPath(), newName)
	done := r.dbg.op("Rename", oldRel, "-> %q", newRel)
	err := r.meta.UpdateLocked(func(tx *bolt.Tx) error {
		fm, err := GetFileTx(tx, oldRel)
		if err != nil {
			return err
		}
		if fm == nil {
			return syscall.ENOENT
		}
		// If destination exists, overwrite it (unlink-like).
		if existing, err := GetFileTx(tx, newRel); err == nil && existing != nil {
			if existing.IsDir() {
				return syscall.EISDIR
			}
			if err := AddLiveBytesTx(tx, existing.HotFragments, -1); err != nil {
				return err
			}
			if err := AddLiveBytesTx(tx, existing.ColdFragments, -1); err != nil {
				return err
			}
			if err := DeleteFileTx(tx, newRel); err != nil {
				return err
			}
		}
		// If renaming a directory, also move all descendants.
		if fm.IsDir() {
			if err := renameSubtree(tx, oldRel, newRel); err != nil {
				return err
			}
		}
		// Move the entry itself.
		fm.Rel = newRel
		fm.Ctime = time.Now().UnixNano()
		fm.Version++
		if err := DeleteFileTx(tx, oldRel); err != nil {
			return err
		}
		return PutFileTx(tx, fm)
	})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			done(errno)
			return errno
		}
		done(fs_errno(err))
		return fs_errno(err)
	}
	r.inos.rename(oldRel, newRel)
	done(0)
	return 0
}

// renameSubtree walks all descendants of oldBase under the paths bucket and
// rewrites the path keys to be rooted at newBase. Inode entries are
// preserved (only their fm.Rel is updated to reflect the new primary path).
func renameSubtree(tx *bolt.Tx, oldBase, newBase string) error {
	cur := tx.Bucket(bucketPaths).Cursor()
	prefix := childKeyPrefix(oldBase)
	type pair struct {
		oldRel string
		newRel string
	}
	var work []pair
	for k, _ := cur.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = cur.Next() {
		oldRel := keyToRel(k)
		newRel := newBase + oldRel[len(oldBase):]
		work = append(work, pair{oldRel, newRel})
	}
	for _, p := range work {
		fm, err := GetFileTx(tx, p.oldRel)
		if err != nil {
			return err
		}
		if fm == nil {
			continue
		}
		if err := DeleteFileTx(tx, p.oldRel); err != nil {
			return err
		}
		fm.Rel = p.newRel
		if err := PutFileTx(tx, fm); err != nil {
			return err
		}
	}
	return nil
}

func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
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
	err := r.meta.UpdateLocked(func(tx *bolt.Tx) error {
		existing, err := GetFileTx(tx, rel)
		if err != nil {
			return err
		}
		if existing != nil {
			return syscall.EEXIST
		}
		return PutFileTx(tx, fm)
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
