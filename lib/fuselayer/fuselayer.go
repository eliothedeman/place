// Package fuselayer is the L5 kernel-protocol adapter. It translates go-fuse
// callbacks to lib/store calls. There should be no business logic here —
// every method is a one-shot translation. If you find yourself reaching for
// the index or a segment directly, fix the layering instead.
package fuselayer

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/eliothedeman/place/lib/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NewRoot constructs the root NodeFS handler for a place filesystem backed
// by s. Mount with:
//
//	server, _ := fs.Mount(mountPoint, fuselayer.NewRoot(s), nil)
func NewRoot(s *store.Store) fs.InodeEmbedder {
	return &node{store: s, inode: store.RootInode}
}

// node is a fs.Inode handler bound to one of the store's inodes.
type node struct {
	fs.Inode
	store *store.Store
	inode uint64
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var e syscall.Errno
	if errors.As(err, &e) {
		return e
	}
	return syscall.EIO
}

func fillAttr(n store.Node, out *fuse.Attr) {
	out.Mode = uint32(n.Mode)
	out.Size = uint64(n.Size)
	out.Uid = n.UID
	out.Gid = n.GID
	out.Mtime = uint64(n.Mtime / 1e9)
	out.Mtimensec = uint32(n.Mtime % 1e9)
	out.Ctime = uint64(n.Ctime / 1e9)
	out.Ctimensec = uint32(n.Ctime % 1e9)
	out.Atime = uint64(n.Atime / 1e9)
	out.Atimensec = uint32(n.Atime % 1e9)
	if n.Mode.IsDir() {
		out.Nlink = 2
	} else if n.Nlink > 0 {
		out.Nlink = n.Nlink
	} else {
		out.Nlink = 1
	}
	out.Blksize = 4096
	out.Blocks = (out.Size + 511) / 512
}

func stableAttr(n store.Node) fs.StableAttr {
	return fs.StableAttr{Mode: uint32(n.Mode) & uint32(store.ModeMask), Ino: n.Inode, Gen: 1}
}

// --- Lookup / Getattr / Setattr ---

var _ fs.NodeLookuper = (*node)(nil)

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	child, err := n.store.Lookup(n.inode, name)
	if err != nil {
		return nil, errno(err)
	}
	fillAttr(child, &out.Attr)
	ino := n.NewInode(ctx, &node{store: n.store, inode: child.Inode}, stableAttr(child))
	return ino, 0
}

var _ fs.NodeGetattrer = (*node)(nil)

func (n *node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	nn, err := n.store.Stat(n.inode)
	if err != nil {
		return errno(err)
	}
	fillAttr(nn, &out.Attr)
	return 0
}

var _ fs.NodeSetattrer = (*node)(nil)

func (n *node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var sa store.SetAttr
	if mode, ok := in.GetMode(); ok {
		m := store.Mode(mode)
		sa.Mode = &m
	}
	if uid, ok := in.GetUID(); ok {
		sa.UID = &uid
	}
	if gid, ok := in.GetGID(); ok {
		sa.GID = &gid
	}
	if size, ok := in.GetSize(); ok {
		ss := int64(size)
		sa.Size = &ss
	}
	if mt, ok := in.GetMTime(); ok {
		nn := mt.UnixNano()
		sa.Mtime = &nn
	}
	if at, ok := in.GetATime(); ok {
		nn := at.UnixNano()
		sa.Atime = &nn
	}
	nn, err := n.store.Setattr(n.inode, sa)
	if err != nil {
		return errno(err)
	}
	fillAttr(nn, &out.Attr)
	return 0
}

// --- Create / Open / Read / Write / Flush / Fsync / Release ---

var _ fs.NodeCreater = (*node)(nil)

func (n *node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	nn, h, err := n.store.Create(n.inode, name, store.Mode(mode))
	if err != nil {
		return nil, nil, 0, errno(err)
	}
	fillAttr(nn, &out.Attr)
	ino := n.NewInode(ctx, &node{store: n.store, inode: nn.Inode}, stableAttr(nn))
	return ino, &handle{h: h, store: n.store}, 0, 0
}

var _ fs.NodeOpener = (*node)(nil)

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	h, err := n.store.OpenInode(n.inode, int(flags))
	if err != nil {
		return nil, 0, errno(err)
	}
	return &handle{h: h, store: n.store}, 0, 0
}

// handle is the FUSE FileHandle, delegating to store.Handle.
type handle struct {
	h     *store.Handle
	store *store.Store
}

var _ fs.FileReader = (*handle)(nil)

func (h *handle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.h.ReadAt(dest, off)
	if err != nil {
		return nil, errno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

var _ fs.FileWriter = (*handle)(nil)

func (h *handle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := h.h.WriteAt(data, off)
	if err != nil {
		return 0, errno(err)
	}
	return uint32(n), 0
}

var _ fs.FileFlusher = (*handle)(nil)

// Flush fires on every close(2) — including ones from briefly-opened
// readers — so it must be cheap. Per-write fsyncs already happened on the
// WriteAt path; the only outstanding bytes are inside an explicit
// BulkWriter session, which isn't reachable via the FUSE handle. So Flush
// is a no-op.
func (h *handle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

var _ fs.FileFsyncer = (*handle)(nil)

// Fsync is an explicit user-requested durability barrier (fsync(2)).
// Force a sync across all open segments + the bbolt index.
func (h *handle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return errno(h.h.Sync())
}

var _ fs.FileReleaser = (*handle)(nil)

func (h *handle) Release(ctx context.Context) syscall.Errno {
	return errno(h.h.Close())
}

// --- Mkdir / Unlink / Rmdir / Rename ---

var _ fs.NodeMkdirer = (*node)(nil)

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	nn, err := n.store.Mkdir(n.inode, name, store.Mode(mode))
	if err != nil {
		return nil, errno(err)
	}
	fillAttr(nn, &out.Attr)
	return n.NewInode(ctx, &node{store: n.store, inode: nn.Inode}, stableAttr(nn)), 0
}

var _ fs.NodeUnlinker = (*node)(nil)

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	return errno(n.store.Unlink(n.inode, name))
}

var _ fs.NodeRmdirer = (*node)(nil)

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return errno(n.store.Rmdir(n.inode, name))
}

var _ fs.NodeRenamer = (*node)(nil)

func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	np, ok := newParent.(*node)
	if !ok {
		return syscall.EINVAL
	}
	return errno(n.store.Rename(n.inode, name, np.inode, newName))
}

// --- Symlink / Readlink / Link ---

var _ fs.NodeSymlinker = (*node)(nil)

func (n *node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	nn, err := n.store.Symlink(n.inode, name, target)
	if err != nil {
		return nil, errno(err)
	}
	fillAttr(nn, &out.Attr)
	return n.NewInode(ctx, &node{store: n.store, inode: nn.Inode}, stableAttr(nn)), 0
}

var _ fs.NodeReadlinker = (*node)(nil)

func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	t, err := n.store.Readlink(n.inode)
	if err != nil {
		return nil, errno(err)
	}
	return []byte(t), 0
}

var _ fs.NodeLinker = (*node)(nil)

func (n *node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	tgt, ok := target.(*node)
	if !ok {
		return nil, syscall.EINVAL
	}
	nn, err := n.store.Link(tgt.inode, n.inode, name)
	if err != nil {
		return nil, errno(err)
	}
	fillAttr(nn, &out.Attr)
	return n.NewInode(ctx, &node{store: n.store, inode: nn.Inode}, stableAttr(nn)), 0
}

// --- Readdir / Statfs ---

var _ fs.NodeReaddirer = (*node)(nil)

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.store.Readdir(n.inode)
	if err != nil {
		return nil, errno(err)
	}
	fuseEnt := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		fuseEnt = append(fuseEnt, fuse.DirEntry{
			Name: e.Name,
			Ino:  e.Inode,
			Mode: uint32(e.Mode) & uint32(store.ModeMask),
		})
	}
	return fs.NewListDirStream(fuseEnt), 0
}

var _ fs.NodeStatfser = (*node)(nil)

func (n *node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	st, err := n.store.Statfs()
	if err != nil {
		return errno(err)
	}
	out.Bsize = st.BlockSize
	out.Blocks = st.Blocks
	out.Bfree = st.BlocksFree
	out.Bavail = st.BlocksAvail
	out.NameLen = 255
	return 0
}

// Avoid unused-import lint when build tags strip something.
var _ = time.Now
