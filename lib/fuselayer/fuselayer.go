// Package fuselayer is the L5 kernel-protocol adapter. It translates go-fuse
// callbacks to lib/store calls. There should be no business logic here —
// every method is a one-shot translation. If you find yourself reaching for
// the index or a segment directly, fix the layering instead.
//
// Every FUSE entrypoint is wrapped by trackOp, which:
//   - increments per-op counters in Metrics (count + error count + total
//     duration), exposed via /metrics; and
//   - emits a debug log line if the call took longer than SlowOpThreshold.
//
// The wrapping costs one map lookup + two atomic adds per call — negligible
// next to the bbolt + segment IO it brackets, and pays for itself the first
// time you need to ask "is the FS slow, and which op is the slow one?"
package fuselayer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"syscall"
	"time"

	"github.com/eliothedeman/place/lib/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Options configures the FUSE root. Zero value is valid — defaults to no
// metrics and discard logging.
type Options struct {
	// Metrics is optional; if nil, op tracking is disabled.
	Metrics *Metrics
	// Logger is optional; if nil, a discard logger is used.
	Logger *slog.Logger
	// SlowOpThreshold: ops slower than this log at debug level. 0 disables
	// the threshold log; counters still apply.
	SlowOpThreshold time.Duration
}

// NewRoot constructs the root NodeFS handler for a place filesystem backed
// by s. Mount with:
//
//	server, _ := fs.Mount(mountPoint, fuselayer.NewRoot(s, fuselayer.Options{}), nil)
func NewRoot(s *store.Store, opts Options) fs.InodeEmbedder {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	r := &root{opts: opts}
	return &node{root: r, store: s, inode: store.RootInode}
}

// root holds the per-mount shared state. Each node carries a pointer to
// it so the wrappers and helpers can reach metrics + the logger without
// taking them as args everywhere.
type root struct {
	opts Options
}

// node is a fs.Inode handler bound to one of the store's inodes.
type node struct {
	fs.Inode
	root  *root
	store *store.Store
	inode uint64
}

// trackOp wraps an op, recording its duration + outcome in metrics, and
// logging slow ops at debug. Pass a closure that returns the syscall.Errno.
// The returned Errno is whatever the closure produced — trackOp is a
// passthrough.
func (r *root) trackOp(name string, fn func() syscall.Errno) syscall.Errno {
	if r.opts.Metrics == nil && r.opts.SlowOpThreshold == 0 {
		return fn()
	}
	start := time.Now()
	e := fn()
	d := time.Since(start)
	if r.opts.Metrics != nil {
		r.opts.Metrics.Observe(name, d, e != 0)
	}
	if r.opts.SlowOpThreshold > 0 && d >= r.opts.SlowOpThreshold {
		r.opts.Logger.Debug("slow fuse op", "op", name, "duration", d, "errno", int(e))
	}
	return e
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
	var resInode *fs.Inode
	var resErrno syscall.Errno
	n.root.trackOp("lookup", func() syscall.Errno {
		child, err := n.store.Lookup(n.inode, name)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		fillAttr(child, &out.Attr)
		resInode = n.NewInode(ctx, &node{root: n.root, store: n.store, inode: child.Inode}, stableAttr(child))
		return 0
	})
	return resInode, resErrno
}

var _ fs.NodeGetattrer = (*node)(nil)

func (n *node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return n.root.trackOp("getattr", func() syscall.Errno {
		nn, err := n.store.Stat(n.inode)
		if err != nil {
			return errno(err)
		}
		fillAttr(nn, &out.Attr)
		return 0
	})
}

var _ fs.NodeSetattrer = (*node)(nil)

func (n *node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return n.root.trackOp("setattr", func() syscall.Errno {
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
	})
}

// --- Create / Open / Read / Write / Flush / Fsync / Release ---

var _ fs.NodeCreater = (*node)(nil)

func (n *node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	var resInode *fs.Inode
	var resFile fs.FileHandle
	var resErrno syscall.Errno
	n.root.trackOp("create", func() syscall.Errno {
		uid, gid := callerCreds(ctx)
		nn, h, err := n.store.Create(n.inode, name, store.Mode(mode), uid, gid)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		fillAttr(nn, &out.Attr)
		resInode = n.NewInode(ctx, &node{root: n.root, store: n.store, inode: nn.Inode}, stableAttr(nn))
		resFile = &handle{h: h, store: n.store, root: n.root}
		return 0
	})
	return resInode, resFile, 0, resErrno
}

// callerCreds returns the calling process's effective uid/gid via the
// FUSE context. If the caller is unavailable (shouldn't happen in real
// requests, but defends against tests), returns 0/0 — same as root,
// which preserves pre-uid-propagation behavior.
func callerCreds(ctx context.Context) (uint32, uint32) {
	if c, ok := fuse.FromContext(ctx); ok {
		return c.Uid, c.Gid
	}
	return 0, 0
}

var _ fs.NodeOpener = (*node)(nil)

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	var resFile fs.FileHandle
	var resErrno syscall.Errno
	n.root.trackOp("open", func() syscall.Errno {
		h, err := n.store.OpenInode(n.inode, int(flags))
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		resFile = &handle{h: h, store: n.store, root: n.root}
		return 0
	})
	return resFile, 0, resErrno
}

// handle is the FUSE FileHandle, delegating to store.Handle.
type handle struct {
	h     *store.Handle
	store *store.Store
	root  *root
}

var _ fs.FileReader = (*handle)(nil)

func (h *handle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	var res fuse.ReadResult
	var resErrno syscall.Errno
	h.root.trackOp("read", func() syscall.Errno {
		n, err := h.h.ReadAt(dest, off)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		res = fuse.ReadResultData(dest[:n])
		return 0
	})
	return res, resErrno
}

var _ fs.FileWriter = (*handle)(nil)

func (h *handle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	var written uint32
	var resErrno syscall.Errno
	h.root.trackOp("write", func() syscall.Errno {
		n, err := h.h.WriteAt(data, off)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		written = uint32(n)
		return 0
	})
	return written, resErrno
}

var _ fs.FileFlusher = (*handle)(nil)

// Flush fires on every close(2). Per-write fsyncs already happened on the
// WriteAt path, so this is a no-op.
func (h *handle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

var _ fs.FileFsyncer = (*handle)(nil)

func (h *handle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return h.root.trackOp("fsync", func() syscall.Errno {
		return errno(h.h.Sync())
	})
}

var _ fs.FileReleaser = (*handle)(nil)

func (h *handle) Release(ctx context.Context) syscall.Errno {
	return h.root.trackOp("release", func() syscall.Errno {
		return errno(h.h.Close())
	})
}

// --- Mkdir / Unlink / Rmdir / Rename ---

var _ fs.NodeMkdirer = (*node)(nil)

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var resInode *fs.Inode
	var resErrno syscall.Errno
	n.root.trackOp("mkdir", func() syscall.Errno {
		uid, gid := callerCreds(ctx)
		nn, err := n.store.Mkdir(n.inode, name, store.Mode(mode), uid, gid)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		fillAttr(nn, &out.Attr)
		resInode = n.NewInode(ctx, &node{root: n.root, store: n.store, inode: nn.Inode}, stableAttr(nn))
		return 0
	})
	return resInode, resErrno
}

var _ fs.NodeUnlinker = (*node)(nil)

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.root.trackOp("unlink", func() syscall.Errno {
		return errno(n.store.Unlink(n.inode, name))
	})
}

var _ fs.NodeRmdirer = (*node)(nil)

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.root.trackOp("rmdir", func() syscall.Errno {
		return errno(n.store.Rmdir(n.inode, name))
	})
}

var _ fs.NodeRenamer = (*node)(nil)

func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return n.root.trackOp("rename", func() syscall.Errno {
		np, ok := newParent.(*node)
		if !ok {
			return syscall.EINVAL
		}
		return errno(n.store.Rename(n.inode, name, np.inode, newName))
	})
}

// --- Symlink / Readlink / Link ---

var _ fs.NodeSymlinker = (*node)(nil)

func (n *node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var resInode *fs.Inode
	var resErrno syscall.Errno
	n.root.trackOp("symlink", func() syscall.Errno {
		uid, gid := callerCreds(ctx)
		nn, err := n.store.Symlink(n.inode, name, target, uid, gid)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		fillAttr(nn, &out.Attr)
		resInode = n.NewInode(ctx, &node{root: n.root, store: n.store, inode: nn.Inode}, stableAttr(nn))
		return 0
	})
	return resInode, resErrno
}

var _ fs.NodeReadlinker = (*node)(nil)

func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	var res []byte
	var resErrno syscall.Errno
	n.root.trackOp("readlink", func() syscall.Errno {
		t, err := n.store.Readlink(n.inode)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		res = []byte(t)
		return 0
	})
	return res, resErrno
}

var _ fs.NodeLinker = (*node)(nil)

func (n *node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var resInode *fs.Inode
	var resErrno syscall.Errno
	n.root.trackOp("link", func() syscall.Errno {
		tgt, ok := target.(*node)
		if !ok {
			resErrno = syscall.EINVAL
			return resErrno
		}
		nn, err := n.store.Link(tgt.inode, n.inode, name)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		fillAttr(nn, &out.Attr)
		resInode = n.NewInode(ctx, &node{root: n.root, store: n.store, inode: nn.Inode}, stableAttr(nn))
		return 0
	})
	return resInode, resErrno
}

// --- Readdir / Statfs ---

var _ fs.NodeReaddirer = (*node)(nil)

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	var resStream fs.DirStream
	var resErrno syscall.Errno
	n.root.trackOp("readdir", func() syscall.Errno {
		entries, err := n.store.Readdir(n.inode)
		if err != nil {
			resErrno = errno(err)
			return resErrno
		}
		fuseEnt := make([]fuse.DirEntry, 0, len(entries))
		for _, e := range entries {
			fuseEnt = append(fuseEnt, fuse.DirEntry{
				Name: e.Name,
				Ino:  e.Inode,
				Mode: uint32(e.Mode) & uint32(store.ModeMask),
			})
		}
		resStream = fs.NewListDirStream(fuseEnt)
		return 0
	})
	return resStream, resErrno
}

var _ fs.NodeStatfser = (*node)(nil)

func (n *node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return n.root.trackOp("statfs", func() syscall.Errno {
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
	})
}
