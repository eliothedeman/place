package place

import (
	"context"
	"io"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// writeBufPool holds reusable byte slices for the "copy incoming FUSE
// write data" step. The FUSE request buffer is only valid for the duration
// of the call, so we must copy — but we don't need to allocate each time.
var writeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1<<20) // typical MaxWrite is 1 MB
		return &b
	},
}

func getWriteBuf(n int) []byte {
	bp := writeBufPool.Get().(*[]byte)
	b := *bp
	if cap(b) < n {
		// Drop the undersized buffer and allocate.
		b = make([]byte, n)
	} else {
		b = b[:n]
	}
	return b
}

func putWriteBuf(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:0]
	writeBufPool.Put(&b)
}

// placeFile is a FUSE file handle backed by the userspace reader/writer.
// No passthrough — all I/O goes through our code.
type placeFile struct {
	rel      string
	root     *placeRoot
	writable bool
}

var _ = (fs.FileReader)((*placeFile)(nil))

func (f *placeFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	done := f.root.dbg.op("Read", f.rel, "off=%d len=%d", off, len(dest))
	n, err := f.root.reader.ReadAt(f.rel, dest, off)
	if err != nil && err != io.EOF {
		// On partial reads (one slice errored, others succeeded) Reader
		// returns (n>0, err). POSIX read(2) permits short returns, so
		// surface the survivor prefix as success — a subsequent read at
		// off+n will retry the failed range and either succeed or surface
		// EIO with n=0. Returning an errno alongside non-zero data would
		// be lost by the kernel anyway.
		if n > 0 {
			done(0, "bytes=%d partial err=%v", n, err)
			return fuse.ReadResultData(dest[:n]), 0
		}
		if errno, ok := err.(syscall.Errno); ok {
			done(errno)
			return nil, errno
		}
		done(fs_errno(err))
		return nil, fs_errno(err)
	}
	done(0, "bytes=%d", n)
	return fuse.ReadResultData(dest[:n]), 0
}

var _ = (fs.FileWriter)((*placeFile)(nil))

func (f *placeFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if !f.writable {
		return 0, syscall.EBADF
	}
	done := f.root.dbg.op("Write", f.rel, "off=%d len=%d", off, len(data))
	// Copy into a pooled buffer — go-fuse reuses its request buffer once we
	// return. Submit is synchronous, so the buffer is released back to the
	// pool as soon as the write has been copied into the segment scratch.
	buf := getWriteBuf(len(data))
	copy(buf, data)
	err := f.root.writer.Submit(f.rel, off, buf)
	putWriteBuf(buf)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			done(errno)
			return 0, errno
		}
		done(fs_errno(err))
		return 0, fs_errno(err)
	}
	done(0, "bytes=%d", len(data))
	return uint32(len(data)), 0
}

var _ = (fs.FileFlusher)((*placeFile)(nil))

func (f *placeFile) Flush(ctx context.Context) syscall.Errno {
	// Writes are synchronously group-committed; nothing to do here.
	return 0
}

var _ = (fs.FileFsyncer)((*placeFile)(nil))

func (f *placeFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if err := f.root.writer.Flush(); err != nil {
		return fs_errno(err)
	}
	return 0
}

var _ = (fs.FileReleaser)((*placeFile)(nil))

func (f *placeFile) Release(ctx context.Context) syscall.Errno {
	return 0
}

var _ = (fs.FileGetattrer)((*placeFile)(nil))

func (f *placeFile) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	fm, err := f.root.meta.GetFile(f.rel)
	if err != nil {
		return fs_errno(err)
	}
	if fm == nil {
		return syscall.ENOENT
	}
	attrFromMeta(fm, &out.Attr)
	return 0
}
