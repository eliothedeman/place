// Package store is the L4 file API of the place storage stack. It is the
// single surface that L5 adapters (FUSE today, anything tomorrow) call. It
// owns the path → inode mapping, directory tree, and per-file metadata
// (mode, nlinks, size, times, symlink targets) — everything that gives the
// L2 index its filesystem semantics.
//
// Store keys live in the same pebble DB the index owns; the index exposes
// its DB() for this exact purpose. The two layers use disjoint key tags
// (kv.NodeKey/DirentKey/etc. for L4, kv.StripeKey for L2) so neither layer
// reaches into the other's state.
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eliothedeman/place/index"
	"github.com/eliothedeman/place/kv"
	"github.com/eliothedeman/place/segment"
)

// Standard mode bits (a subset of <sys/stat.h>). Mirrored here so callers
// don't have to import syscall for the common case.
const (
	ModeRegular Mode = 0o100000
	ModeDir     Mode = 0o040000
	ModeSymlink Mode = 0o120000
	ModeMask    Mode = 0o170000 // S_IFMT
	PermMask    Mode = 0o7777
)

// Mode is the unix mode bits + file type bits.
type Mode uint32

func (m Mode) IsDir() bool     { return m&ModeMask == ModeDir }
func (m Mode) IsRegular() bool { return m&ModeMask == ModeRegular }
func (m Mode) IsSymlink() bool { return m&ModeMask == ModeSymlink }

// RootInode is the well-known root directory id.
const RootInode uint64 = 1

// Node is the per-inode metadata stored under kv.NodeKey(inode).
type Node struct {
	Inode         uint64
	Mode          Mode
	Nlink         uint32
	UID           uint32
	GID           uint32
	Size          int64
	Mtime         int64 // unix nanos
	Ctime         int64
	Atime         int64
	SymlinkTarget string // populated iff Mode.IsSymlink()
}

// DirEntry is one entry in a directory listing.
type DirEntry struct {
	Name  string
	Inode uint64
	Mode  Mode
}

// Store is the L4 handle. Constructed with Open.
type Store struct {
	idx *index.Index
	db  *kv.DB

	// openMu guards path lookups against concurrent rename, since rename is
	// otherwise the only operation that violates "ancestor exists ⇒ stays
	// valid for the duration of a path walk."
	openMu sync.RWMutex
}

// Open builds a Store on top of an already-open index.
func Open(idx *index.Index) (*Store, error) {
	s := &Store{idx: idx, db: idx.DB()}
	// Seed the inode allocator and create the root node if absent.
	v, err := s.db.Get(kv.StoreNextInodeKey)
	if err != nil {
		return nil, err
	}
	var next uint64 = RootInode + 1
	if len(v) == 8 {
		next = binary.LittleEndian.Uint64(v)
		if next < RootInode+1 {
			next = RootInode + 1
		}
	}
	batch := s.db.NewBatch()
	var nb [8]byte
	binary.LittleEndian.PutUint64(nb[:], next)
	if err := batch.Set(kv.StoreNextInodeKey, nb[:]); err != nil {
		batch.Close()
		return nil, err
	}
	if rv, err := s.db.Get(kv.NodeKey(RootInode)); err != nil {
		batch.Close()
		return nil, err
	} else if rv == nil {
		now := time.Now().UnixNano()
		root := Node{
			Inode: RootInode,
			Mode:  ModeDir | 0o755,
			Nlink: 2,
			Size:  0,
			Mtime: now, Ctime: now, Atime: now,
		}
		if err := batch.Set(kv.NodeKey(RootInode), encodeNode(root)); err != nil {
			batch.Close()
			return nil, err
		}
	}
	if err := batch.Commit(true); err != nil {
		return nil, err
	}
	return s, nil
}

// Close closes the underlying index (which closes the DB).
func (s *Store) Close() error {
	return s.idx.Close()
}

// Index exposes the L2 index for layers (L3 movers, tests) that need the
// lower-level vocabulary. Callers should not normally need this.
func (s *Store) Index() *index.Index { return s.idx }

// --- node encoding ------------------------------------------------------

const nodeFixedSize = 58

func encodeNode(n Node) []byte {
	buf := make([]byte, nodeFixedSize+len(n.SymlinkTarget))
	binary.LittleEndian.PutUint64(buf[0:], n.Inode)
	binary.LittleEndian.PutUint32(buf[8:], uint32(n.Mode))
	binary.LittleEndian.PutUint32(buf[12:], n.Nlink)
	binary.LittleEndian.PutUint32(buf[16:], n.UID)
	binary.LittleEndian.PutUint32(buf[20:], n.GID)
	binary.LittleEndian.PutUint64(buf[24:], uint64(n.Size))
	binary.LittleEndian.PutUint64(buf[32:], uint64(n.Mtime))
	binary.LittleEndian.PutUint64(buf[40:], uint64(n.Ctime))
	binary.LittleEndian.PutUint64(buf[48:], uint64(n.Atime))
	binary.LittleEndian.PutUint16(buf[56:], uint16(len(n.SymlinkTarget)))
	copy(buf[nodeFixedSize:], n.SymlinkTarget)
	return buf
}

func decodeNode(buf []byte) (Node, error) {
	if len(buf) < nodeFixedSize {
		return Node{}, fmt.Errorf("store: node blob too short (%d bytes)", len(buf))
	}
	n := Node{
		Inode: binary.LittleEndian.Uint64(buf[0:]),
		Mode:  Mode(binary.LittleEndian.Uint32(buf[8:])),
		Nlink: binary.LittleEndian.Uint32(buf[12:]),
		UID:   binary.LittleEndian.Uint32(buf[16:]),
		GID:   binary.LittleEndian.Uint32(buf[20:]),
		Size:  int64(binary.LittleEndian.Uint64(buf[24:])),
		Mtime: int64(binary.LittleEndian.Uint64(buf[32:])),
		Ctime: int64(binary.LittleEndian.Uint64(buf[40:])),
		Atime: int64(binary.LittleEndian.Uint64(buf[48:])),
	}
	sl := binary.LittleEndian.Uint16(buf[56:])
	if int(sl) > len(buf)-nodeFixedSize {
		return Node{}, fmt.Errorf("store: node symlink len %d > available %d", sl, len(buf)-nodeFixedSize)
	}
	n.SymlinkTarget = string(buf[nodeFixedSize : nodeFixedSize+int(sl)])
	return n, nil
}

// allocInode reserves a fresh inode id. Reads the current counter from
// the DB, returns it, and queues the bumped value in the batch. The bump
// only becomes durable when the batch commits, so a Rollback (Close
// without Commit) leaves the counter where it was.
func (s *Store) allocInode(b *kv.Batch) (uint64, error) {
	v, err := s.db.Get(kv.StoreNextInodeKey)
	if err != nil {
		return 0, err
	}
	var next uint64 = RootInode + 1
	if len(v) == 8 {
		next = binary.LittleEndian.Uint64(v)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], next+1)
	if err := b.Set(kv.StoreNextInodeKey, buf[:]); err != nil {
		return 0, err
	}
	return next, nil
}

// --- error sentinels ----------------------------------------------------

// ErrNotExist mirrors syscall.ENOENT for missing entries.
var ErrNotExist = syscall.ENOENT

// ErrExist mirrors EEXIST.
var ErrExist = syscall.EEXIST

// ErrNotDir mirrors ENOTDIR.
var ErrNotDir = syscall.ENOTDIR

// ErrIsDir mirrors EISDIR.
var ErrIsDir = syscall.EISDIR

// ErrNotEmpty mirrors ENOTEMPTY.
var ErrNotEmpty = syscall.ENOTEMPTY

// ErrInvalid mirrors EINVAL.
var ErrInvalid = syscall.EINVAL

// --- read helpers -------------------------------------------------------

// Stat returns the node metadata for inode.
func (s *Store) Stat(inode uint64) (Node, error) {
	v, err := s.db.Get(kv.NodeKey(inode))
	if err != nil {
		return Node{}, err
	}
	if v == nil {
		return Node{}, ErrNotExist
	}
	return decodeNode(v)
}

// readNode is a small helper for code paths inside a write batch that
// want the parent-DB state for an inode. (Batch.Get goes to the parent.)
func (s *Store) readNode(inode uint64) (Node, error) {
	return s.Stat(inode)
}

// requireDir checks that inode exists and is a directory.
func (s *Store) requireDir(inode uint64) error {
	n, err := s.Stat(inode)
	if err != nil {
		return err
	}
	if !n.Mode.IsDir() {
		return ErrNotDir
	}
	return nil
}

// readDirent returns the child inode encoded in a dirent value, or 0/false
// if the dirent is absent.
func (s *Store) readDirent(parent uint64, name string) (uint64, bool, error) {
	v, err := s.db.Get(kv.DirentKey(parent, name))
	if err != nil {
		return 0, false, err
	}
	if v == nil {
		return 0, false, nil
	}
	if len(v) != 8 {
		return 0, false, fmt.Errorf("store: dirent blob %d bytes (want 8)", len(v))
	}
	return binary.BigEndian.Uint64(v), true, nil
}

// Lookup resolves one (parent, name) pair to a child node.
func (s *Store) Lookup(parent uint64, name string) (Node, error) {
	if err := validName(name); err != nil {
		return Node{}, err
	}
	child, ok, err := s.readDirent(parent, name)
	if err != nil {
		return Node{}, err
	}
	if !ok {
		return Node{}, ErrNotExist
	}
	n, err := s.Stat(child)
	if err != nil {
		return Node{}, fmt.Errorf("store: dangling dirent %d/%q → inode %d", parent, name, child)
	}
	return n, nil
}

// LookupPath walks "/a/b/c" from root and returns the leaf node. An empty
// or "/" path returns the root.
func (s *Store) LookupPath(p string) (Node, error) {
	parts := splitPath(p)
	s.openMu.RLock()
	defer s.openMu.RUnlock()
	node, err := s.Stat(RootInode)
	if err != nil {
		return Node{}, err
	}
	for _, name := range parts {
		if !node.Mode.IsDir() {
			return Node{}, ErrNotDir
		}
		child, err := s.Lookup(node.Inode, name)
		if err != nil {
			return Node{}, err
		}
		node = child
	}
	return node, nil
}

func splitPath(p string) []string {
	p = path.Clean("/" + p)
	if p == "/" {
		return nil
	}
	return strings.Split(strings.TrimPrefix(p, "/"), "/")
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') || strings.ContainsRune(name, 0) {
		return ErrInvalid
	}
	return nil
}

// direntValue encodes a child inode for storage as a dirent value.
func direntValue(inode uint64) []byte {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], inode)
	return v[:]
}

// --- create / mkdir / symlink / link ------------------------------------

// Create makes a new regular file at (parent, name). Returns the new node
// and an open Handle. mode's permission bits are taken from mode & 0o7777;
// the type bits are forced to S_IFREG. The new file is owned by (uid, gid).
func (s *Store) Create(parent uint64, name string, mode Mode, uid, gid uint32) (Node, *Handle, error) {
	if err := validName(name); err != nil {
		return Node{}, nil, err
	}
	if err := s.requireDir(parent); err != nil {
		return Node{}, nil, err
	}
	if _, ok, err := s.readDirent(parent, name); err != nil {
		return Node{}, nil, err
	} else if ok {
		return Node{}, nil, ErrExist
	}
	b := s.db.NewBatch()
	id, err := s.allocInode(b)
	if err != nil {
		b.Close()
		return Node{}, nil, err
	}
	now := time.Now().UnixNano()
	n := Node{
		Inode: id,
		Mode:  ModeRegular | (mode & PermMask),
		Nlink: 1,
		UID:   uid,
		GID:   gid,
		Size:  0,
		Mtime: now, Ctime: now, Atime: now,
	}
	if err := b.Set(kv.NodeKey(id), encodeNode(n)); err != nil {
		b.Close()
		return Node{}, nil, err
	}
	if err := b.Set(kv.DirentKey(parent, name), direntValue(id)); err != nil {
		b.Close()
		return Node{}, nil, err
	}
	if err := b.Commit(true); err != nil {
		return Node{}, nil, err
	}
	return n, &Handle{store: s, inode: n.Inode}, nil
}

// Mkdir creates a directory at (parent, name) owned by (uid, gid).
func (s *Store) Mkdir(parent uint64, name string, mode Mode, uid, gid uint32) (Node, error) {
	if err := validName(name); err != nil {
		return Node{}, err
	}
	parentNode, err := s.Stat(parent)
	if err != nil {
		return Node{}, err
	}
	if !parentNode.Mode.IsDir() {
		return Node{}, ErrNotDir
	}
	if _, ok, err := s.readDirent(parent, name); err != nil {
		return Node{}, err
	} else if ok {
		return Node{}, ErrExist
	}
	b := s.db.NewBatch()
	id, err := s.allocInode(b)
	if err != nil {
		b.Close()
		return Node{}, err
	}
	now := time.Now().UnixNano()
	n := Node{
		Inode: id,
		Mode:  ModeDir | (mode & PermMask),
		Nlink: 2,
		UID:   uid,
		GID:   gid,
		Mtime: now, Ctime: now, Atime: now,
	}
	if err := b.Set(kv.NodeKey(id), encodeNode(n)); err != nil {
		b.Close()
		return Node{}, err
	}
	if err := b.Set(kv.DirentKey(parent, name), direntValue(id)); err != nil {
		b.Close()
		return Node{}, err
	}
	// Bump parent nlink (one more directory child).
	parentNode.Nlink++
	parentNode.Ctime = now
	if err := b.Set(kv.NodeKey(parent), encodeNode(parentNode)); err != nil {
		b.Close()
		return Node{}, err
	}
	if err := b.Commit(true); err != nil {
		return Node{}, err
	}
	return n, nil
}

// Symlink creates a symlink at (parent, name) pointing to target, owned
// by (uid, gid).
func (s *Store) Symlink(parent uint64, name, target string, uid, gid uint32) (Node, error) {
	if err := validName(name); err != nil {
		return Node{}, err
	}
	if err := s.requireDir(parent); err != nil {
		return Node{}, err
	}
	if _, ok, err := s.readDirent(parent, name); err != nil {
		return Node{}, err
	} else if ok {
		return Node{}, ErrExist
	}
	b := s.db.NewBatch()
	id, err := s.allocInode(b)
	if err != nil {
		b.Close()
		return Node{}, err
	}
	now := time.Now().UnixNano()
	n := Node{
		Inode:         id,
		Mode:          ModeSymlink | 0o777,
		Nlink:         1,
		UID:           uid,
		GID:           gid,
		Size:          int64(len(target)),
		Mtime:         now, Ctime: now, Atime: now,
		SymlinkTarget: target,
	}
	if err := b.Set(kv.NodeKey(id), encodeNode(n)); err != nil {
		b.Close()
		return Node{}, err
	}
	if err := b.Set(kv.DirentKey(parent, name), direntValue(id)); err != nil {
		b.Close()
		return Node{}, err
	}
	if err := b.Commit(true); err != nil {
		return Node{}, err
	}
	return n, nil
}

// Readlink returns the target string of a symlink inode.
func (s *Store) Readlink(inode uint64) (string, error) {
	n, err := s.Stat(inode)
	if err != nil {
		return "", err
	}
	if !n.Mode.IsSymlink() {
		return "", ErrInvalid
	}
	return n.SymlinkTarget, nil
}

// Link makes another dir entry pointing at an existing inode (hardlink).
func (s *Store) Link(targetInode, newParent uint64, newName string) (Node, error) {
	if err := validName(newName); err != nil {
		return Node{}, err
	}
	tn, err := s.Stat(targetInode)
	if err != nil {
		return Node{}, err
	}
	if tn.Mode.IsDir() {
		return Node{}, syscall.EPERM // POSIX disallows directory hardlinks
	}
	if err := s.requireDir(newParent); err != nil {
		return Node{}, err
	}
	if _, ok, err := s.readDirent(newParent, newName); err != nil {
		return Node{}, err
	} else if ok {
		return Node{}, ErrExist
	}
	tn.Nlink++
	tn.Ctime = time.Now().UnixNano()
	b := s.db.NewBatch()
	if err := b.Set(kv.NodeKey(targetInode), encodeNode(tn)); err != nil {
		b.Close()
		return Node{}, err
	}
	if err := b.Set(kv.DirentKey(newParent, newName), direntValue(targetInode)); err != nil {
		b.Close()
		return Node{}, err
	}
	if err := b.Commit(true); err != nil {
		return Node{}, err
	}
	return tn, nil
}

// --- unlink / rmdir / rename --------------------------------------------

// Unlink removes a regular-file or symlink entry. If nlink hits zero, the
// inode's node entry is dropped and L2 fragments are deleted.
func (s *Store) Unlink(parent uint64, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	child, ok, err := s.readDirent(parent, name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotExist
	}
	n, err := s.Stat(child)
	if err != nil {
		return fmt.Errorf("store: dangling dirent %d/%q", parent, name)
	}
	if n.Mode.IsDir() {
		return ErrIsDir
	}
	b := s.db.NewBatch()
	if err := b.Delete(kv.DirentKey(parent, name)); err != nil {
		b.Close()
		return err
	}
	n.Nlink--
	n.Ctime = time.Now().UnixNano()
	var freeInode uint64
	if n.Nlink == 0 {
		freeInode = child
		if err := b.Delete(kv.NodeKey(child)); err != nil {
			b.Close()
			return err
		}
	} else {
		if err := b.Set(kv.NodeKey(child), encodeNode(n)); err != nil {
			b.Close()
			return err
		}
	}
	if err := b.Commit(true); err != nil {
		return err
	}
	if freeInode != 0 {
		return s.idx.DeleteInode(freeInode)
	}
	return nil
}

// hasDirentChildren reports whether any dirent exists under parent. Used
// for the "directory must be empty" precondition on Rmdir/Rename.
func (s *Store) hasDirentChildren(parent uint64) (bool, error) {
	prefix := kv.DirentPrefix(parent)
	it, err := s.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return false, err
	}
	defer it.Close()
	return it.First(), nil
}

// Rmdir removes an empty directory entry.
func (s *Store) Rmdir(parent uint64, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	child, ok, err := s.readDirent(parent, name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotExist
	}
	n, err := s.Stat(child)
	if err != nil {
		return fmt.Errorf("store: dangling dirent %d/%q", parent, name)
	}
	if !n.Mode.IsDir() {
		return ErrNotDir
	}
	hasChildren, err := s.hasDirentChildren(child)
	if err != nil {
		return err
	}
	if hasChildren {
		return ErrNotEmpty
	}
	parentNode, err := s.Stat(parent)
	if err != nil {
		return err
	}
	parentNode.Nlink--
	parentNode.Ctime = time.Now().UnixNano()
	b := s.db.NewBatch()
	if err := b.Delete(kv.DirentKey(parent, name)); err != nil {
		b.Close()
		return err
	}
	if err := b.Delete(kv.NodeKey(child)); err != nil {
		b.Close()
		return err
	}
	if err := b.Set(kv.NodeKey(parent), encodeNode(parentNode)); err != nil {
		b.Close()
		return err
	}
	return b.Commit(true)
}

// Rename moves (oldParent, oldName) to (newParent, newName). If the target
// exists, it's replaced (subject to POSIX: dir-over-dir only if empty,
// nondir-over-dir / dir-over-nondir disallowed).
func (s *Store) Rename(oldParent uint64, oldName string, newParent uint64, newName string) error {
	if err := validName(oldName); err != nil {
		return err
	}
	if err := validName(newName); err != nil {
		return err
	}
	s.openMu.Lock()
	defer s.openMu.Unlock()

	srcInode, ok, err := s.readDirent(oldParent, oldName)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotExist
	}
	srcNode, err := s.Stat(srcInode)
	if err != nil {
		return fmt.Errorf("store: dangling src dirent")
	}
	if err := s.requireDir(newParent); err != nil {
		return err
	}

	// Prevent moving a directory into its own subtree (cycle).
	if srcNode.Mode.IsDir() {
		if newParent == srcInode {
			return ErrInvalid
		}
		isDesc, err := s.isDescendant(newParent, srcInode)
		if err != nil {
			return err
		}
		if isDesc {
			return ErrInvalid
		}
	}

	b := s.db.NewBatch()
	var freeInode uint64

	if dstInode, ok, err := s.readDirent(newParent, newName); err != nil {
		b.Close()
		return err
	} else if ok {
		if dstInode == srcInode {
			// Renaming to itself: no-op.
			b.Close()
			return nil
		}
		dstNode, err := s.Stat(dstInode)
		if err != nil {
			b.Close()
			return fmt.Errorf("store: dangling dst dirent")
		}
		if srcNode.Mode.IsDir() != dstNode.Mode.IsDir() {
			b.Close()
			if srcNode.Mode.IsDir() {
				return ErrNotDir
			}
			return ErrIsDir
		}
		if dstNode.Mode.IsDir() {
			hasChildren, err := s.hasDirentChildren(dstInode)
			if err != nil {
				b.Close()
				return err
			}
			if hasChildren {
				b.Close()
				return ErrNotEmpty
			}
			if err := b.Delete(kv.NodeKey(dstInode)); err != nil {
				b.Close()
				return err
			}
			// New parent's nlink decreases (its dir-child went away).
			np, err := s.Stat(newParent)
			if err != nil {
				b.Close()
				return err
			}
			np.Nlink--
			np.Ctime = time.Now().UnixNano()
			if err := b.Set(kv.NodeKey(newParent), encodeNode(np)); err != nil {
				b.Close()
				return err
			}
		} else {
			dstNode.Nlink--
			dstNode.Ctime = time.Now().UnixNano()
			if dstNode.Nlink == 0 {
				freeInode = dstInode
				if err := b.Delete(kv.NodeKey(dstInode)); err != nil {
					b.Close()
					return err
				}
			} else {
				if err := b.Set(kv.NodeKey(dstInode), encodeNode(dstNode)); err != nil {
					b.Close()
					return err
				}
			}
		}
	}

	// Apply the move.
	if err := b.Delete(kv.DirentKey(oldParent, oldName)); err != nil {
		b.Close()
		return err
	}
	if err := b.Set(kv.DirentKey(newParent, newName), direntValue(srcInode)); err != nil {
		b.Close()
		return err
	}
	// nlink for parent dirs (if src is a directory and parents differ).
	if srcNode.Mode.IsDir() && oldParent != newParent {
		op, err := s.Stat(oldParent)
		if err != nil {
			b.Close()
			return err
		}
		op.Nlink--
		op.Ctime = time.Now().UnixNano()
		if err := b.Set(kv.NodeKey(oldParent), encodeNode(op)); err != nil {
			b.Close()
			return err
		}
		np, err := s.Stat(newParent)
		if err != nil {
			b.Close()
			return err
		}
		np.Nlink++
		np.Ctime = time.Now().UnixNano()
		if err := b.Set(kv.NodeKey(newParent), encodeNode(np)); err != nil {
			b.Close()
			return err
		}
	}
	if err := b.Commit(true); err != nil {
		return err
	}
	if freeInode != 0 {
		return s.idx.DeleteInode(freeInode)
	}
	return nil
}

// isDescendant returns true if candidate lives somewhere underneath root.
// Used by Rename to refuse cycle-creating moves like rename(/a, /a/b/c).
// O(N) in dirent count; only invoked when renaming directories.
func (s *Store) isDescendant(candidate, root uint64) (bool, error) {
	cur := candidate
	for i := 0; i < 1000 && cur != RootInode; i++ {
		parent, ok, err := s.findParent(cur)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if parent == root {
			return true, nil
		}
		cur = parent
	}
	return false, nil
}

// findParent scans every dirent looking for one whose value matches inode,
// returning the parent embedded in the key. Linear in total dirent count,
// run only at rename time — fine.
func (s *Store) findParent(inode uint64) (uint64, bool, error) {
	want := direntValue(inode)
	prefix := []byte{'d'} // kv.tagDirent — full dirent space
	it, err := s.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return 0, false, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if equalBytes(it.Value(), want) {
			return kv.DirentParentFromKey(it.Key()), true, nil
		}
	}
	return 0, false, nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- attrs --------------------------------------------------------------

// SetAttr describes which fields to update. A nil pointer means "leave
// alone." Times in unix nanos.
type SetAttr struct {
	Mode  *Mode
	UID   *uint32
	GID   *uint32
	Size  *int64
	Mtime *int64
	Atime *int64
}

// Setattr updates inode metadata. Size changes truncate or zero-extend the
// file via L2.
//
// Truncation is intentionally ordered "fragments first, size last": we
// drop the now-dead fragments before advertising the smaller Size, so a
// crash between the two leaves at most "fragments past advertised end of
// file" (harmless — readers clamp by Size and GC reaps the orphaned
// segments) rather than "Size advertises bytes that no fragment covers"
// (silent zero-fill).
func (s *Store) Setattr(inode uint64, sa SetAttr) (Node, error) {
	if sa.Size != nil {
		cur, err := s.Stat(inode)
		if err != nil {
			return Node{}, err
		}
		if *sa.Size < cur.Size {
			if err := s.idx.Truncate(inode, *sa.Size); err != nil {
				return Node{}, err
			}
		}
	}
	nn, err := s.Stat(inode)
	if err != nil {
		return Node{}, err
	}
	now := time.Now().UnixNano()
	if sa.Mode != nil {
		nn.Mode = (nn.Mode & ModeMask) | (*sa.Mode & PermMask)
	}
	if sa.UID != nil {
		nn.UID = *sa.UID
	}
	if sa.GID != nil {
		nn.GID = *sa.GID
	}
	if sa.Mtime != nil {
		nn.Mtime = *sa.Mtime
	}
	if sa.Atime != nil {
		nn.Atime = *sa.Atime
	}
	if sa.Size != nil {
		nn.Size = *sa.Size
	}
	nn.Ctime = now
	b := s.db.NewBatch()
	if err := b.Set(kv.NodeKey(inode), encodeNode(nn)); err != nil {
		b.Close()
		return Node{}, err
	}
	if err := b.Commit(true); err != nil {
		return Node{}, err
	}
	return nn, nil
}

// --- readdir ------------------------------------------------------------

// Readdir returns the children of a directory.
func (s *Store) Readdir(inode uint64) ([]DirEntry, error) {
	dn, err := s.Stat(inode)
	if err != nil {
		return nil, err
	}
	if !dn.Mode.IsDir() {
		return nil, ErrNotDir
	}
	prefix := kv.DirentPrefix(inode)
	it, err := s.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []DirEntry
	for it.First(); it.Valid(); it.Next() {
		name := kv.DirentNameFromKey(it.Key())
		v := it.Value()
		if len(v) != 8 {
			continue
		}
		childInode := binary.BigEndian.Uint64(v)
		// Read the child node. A short scan calling Stat per entry is
		// fine here — Pebble Get on hot metadata is sub-µs from the
		// block cache, and readdir is not a hot path.
		cn, err := s.Stat(childInode)
		if err != nil {
			// Skip dangling entries rather than failing readdir.
			continue
		}
		out = append(out, DirEntry{Name: name, Inode: childInode, Mode: cn.Mode})
	}
	return out, nil
}

// OpenInode returns a Handle to an existing inode. flags is reserved for
// future O_APPEND-style semantics; ignored today.
func (s *Store) OpenInode(inode uint64, flags int) (*Handle, error) {
	if _, err := s.Stat(inode); err != nil {
		return nil, err
	}
	return &Handle{store: s, inode: inode}, nil
}

// Statfs reports the underlying disk usage for the hot tier. Cold is
// excluded by design — POSIX statfs reflects the filesystem the user is
// looking at, which for hot-heavy workloads is the active write area.
// Adapters can post-process this if they want combined stats.
func (s *Store) Statfs() (Statfs, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.idx.HotDir(), &st); err != nil {
		return Statfs{}, err
	}
	return Statfs{
		BlockSize:   uint32(st.Bsize),
		Blocks:      st.Blocks,
		BlocksFree:  st.Bfree,
		BlocksAvail: st.Bavail,
	}, nil
}

// Statfs is the lightweight filesystem-usage struct returned by Store.Statfs.
type Statfs struct {
	BlockSize   uint32
	Blocks      uint64
	BlocksFree  uint64
	BlocksAvail uint64
}

// Ensure these aren't unused-warned.
var _ = errors.New
var _ = segment.TierHot
