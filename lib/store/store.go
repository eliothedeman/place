// Package store is the L4 file API of the place storage stack. It is the
// single surface that L5 adapters (FUSE today, anything tomorrow) call. It
// owns the path → inode mapping, directory tree, and per-file metadata
// (mode, nlinks, size, times, symlink targets) — everything that gives the
// L2 index its filesystem semantics.
//
// Store buckets live in the same bbolt DB that the index owns; the index
// exposes its DB() for this exact purpose. The buckets used here are kept
// disjoint from L2's stripes/_meta buckets so neither layer reaches into
// the other's state.
package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eliothedeman/place/lib/index"
	"github.com/eliothedeman/place/lib/segment"
	bolt "go.etcd.io/bbolt"
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

var (
	bucketL4Meta  = []byte("_l4_meta")
	bucketNodes   = []byte("nodes")
	bucketDirents = []byte("dirents")

	metaNextInode = []byte("next_inode")
)

// Node is the per-inode metadata stored in bucketNodes.
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
	db  *bolt.DB

	// openMu guards path lookups against concurrent rename, since rename is
	// otherwise the only operation that violates "ancestor exists ⇒ stays
	// valid for the duration of a path walk."
	openMu sync.RWMutex
}

// Open builds a Store on top of an already-open index.
func Open(idx *index.Index) (*Store, error) {
	s := &Store{idx: idx, db: idx.DB()}
	err := s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketL4Meta, bucketNodes, bucketDirents} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		// Ensure inode allocator has reserved RootInode = 1, then create the
		// root node if it's not already there.
		mb := tx.Bucket(bucketL4Meta)
		v := mb.Get(metaNextInode)
		var next uint64 = RootInode + 1
		if len(v) == 8 {
			next = binary.LittleEndian.Uint64(v)
			if next < RootInode+1 {
				next = RootInode + 1
			}
		}
		var nb [8]byte
		binary.LittleEndian.PutUint64(nb[:], next)
		if err := mb.Put(metaNextInode, nb[:]); err != nil {
			return err
		}
		nodes := tx.Bucket(bucketNodes)
		if nodes.Get(inodeKey(RootInode)) == nil {
			now := time.Now().UnixNano()
			root := Node{
				Inode: RootInode,
				Mode:  ModeDir | 0o755,
				Nlink: 2,
				Size:  0,
				Mtime: now, Ctime: now, Atime: now,
			}
			if err := nodes.Put(inodeKey(RootInode), encodeNode(root)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
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

// --- helpers ------------------------------------------------------------

func inodeKey(inode uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], inode)
	return k[:]
}

func direntKey(parent uint64, name string) []byte {
	k := make([]byte, 8+len(name))
	binary.BigEndian.PutUint64(k[:8], parent)
	copy(k[8:], name)
	return k
}

func parentPrefix(parent uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], parent)
	return k[:]
}

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

func allocInodeTx(tx *bolt.Tx) (uint64, error) {
	mb := tx.Bucket(bucketL4Meta)
	v := mb.Get(metaNextInode)
	var next uint64 = RootInode + 1
	if len(v) == 8 {
		next = binary.LittleEndian.Uint64(v)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], next+1)
	if err := mb.Put(metaNextInode, buf[:]); err != nil {
		return 0, err
	}
	return next, nil
}

// --- lookups ------------------------------------------------------------

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

// Stat returns the node metadata for inode.
func (s *Store) Stat(inode uint64) (Node, error) {
	var n Node
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketNodes).Get(inodeKey(inode))
		if v == nil {
			return ErrNotExist
		}
		var derr error
		n, derr = decodeNode(v)
		return derr
	})
	return n, err
}

// Lookup resolves one (parent, name) pair to a child node.
func (s *Store) Lookup(parent uint64, name string) (Node, error) {
	if err := validName(name); err != nil {
		return Node{}, err
	}
	var n Node
	err := s.db.View(func(tx *bolt.Tx) error {
		nv, err := lookupTx(tx, parent, name)
		if err != nil {
			return err
		}
		n = nv
		return nil
	})
	return n, err
}

func lookupTx(tx *bolt.Tx, parent uint64, name string) (Node, error) {
	dv := tx.Bucket(bucketDirents).Get(direntKey(parent, name))
	if dv == nil {
		return Node{}, ErrNotExist
	}
	if len(dv) != 8 {
		return Node{}, fmt.Errorf("store: dirent blob %d bytes (want 8)", len(dv))
	}
	child := binary.BigEndian.Uint64(dv)
	nv := tx.Bucket(bucketNodes).Get(inodeKey(child))
	if nv == nil {
		return Node{}, fmt.Errorf("store: dangling dirent %d/%q → inode %d", parent, name, child)
	}
	return decodeNode(nv)
}

// LookupPath walks "/a/b/c" from root and returns the leaf node. An empty
// or "/" path returns the root. Components are split on "/".
func (s *Store) LookupPath(p string) (Node, error) {
	parts := splitPath(p)
	s.openMu.RLock()
	defer s.openMu.RUnlock()
	var node Node
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketNodes).Get(inodeKey(RootInode))
		root, err := decodeNode(v)
		if err != nil {
			return err
		}
		node = root
		for _, name := range parts {
			if !node.Mode.IsDir() {
				return ErrNotDir
			}
			child, err := lookupTx(tx, node.Inode, name)
			if err != nil {
				return err
			}
			node = child
		}
		return nil
	})
	return node, err
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

// --- create / mkdir / symlink / link ------------------------------------

// Create makes a new regular file at (parent, name). Returns the new node
// and an open Handle. mode's permission bits are taken from mode & 0o7777;
// the type bits are forced to S_IFREG.
func (s *Store) Create(parent uint64, name string, mode Mode) (Node, *Handle, error) {
	if err := validName(name); err != nil {
		return Node{}, nil, err
	}
	var n Node
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := requireDirTx(tx, parent); err != nil {
			return err
		}
		if tx.Bucket(bucketDirents).Get(direntKey(parent, name)) != nil {
			return ErrExist
		}
		id, err := allocInodeTx(tx)
		if err != nil {
			return err
		}
		now := time.Now().UnixNano()
		n = Node{
			Inode: id,
			Mode:  ModeRegular | (mode & PermMask),
			Nlink: 1,
			Size:  0,
			Mtime: now, Ctime: now, Atime: now,
		}
		if err := tx.Bucket(bucketNodes).Put(inodeKey(id), encodeNode(n)); err != nil {
			return err
		}
		var cb [8]byte
		binary.BigEndian.PutUint64(cb[:], id)
		return tx.Bucket(bucketDirents).Put(direntKey(parent, name), cb[:])
	})
	if err != nil {
		return Node{}, nil, err
	}
	return n, &Handle{store: s, inode: n.Inode}, nil
}

// Mkdir creates a directory at (parent, name).
func (s *Store) Mkdir(parent uint64, name string, mode Mode) (Node, error) {
	if err := validName(name); err != nil {
		return Node{}, err
	}
	var n Node
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := requireDirTx(tx, parent); err != nil {
			return err
		}
		if tx.Bucket(bucketDirents).Get(direntKey(parent, name)) != nil {
			return ErrExist
		}
		id, err := allocInodeTx(tx)
		if err != nil {
			return err
		}
		now := time.Now().UnixNano()
		n = Node{
			Inode: id,
			Mode:  ModeDir | (mode & PermMask),
			Nlink: 2,
			Mtime: now, Ctime: now, Atime: now,
		}
		if err := tx.Bucket(bucketNodes).Put(inodeKey(id), encodeNode(n)); err != nil {
			return err
		}
		var cb [8]byte
		binary.BigEndian.PutUint64(cb[:], id)
		if err := tx.Bucket(bucketDirents).Put(direntKey(parent, name), cb[:]); err != nil {
			return err
		}
		return bumpParentNlinkTx(tx, parent, +1)
	})
	if err != nil {
		return Node{}, err
	}
	return n, nil
}

// Symlink creates a symlink at (parent, name) pointing to target.
func (s *Store) Symlink(parent uint64, name, target string) (Node, error) {
	if err := validName(name); err != nil {
		return Node{}, err
	}
	var n Node
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := requireDirTx(tx, parent); err != nil {
			return err
		}
		if tx.Bucket(bucketDirents).Get(direntKey(parent, name)) != nil {
			return ErrExist
		}
		id, err := allocInodeTx(tx)
		if err != nil {
			return err
		}
		now := time.Now().UnixNano()
		n = Node{
			Inode:         id,
			Mode:          ModeSymlink | 0o777,
			Nlink:         1,
			Size:          int64(len(target)),
			Mtime:         now, Ctime: now, Atime: now,
			SymlinkTarget: target,
		}
		if err := tx.Bucket(bucketNodes).Put(inodeKey(id), encodeNode(n)); err != nil {
			return err
		}
		var cb [8]byte
		binary.BigEndian.PutUint64(cb[:], id)
		return tx.Bucket(bucketDirents).Put(direntKey(parent, name), cb[:])
	})
	return n, err
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
	var n Node
	err := s.db.Update(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketNodes).Get(inodeKey(targetInode))
		if v == nil {
			return ErrNotExist
		}
		tn, err := decodeNode(v)
		if err != nil {
			return err
		}
		if tn.Mode.IsDir() {
			return syscall.EPERM // POSIX disallows directory hardlinks
		}
		if err := requireDirTx(tx, newParent); err != nil {
			return err
		}
		if tx.Bucket(bucketDirents).Get(direntKey(newParent, newName)) != nil {
			return ErrExist
		}
		tn.Nlink++
		tn.Ctime = time.Now().UnixNano()
		if err := tx.Bucket(bucketNodes).Put(inodeKey(targetInode), encodeNode(tn)); err != nil {
			return err
		}
		var cb [8]byte
		binary.BigEndian.PutUint64(cb[:], targetInode)
		if err := tx.Bucket(bucketDirents).Put(direntKey(newParent, newName), cb[:]); err != nil {
			return err
		}
		n = tn
		return nil
	})
	return n, err
}

// --- unlink / rmdir / rename --------------------------------------------

// Unlink removes a regular-file or symlink entry. If nlink hits zero, the
// inode's node entry is dropped and L2 fragments are deleted.
func (s *Store) Unlink(parent uint64, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	var freeInode uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		dv := tx.Bucket(bucketDirents).Get(direntKey(parent, name))
		if dv == nil {
			return ErrNotExist
		}
		child := binary.BigEndian.Uint64(dv)
		nv := tx.Bucket(bucketNodes).Get(inodeKey(child))
		if nv == nil {
			return fmt.Errorf("store: dangling dirent %d/%q", parent, name)
		}
		n, err := decodeNode(nv)
		if err != nil {
			return err
		}
		if n.Mode.IsDir() {
			return ErrIsDir
		}
		if err := tx.Bucket(bucketDirents).Delete(direntKey(parent, name)); err != nil {
			return err
		}
		n.Nlink--
		n.Ctime = time.Now().UnixNano()
		if n.Nlink == 0 {
			freeInode = child
			if err := tx.Bucket(bucketNodes).Delete(inodeKey(child)); err != nil {
				return err
			}
		} else {
			if err := tx.Bucket(bucketNodes).Put(inodeKey(child), encodeNode(n)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if freeInode != 0 {
		return s.idx.DeleteInode(freeInode)
	}
	return nil
}

// Rmdir removes an empty directory entry.
func (s *Store) Rmdir(parent uint64, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		dv := tx.Bucket(bucketDirents).Get(direntKey(parent, name))
		if dv == nil {
			return ErrNotExist
		}
		child := binary.BigEndian.Uint64(dv)
		nv := tx.Bucket(bucketNodes).Get(inodeKey(child))
		if nv == nil {
			return fmt.Errorf("store: dangling dirent %d/%q", parent, name)
		}
		n, err := decodeNode(nv)
		if err != nil {
			return err
		}
		if !n.Mode.IsDir() {
			return ErrNotDir
		}
		// Empty check: any dirents under this inode?
		c := tx.Bucket(bucketDirents).Cursor()
		k, _ := c.Seek(parentPrefix(child))
		if k != nil && bytes.HasPrefix(k, parentPrefix(child)) {
			return ErrNotEmpty
		}
		if err := tx.Bucket(bucketDirents).Delete(direntKey(parent, name)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketNodes).Delete(inodeKey(child)); err != nil {
			return err
		}
		return bumpParentNlinkTx(tx, parent, -1)
	})
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
	var freeInode uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		dd := tx.Bucket(bucketDirents)
		nodes := tx.Bucket(bucketNodes)

		srcVal := dd.Get(direntKey(oldParent, oldName))
		if srcVal == nil {
			return ErrNotExist
		}
		srcInode := binary.BigEndian.Uint64(srcVal)
		srcNodeV := nodes.Get(inodeKey(srcInode))
		if srcNodeV == nil {
			return fmt.Errorf("store: dangling src dirent")
		}
		srcNode, err := decodeNode(srcNodeV)
		if err != nil {
			return err
		}

		if err := requireDirTx(tx, newParent); err != nil {
			return err
		}

		// Prevent moving a directory into its own subtree (cycle).
		if srcNode.Mode.IsDir() && (newParent == srcInode || isDescendantTx(tx, newParent, srcInode)) {
			return ErrInvalid
		}

		// Handle target.
		if dstVal := dd.Get(direntKey(newParent, newName)); dstVal != nil {
			dstInode := binary.BigEndian.Uint64(dstVal)
			if dstInode == srcInode {
				// Renaming to itself: no-op.
				return nil
			}
			dstNodeV := nodes.Get(inodeKey(dstInode))
			if dstNodeV == nil {
				return fmt.Errorf("store: dangling dst dirent")
			}
			dstNode, err := decodeNode(dstNodeV)
			if err != nil {
				return err
			}
			// Type compatibility.
			if srcNode.Mode.IsDir() != dstNode.Mode.IsDir() {
				if srcNode.Mode.IsDir() {
					return ErrNotDir
				}
				return ErrIsDir
			}
			if dstNode.Mode.IsDir() {
				// Must be empty.
				c := dd.Cursor()
				k, _ := c.Seek(parentPrefix(dstInode))
				if k != nil && bytes.HasPrefix(k, parentPrefix(dstInode)) {
					return ErrNotEmpty
				}
				if err := nodes.Delete(inodeKey(dstInode)); err != nil {
					return err
				}
				if err := bumpParentNlinkTx(tx, newParent, -1); err != nil {
					return err
				}
			} else {
				dstNode.Nlink--
				dstNode.Ctime = time.Now().UnixNano()
				if dstNode.Nlink == 0 {
					freeInode = dstInode
					if err := nodes.Delete(inodeKey(dstInode)); err != nil {
						return err
					}
				} else {
					if err := nodes.Put(inodeKey(dstInode), encodeNode(dstNode)); err != nil {
						return err
					}
				}
			}
		}

		// Apply the move.
		if err := dd.Delete(direntKey(oldParent, oldName)); err != nil {
			return err
		}
		if err := dd.Put(direntKey(newParent, newName), srcVal); err != nil {
			return err
		}
		// nlink for parent dirs (if src is a directory and parents differ).
		if srcNode.Mode.IsDir() && oldParent != newParent {
			if err := bumpParentNlinkTx(tx, oldParent, -1); err != nil {
				return err
			}
			if err := bumpParentNlinkTx(tx, newParent, +1); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if freeInode != 0 {
		return s.idx.DeleteInode(freeInode)
	}
	return nil
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
func (s *Store) Setattr(inode uint64, sa SetAttr) (Node, error) {
	var truncatedTo *int64
	var n Node
	err := s.db.Update(func(tx *bolt.Tx) error {
		nv := tx.Bucket(bucketNodes).Get(inodeKey(inode))
		if nv == nil {
			return ErrNotExist
		}
		nn, err := decodeNode(nv)
		if err != nil {
			return err
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
			newSize := *sa.Size
			if newSize < nn.Size {
				size := newSize
				truncatedTo = &size
			}
			nn.Size = newSize
		}
		nn.Ctime = now
		if err := tx.Bucket(bucketNodes).Put(inodeKey(inode), encodeNode(nn)); err != nil {
			return err
		}
		n = nn
		return nil
	})
	if err != nil {
		return Node{}, err
	}
	if truncatedTo != nil {
		if err := s.idx.Truncate(inode, *truncatedTo); err != nil {
			return Node{}, err
		}
	}
	return n, nil
}

// --- readdir ------------------------------------------------------------

// Readdir returns the children of a directory.
func (s *Store) Readdir(inode uint64) ([]DirEntry, error) {
	var out []DirEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		dv := tx.Bucket(bucketNodes).Get(inodeKey(inode))
		if dv == nil {
			return ErrNotExist
		}
		dn, err := decodeNode(dv)
		if err != nil {
			return err
		}
		if !dn.Mode.IsDir() {
			return ErrNotDir
		}
		c := tx.Bucket(bucketDirents).Cursor()
		prefix := parentPrefix(inode)
		nodes := tx.Bucket(bucketNodes)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			name := string(k[8:])
			childInode := binary.BigEndian.Uint64(v)
			nv := nodes.Get(inodeKey(childInode))
			if nv == nil {
				// Skip dangling entries rather than failing readdir.
				continue
			}
			cn, err := decodeNode(nv)
			if err != nil {
				return err
			}
			out = append(out, DirEntry{Name: name, Inode: childInode, Mode: cn.Mode})
		}
		return nil
	})
	return out, err
}

// --- internal helpers used in txs ---------------------------------------

func requireDirTx(tx *bolt.Tx, inode uint64) error {
	nv := tx.Bucket(bucketNodes).Get(inodeKey(inode))
	if nv == nil {
		return ErrNotExist
	}
	n, err := decodeNode(nv)
	if err != nil {
		return err
	}
	if !n.Mode.IsDir() {
		return ErrNotDir
	}
	return nil
}

func bumpParentNlinkTx(tx *bolt.Tx, parent uint64, delta int32) error {
	nv := tx.Bucket(bucketNodes).Get(inodeKey(parent))
	if nv == nil {
		return ErrNotExist
	}
	n, err := decodeNode(nv)
	if err != nil {
		return err
	}
	n.Nlink = uint32(int32(n.Nlink) + delta)
	n.Ctime = time.Now().UnixNano()
	return tx.Bucket(bucketNodes).Put(inodeKey(parent), encodeNode(n))
}

// isDescendantTx returns true if `candidate` lives somewhere underneath
// `root` in the directory tree. Used by Rename to refuse cycle-creating
// moves like rename(/a, /a/b/c).
func isDescendantTx(tx *bolt.Tx, candidate, root uint64) bool {
	// Walk ancestors of candidate by scanning dirents for back-pointers.
	// We don't store parent links explicitly, so iterate. This is O(N) in
	// the dirent count, run only at rename time — fine for simplicity.
	cur := candidate
	for i := 0; i < 1000 && cur != RootInode; i++ {
		parent, ok := findParentTx(tx, cur)
		if !ok {
			return false
		}
		if parent == root {
			return true
		}
		cur = parent
	}
	return false
}

func findParentTx(tx *bolt.Tx, inode uint64) (uint64, bool) {
	c := tx.Bucket(bucketDirents).Cursor()
	want := make([]byte, 8)
	binary.BigEndian.PutUint64(want, inode)
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if bytes.Equal(v, want) {
			return binary.BigEndian.Uint64(k[:8]), true
		}
	}
	return 0, false
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
		BlockSize:  uint32(st.Bsize),
		Blocks:     st.Blocks,
		BlocksFree: st.Bfree,
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
