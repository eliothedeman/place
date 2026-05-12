package store

import (
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Handle is the per-open-file reference returned by Open / Create / OpenInode.
// It is concurrency-safe: many goroutines may call ReadAt / WriteAt on the
// same Handle in parallel, and the index serializes appends internally.
type Handle struct {
	store *Store
	inode uint64
}

// Inode returns the inode this Handle points at.
func (h *Handle) Inode() uint64 { return h.inode }

// ReadAt fills p with bytes starting at off. Returns the number read; n < len(p)
// means EOF. Sparse regions are zero-filled. Reads past EOF return n=0.
func (h *Handle) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	node, err := h.store.Stat(h.inode)
	if err != nil {
		return 0, err
	}
	if off >= node.Size {
		return 0, nil
	}
	if off+int64(len(p)) > node.Size {
		p = p[:node.Size-off]
	}
	plan, err := h.store.idx.PlanRead(h.inode, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	for _, sl := range plan {
		dst := p[sl.LogicalOff-off : sl.LogicalOff-off+sl.Length]
		if sl.Sparse {
			for i := range dst {
				dst[i] = 0
			}
			continue
		}
		if _, err := h.store.idx.ReadAt(sl.Locator, dst); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// WriteAt writes p at off. The file grows if off+len(p) exceeds current size.
// Mtime/Ctime are bumped.
func (h *Handle) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := h.store.idx.Append(h.inode, off, p); err != nil {
		return 0, err
	}
	end := off + int64(len(p))
	now := time.Now().UnixNano()
	err := h.store.db.Update(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketNodes).Get(inodeKey(h.inode))
		if v == nil {
			return fmt.Errorf("store: write to inode %d with no node entry", h.inode)
		}
		n, err := decodeNode(v)
		if err != nil {
			return err
		}
		if end > n.Size {
			n.Size = end
		}
		n.Mtime = now
		n.Ctime = now
		return tx.Bucket(bucketNodes).Put(inodeKey(h.inode), encodeNode(n))
	})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Sync fsyncs the underlying storage to make all preceding writes durable.
func (h *Handle) Sync() error {
	return h.store.idx.Sync()
}

// Close releases the Handle. The current implementation has no per-handle
// state, but adapters should still call Close so the contract is stable.
func (h *Handle) Close() error {
	return nil
}

// Avoid unused-import lint:
var _ = binary.LittleEndian
