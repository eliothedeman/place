package place

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

	bolt "go.etcd.io/bbolt"
)

var (
	// bucketFiles is the legacy v0 schema bucket (rel → encoded FileMeta).
	// The V0→V1 migration drains and deletes it; nothing reads it after.
	bucketFiles = []byte("files")

	// V1 schema buckets.
	bucketPaths    = []byte("paths")    // rel → 8-byte inodeID (LE)
	bucketInodes   = []byte("inodes")   // 8-byte inodeID (LE) → encoded FileMeta
	bucketSegments = []byte("segments") // unchanged
	bucketConfig   = []byte("config")   // unchanged
	bucketMeta     = []byte("_meta")    // schema_version, next_inode_id, next_segment_id_*

	metaKeySchemaVersion   = []byte("schema_version")
	metaKeyNextInodeID     = []byte("next_inode_id")
	metaKeyNextSegmentIDHot  = []byte("next_segment_id_hot")
	metaKeyNextSegmentIDCold = []byte("next_segment_id_cold")
)

// segmentIDKey returns the _meta bucket key for the next-segment-id counter
// for tier. Per-tier because SegmentKey is (tier, id) — IDs don't collide
// across tiers, so each tier has an independent monotonic sequence.
func segmentIDKey(tier Tier) []byte {
	if tier == TierCold {
		return metaKeyNextSegmentIDCold
	}
	return metaKeyNextSegmentIDHot
}

// inodeKey encodes a uint64 inodeID as the bbolt key for bucketInodes.
// 8 bytes, big-endian so cursor iteration is in numeric order.
func inodeKey(id uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return b[:]
}

func keyToInode(k []byte) uint64 {
	if len(k) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(k)
}

// allocInodeID reserves the next inodeID (starting at 1; 0 means "none").
// Must be called inside a write tx.
func allocInodeID(tx *bolt.Tx) (uint64, error) {
	mb := tx.Bucket(bucketMeta)
	if mb == nil {
		return 0, errors.New("missing _meta bucket")
	}
	var next uint64 = 1
	if v := mb.Get(metaKeyNextInodeID); v != nil && len(v) == 8 {
		next = binary.LittleEndian.Uint64(v)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], next+1)
	if err := mb.Put(metaKeyNextInodeID, buf[:]); err != nil {
		return 0, err
	}
	return next, nil
}

// readNextSegmentIDTx returns the persisted next-segment-id counter for tier
// without mutating it. Returns 0 if never written. Must be called inside a tx.
func readNextSegmentIDTx(tx *bolt.Tx, tier Tier) (uint32, error) {
	mb := tx.Bucket(bucketMeta)
	if mb == nil {
		return 0, errors.New("missing _meta bucket")
	}
	v := mb.Get(segmentIDKey(tier))
	if v == nil {
		return 0, nil
	}
	if len(v) != 4 {
		return 0, fmt.Errorf("next_segment_id_%d: bad encoding (%d bytes)", tier, len(v))
	}
	return binary.LittleEndian.Uint32(v), nil
}

// AllocSegmentID reserves and persists the next segment id for tier in its
// own write tx, syncing bbolt before returning. Mirrors allocInodeID for a
// crash-safe monotonic counter — the caller can then create the .seg file
// knowing the id is durably reserved (so a later restart, even one that
// rolls back to before the .seg file was unlinked, won't reuse it).
//
// Caller passes the in-memory floor (max disk id + 1, etc.) so we never
// regress: persisted = max(persisted, floor) + 1.
func (m *Meta) AllocSegmentID(tier Tier, floor uint32) (uint32, error) {
	var id uint32
	err := m.UpdateLocked(func(tx *bolt.Tx) error {
		mb := tx.Bucket(bucketMeta)
		if mb == nil {
			return errors.New("missing _meta bucket")
		}
		next := floor
		if v := mb.Get(segmentIDKey(tier)); v != nil && len(v) == 4 {
			if persisted := binary.LittleEndian.Uint32(v); persisted > next {
				next = persisted
			}
		}
		id = next
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], next+1)
		return mb.Put(segmentIDKey(tier), buf[:])
	})
	if err != nil {
		return 0, err
	}
	// Sync so the reservation survives a crash before the .seg file is
	// created — otherwise a restart could roll back the counter and reissue
	// the same id, defeating the whole point.
	if err := m.db.Sync(); err != nil {
		return 0, err
	}
	return id, nil
}

// relKey encodes a rel path as a bbolt key. Every key starts with '/' so
// that root ("") has a non-empty key (bbolt forbids empty keys).
//   ""          -> "/"
//   "foo"       -> "/foo"
//   "foo/bar"   -> "/foo/bar"
func relKey(rel string) []byte {
	b := make([]byte, len(rel)+1)
	b[0] = '/'
	copy(b[1:], rel)
	return b
}

// keyToRel inverts relKey.
func keyToRel(k []byte) string {
	if len(k) == 0 || k[0] != '/' {
		return string(k)
	}
	return string(k[1:])
}

type Tier uint8

const (
	TierHot  Tier = 0
	TierCold Tier = 1
)

type Fragment struct {
	LogicalOffset int64
	Length        int64
	Tier          Tier
	SegmentID     uint32
	SegmentOffset int64
}

// End returns LogicalOffset + Length.
func (f Fragment) End() int64 { return f.LogicalOffset + f.Length }

type FileMeta struct {
	Rel            string
	Mode           uint32
	Size           int64
	Mtime          int64 // UnixNano
	Ctime          int64
	Atime          int64
	Uid            uint32
	Gid            uint32
	LinkTarget     string     // only for S_IFLNK
	HotFragments   []Fragment // sorted by LogicalOffset, non-overlapping
	ColdFragments  []Fragment // sorted by LogicalOffset, non-overlapping
	Version        uint64
	Nlink          uint32 // count of paths pointing at this inode (>=1 while live)
	// ColdDirty is true when hot has bytes newer than cold for some range
	// in [0, Size). Set on every hot mutation, cleared by replicateOne.
	// Required because ColdFragments coverage alone doesn't say anything
	// about whether hot has fresher bytes — without this, eviction can
	// drop newer hot bytes in favour of stale cold bytes.
	ColdDirty bool

	// InodeID is the underlying inode identifier — populated by GetFileTx
	// from the paths bucket lookup. Not serialized (the inodeID is the
	// inodes-bucket key, so storing it inside the value would be redundant).
	// Callers may read it (e.g. to wire FUSE Ino → internal inodeID); it's
	// ignored on PutFileTx, which always looks up via paths[fm.Rel].
	InodeID uint64
}

func (fm *FileMeta) IsDir() bool     { return fm.Mode&syscall.S_IFMT == syscall.S_IFDIR }
func (fm *FileMeta) IsLink() bool    { return fm.Mode&syscall.S_IFMT == syscall.S_IFLNK }
func (fm *FileMeta) IsRegular() bool { return fm.Mode&syscall.S_IFMT == syscall.S_IFREG }

// HasColdCopy reports whether ColdFragments fully cover [0, Size) AND no
// hot mutations are outstanding (ColdDirty=false). When true, eviction can
// safely drop all HotFragments.
func (fm *FileMeta) HasColdCopy() bool {
	if fm.Size == 0 {
		return true
	}
	if fm.ColdDirty {
		return false
	}
	var pos int64
	for _, f := range fm.ColdFragments {
		if f.LogicalOffset != pos {
			return false
		}
		pos = f.End()
		if pos >= fm.Size {
			return true
		}
	}
	return pos >= fm.Size
}

// HotBytes returns the total bytes this file consumes in hot segments.
func (fm *FileMeta) HotBytes() int64 {
	var n int64
	for _, f := range fm.HotFragments {
		n += f.Length
	}
	return n
}

// SegmentMeta tracks live vs total bytes for a segment.
type SegmentMeta struct {
	ID        uint32
	Tier      Tier
	Total     int64 // bytes written
	Live      int64 // bytes still referenced by a FileMeta
	Sealed    bool  // no more appends (active rotated away)
	CreatedAt int64
}

// segKey identifies a segment by (tier, id) for the overlay.
type segKey struct {
	tier Tier
	id   uint32
}

// Meta holds the bbolt metadata plus an in-memory write overlay. All
// FileMeta / SegmentMeta mutations land in the overlay first and are
// batch-committed to bbolt by the writer's background flusher. Reads go
// through the overlay (which falls back to bbolt on miss), so reads always
// see the latest writes within the same process.
//
// The mu mutex serializes:
//   - overlay reads/writes from the writer path
//   - flushOverlayLocked (drain + commit)
//   - non-writer bbolt mutations via WithWriteLock (which flushes first,
//     then runs the caller's bbolt.Update under the same lock)
//
// so external ops can safely interleave with the writer.
type Meta struct {
	db *bolt.DB

	mu    sync.Mutex
	files map[uint64]*FileMeta    // overlay entries keyed by inodeID; pointer-shared with callers (under mu)
	segs  map[segKey]*SegmentMeta // segment-meta overlay
}

func NewMeta(path string) (*Meta, error) {
	// NoSync=true: commits do not fsync. Durability is provided by the
	// writer's background flusher (segment fsync before db.Sync()) and by
	// explicit Flush() on fsync/unmount.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second, NoSync: true})
	if err != nil {
		return nil, err
	}
	// Ensure all v1 schema buckets exist (idempotent on every open).
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketPaths, bucketInodes, bucketMeta, bucketSegments, bucketConfig} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	// Apply pending schema migrations. Backup is written to .migrations/
	// alongside the db before any writes.
	if err := runMigrations(db, path); err != nil {
		db.Close()
		return nil, err
	}

	// Ensure root directory entry exists. Goes through PutFileTx so it
	// lands in the right buckets regardless of how we got here (fresh db,
	// post-migration db, or an existing db where someone deleted root).
	err = db.Update(func(tx *bolt.Tx) error {
		root, err := GetFileTx(tx, "")
		if err != nil {
			return err
		}
		if root != nil {
			return nil
		}
		now := time.Now().UnixNano()
		return PutFileTx(tx, &FileMeta{
			Rel:   "",
			Mode:  syscall.S_IFDIR | 0755,
			Mtime: now,
			Ctime: now,
			Atime: now,
			Nlink: 1,
		})
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Meta{
		db:    db,
		files: make(map[uint64]*FileMeta),
		segs:  make(map[segKey]*SegmentMeta),
	}, nil
}

// Close flushes pending overlay entries and closes the db.
func (m *Meta) Close() error {
	m.mu.Lock()
	_ = m.flushOverlayLocked()
	_ = m.db.Sync()
	m.mu.Unlock()
	return m.db.Close()
}

func (m *Meta) DB() *bolt.DB { return m.db }

// --- overlay plumbing ---

// getFileLocked returns the in-memory FileMeta for rel, loading from bbolt
// on miss and caching the loaded copy in the overlay (keyed by inodeID)
// for subsequent mutations. Returns (nil, nil) for missing files.
//
// Keying by inodeID is required for hardlink correctness: two paths to
// one inode MUST share a single FileMeta in the overlay so concurrent
// writes through different rels accumulate into the same fragment list.
// Pre-fix the overlay was rel-keyed, so two paths produced two FileMeta
// copies that both wrote back to the same inodes-bucket key — and
// last-flush-wins silently dropped one writer's fragments.
//
// The returned *FileMeta is the overlay's own copy; callers that mutate
// it (under m.mu) thereby stage the mutation for the next flush.
func (m *Meta) getFileLocked(rel string) (*FileMeta, error) {
	var (
		id  uint64
		out *FileMeta
	)
	err := m.db.View(func(tx *bolt.Tx) error {
		var err error
		id, err = inodeForPathTx(tx, rel)
		if err != nil || id == 0 {
			return err
		}
		if fm, ok := m.files[id]; ok {
			out = fm
			return nil
		}
		out, err = getInodeTx(tx, id)
		return err
	})
	if err != nil || out == nil {
		return out, err
	}
	if _, cached := m.files[id]; !cached {
		m.files[id] = out
	}
	return out, nil
}

// getSegLocked returns the in-memory SegmentMeta, loading from bbolt on miss.
func (m *Meta) getSegLocked(tier Tier, id uint32) (*SegmentMeta, error) {
	k := segKey{tier, id}
	if sm, ok := m.segs[k]; ok {
		return sm, nil
	}
	var out *SegmentMeta
	err := m.db.View(func(tx *bolt.Tx) error {
		sm, err := GetSegmentTx(tx, tier, id)
		if err != nil {
			return err
		}
		out = sm
		return nil
	})
	if err != nil || out == nil {
		return out, err
	}
	m.segs[k] = out
	return out, nil
}

// putSegLocked stores sm in the overlay.
func (m *Meta) putSegLocked(sm *SegmentMeta) {
	m.segs[segKey{sm.Tier, sm.ID}] = sm
}

// flushOverlayLocked commits the overlay to bbolt (no db.Sync). Clears the
// overlay on success.
//
// Files are persisted via putInodeTx keyed by the overlay's inodeID, NOT
// via PutFileTx (which routes through paths[fm.Rel]). With a per-inode
// overlay, fm.Rel is the inode's stored primary name and may not match
// the rel a writer used to reach it; addressing the inode directly is
// the only correct way to flush.
func (m *Meta) flushOverlayLocked() error {
	if len(m.files) == 0 && len(m.segs) == 0 {
		return nil
	}
	err := m.db.Update(func(tx *bolt.Tx) error {
		for id, fm := range m.files {
			if err := putInodeTx(tx, id, fm); err != nil {
				return err
			}
		}
		for _, sm := range m.segs {
			if err := PutSegmentTx(tx, sm); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Reset overlay.
	m.files = make(map[uint64]*FileMeta)
	m.segs = make(map[segKey]*SegmentMeta)
	return nil
}

// WithOverlay runs fn under m.mu so fn can safely call getFileLocked /
// getSegLocked / etc. Used by the writer path.
func (m *Meta) WithOverlay(fn func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fn()
}

// WithWriteLock acquires m.mu, commits the overlay to bbolt, and runs fn.
// Used by non-writer bbolt mutations (node.go, evict, compact) so their
// cursor scans and db.Update calls see a consistent state — and won't be
// overwritten by a later flush of stale overlay data.
func (m *Meta) WithWriteLock(fn func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.flushOverlayLocked(); err != nil {
		return err
	}
	return fn()
}

// UpdateLocked is a convenience wrapper around WithWriteLock + db.Update:
// it flushes the overlay first, then runs fn inside a bbolt write tx,
// holding m.mu for the whole operation. Callers just replace
// `meta.db.Update(...)` with `meta.UpdateLocked(...)`.
func (m *Meta) UpdateLocked(fn func(tx *bolt.Tx) error) error {
	return m.WithWriteLock(func() error {
		return m.db.Update(fn)
	})
}

// ViewLocked is the read-only companion: flush overlay so the view sees
// committed data, then run fn in a bbolt read tx.
func (m *Meta) ViewLocked(fn func(tx *bolt.Tx) error) error {
	return m.WithWriteLock(func() error {
		return m.db.View(fn)
	})
}

// Flush commits the overlay to bbolt and fsyncs the db. Called by the
// writer's background flusher and on explicit fsync / shutdown.
func (m *Meta) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.flushOverlayLocked(); err != nil {
		return err
	}
	return m.db.Sync()
}

// FlushNoSync drains the overlay to bbolt without fsyncing. Used by read
// paths (DirChildren, cursor walks) that need overlay+bbolt to be unified
// but don't need durability.
func (m *Meta) FlushNoSync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushOverlayLocked()
}

// copyFileMeta returns a snapshot of fm whose fragment slices are safe to
// read concurrently with future writers. It does NOT deep-copy the
// fragment arrays, because fragments are never mutated in place — every
// modification path (mergeFragment, truncateFragments, eviction, GC)
// replaces the slice on fm, never edits an existing Fragment value. The
// caller's snapshot keeps its own slice header (len/cap/ptr), so a
// later writer that points fm.HotFragments at a fresh slice doesn't
// disturb this view, and an in-place append-with-spare-cap doesn't
// change the snapshot's len.
//
// Deep-copying these slices on every read was ~32% of CPU under mixed
// rsync+hash workloads (35 GB files end up with ~140K fragments).
func copyFileMeta(fm *FileMeta) *FileMeta {
	if fm == nil {
		return nil
	}
	out := *fm
	return &out
}

// GetFile returns the FileMeta for rel, or nil if it doesn't exist.
// Consults the in-memory overlay first, then falls back to bbolt.
// The returned value is a deep copy; callers may mutate it freely.
func (m *Meta) GetFile(rel string) (*FileMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fm, err := m.getFileLocked(rel)
	if err != nil || fm == nil {
		return fm, err
	}
	return copyFileMeta(fm), nil
}

// inodeForPathTx looks up the inodeID for rel. Returns 0 if no path entry.
func inodeForPathTx(tx *bolt.Tx, rel string) (uint64, error) {
	pb := tx.Bucket(bucketPaths)
	if pb == nil {
		return 0, nil
	}
	v := pb.Get(relKey(rel))
	if v == nil {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("paths[%q]: bad inodeID encoding (%d bytes)", rel, len(v))
	}
	return binary.LittleEndian.Uint64(v), nil
}

// putPathTx inserts paths[rel] = inodeID.
func putPathTx(tx *bolt.Tx, rel string, id uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], id)
	return tx.Bucket(bucketPaths).Put(relKey(rel), b[:])
}

// GetFileTx returns the FileMeta for rel using an active transaction.
// Walks paths → inodes. Returns (nil, nil) for a missing path. The
// returned FileMeta has InodeID populated.
func GetFileTx(tx *bolt.Tx, rel string) (*FileMeta, error) {
	id, err := inodeForPathTx(tx, rel)
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, nil
	}
	ib := tx.Bucket(bucketInodes)
	if ib == nil {
		return nil, nil
	}
	v := ib.Get(inodeKey(id))
	if v == nil {
		return nil, nil
	}
	fm, err := decodeFileMeta(v)
	if err != nil {
		return nil, err
	}
	fm.InodeID = id
	return fm, nil
}

// getInodeTx returns the FileMeta for an inodeID directly, without going
// through paths. Used by Link (where target's inodeID is already known) and
// by future Open lifecycle code.
func getInodeTx(tx *bolt.Tx, id uint64) (*FileMeta, error) {
	if id == 0 {
		return nil, nil
	}
	ib := tx.Bucket(bucketInodes)
	if ib == nil {
		return nil, nil
	}
	v := ib.Get(inodeKey(id))
	if v == nil {
		return nil, nil
	}
	fm, err := decodeFileMeta(v)
	if err != nil {
		return nil, err
	}
	fm.InodeID = id
	return fm, nil
}

// putInodeTx writes fm directly to inodes[id] without touching paths. Used
// by Link, Rename, and helpers that need to update an inode in place.
//
// Nlink==0 is an error: encodeFileMeta treats 0 as a deletion marker and
// silently rewrites it to 1 on the way out, which would resurrect an
// inode the caller meant to remove. Callers that mean to delete should
// go through DeleteFileTx (which clears the inode on the last link); a
// live inode always has Nlink>=1.
func putInodeTx(tx *bolt.Tx, id uint64, fm *FileMeta) error {
	if id == 0 {
		return errors.New("putInodeTx: id=0")
	}
	if fm.Nlink == 0 {
		return fmt.Errorf("putInodeTx: Nlink=0 for inode %d (use DeleteFileTx to remove)", id)
	}
	enc, err := encodeFileMeta(fm)
	if err != nil {
		return err
	}
	return tx.Bucket(bucketInodes).Put(inodeKey(id), enc)
}

// findPathForInodeTx returns any rel that maps to id, or "" if no path
// resolves to it. O(N) scan of the paths bucket — used by DeleteFileTx
// and the startup repair pass to repoint a stale fm.Rel at a surviving
// hardlink. Returns the first key found in cursor order; callers don't
// need a specific name, only one that round-trips through the paths
// bucket back to this inode.
func findPathForInodeTx(tx *bolt.Tx, id uint64) (string, error) {
	pb := tx.Bucket(bucketPaths)
	if pb == nil {
		return "", nil
	}
	c := pb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(v) != 8 {
			continue
		}
		if binary.LittleEndian.Uint64(v) == id {
			return keyToRel(k), nil
		}
	}
	return "", nil
}

// iterateInodesTx walks every (inodeID, FileMeta) pair in bucketInodes.
// Compaction and eviction use this so they can operate on inode identity
// directly — fm.Rel can become stale across hardlink unlink and rename, so
// re-resolving via paths[fm.Rel] is unsafe. fn returning a non-nil error
// stops the walk and propagates the error.
func iterateInodesTx(tx *bolt.Tx, fn func(id uint64, fm *FileMeta) error) error {
	ib := tx.Bucket(bucketInodes)
	if ib == nil {
		return nil
	}
	c := ib.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fm, err := decodeFileMeta(v)
		if err != nil {
			return err
		}
		id := keyToInode(k)
		fm.InodeID = id
		if err := fn(id, fm); err != nil {
			return err
		}
	}
	return nil
}

// movePathTx renames paths[oldRel] → paths[newRel], preserving the inodeID.
// If the inode's fm.Rel is currently oldRel, it's updated to newRel (so
// cold-record framing uses the up-to-date primary name). Returns ENOENT if
// oldRel doesn't exist; no check on newRel — caller should handle dst-exists.
func movePathTx(tx *bolt.Tx, oldRel, newRel string) error {
	pb := tx.Bucket(bucketPaths)
	v := pb.Get(relKey(oldRel))
	if v == nil {
		return syscall.ENOENT
	}
	if len(v) != 8 {
		return fmt.Errorf("paths[%q]: bad inodeID encoding", oldRel)
	}
	// Copy the value out of bbolt's mmap before any mutation — bbolt's docs
	// warn that Put can rebalance pages and invalidate slices returned by
	// Get within the same tx.
	id := binary.LittleEndian.Uint64(v)
	var idBytes [8]byte
	copy(idBytes[:], v)
	if err := pb.Put(relKey(newRel), idBytes[:]); err != nil {
		return err
	}
	if err := pb.Delete(relKey(oldRel)); err != nil {
		return err
	}
	// Refresh fm.Rel if it pointed at oldRel.
	ib := tx.Bucket(bucketInodes)
	iv := ib.Get(inodeKey(id))
	if iv == nil {
		return nil
	}
	fm, err := decodeFileMeta(iv)
	if err != nil {
		return err
	}
	if fm.Rel == oldRel {
		fm.Rel = newRel
		fm.InodeID = id
		enc, err := encodeFileMeta(fm)
		if err != nil {
			return err
		}
		return ib.Put(inodeKey(id), enc)
	}
	return nil
}

// PutFileTx writes FileMeta within an active transaction. If the path is new
// it allocates a fresh inodeID (and sets fm.Nlink=1). Updates to an existing
// path go to the same inode and preserve its Nlink.
func PutFileTx(tx *bolt.Tx, fm *FileMeta) error {
	id, err := inodeForPathTx(tx, fm.Rel)
	if err != nil {
		return err
	}
	if id == 0 {
		// If the FileMeta carries an InodeID from a prior load, the caller
		// is updating an existing inode whose path entry was concurrently
		// removed (e.g. writer overlay flushing after Unlink). Re-creating
		// a fresh inode would resurrect the file with a new identity. Use
		// the original inode if it still exists; otherwise this update is
		// for a deleted file and should be dropped.
		if fm.InodeID != 0 {
			ib := tx.Bucket(bucketInodes)
			if ib != nil && ib.Get(inodeKey(fm.InodeID)) != nil {
				id = fm.InodeID
			} else {
				return nil // inode gone — silently drop the stale update
			}
		} else {
			id, err = allocInodeID(tx)
			if err != nil {
				return err
			}
			if err := putPathTx(tx, fm.Rel, id); err != nil {
				return err
			}
			if fm.Nlink == 0 {
				fm.Nlink = 1
			}
		}
	} else if fm.Nlink == 0 {
		// Preserve existing Nlink on update — load it from the inode if the
		// caller didn't set one. (Most call sites read fm via GetFileTx,
		// mutate, and write back — so Nlink is already populated.)
		if v := tx.Bucket(bucketInodes).Get(inodeKey(id)); v != nil {
			if existing, derr := decodeFileMeta(v); derr == nil {
				fm.Nlink = existing.Nlink
			}
		}
		if fm.Nlink == 0 {
			fm.Nlink = 1
		}
	}
	enc, err := encodeFileMeta(fm)
	if err != nil {
		return err
	}
	fm.InodeID = id
	return tx.Bucket(bucketInodes).Put(inodeKey(id), enc)
}

// DeleteFileTx removes the path entry for rel. If that was the last path
// pointing at the underlying inode (Nlink would drop to 0), the inode is
// also removed and its segment-live-bytes credit is returned (caller is
// responsible for AddLiveBytesTx — historically callers do this *before*
// DeleteFileTx, so the contract here is unchanged: Nlink-aware unlink is a
// follow-up task; for V1 every inode has Nlink=1 and unlink fully removes).
//
// Invariant: while Nlink>0, fm.Rel must name a path that resolves back to
// the inode. Compaction streams record framing through fm.Rel and looks
// the inode up via paths[fm.Rel] — a stale Rel reads as "file gone" and
// silently skips the inode, causing forwardCompact to drop the source
// segment without relocating its fragments. When the deleted path
// matches fm.Rel, repoint Rel at any surviving hardlink before persisting.
func DeleteFileTx(tx *bolt.Tx, rel string) error {
	id, err := inodeForPathTx(tx, rel)
	if err != nil {
		return err
	}
	if id == 0 {
		return nil
	}
	if err := tx.Bucket(bucketPaths).Delete(relKey(rel)); err != nil {
		return err
	}
	ib := tx.Bucket(bucketInodes)
	v := ib.Get(inodeKey(id))
	if v == nil {
		return nil
	}
	fm, derr := decodeFileMeta(v)
	if derr != nil {
		// Best-effort: if we can't decode we still drop the inode entry to
		// keep the path/inode buckets consistent.
		return ib.Delete(inodeKey(id))
	}
	if fm.Nlink > 1 {
		fm.Nlink--
		// Repoint Rel if the deleted path was the one it tracked.
		// findPathForInodeTx is O(N) over the paths bucket but only fires
		// on hardlink unlink (rare) and only when the unlink hits the
		// primary path.
		if fm.Rel == rel {
			survivor, ferr := findPathForInodeTx(tx, id)
			if ferr != nil {
				return ferr
			}
			if survivor != "" {
				fm.Rel = survivor
			}
			// If no survivor was found despite Nlink>1, the paths bucket
			// is inconsistent with Nlink — leave Rel as-is (stale) and
			// proceed; the startup repair pass will sweep it up.
		}
		enc, eerr := encodeFileMeta(fm)
		if eerr != nil {
			return eerr
		}
		return ib.Put(inodeKey(id), enc)
	}
	return ib.Delete(inodeKey(id))
}

// PutFile writes a single FileMeta in its own transaction.
func (m *Meta) PutFile(fm *FileMeta) error {
	return m.db.Update(func(tx *bolt.Tx) error { return PutFileTx(tx, fm) })
}

// DirChildren returns direct children of parent (excluding parent itself).
// The overlay is flushed to bbolt first so the cursor walk sees every file.
func (m *Meta) DirChildren(parent string) ([]*FileMeta, error) {
	m.mu.Lock()
	if err := m.flushOverlayLocked(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()
	var out []*FileMeta
	err := m.db.View(func(tx *bolt.Tx) error {
		pb := tx.Bucket(bucketPaths)
		ib := tx.Bucket(bucketInodes)
		c := pb.Cursor()
		prefix := childKeyPrefix(parent)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			rel := keyToRel(k)
			if !isDirectChild(parent, rel) {
				continue
			}
			if len(v) != 8 {
				continue
			}
			id := binary.LittleEndian.Uint64(v)
			ev := ib.Get(inodeKey(id))
			if ev == nil {
				continue
			}
			fm, err := decodeFileMeta(ev)
			if err != nil {
				return err
			}
			// Override fm.Rel with the path actually being iterated.
			// Otherwise hardlinks (multiple paths sharing one inode) would
			// all surface in a directory listing under the inode's stored
			// primary name. Each directory entry needs to display under
			// its own name.
			fm.Rel = rel
			fm.InodeID = id
			out = append(out, fm)
		}
		return nil
	})
	return out, err
}

// childKeyPrefix returns the bbolt-key prefix common to all descendants of
// parent (direct + deeper). Keys are rooted at "/".
//   parent ""         -> "/"
//   parent "foo"      -> "/foo/"
//   parent "a/b"      -> "/a/b/"
func childKeyPrefix(parent string) []byte {
	if parent == "" {
		return []byte("/")
	}
	return []byte("/" + parent + "/")
}

// isDirectChild reports whether rel is a direct child of parent.
func isDirectChild(parent, rel string) bool {
	if rel == parent || rel == "" {
		return false
	}
	var inner string
	if parent == "" {
		inner = rel
	} else {
		if !strings.HasPrefix(rel, parent+"/") {
			return false
		}
		inner = rel[len(parent)+1:]
	}
	return inner != "" && !strings.Contains(inner, "/")
}

// HasChildren reports whether parent has any children (used by Rmdir).
func (m *Meta) HasChildren(parent string) (bool, error) {
	m.mu.Lock()
	if err := m.flushOverlayLocked(); err != nil {
		m.mu.Unlock()
		return false, err
	}
	m.mu.Unlock()
	var found bool
	err := m.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketPaths).Cursor()
		prefix := childKeyPrefix(parent)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			rel := keyToRel(k)
			if rel == parent || rel == "" {
				continue
			}
			found = true
			return nil
		}
		return nil
	})
	return found, err
}

// SegmentKey encodes (tier, id) as the bbolt key.
func SegmentKey(tier Tier, id uint32) []byte {
	k := make([]byte, 5)
	k[0] = byte(tier)
	binary.BigEndian.PutUint32(k[1:], id)
	return k
}

// GetSegmentTx reads SegmentMeta within a transaction.
func GetSegmentTx(tx *bolt.Tx, tier Tier, id uint32) (*SegmentMeta, error) {
	v := tx.Bucket(bucketSegments).Get(SegmentKey(tier, id))
	if v == nil {
		return nil, nil
	}
	return decodeSegmentMeta(v)
}

// PutSegmentTx writes SegmentMeta within a transaction.
func PutSegmentTx(tx *bolt.Tx, sm *SegmentMeta) error {
	enc, err := encodeSegmentMeta(sm)
	if err != nil {
		return err
	}
	return tx.Bucket(bucketSegments).Put(SegmentKey(sm.Tier, sm.ID), enc)
}

// DeleteSegmentTx removes a segment entry.
func DeleteSegmentTx(tx *bolt.Tx, tier Tier, id uint32) error {
	return tx.Bucket(bucketSegments).Delete(SegmentKey(tier, id))
}

// ListSegments returns all segments for a tier.
func (m *Meta) ListSegments(tier Tier) ([]*SegmentMeta, error) {
	var out []*SegmentMeta
	err := m.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketSegments).Cursor()
		prefix := []byte{byte(tier)}
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			sm, err := decodeSegmentMeta(v)
			if err != nil {
				return err
			}
			out = append(out, sm)
		}
		return nil
	})
	return out, err
}

// AddLiveBytesTx adjusts live-byte counters for a set of fragments.
// delta is applied per fragment.Length to each fragment's segment.
func AddLiveBytesTx(tx *bolt.Tx, frags []Fragment, delta int64) error {
	type key struct {
		tier Tier
		id   uint32
	}
	agg := map[key]int64{}
	for _, f := range frags {
		agg[key{f.Tier, f.SegmentID}] += delta * f.Length
	}
	for k, d := range agg {
		sm, err := GetSegmentTx(tx, k.tier, k.id)
		if err != nil {
			return err
		}
		if sm == nil {
			continue
		}
		sm.Live += d
		if sm.Live < 0 {
			sm.Live = 0
		}
		if err := PutSegmentTx(tx, sm); err != nil {
			return err
		}
	}
	return nil
}

// --- encoding ---

const (
	// fileMetaVersion is the on-disk encoding version. v2 added Nlink at
	// the tail; v1 records (read only during the V0→V1 schema migration)
	// default Nlink=1 on decode. v3 adds ColdDirty; v2 records default
	// ColdDirty=true on decode — that's the safe choice (forces a
	// re-replicate on the next pass) since we can't tell whether hot had
	// outstanding bytes when the v2 record was last written.
	fileMetaVersion   byte = 3
	segmentMetaVersion byte = 1
)

// fragmentEncodedSize is the on-disk size of one encoded fragment:
// 8 (LogicalOffset) + 8 (Length) + 1 (Tier) + 4 (SegmentID) + 8 (SegmentOffset)
const fragmentEncodedSize = 8 + 8 + 1 + 4 + 8

func encodeFileMeta(fm *FileMeta) ([]byte, error) {
	// Precompute total size: version byte + fixed fields + two strings + two frag lists.
	//   1 (version)
	//   2 + len(Rel)
	//   4 (Mode)
	//   8 (Size) + 8 (Mtime) + 8 (Ctime) + 8 (Atime)
	//   4 (Uid) + 4 (Gid)
	//   2 + len(LinkTarget)
	//   8 (Version)
	//   4 + N*fragmentEncodedSize  (HotFragments)
	//   4 + N*fragmentEncodedSize  (ColdFragments)
	//   4 (Nlink, v2)
	//   1 (ColdDirty, v3)
	total := 1 + 2 + len(fm.Rel) + 4 + 8*4 + 4 + 4 + 2 + len(fm.LinkTarget) + 8 +
		4 + len(fm.HotFragments)*fragmentEncodedSize +
		4 + len(fm.ColdFragments)*fragmentEncodedSize +
		4 + // Nlink (v2)
		1 // ColdDirty (v3)
	b := make([]byte, total)
	p := 0
	b[p] = fileMetaVersion
	p++
	le := binary.LittleEndian
	le.PutUint16(b[p:], uint16(len(fm.Rel)))
	p += 2
	p += copy(b[p:], fm.Rel)
	le.PutUint32(b[p:], fm.Mode)
	p += 4
	le.PutUint64(b[p:], uint64(fm.Size))
	p += 8
	le.PutUint64(b[p:], uint64(fm.Mtime))
	p += 8
	le.PutUint64(b[p:], uint64(fm.Ctime))
	p += 8
	le.PutUint64(b[p:], uint64(fm.Atime))
	p += 8
	le.PutUint32(b[p:], fm.Uid)
	p += 4
	le.PutUint32(b[p:], fm.Gid)
	p += 4
	le.PutUint16(b[p:], uint16(len(fm.LinkTarget)))
	p += 2
	p += copy(b[p:], fm.LinkTarget)
	le.PutUint64(b[p:], fm.Version)
	p += 8
	p = writeFragments(b, p, fm.HotFragments)
	p = writeFragments(b, p, fm.ColdFragments)
	nlink := fm.Nlink
	if nlink == 0 {
		nlink = 1 // never persist Nlink=0 — that's a deletion marker
	}
	le.PutUint32(b[p:], nlink)
	p += 4
	if fm.ColdDirty {
		b[p] = 1
	}
	p++
	return b[:p], nil
}

func writeFragments(b []byte, p int, frags []Fragment) int {
	le := binary.LittleEndian
	le.PutUint32(b[p:], uint32(len(frags)))
	p += 4
	for i := range frags {
		f := &frags[i]
		le.PutUint64(b[p:], uint64(f.LogicalOffset))
		p += 8
		le.PutUint64(b[p:], uint64(f.Length))
		p += 8
		b[p] = byte(f.Tier)
		p++
		le.PutUint32(b[p:], f.SegmentID)
		p += 4
		le.PutUint64(b[p:], uint64(f.SegmentOffset))
		p += 8
	}
	return p
}

func decodeFileMeta(b []byte) (*FileMeta, error) {
	if len(b) == 0 {
		return nil, errors.New("empty filemeta")
	}
	ver := b[0]
	if ver != 1 && ver != 2 && ver != 3 {
		return nil, fmt.Errorf("unsupported filemeta version %d", ver)
	}
	le := binary.LittleEndian
	p := 1
	need := func(n int) error {
		if p+n > len(b) {
			return errors.New("truncated filemeta")
		}
		return nil
	}
	fm := &FileMeta{}
	if err := need(2); err != nil {
		return nil, err
	}
	relLen := int(le.Uint16(b[p:]))
	p += 2
	if err := need(relLen); err != nil {
		return nil, err
	}
	fm.Rel = string(b[p : p+relLen])
	p += relLen
	if err := need(4 + 8*4 + 4 + 4); err != nil {
		return nil, err
	}
	fm.Mode = le.Uint32(b[p:])
	p += 4
	fm.Size = int64(le.Uint64(b[p:]))
	p += 8
	fm.Mtime = int64(le.Uint64(b[p:]))
	p += 8
	fm.Ctime = int64(le.Uint64(b[p:]))
	p += 8
	fm.Atime = int64(le.Uint64(b[p:]))
	p += 8
	fm.Uid = le.Uint32(b[p:])
	p += 4
	fm.Gid = le.Uint32(b[p:])
	p += 4
	if err := need(2); err != nil {
		return nil, err
	}
	linkLen := int(le.Uint16(b[p:]))
	p += 2
	if err := need(linkLen); err != nil {
		return nil, err
	}
	fm.LinkTarget = string(b[p : p+linkLen])
	p += linkLen
	if err := need(8); err != nil {
		return nil, err
	}
	fm.Version = le.Uint64(b[p:])
	p += 8
	var err error
	if fm.HotFragments, p, err = readFragments(b, p); err != nil {
		return nil, err
	}
	if fm.ColdFragments, p, err = readFragments(b, p); err != nil {
		return nil, err
	}
	if ver >= 2 {
		if err := need(4); err != nil {
			return nil, err
		}
		fm.Nlink = le.Uint32(b[p:])
		p += 4
	} else {
		fm.Nlink = 1 // v1 entries had no Nlink — implied 1.
	}
	if ver >= 3 {
		if err := need(1); err != nil {
			return nil, err
		}
		fm.ColdDirty = b[p] != 0
		p++
	} else {
		// v2 records pre-date the dirty bit; we have to assume hot may
		// have been newer than cold, so force a re-replicate.
		fm.ColdDirty = true
	}
	return fm, nil
}

func readFragments(b []byte, p int) ([]Fragment, int, error) {
	if p+4 > len(b) {
		return nil, p, errors.New("truncated fragments count")
	}
	le := binary.LittleEndian
	n := int(le.Uint32(b[p:]))
	p += 4
	if p+n*fragmentEncodedSize > len(b) {
		return nil, p, errors.New("truncated fragments")
	}
	out := make([]Fragment, n)
	for i := range out {
		f := &out[i]
		f.LogicalOffset = int64(le.Uint64(b[p:]))
		p += 8
		f.Length = int64(le.Uint64(b[p:]))
		p += 8
		f.Tier = Tier(b[p])
		p++
		f.SegmentID = le.Uint32(b[p:])
		p += 4
		f.SegmentOffset = int64(le.Uint64(b[p:]))
		p += 8
	}
	return out, p, nil
}

func encodeSegmentMeta(sm *SegmentMeta) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(segmentMetaVersion)
	w := func(v any) { _ = binary.Write(&buf, binary.LittleEndian, v) }
	w(sm.ID)
	w(uint8(sm.Tier))
	w(sm.Total)
	w(sm.Live)
	var sealed uint8
	if sm.Sealed {
		sealed = 1
	}
	w(sealed)
	w(sm.CreatedAt)
	return buf.Bytes(), nil
}

func decodeSegmentMeta(b []byte) (*SegmentMeta, error) {
	if len(b) == 0 || b[0] != segmentMetaVersion {
		return nil, fmt.Errorf("bad segment meta version")
	}
	r := bytes.NewReader(b[1:])
	read := func(v any) error { return binary.Read(r, binary.LittleEndian, v) }
	sm := &SegmentMeta{}
	if err := read(&sm.ID); err != nil {
		return nil, err
	}
	var t uint8
	if err := read(&t); err != nil {
		return nil, err
	}
	sm.Tier = Tier(t)
	if err := read(&sm.Total); err != nil {
		return nil, err
	}
	if err := read(&sm.Live); err != nil {
		return nil, err
	}
	var sealed uint8
	if err := read(&sealed); err != nil {
		return nil, err
	}
	sm.Sealed = sealed != 0
	if err := read(&sm.CreatedAt); err != nil {
		return nil, err
	}
	return sm, nil
}

// ParentOf returns the parent directory rel of a path, "" for root-level.
func ParentOf(rel string) string {
	if rel == "" {
		return ""
	}
	i := strings.LastIndex(rel, "/")
	if i < 0 {
		return ""
	}
	return rel[:i]
}

// BaseName returns the final component of rel.
func BaseName(rel string) string {
	if rel == "" {
		return ""
	}
	return path.Base(rel)
}

