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
	bucketFiles    = []byte("files")
	bucketSegments = []byte("segments")
	bucketConfig   = []byte("config")
)

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
}

func (fm *FileMeta) IsDir() bool     { return fm.Mode&syscall.S_IFMT == syscall.S_IFDIR }
func (fm *FileMeta) IsLink() bool    { return fm.Mode&syscall.S_IFMT == syscall.S_IFLNK }
func (fm *FileMeta) IsRegular() bool { return fm.Mode&syscall.S_IFMT == syscall.S_IFREG }

// HasColdCopy reports whether ColdFragments cover [0, Size) contiguously.
// When true, eviction can safely drop all HotFragments.
func (fm *FileMeta) HasColdCopy() bool {
	if fm.Size == 0 {
		return true
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

	mu      sync.Mutex
	files   map[string]*FileMeta   // overlay entries; pointer-shared with callers (under mu)
	deleted map[string]bool        // tombstones (overlay-level deletes)
	segs    map[segKey]*SegmentMeta // segment-meta overlay
}

func NewMeta(path string) (*Meta, error) {
	// NoSync=true: commits do not fsync. Durability is provided by the
	// writer's background flusher (segment fsync before db.Sync()) and by
	// explicit Flush() on fsync/unmount.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second, NoSync: true})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketFiles, bucketSegments, bucketConfig} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		// Ensure root directory exists.
		fb := tx.Bucket(bucketFiles)
		if fb.Get(relKey("")) == nil {
			root := &FileMeta{
				Rel:   "",
				Mode:  syscall.S_IFDIR | 0755,
				Mtime: time.Now().UnixNano(),
				Ctime: time.Now().UnixNano(),
				Atime: time.Now().UnixNano(),
			}
			enc, err := encodeFileMeta(root)
			if err != nil {
				return err
			}
			if err := fb.Put(relKey(""), enc); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Meta{
		db:      db,
		files:   make(map[string]*FileMeta),
		deleted: make(map[string]bool),
		segs:    make(map[segKey]*SegmentMeta),
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

// getFileLocked returns the in-memory FileMeta for rel, loading from bbolt on
// miss and caching the loaded copy in the overlay for subsequent mutations.
// Returns (nil, nil) for tombstoned or missing files.
//
// The returned *FileMeta is the overlay's own copy; callers that mutate it
// (under m.mu) thereby stage the mutation for the next flush.
func (m *Meta) getFileLocked(rel string) (*FileMeta, error) {
	if m.deleted[rel] {
		return nil, nil
	}
	if fm, ok := m.files[rel]; ok {
		return fm, nil
	}
	var out *FileMeta
	err := m.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketFiles).Get(relKey(rel))
		if v == nil {
			return nil
		}
		fm, err := decodeFileMeta(v)
		if err != nil {
			return err
		}
		out = fm
		return nil
	})
	if err != nil || out == nil {
		return out, err
	}
	m.files[rel] = out
	return out, nil
}

// putFileLocked stores fm in the overlay (pointer-shared). Must be called
// with m.mu held.
func (m *Meta) putFileLocked(fm *FileMeta) {
	m.files[fm.Rel] = fm
	delete(m.deleted, fm.Rel)
}

// deleteFileLocked marks rel as tombstoned in the overlay.
func (m *Meta) deleteFileLocked(rel string) {
	delete(m.files, rel)
	m.deleted[rel] = true
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
func (m *Meta) flushOverlayLocked() error {
	if len(m.files) == 0 && len(m.deleted) == 0 && len(m.segs) == 0 {
		return nil
	}
	err := m.db.Update(func(tx *bolt.Tx) error {
		for rel := range m.deleted {
			if err := DeleteFileTx(tx, rel); err != nil {
				return err
			}
		}
		for _, fm := range m.files {
			if err := PutFileTx(tx, fm); err != nil {
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
	m.files = make(map[string]*FileMeta)
	m.deleted = make(map[string]bool)
	m.segs = make(map[segKey]*SegmentMeta)
	return nil
}

// WithOverlay runs fn under m.mu so fn can safely call getFileLocked /
// putFileLocked / etc. Used by the writer path.
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

// copyFileMeta makes a deep copy of a FileMeta (fragments are copied).
func copyFileMeta(fm *FileMeta) *FileMeta {
	if fm == nil {
		return nil
	}
	out := *fm
	if len(fm.HotFragments) > 0 {
		out.HotFragments = append([]Fragment(nil), fm.HotFragments...)
	}
	if len(fm.ColdFragments) > 0 {
		out.ColdFragments = append([]Fragment(nil), fm.ColdFragments...)
	}
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

// GetFileTx returns the FileMeta for rel using an active transaction.
func GetFileTx(tx *bolt.Tx, rel string) (*FileMeta, error) {
	v := tx.Bucket(bucketFiles).Get(relKey(rel))
	if v == nil {
		return nil, nil
	}
	return decodeFileMeta(v)
}

// PutFileTx writes FileMeta within an active transaction.
func PutFileTx(tx *bolt.Tx, fm *FileMeta) error {
	enc, err := encodeFileMeta(fm)
	if err != nil {
		return err
	}
	return tx.Bucket(bucketFiles).Put(relKey(fm.Rel), enc)
}

// DeleteFileTx removes the key for rel.
func DeleteFileTx(tx *bolt.Tx, rel string) error {
	return tx.Bucket(bucketFiles).Delete(relKey(rel))
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
		c := tx.Bucket(bucketFiles).Cursor()
		prefix := childKeyPrefix(parent)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			rel := keyToRel(k)
			if !isDirectChild(parent, rel) {
				continue
			}
			fm, err := decodeFileMeta(v)
			if err != nil {
				return err
			}
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
		c := tx.Bucket(bucketFiles).Cursor()
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

const fileMetaVersion byte = 1
const segmentMetaVersion byte = 1

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
	total := 1 + 2 + len(fm.Rel) + 4 + 8*4 + 4 + 4 + 2 + len(fm.LinkTarget) + 8 +
		4 + len(fm.HotFragments)*fragmentEncodedSize +
		4 + len(fm.ColdFragments)*fragmentEncodedSize
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
	if b[0] != fileMetaVersion {
		return nil, fmt.Errorf("unsupported filemeta version %d", b[0])
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

