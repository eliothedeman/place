// Package segment is the L1 substrate of the place storage stack: append-only
// files of CRC'd records. It knows nothing about files, fragments, tiering
// policy, or shadowing — those are L2's job. Records do carry enough
// self-describing data (inode, stripe, seq, logical_off) that the L2 index
// could be rebuilt from segment scans if it were ever lost.
//
// Only L2 (the lib/index package) should import this. Higher layers must not
// reach in here.
package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Tier names which kind of storage a Set lives on. The byte value is durable
// in the locator encoding; do not reorder.
type Tier uint8

const (
	TierHot  Tier = 1
	TierCold Tier = 2
)

func (t Tier) String() string {
	switch t {
	case TierHot:
		return "hot"
	case TierCold:
		return "cold"
	default:
		return fmt.Sprintf("tier(%d)", t)
	}
}

// Locator pins one payload on disk. The tuple (Tier, SegmentID, Offset) is
// the unique address of bytes; Length lets readers know how many to pread.
type Locator struct {
	Tier      Tier
	SegmentID uint32
	Offset    int64
	Length    int64
}

// recordMagic is "PLSE" little-endian.
const recordMagic uint32 = 0x45534C50

const recordVersion uint8 = 1

// Header layout: magic(4) version(1) flags(1) pad(2) inode(8) stripe(4)
//                seq(8) logical_off(8) length(4) = 40 bytes
// Trailer: crc32(4)
const (
	headerSize  = 40
	trailerSize = 4
)

// FramedSize returns the total on-disk size of one record framing a payload
// of payloadLen bytes. Use it to size rotation checks: Set.Active(FramedSize(n)).
func FramedSize(payloadLen int) int64 {
	return int64(headerSize + payloadLen + trailerSize)
}

// RecordHeader is the decoded record header. Self-describing so a full
// segment scan can rebuild the L2 index from scratch.
type RecordHeader struct {
	Inode      uint64
	StripeID   uint32
	Seq        uint64
	LogicalOff int64
	Length     uint32
}

// Segment is one open .seg file. Created and owned by a Set.
type Segment struct {
	id   uint32
	path string

	mu   sync.Mutex
	f    *os.File
	size int64
}

func (s *Segment) ID() uint32 { return s.id }

func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// ReadAt reads len(p) bytes at off (an absolute byte offset in this segment
// file — typically the payload offset returned by Append). Concurrent-safe.
func (s *Segment) ReadAt(p []byte, off int64) (int, error) {
	return s.f.ReadAt(p, off)
}

// Append frames one record (header + payload + crc) and writes it. Returns
// the absolute byte offset of the payload within the segment, which the
// caller embeds into a Locator. h.Length must equal len(payload).
func (s *Segment) Append(h RecordHeader, payload []byte) (payloadOff int64, err error) {
	if int(h.Length) != len(payload) {
		return 0, fmt.Errorf("segment: header length %d != payload %d", h.Length, len(payload))
	}
	total := headerSize + len(payload) + trailerSize
	buf := make([]byte, total)
	o := 0
	binary.LittleEndian.PutUint32(buf[o:], recordMagic)
	o += 4
	buf[o] = recordVersion
	o++
	buf[o] = 0
	o++ // flags
	binary.LittleEndian.PutUint16(buf[o:], 0)
	o += 2
	binary.LittleEndian.PutUint64(buf[o:], h.Inode)
	o += 8
	binary.LittleEndian.PutUint32(buf[o:], h.StripeID)
	o += 4
	binary.LittleEndian.PutUint64(buf[o:], h.Seq)
	o += 8
	binary.LittleEndian.PutUint64(buf[o:], uint64(h.LogicalOff))
	o += 8
	binary.LittleEndian.PutUint32(buf[o:], h.Length)
	o += 4
	payloadStart := o
	copy(buf[o:], payload)
	o += len(payload)
	crc := crc32.ChecksumIEEE(buf[4:o]) // skip magic; cover version..payload
	binary.LittleEndian.PutUint32(buf[o:], crc)

	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.f.Write(buf)
	if err != nil {
		return 0, err
	}
	if n != total {
		return 0, io.ErrShortWrite
	}
	payloadOff = s.size + int64(payloadStart)
	s.size += int64(total)
	return payloadOff, nil
}

// Sync fsyncs the file. Intentionally does NOT hold s.mu — fsync is the
// slow part of the write path (5–20 ms typical) and serializing all
// callers behind one fsync turns concurrent appends into a queue. The
// kernel safely handles concurrent fsync(fd) from many threads; if one
// fsync is in flight when another arrives, the second one finds the
// dirty pages already being flushed and returns when they're done.
//
// The unsynchronised f.Sync() call races against Close(), which sets s.f
// to nil under s.mu. We accept that race: Close is only called from set
// teardown paths (GC.Remove, set.CloseAll) which the caller is
// responsible for serialising against in-flight Append/Sync. A nil
// dereference here would mean caller bug, not a concurrency hazard.
func (s *Segment) Sync() error {
	f := s.f
	if f == nil {
		return nil
	}
	return f.Sync()
}

// Close closes the file descriptor.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	f := s.f
	s.f = nil
	return f.Close()
}

// truncate trims the file (used by recovery and Set.RepairTail).
func (s *Segment) truncate(size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if size == s.size {
		return nil
	}
	if size > s.size {
		return fmt.Errorf("segment: truncate grow not supported (have %d, want %d)", s.size, size)
	}
	if err := s.f.Truncate(size); err != nil {
		return err
	}
	if _, err := s.f.Seek(size, io.SeekStart); err != nil {
		return err
	}
	s.size = size
	return s.f.Sync()
}

// Scan walks every valid record in the segment, invoking visit for each.
// Returns the byte offset of the first invalid record (or file end) — the
// caller should truncate to that offset to recover from a torn tail. The
// segment must not be mutated concurrently with a Scan.
func (s *Segment) Scan(visit func(RecordHeader, int64) error) (truncateAt int64, err error) {
	s.mu.Lock()
	size := s.size
	s.mu.Unlock()
	if size == 0 {
		return 0, nil
	}
	data := make([]byte, size)
	if _, err := s.f.ReadAt(data, 0); err != nil {
		return 0, err
	}
	var pos int64
	for pos < size {
		if size-pos < int64(headerSize+trailerSize) {
			return pos, nil
		}
		if binary.LittleEndian.Uint32(data[pos:]) != recordMagic {
			return pos, nil
		}
		if data[pos+4] != recordVersion {
			return pos, nil
		}
		// data[pos+5] is flags (currently always 0), pos+6..pos+8 is padding.
		h := RecordHeader{
			Inode:      binary.LittleEndian.Uint64(data[pos+8:]),
			StripeID:   binary.LittleEndian.Uint32(data[pos+16:]),
			Seq:        binary.LittleEndian.Uint64(data[pos+20:]),
			LogicalOff: int64(binary.LittleEndian.Uint64(data[pos+28:])),
			Length:     binary.LittleEndian.Uint32(data[pos+36:]),
		}
		recordEnd := pos + int64(headerSize) + int64(h.Length) + int64(trailerSize)
		if recordEnd > size {
			return pos, nil
		}
		crcOff := pos + int64(headerSize) + int64(h.Length)
		want := binary.LittleEndian.Uint32(data[crcOff:])
		got := crc32.ChecksumIEEE(data[pos+4 : crcOff])
		if got != want {
			return pos, nil
		}
		payloadOff := pos + int64(headerSize)
		if err := visit(h, payloadOff); err != nil {
			return 0, err
		}
		pos = recordEnd
	}
	return pos, nil
}

// Set is the per-tier collection of segments living under one directory.
// Hot and cold have their own Sets with independent segment-id counters.
type Set struct {
	dir     string
	tier    Tier
	maxSize int64

	mu       sync.Mutex
	segments map[uint32]*Segment
	active   *Segment
	nextID   uint32
}

// OpenSet opens (or creates) a Set at dir. Existing .seg files are reopened
// and their highest id seeds the rotation counter. maxSize is the soft cap
// triggering rotation in Active.
func OpenSet(dir string, tier Tier, maxSize int64) (*Set, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Set{
		dir:      dir,
		tier:     tier,
		maxSize:  maxSize,
		segments: map[uint32]*Segment{},
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ".seg")
		id64, err := strconv.ParseUint(base, 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, uint32(id64))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		seg, err := openSegment(filepath.Join(dir, segFileName(id)), id)
		if err != nil {
			return nil, err
		}
		s.segments[id] = seg
		if id+1 > s.nextID {
			s.nextID = id + 1
		}
	}
	if len(ids) > 0 {
		s.active = s.segments[ids[len(ids)-1]]
	}
	return s, nil
}

func (s *Set) Tier() Tier { return s.tier }
func (s *Set) Dir() string { return s.dir }

// Active returns a segment with at least recordSize bytes of headroom,
// rotating (sealing current + opening a new file) if needed.
//
// Rotation order: fsync the outgoing segment so its records are durable;
// create the new file; fsync the parent directory so the new file's
// dentry survives a power loss; install it as the active write target.
// Without the directory fsync, ext4/xfs can keep the new file invisible
// after crash and the next process would think the rotated-out bytes
// were the last word — corrupting any new appends made just before crash.
func (s *Set) Active(recordSize int64) (*Segment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil {
		s.active.mu.Lock()
		fits := s.active.size+recordSize <= s.maxSize
		s.active.mu.Unlock()
		if fits {
			return s.active, nil
		}
	}
	if s.active != nil {
		if err := s.active.Sync(); err != nil {
			return nil, err
		}
	}
	id := s.nextID
	s.nextID++
	seg, err := openSegment(filepath.Join(s.dir, segFileName(id)), id)
	if err != nil {
		return nil, err
	}
	if err := fsyncDir(s.dir); err != nil {
		seg.Close()
		return nil, fmt.Errorf("segment: fsync rotation dir: %w", err)
	}
	s.segments[id] = seg
	s.active = seg
	return seg, nil
}

// fsyncDir opens dir read-only and fsyncs it. Used after a new segment
// file is created so the dirent itself survives a power loss.
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// ActiveID returns the id of the current write target without rotating or
// allocating one if none exists. ok=false means the set has no segment open
// yet (the next Active call will create id 0).
func (s *Set) ActiveID() (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		return 0, false
	}
	return s.active.id, true
}

// Get returns the segment with the given id, or nil if absent.
func (s *Set) Get(id uint32) *Segment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.segments[id]
}

// All returns a sorted snapshot of segment ids.
func (s *Set) All() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint32, 0, len(s.segments))
	for id := range s.segments {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Remove closes and unlinks one segment. Caller must ensure nothing in L2
// still references it. The parent dir is fsynced after unlink so the
// removal survives a power loss — otherwise a crash could re-expose a
// .seg file whose contents are stale relative to the bbolt index.
func (s *Set) Remove(id uint32) error {
	s.mu.Lock()
	seg, ok := s.segments[id]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	delete(s.segments, id)
	if s.active == seg {
		s.active = nil
	}
	s.mu.Unlock()
	_ = seg.Close()
	if err := os.Remove(filepath.Join(s.dir, segFileName(id))); err != nil {
		return err
	}
	return fsyncDir(s.dir)
}

// RepairTail scans only the active segment and truncates any torn record at
// its end. Sealed segments are assumed durable. Run once at startup.
func (s *Set) RepairTail() error {
	s.mu.Lock()
	a := s.active
	s.mu.Unlock()
	if a == nil {
		return nil
	}
	truncAt, err := a.Scan(func(RecordHeader, int64) error { return nil })
	if err != nil {
		return err
	}
	a.mu.Lock()
	size := a.size
	a.mu.Unlock()
	if truncAt < size {
		return a.truncate(truncAt)
	}
	return nil
}

// ScanAll iterates every record in every segment of the set in id order.
// Intended for the rebuild-index-from-segments path.
func (s *Set) ScanAll(visit func(segID uint32, h RecordHeader, payloadOff int64) error) error {
	for _, id := range s.All() {
		seg := s.Get(id)
		if seg == nil {
			continue
		}
		truncAt, err := seg.Scan(func(h RecordHeader, off int64) error {
			return visit(id, h, off)
		})
		if err != nil {
			return err
		}
		seg.mu.Lock()
		size := seg.size
		seg.mu.Unlock()
		if truncAt < size {
			if err := seg.truncate(truncAt); err != nil {
				return err
			}
		}
	}
	return nil
}

// SyncAll fsyncs every open segment.
func (s *Set) SyncAll() error {
	s.mu.Lock()
	segs := make([]*Segment, 0, len(s.segments))
	for _, seg := range s.segments {
		segs = append(segs, seg)
	}
	s.mu.Unlock()
	for _, seg := range segs {
		if err := seg.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// CloseAll closes every segment file descriptor.
func (s *Set) CloseAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, seg := range s.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.segments = nil
	s.active = nil
	return firstErr
}

func openSegment(path string, id uint32) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{id: id, path: path, f: f, size: st.Size()}, nil
}

func segFileName(id uint32) string {
	return fmt.Sprintf("%010d.seg", id)
}
