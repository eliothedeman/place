package place

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Segment record framing:
//
//   [magic:4]       "PLSE"
//   [type:1]        RecordData | RecordTombstone
//   [rel_len:2]     length of rel path
//   [rel:N]         rel path
//   [log_off:8]     logical offset of this chunk in the file
//   [length:4]      payload byte length
//   [payload:L]     raw bytes (empty for tombstones)
//   [crc32:4]       IEEE CRC over [type .. payload]
//
// The caller receives the offset of the payload (not the record start), so
// reads are a single pread with no header parsing.

var recordMagic = [4]byte{'P', 'L', 'S', 'E'}

type recordType uint8

const (
	recordData      recordType = 1
	recordTombstone recordType = 2
)

const (
	headerFixedSize = 4 + 1 + 2 + 8 + 4 // magic + type + rel_len + log_off + length
	trailerSize     = 4                 // crc32
)

// frameRecordInto writes a framed record into buf (which must be sized at
// exactly headerFixedSize + len(rel) + len(payload) + trailerSize). Returns
// the offset within buf where the payload starts.
func frameRecordInto(buf []byte, typ recordType, rel string, logicalOff int64, payload []byte) int {
	off := 0
	copy(buf[off:], recordMagic[:])
	off += 4
	buf[off] = byte(typ)
	off++
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(rel)))
	off += 2
	off += copy(buf[off:], rel)
	binary.LittleEndian.PutUint64(buf[off:], uint64(logicalOff))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(payload)))
	off += 4
	payloadStart := off
	off += copy(buf[off:], payload)
	crc := crc32.ChecksumIEEE(buf[4:off]) // skip magic; cover type..payload
	binary.LittleEndian.PutUint32(buf[off:], crc)
	return payloadStart
}

// parseRecord reads one record starting at offset `at` in data. Returns the
// parsed fields and the offset just past the record (for the next scan step).
// If the record is invalid/torn, returns an error and the caller should treat
// `at` as the truncate point.
func parseRecord(data []byte, at int64) (typ recordType, rel string, logicalOff int64, payloadStart int64, payloadLen int, next int64, err error) {
	if int(at)+headerFixedSize+trailerSize > len(data) {
		err = io.ErrUnexpectedEOF
		return
	}
	p := data[at:]
	if [4]byte{p[0], p[1], p[2], p[3]} != recordMagic {
		err = errors.New("bad magic")
		return
	}
	typ = recordType(p[4])
	relLen := int(binary.LittleEndian.Uint16(p[5:7]))
	off := 7
	if off+relLen+8+4+trailerSize > len(p) {
		err = io.ErrUnexpectedEOF
		return
	}
	rel = string(p[off : off+relLen])
	off += relLen
	logicalOff = int64(binary.LittleEndian.Uint64(p[off : off+8]))
	off += 8
	payloadLen = int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	payloadStart = at + int64(off)
	if off+payloadLen+trailerSize > len(p) {
		err = io.ErrUnexpectedEOF
		return
	}
	off += payloadLen
	gotCRC := binary.LittleEndian.Uint32(p[off : off+4])
	wantCRC := crc32.ChecksumIEEE(p[4:off])
	if gotCRC != wantCRC {
		err = errors.New("crc mismatch")
		return
	}
	next = at + int64(off) + trailerSize
	return
}

// Segment is an append-only file. The active segment's writes are serialized
// by the owning Writer; all other segments are read-only. Reads on any
// segment happen via pread and are safe concurrent.
type Segment struct {
	id      uint32
	tier    Tier
	path    string
	f       *os.File
	size    int64 // bytes written (from fstat or tracked)
	mu      sync.Mutex
	scratch []byte // reusable frame buffer, protected by mu
}

func openSegment(path string, id uint32, tier Tier) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{id: id, tier: tier, path: path, f: f, size: st.Size()}, nil
}

func (s *Segment) Close() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// ReadAt reads `len(p)` bytes from the payload at `off` (an absolute byte
// offset in the segment file). Safe for concurrent callers.
func (s *Segment) ReadAt(p []byte, off int64) (int, error) {
	return s.f.ReadAt(p, off)
}

// Size returns the current byte length of the segment.
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// maxRetainedScratch caps the size of the reusable per-segment scratch
// buffer. Larger records are still written correctly, but their buffer is
// dropped after the write so we don't hold tens of MB (or more) forever in
// every long-lived segment.
const maxRetainedScratch = 32 << 20 // 32 MiB

// Append writes a framed record to the segment (must be the active segment).
// Returns the offset of the payload within the segment. The per-segment
// scratch buffer is reused across calls to avoid allocating the frame each
// time; s.mu serializes both the scratch usage and the underlying file.
func (s *Segment) Append(typ recordType, rel string, logicalOff int64, payload []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := headerFixedSize + len(rel) + len(payload) + trailerSize
	if cap(s.scratch) < total {
		s.scratch = make([]byte, total)
	} else {
		s.scratch = s.scratch[:total]
	}
	payloadStart := frameRecordInto(s.scratch, typ, rel, logicalOff, payload)
	n, err := s.f.Write(s.scratch)
	// Release the scratch if it grew beyond the retention cap (or if the
	// write failed partially — avoid keeping a suspect buffer around).
	if cap(s.scratch) > maxRetainedScratch || (err == nil && n != len(s.scratch)) {
		s.scratch = nil
	}
	if err != nil {
		return 0, err
	}
	if n != total {
		return 0, io.ErrShortWrite
	}
	segOff := s.size + int64(payloadStart)
	s.size += int64(n)
	return segOff, nil
}

// Sync fsyncs the segment file.
func (s *Segment) Sync() error {
	return s.f.Sync()
}

// releaseScratch drops the append scratch buffer; called when the segment
// is rotated out (sealed) so we don't retain the per-segment scratch
// forever. Safe to call on a segment that may still be used — the next
// Append will re-allocate.
func (s *Segment) releaseScratch() {
	s.mu.Lock()
	s.scratch = nil
	s.mu.Unlock()
}

// Truncate shrinks the segment to the given size and updates internal state.
func (s *Segment) Truncate(size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if size > s.size {
		return fmt.Errorf("truncate grow not supported")
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

// SegmentSet tracks all segments for a tier.
type SegmentSet struct {
	tier    Tier
	dir     string
	maxSize int64

	mu       sync.RWMutex
	segments map[uint32]*Segment
	active   *Segment
	nextID   uint32
}

func NewSegmentSet(tier Tier, rootPath string, maxSize int64) (*SegmentSet, error) {
	dir := filepath.Join(rootPath, ".place", "segments")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	s := &SegmentSet{
		tier:     tier,
		dir:      dir,
		maxSize:  maxSize,
		segments: map[uint32]*Segment{},
	}
	if err := s.loadExisting(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SegmentSet) loadExisting() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
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
		seg, err := openSegment(filepath.Join(s.dir, segFileName(id)), id, s.tier)
		if err != nil {
			return err
		}
		s.segments[id] = seg
		if id >= s.nextID {
			s.nextID = id + 1
		}
	}
	// Active segment = highest-numbered existing one, if any.
	if len(ids) > 0 {
		s.active = s.segments[ids[len(ids)-1]]
	}
	return nil
}

func segFileName(id uint32) string {
	return fmt.Sprintf("%08d.seg", id)
}

// Get returns a segment by id, or nil if missing.
func (s *SegmentSet) Get(id uint32) *Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.segments[id]
}

// Active returns the current active segment, creating one if none exists.
func (s *SegmentSet) Active() (*Segment, error) {
	s.mu.RLock()
	a := s.active
	s.mu.RUnlock()
	if a != nil {
		return a, nil
	}
	return s.newActive()
}

func (s *SegmentSet) newActive() (*Segment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil {
		return s.active, nil
	}
	id := s.nextID
	s.nextID++
	seg, err := openSegment(filepath.Join(s.dir, segFileName(id)), id, s.tier)
	if err != nil {
		return nil, err
	}
	s.segments[id] = seg
	s.active = seg
	return seg, nil
}

// RotateIfFull seals the active segment and creates a new one if appending
// `next` bytes would exceed maxSize. Caller must hold no locks.
func (s *SegmentSet) RotateIfFull(next int64) (*Segment, bool, error) {
	s.mu.Lock()
	a := s.active
	if a != nil && a.Size()+next <= s.maxSize {
		s.mu.Unlock()
		return a, false, nil
	}
	if a != nil {
		if err := a.Sync(); err != nil {
			s.mu.Unlock()
			return nil, false, err
		}
		// The rotated-out segment won't receive more appends; drop its
		// scratch buffer so we don't hold 16+ MB per retired segment.
		a.releaseScratch()
	}
	id := s.nextID
	s.nextID++
	seg, err := openSegment(filepath.Join(s.dir, segFileName(id)), id, s.tier)
	if err != nil {
		s.mu.Unlock()
		return nil, false, err
	}
	s.segments[id] = seg
	s.active = seg
	s.mu.Unlock()
	return seg, a != nil, nil // rotated = true if there was a previous active
}

// Remove closes and unlinks a segment by id. Caller must ensure no FileMeta
// references it any more.
func (s *SegmentSet) Remove(id uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	seg, ok := s.segments[id]
	if !ok {
		return nil
	}
	_ = seg.Close()
	delete(s.segments, id)
	if s.active == seg {
		s.active = nil
	}
	return os.Remove(filepath.Join(s.dir, segFileName(id)))
}

// All returns a snapshot of all segment ids.
func (s *SegmentSet) All() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint32, 0, len(s.segments))
	for id := range s.segments {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Dir returns the on-disk directory containing segment files.
func (s *SegmentSet) Dir() string { return s.dir }

// CloseAll closes all open segment fds.
func (s *SegmentSet) CloseAll() error {
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

// RegisterInMeta records a freshly-seen segment in bbolt (creation time, zero
// live bytes, not sealed). Idempotent.
func (s *SegmentSet) RegisterInMeta(meta *Meta) error {
	return meta.db.Update(func(tx *bolt.Tx) error {
		for id, seg := range s.segments {
			existing, err := GetSegmentTx(tx, s.tier, id)
			if err != nil {
				return err
			}
			if existing == nil {
				sm := &SegmentMeta{
					ID:        id,
					Tier:      s.tier,
					Total:     seg.size,
					Live:      seg.size, // will be reconciled by recovery if needed
					Sealed:    s.active != seg,
					CreatedAt: time.Now().UnixNano(),
				}
				if err := PutSegmentTx(tx, sm); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
