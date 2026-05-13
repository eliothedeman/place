// Package index is the L2 storage engine. It owns:
//   - the bbolt-backed range index (per-inode, per-stripe fragment lists),
//   - the L1 segment substrate (private to this package),
//   - the inode-id counter,
//   - the Move primitive that re-canonicalizes a stripe (optionally moving
//     it to a different tier), which is the only "mover" operation,
//   - the GC primitive that drops segments with no live references.
//
// The index has no concept of paths, modes, or directory trees. Build those
// at L4 (the lib/store package) on top.
package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/eliothedeman/place/lib/segment"
	bolt "go.etcd.io/bbolt"
)

// Tier and Locator are re-exported from L1 for convenience; higher layers
// should reference these names rather than reaching into lib/segment.
type Tier = segment.Tier
type Locator = segment.Locator

const (
	TierHot  = segment.TierHot
	TierCold = segment.TierCold
)

// DefaultStripeSize is the logical-byte width of one stripe. Files are
// partitioned into stripes; each stripe has its own fragment list. 64 MiB
// is a workload-fit default for very large files written via torrents.
const DefaultStripeSize int64 = 64 << 20

// DefaultSegmentMaxSize is the size at which a segment file rotates. 1 GiB
// keeps fd count bounded for tens-of-TB datasets without making any single
// segment painful to fsync.
const DefaultSegmentMaxSize int64 = 1 << 30

// subdir is the dotfile prefix the new format uses inside HotDir/ColdDir.
// Picked to never collide with the old format's ".place/" — both layouts
// can live in the same hot/cold directories so an old → new migration
// doesn't need to move bytes around.
const subdir = ".placefs"

var (
	bucketMeta    = []byte("_meta")
	bucketStripes = []byte("stripes")
)

// Config controls Index startup.
type Config struct {
	// HotDir / ColdDir are the user-supplied paths the legacy binary called
	// --hot and --cold. The index keeps its own state inside HotDir/.placefs/
	// and ColdDir/.placefs/, leaving the rest of those directories alone.
	HotDir, ColdDir string
	// DBPath is the bbolt file. Defaults to HotDir/.placefs/db.bolt.
	DBPath string
	// StripeSize is the logical width of one stripe. Defaults to DefaultStripeSize.
	StripeSize int64
	// SegmentMaxSize is the size at which segments rotate. Defaults to DefaultSegmentMaxSize.
	SegmentMaxSize int64

	// Root is a test convenience: if set (and HotDir/ColdDir are not), the
	// index treats Root/hot and Root/cold as the hot and cold directories
	// and puts the DB at Root/.placefs/db.bolt. Production code should set
	// HotDir/ColdDir explicitly.
	Root string
}

// Index is the L2 storage engine handle.
type Index struct {
	cfg        Config
	stripeSize int64

	db   *bolt.DB
	hot  *segment.Set
	cold *segment.Set

	// writeMu serializes Append and Move at the index level so they can
	// safely interleave bbolt updates with segment IO without holding the
	// bbolt write lock across all of it. Bbolt's own writer-lock would also
	// serialize, but we prefer to keep our segment writes outside any
	// bbolt tx so that a slow fsync doesn't block read txs.
	writeMu sync.Mutex
}

// Open creates or opens the index on the given hot/cold directories.
func Open(cfg Config) (*Index, error) {
	if cfg.Root != "" && cfg.HotDir == "" {
		cfg.HotDir = filepath.Join(cfg.Root, "hot")
	}
	if cfg.Root != "" && cfg.ColdDir == "" {
		cfg.ColdDir = filepath.Join(cfg.Root, "cold")
	}
	if cfg.HotDir == "" || cfg.ColdDir == "" {
		return nil, errors.New("index: HotDir and ColdDir are required (or Root for tests)")
	}
	if cfg.StripeSize == 0 {
		cfg.StripeSize = DefaultStripeSize
	}
	if cfg.SegmentMaxSize == 0 {
		cfg.SegmentMaxSize = DefaultSegmentMaxSize
	}
	if cfg.StripeSize <= 0 || cfg.StripeSize&(cfg.StripeSize-1) != 0 {
		// Not required mathematically, but lets us stay simple about the
		// "fragment can't span a stripe" invariant and makes stripe math
		// cheap.
		return nil, fmt.Errorf("index: StripeSize must be a positive power of 2 (got %d)", cfg.StripeSize)
	}
	hotSubdir := filepath.Join(cfg.HotDir, subdir)
	coldSubdir := filepath.Join(cfg.ColdDir, subdir)
	if err := os.MkdirAll(hotSubdir, 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(coldSubdir, 0o700); err != nil {
		return nil, err
	}
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(hotSubdir, "db.bolt")
	}
	db, err := bolt.Open(cfg.DBPath, 0o600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketStripes} {
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
	hot, err := segment.OpenSet(filepath.Join(hotSubdir, "segments"), segment.TierHot, cfg.SegmentMaxSize)
	if err != nil {
		db.Close()
		return nil, err
	}
	cold, err := segment.OpenSet(filepath.Join(coldSubdir, "segments"), segment.TierCold, cfg.SegmentMaxSize)
	if err != nil {
		hot.CloseAll()
		db.Close()
		return nil, err
	}
	if err := hot.RepairTail(); err != nil {
		cold.CloseAll()
		hot.CloseAll()
		db.Close()
		return nil, err
	}
	if err := cold.RepairTail(); err != nil {
		cold.CloseAll()
		hot.CloseAll()
		db.Close()
		return nil, err
	}
	return &Index{
		cfg:        cfg,
		stripeSize: cfg.StripeSize,
		db:         db,
		hot:        hot,
		cold:       cold,
	}, nil
}

// Close releases all resources. Idempotent.
func (idx *Index) Close() error {
	var firstErr error
	if err := idx.hot.SyncAll(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := idx.cold.SyncAll(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := idx.db.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := idx.hot.CloseAll(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := idx.cold.CloseAll(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// StripeSize returns the configured stripe width.
func (idx *Index) StripeSize() int64 { return idx.stripeSize }

// DB exposes the underlying bbolt DB for layers above (L4 uses it for its
// own buckets so the whole filesystem stays in a single durable DB). L2's
// own buckets are off-limits to other layers.
func (idx *Index) DB() *bolt.DB { return idx.db }

// stripeKey returns the bbolt key for (inode, stripe). Big-endian so cursor
// order matches numeric order — handy for prefix scans by inode.
func stripeKey(inode uint64, stripeID uint32) []byte {
	var k [12]byte
	binary.BigEndian.PutUint64(k[0:], inode)
	binary.BigEndian.PutUint32(k[8:], stripeID)
	return k[:]
}

func inodePrefix(inode uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], inode)
	return k[:]
}

func (idx *Index) stripeOf(logicalOff int64) uint32 {
	return uint32(logicalOff / idx.stripeSize)
}

func (idx *Index) setFor(tier Tier) *segment.Set {
	if tier == segment.TierCold {
		return idx.cold
	}
	return idx.hot
}

// Append writes payload at logicalOff for inode, landing it in the hot
// tier. The write may be split into multiple per-stripe records if it
// crosses a stripe boundary; each piece becomes one Fragment.
func (idx *Index) Append(inode uint64, logicalOff int64, payload []byte) error {
	return idx.AppendTo(inode, logicalOff, payload, segment.TierHot)
}

// AppendTo is the same as Append but lets the caller pick which tier the
// bytes land in. Used by the migrator to write directly to cold so a bulk
// migration doesn't fill up the (typically smaller) hot drive.
//
// The flow per chunk: take writeMu, write the framed record to the active
// segment of the chosen tier, fsync the segment, then in a bbolt write tx
// allocate a fresh per-stripe seq and append the Fragment to the stripe's
// list. If the bbolt tx fails for any reason, the bytes in the segment
// become orphans and GC will reclaim them — the index never holds a
// fragment pointing at non-durable bytes.
func (idx *Index) AppendTo(inode uint64, logicalOff int64, payload []byte, tier Tier) error {
	if len(payload) == 0 {
		return nil
	}
	rem := payload
	off := logicalOff
	for len(rem) > 0 {
		stripeID := idx.stripeOf(off)
		stripeEnd := (int64(stripeID) + 1) * idx.stripeSize
		chunkLen := int64(len(rem))
		if off+chunkLen > stripeEnd {
			chunkLen = stripeEnd - off
		}
		if err := idx.appendChunk(inode, stripeID, off, rem[:chunkLen], tier); err != nil {
			return err
		}
		rem = rem[chunkLen:]
		off += chunkLen
	}
	return nil
}

func (idx *Index) appendChunk(inode uint64, stripeID uint32, logicalOff int64, payload []byte, tier Tier) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()

	// Pre-flight: pick a seq one greater than anything currently in the
	// stripe. Read-only tx is cheap.
	var nextSeq uint64 = 1
	err := idx.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketStripes).Get(stripeKey(inode, stripeID))
		for _, f := range decodeFragments(v) {
			if f.Seq >= nextSeq {
				nextSeq = f.Seq + 1
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Write the payload to the active segment of the target tier + fsync.
	set := idx.setFor(tier)
	seg, err := set.Active(segment.FramedSize(len(payload)))
	if err != nil {
		return err
	}
	payloadOff, err := seg.Append(segment.RecordHeader{
		Inode:      inode,
		StripeID:   stripeID,
		Seq:        nextSeq,
		LogicalOff: logicalOff,
		Length:     uint32(len(payload)),
	}, payload)
	if err != nil {
		return err
	}
	if err := seg.Sync(); err != nil {
		return err
	}

	// Commit the fragment to bbolt. If another caller raced us between the
	// View tx above and now, our nextSeq might collide. Resolve by re-reading
	// inside the Update tx and bumping if needed — fragments are immutable
	// so the only collision-risk is the seq number itself.
	frag := Fragment{
		Seq:           nextSeq,
		LogicalOff:    logicalOff,
		Length:        int64(len(payload)),
		Tier:          tier,
		SegmentID:     seg.ID(),
		SegmentOffset: payloadOff,
	}
	return idx.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketStripes)
		key := stripeKey(inode, stripeID)
		existing := decodeFragments(sb.Get(key))
		for _, f := range existing {
			if f.Seq >= frag.Seq {
				frag.Seq = f.Seq + 1
			}
		}
		existing = append(existing, frag)
		return sb.Put(key, encodeFragments(existing))
	})
}

// PlanRead returns the slices required to satisfy a read of [off, off+length)
// for inode. Each slice is either a Locator (caller should pread) or a
// Sparse marker (caller chooses how to fill — block, zero-fill, etc.).
// Slices are sorted by logical offset and non-overlapping.
func (idx *Index) PlanRead(inode uint64, off, length int64) ([]ReadSlice, error) {
	if length <= 0 {
		return nil, nil
	}
	first := idx.stripeOf(off)
	last := idx.stripeOf(off + length - 1)
	var allFrags []Fragment
	err := idx.db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketStripes)
		for s := first; s <= last; s++ {
			v := sb.Get(stripeKey(inode, s))
			if v == nil {
				continue
			}
			allFrags = append(allFrags, decodeFragments(v)...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return planRead(allFrags, off, length), nil
}

// ReadAt pulls bytes from disk for one Locator. Returns the number of bytes
// read. Concurrent-safe.
func (idx *Index) ReadAt(loc Locator, p []byte) (int, error) {
	set := idx.setFor(loc.Tier)
	seg := set.Get(loc.SegmentID)
	if seg == nil {
		return 0, fmt.Errorf("index: missing segment %d in %s", loc.SegmentID, loc.Tier)
	}
	if int64(len(p)) > loc.Length {
		p = p[:loc.Length]
	}
	return seg.ReadAt(p, loc.Offset)
}

// Truncate drops any fragment data past `size` for inode. Stripes wholly
// past `size` are removed; the stripe containing the truncate point has its
// fragments clipped or removed. Data in segments still on disk remains until
// GC reclaims any segments whose ref-count hits zero.
func (idx *Index) Truncate(inode uint64, size int64) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	return idx.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketStripes)
		c := sb.Cursor()
		prefix := inodePrefix(inode)
		// Collect keys/updates first; mutating during cursor iter is dicey.
		type op struct {
			key []byte
			val []byte // nil => delete
		}
		var ops []op
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			stripeID := binary.BigEndian.Uint32(k[8:])
			stripeStart := int64(stripeID) * idx.stripeSize
			if stripeStart >= size {
				kc := append([]byte{}, k...)
				ops = append(ops, op{key: kc})
				continue
			}
			frags := decodeFragments(v)
			var kept []Fragment
			for _, f := range frags {
				if f.LogicalOff >= size {
					continue
				}
				end := f.LogicalOff + f.Length
				if end > size {
					// Clip: keep [LogicalOff, size). Note this leaves the
					// trailing bytes in the segment as future GC fodder; the
					// fragment record itself just stops describing them.
					f.Length = size - f.LogicalOff
				}
				kept = append(kept, f)
			}
			kc := append([]byte{}, k...)
			if len(kept) == 0 {
				ops = append(ops, op{key: kc})
			} else {
				ops = append(ops, op{key: kc, val: encodeFragments(kept)})
			}
		}
		for _, o := range ops {
			if o.val == nil {
				if err := sb.Delete(o.key); err != nil {
					return err
				}
			} else {
				if err := sb.Put(o.key, o.val); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// DeleteInode removes every fragment for inode. Bytes in segments remain
// until GC reclaims them.
func (idx *Index) DeleteInode(inode uint64) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	return idx.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketStripes)
		c := sb.Cursor()
		prefix := inodePrefix(inode)
		var keys [][]byte
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			keys = append(keys, append([]byte{}, k...))
		}
		for _, k := range keys {
			if err := sb.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// StripeInfo summarises one stripe — used by L3 to pick movement candidates.
type StripeInfo struct {
	Inode       uint64
	StripeID    uint32
	HotBytes    int64
	ColdBytes   int64
	// LiveBytes is the logical byte count after canonical-view shadowing.
	// (Total HotBytes+ColdBytes can be larger if there's overlap.)
	LiveBytes int64
}

// IterStripes calls fn for every stripe that has at least one fragment.
// Stopping (fn returns false) ends iteration. The callback must not call
// back into idx (would deadlock the read tx).
func (idx *Index) IterStripes(fn func(StripeInfo) bool) error {
	return idx.db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketStripes)
		c := sb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(k) != 12 {
				continue
			}
			info := StripeInfo{
				Inode:    binary.BigEndian.Uint64(k[0:8]),
				StripeID: binary.BigEndian.Uint32(k[8:12]),
			}
			frags := decodeFragments(v)
			for _, f := range frags {
				switch f.Tier {
				case segment.TierHot:
					info.HotBytes += f.Length
				case segment.TierCold:
					info.ColdBytes += f.Length
				}
			}
			for _, f := range canonicalView(frags) {
				info.LiveBytes += f.Length
			}
			if !fn(info) {
				return nil
			}
		}
		return nil
	})
}

// Move re-canonicalizes one stripe into target tier. Reads the current
// fragment list, computes the canonical view (newer-wins), and for any
// canonical fragment not yet in target tier, copies the bytes into a
// target-tier segment and replaces the fragment list with the new
// canonical layout. Old fragments stay on disk as orphans until GC.
//
// If the stripe is already entirely in target tier this is still useful:
// it collapses overlap into a clean fragment list and frees the now-dead
// underlying bytes for GC.
//
// Move acquires writeMu to keep its segment IO ordered against Append.
// The bytes to copy are read with no lock held; bbolt's tx is only opened
// after the IO completes to make the swap atomic relative to readers.
func (idx *Index) Move(inode uint64, stripeID uint32, target Tier) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()

	// Snapshot the current fragment list.
	var frags []Fragment
	err := idx.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketStripes).Get(stripeKey(inode, stripeID))
		frags = decodeFragments(v)
		return nil
	})
	if err != nil {
		return err
	}
	if len(frags) == 0 {
		return nil
	}

	view := canonicalView(frags)

	// Determine starting seq for the rewritten fragments. Bump above the
	// existing max so readers consistently see "the newer view wins."
	var maxSeq uint64
	for _, f := range frags {
		if f.Seq > maxSeq {
			maxSeq = f.Seq
		}
	}
	nextSeq := maxSeq + 1

	// Walk the canonical view; for fragments not in target tier, copy bytes.
	var newFrags []Fragment
	targetSet := idx.setFor(target)
	for _, f := range view {
		if f.Tier == target {
			// Re-emit with a new seq so the rewritten list is a clean tail-
			// free canonical layout.
			f.Seq = nextSeq
			nextSeq++
			newFrags = append(newFrags, f)
			continue
		}
		buf := make([]byte, f.Length)
		sourceSet := idx.setFor(f.Tier)
		sourceSeg := sourceSet.Get(f.SegmentID)
		if sourceSeg == nil {
			return fmt.Errorf("index: Move: source segment %d in %s missing", f.SegmentID, f.Tier)
		}
		if _, err := sourceSeg.ReadAt(buf, f.SegmentOffset); err != nil {
			return err
		}
		seg, err := targetSet.Active(segment.FramedSize(len(buf)))
		if err != nil {
			return err
		}
		payloadOff, err := seg.Append(segment.RecordHeader{
			Inode: inode, StripeID: stripeID, Seq: nextSeq,
			LogicalOff: f.LogicalOff, Length: uint32(len(buf)),
		}, buf)
		if err != nil {
			return err
		}
		if err := seg.Sync(); err != nil {
			return err
		}
		newFrags = append(newFrags, Fragment{
			Seq:           nextSeq,
			LogicalOff:    f.LogicalOff,
			Length:        f.Length,
			Tier:          target,
			SegmentID:     seg.ID(),
			SegmentOffset: payloadOff,
		})
		nextSeq++
	}

	// Swap in the new list. We must re-read inside the tx to detect a
	// concurrent Append that arrived between our snapshot and now. With
	// writeMu held that should be impossible (Append also takes writeMu),
	// but a defensive re-read keeps the invariant local and obvious.
	return idx.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketStripes)
		key := stripeKey(inode, stripeID)
		current := decodeFragments(sb.Get(key))
		if !sameFragments(current, frags) {
			return errors.New("index: Move: concurrent modification (should not happen with writeMu held)")
		}
		return sb.Put(key, encodeFragments(newFrags))
	})
}

func sameFragments(a, b []Fragment) bool {
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

// GC removes any segment that no fragment references. Active segments (the
// current write target for each tier) are always retained even if empty,
// since they'll be written to next.
func (idx *Index) GC() error {
	// Build referenced-set from bbolt.
	referenced := map[segment.Tier]map[uint32]bool{
		segment.TierHot:  {},
		segment.TierCold: {},
	}
	err := idx.db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketStripes)
		return sb.ForEach(func(k, v []byte) error {
			for _, f := range decodeFragments(v) {
				referenced[f.Tier][f.SegmentID] = true
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	// Preserve the active segment of each tier even if empty — it's the
	// next write target and shouldn't be unlinked from under the writer.
	for _, s := range []*segment.Set{idx.hot, idx.cold} {
		if id, ok := s.ActiveID(); ok {
			referenced[s.Tier()][id] = true
		}
	}
	// Remove unreferenced segments. Two passes per tier:
	//   1. Iterate the in-memory set and Remove() any id with no live ref.
	//      This drops the fd and unlinks the .seg file.
	//   2. Walk the segments dir on disk for stray .seg files that aren't
	//      in our in-memory set at all — i.e. files that appeared between
	//      OpenSet and now (operator dropped a file, crash recovery left a
	//      partially-flushed segment, etc.). Unlink any whose id isn't
	//      referenced.
	for _, s := range []*segment.Set{idx.hot, idx.cold} {
		refs := referenced[s.Tier()]
		for _, id := range s.All() {
			if !refs[id] {
				if err := s.Remove(id); err != nil {
					return err
				}
			}
		}
		entries, err := os.ReadDir(s.Dir())
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
				continue
			}
			base := strings.TrimSuffix(e.Name(), ".seg")
			id64, perr := strconv.ParseUint(base, 10, 32)
			if perr != nil {
				continue
			}
			id := uint32(id64)
			if refs[id] || s.Get(id) != nil {
				continue
			}
			if err := os.Remove(filepath.Join(s.Dir(), e.Name())); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// SegmentIDs returns the live segment ids by tier. Useful for telemetry
// and for L3 policies that watch fragmentation.
func (idx *Index) SegmentIDs(tier Tier) []uint32 {
	return idx.setFor(tier).All()
}

// HotDir / ColdDir return the user-supplied directories — useful for
// adapters that want to syscall.Statfs the user's actual filesystem and
// for the migrator's detect-old-layout step. (Internally segments live in
// {HotDir}/.placefs/segments and {ColdDir}/.placefs/segments.)
func (idx *Index) HotDir() string  { return idx.cfg.HotDir }
func (idx *Index) ColdDir() string { return idx.cfg.ColdDir }

// HotUsedBytes is the sum of bytes across hot segments — what L3 watches
// for eviction pressure.
func (idx *Index) HotUsedBytes() int64 {
	var n int64
	for _, id := range idx.hot.All() {
		if seg := idx.hot.Get(id); seg != nil {
			n += seg.Size()
		}
	}
	return n
}

// Sync makes every accumulated write durable on disk (segments + bbolt).
// Append already fsyncs the touched segment + commits the tx, so this is
// only needed if a caller wants to force a global sync barrier.
func (idx *Index) Sync() error {
	if err := idx.hot.SyncAll(); err != nil {
		return err
	}
	if err := idx.cold.SyncAll(); err != nil {
		return err
	}
	return idx.db.Sync()
}

// FragmentsOf returns a snapshot of the fragment list for one stripe.
// Used by tests and by L3 for inspection. Returns nil if absent.
func (idx *Index) FragmentsOf(inode uint64, stripeID uint32) ([]Fragment, error) {
	var out []Fragment
	err := idx.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketStripes).Get(stripeKey(inode, stripeID))
		out = decodeFragments(v)
		return nil
	})
	return out, err
}

// StripesOf returns every stripe id that has fragments for inode.
func (idx *Index) StripesOf(inode uint64) ([]uint32, error) {
	var out []uint32
	err := idx.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketStripes).Cursor()
		prefix := inodePrefix(inode)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			out = append(out, binary.BigEndian.Uint32(k[8:]))
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, err
}
