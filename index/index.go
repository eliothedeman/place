// Package index is the L2 storage engine. It owns:
//   - the kv-backed range index (per-inode, per-stripe fragment lists),
//   - the L1 segment substrate (private to this package),
//   - the inode-id counter,
//   - the Move primitive that re-canonicalizes a stripe (optionally moving
//     it to a different tier), which is the only "mover" operation,
//   - the GC primitive that drops segments with no live references.
//
// The index has no concept of paths, modes, or directory trees. Build those
// at L4 (the store package) on top.
package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eliothedeman/place/kv"
	"github.com/eliothedeman/place/obs"
	"github.com/eliothedeman/place/segment"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tier and Locator are re-exported from L1 for convenience; higher layers
// should reference these names rather than reaching into segment.
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

// pebbleSubdir is the directory under .placefs/ that holds the pebble DB.
// Kept separate from any legacy db.bolt file so the bbolt-to-pebble
// migration can run with both files present.
const pebbleSubdir = "pebble"

// Config controls Index startup.
type Config struct {
	// HotDir / ColdDir are the user-supplied paths the legacy binary called
	// --hot and --cold. The index keeps its own state inside HotDir/.placefs/
	// and ColdDir/.placefs/, leaving the rest of those directories alone.
	HotDir, ColdDir string
	// DBPath is the pebble DB directory. Defaults to HotDir/.placefs/pebble/.
	DBPath string
	// StripeSize is the logical width of one stripe. Defaults to DefaultStripeSize.
	StripeSize int64
	// SegmentMaxSize is the size at which segments rotate. Defaults to DefaultSegmentMaxSize.
	SegmentMaxSize int64

	// Root is a test convenience: if set (and HotDir/ColdDir are not), the
	// index treats Root/hot and Root/cold as the hot and cold directories
	// and puts the DB at Root/.placefs/pebble/. Production code should set
	// HotDir/ColdDir explicitly.
	Root string
}

// Index is the L2 storage engine handle.
type Index struct {
	cfg        Config
	stripeSize int64

	db   *kv.DB
	hot  *segment.Set
	cold *segment.Set

	// writeMu coordinates appends vs structural mutations.
	//
	//   - Append takes RLock: many appends run concurrently (different
	//     stripes are fully independent).
	//   - Move / Truncate / DeleteInode / BulkSession.Commit take Lock:
	//     they need exclusive access because they snapshot fragment
	//     state and assume nothing changes underneath them.
	//
	// Concurrent appends are essential for FUSE throughput: clients like
	// qbittorrent issue many simultaneous writes from different connections,
	// and a single Mutex turns them into a serial queue waiting on fsync.
	writeMu sync.RWMutex

	// commitLocks serializes the read-modify-write phase of concurrent
	// appends to the same inode WHEN there is a txHook. With per-
	// fragment keys, the fragment writes themselves are independent and
	// don't need the lock — each writer Sets a distinct key. The hook
	// (today: store node Size/Mtime update) still does a read-modify-
	// write of the node, so concurrent writers to the same inode would
	// lose each other's Size bumps without coordination.
	//
	// Per-inode striping keeps cross-inode concurrency. The pool is
	// small and hashed; collisions just add a touch of pessimistic
	// locking, never correctness loss.
	commitLocks [numCommitLocks]sync.Mutex

	// seqGen issues monotonically increasing fragment seqs. Seeded at
	// startup from time.Now().UnixNano(), which guarantees new seqs sort
	// strictly above seqs from any previous boot (assuming the wall
	// clock didn't go backward across the restart). Atomic so concurrent
	// appenders can claim unique seqs without locking.
	seqGen atomic.Uint64
}

// numCommitLocks is the size of the per-inode commit-lock pool. A power
// of two so the hash is a mask. 256 is more than enough for typical FUSE
// loads (rarely more than a handful of concurrently-written files).
const numCommitLocks = 256

func (idx *Index) commitLockFor(inode uint64) *sync.Mutex {
	return &idx.commitLocks[inode&(numCommitLocks-1)]
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
		cfg.DBPath = filepath.Join(hotSubdir, pebbleSubdir)
	}
	// Migrate from legacy bbolt format if a db.bolt exists alongside the
	// new pebble directory. Idempotent — does nothing when the pebble DB
	// already exists. We invoke it here rather than at the main() layer
	// so packaging tests + the placefs binary share the migration path.
	legacyBolt := filepath.Join(hotSubdir, "db.bolt")
	if err := kv.MigrateFromBolt(legacyBolt, cfg.DBPath, nil); err != nil && !errors.Is(err, kv.ErrAlreadyMigrated) {
		return nil, fmt.Errorf("index: migrate legacy bbolt: %w", err)
	}
	db, err := kv.Open(cfg.DBPath)
	if err != nil {
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
	idx := &Index{
		cfg:        cfg,
		stripeSize: cfg.StripeSize,
		db:         db,
		hot:        hot,
		cold:       cold,
	}
	// Seed the per-fragment seq counter. Using wall-clock nanos as the
	// seed gives a value that's monotonically above anything any earlier
	// boot wrote, so we don't need to scan the DB at startup. If the
	// wall clock skews backward across a restart, the next migrateBlobs
	// scan still catches any too-low seqs and bumps the counter past
	// them.
	idx.seqGen.Store(uint64(time.Now().UnixNano()))
	if err := idx.migrateBlobsToFragments(); err != nil {
		idx.Close()
		return nil, fmt.Errorf("index: blob→fragment migration: %w", err)
	}
	return idx, nil
}

// nextSeq returns a fresh, never-reused fragment seq. Atomic Add gives
// us monotonicity across concurrent appenders for free.
func (idx *Index) nextSeq() uint64 {
	return idx.seqGen.Add(1)
}

// migrateBlobsToFragments walks the legacy blob-per-stripe layout (length-13
// keys under the 's' tag, value = concatenated Fragment list) and splits each
// blob into per-fragment keys (length-21, value = single Fragment), then
// deletes the blob. Idempotent: a clean DB has no length-13 keys and the
// scan is a single Pebble prefix iter that completes in µs.
//
// Run once at Open. Concurrent writes haven't started yet, so the scan
// sees a stable snapshot. After migration the data path only handles
// per-fragment keys.
func (idx *Index) migrateBlobsToFragments() error {
	prefix := kv.StripeAllPrefix()
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	var (
		legacyKeys [][]byte
		newSets    []struct{ k, v []byte }
		maxSeq     uint64
	)
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if !kv.IsLegacyStripeKey(k) {
			// Stray per-fragment key — possibly left over from a partial
			// migration. Treat its seq as authoritative when seeding
			// the counter.
			if kv.IsFragmentKey(k) {
				if s := kv.SeqFromFragmentKey(k); s > maxSeq {
					maxSeq = s
				}
			}
			continue
		}
		inode := kv.StripeInodeFromFragmentKey(k) // legacy keys share the same inode encoding
		stripeID := kv.StripeIDFromFragmentKey(k)
		legacyKeys = append(legacyKeys, append([]byte{}, k...))
		for _, f := range decodeFragments(it.Value()) {
			if f.Seq > maxSeq {
				maxSeq = f.Seq
			}
			newSets = append(newSets, struct{ k, v []byte }{
				k: kv.FragmentKey(inode, stripeID, f.Seq),
				v: encodeFragments([]Fragment{f}),
			})
		}
	}
	if err := it.Close(); err != nil {
		return err
	}
	if len(legacyKeys) == 0 && maxSeq == 0 {
		return nil
	}
	// Make sure the in-memory seq generator starts above any
	// previously-issued seq.
	for {
		cur := idx.seqGen.Load()
		if cur > maxSeq {
			break
		}
		if idx.seqGen.CompareAndSwap(cur, maxSeq+1) {
			break
		}
	}
	if len(legacyKeys) == 0 {
		return nil
	}
	b := idx.db.NewBatch()
	for _, s := range newSets {
		if err := b.Set(s.k, s.v); err != nil {
			b.Close()
			return err
		}
	}
	for _, k := range legacyKeys {
		if err := b.Delete(k); err != nil {
			b.Close()
			return err
		}
	}
	return b.Commit(true)
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

// DB exposes the underlying kv handle for layers above (L4 uses it for its
// own keys so the whole filesystem stays in a single durable DB). L2's
// own key prefixes are off-limits to other layers.
func (idx *Index) DB() *kv.DB { return idx.db }

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
	return idx.AppendToCtx(context.Background(), inode, logicalOff, payload, segment.TierHot)
}

// AppendCtx is the ctx-aware variant of Append. Spans started inside the
// call hang off ctx so a single FUSE write shows the full breakdown
// (segment append + fsync + pebble commit) in one trace.
func (idx *Index) AppendCtx(ctx context.Context, inode uint64, logicalOff int64, payload []byte) error {
	return idx.AppendToCtx(ctx, inode, logicalOff, payload, segment.TierHot)
}

// AppendTo is the same as Append but lets the caller pick which tier the
// bytes land in. Used by the migrator to write directly to cold so a bulk
// migration doesn't fill up the (typically smaller) hot drive.
//
// The flow per chunk: take writeMu, write the framed record to the active
// segment of the chosen tier, fsync the segment, then in a single pebble
// batch write every fragment record. If the commit fails for any reason,
// the bytes in the segment become orphans and GC will reclaim them — the
// index never holds a fragment pointing at non-durable bytes.
func (idx *Index) AppendTo(inode uint64, logicalOff int64, payload []byte, tier Tier) error {
	return idx.AppendToCtx(context.Background(), inode, logicalOff, payload, tier)
}

// AppendToCtx is the ctx-aware variant of AppendTo. See AppendTo.
func (idx *Index) AppendToCtx(ctx context.Context, inode uint64, logicalOff int64, payload []byte, tier Tier) error {
	return idx.AppendToWithTxCtx(ctx, inode, logicalOff, payload, tier, nil)
}

// AppendToWithTxCtx is AppendToCtx + an optional txHook that runs inside
// the same pebble batch that commits the fragments. The hook lets a
// caller (today: store.WriteAtTierCtx) fold an unrelated key update —
// e.g. bumping the file's Size/Mtime — into the same WAL fsync, halving
// the commit count per logical write.
//
// All chunks of one call are committed in a single batch. The batch is a
// kv.Batch, not a bbolt.Tx; the parent DB still serves reads through the
// hook so existing read-then-write patterns work without read-your-own-
// writes from inside the batch.
//
// Unlike the bbolt era, pebble's group commit in the WAL coalesces
// concurrent commits automatically — there's no MaxBatchDelay floor.
func (idx *Index) AppendToWithTxCtx(ctx context.Context, inode uint64, logicalOff int64, payload []byte, tier Tier, txHook func(*kv.Batch) error) error {
	if len(payload) == 0 {
		if txHook == nil {
			return nil
		}
		b := idx.db.NewBatch()
		if err := txHook(b); err != nil {
			b.Close()
			return err
		}
		return b.Commit(true)
	}
	ctx, span := obs.Tracer().Start(ctx, "index.append")
	span.SetAttributes(
		attribute.Int64("inode", int64(inode)),
		attribute.Int64("logical_off", logicalOff),
		attribute.Int("payload_bytes", len(payload)),
		attribute.String("tier", tier.String()),
		attribute.Bool("has_tx_hook", txHook != nil),
	)
	defer span.End()

	// Hold writeMu.RLock for the whole call: every chunk's segment write
	// happens under the same reader-section, and the single pebble batch
	// below also runs under it. Move/Truncate/DeleteInode are the only
	// things that take the writer lock; they block all of this.
	_, acqSpan := obs.Tracer().Start(ctx, "index.write_lock_rlock")
	idx.writeMu.RLock()
	acqSpan.End()
	heldCtx, heldSpan := obs.Tracer().Start(ctx, "index.write_lock_held")
	defer func() {
		heldSpan.End()
		idx.writeMu.RUnlock()
	}()
	ctx = heldCtx

	// Stage every chunk: assign a fresh seq atomically (no lock needed —
	// seqGen.Add is contention-free), write to segment + fsync, record
	// the fragment for the batch below.
	type pending struct {
		stripeID uint32
		seq      uint64
		frag     Fragment
	}
	var pendings []pending
	rem := payload
	off := logicalOff
	chunkIdx := 0
	for len(rem) > 0 {
		stripeID := idx.stripeOf(off)
		stripeEnd := (int64(stripeID) + 1) * idx.stripeSize
		chunkLen := int64(len(rem))
		if off+chunkLen > stripeEnd {
			chunkLen = stripeEnd - off
		}
		chunk := rem[:chunkLen]

		seq := idx.nextSeq()
		set := idx.setFor(tier)
		seg, err := set.ActiveCtx(ctx, segment.FramedSize(len(chunk)))
		if err != nil {
			recordSpanError(span, err)
			return err
		}
		appCtx, appSpan := obs.Tracer().Start(ctx, "segment.append")
		appSpan.SetAttributes(
			attribute.Int("bytes", len(chunk)),
			attribute.Int("chunk_idx", chunkIdx),
			attribute.Int64("segment_id", int64(seg.ID())),
		)
		payloadOff, err := seg.AppendCtx(appCtx, segment.RecordHeader{
			Inode:      inode,
			StripeID:   stripeID,
			Seq:        seq,
			LogicalOff: off,
			Length:     uint32(len(chunk)),
		}, chunk)
		appSpan.End()
		if err != nil {
			recordSpanError(span, err)
			return err
		}
		if err := seg.SyncCtx(ctx); err != nil {
			recordSpanError(span, err)
			return err
		}

		pendings = append(pendings, pending{
			stripeID: stripeID,
			seq:      seq,
			frag: Fragment{
				Seq:           seq,
				LogicalOff:    off,
				Length:        chunkLen,
				Tier:          tier,
				SegmentID:     seg.ID(),
				SegmentOffset: payloadOff,
			},
		})
		rem = rem[chunkLen:]
		off += chunkLen
		chunkIdx++
	}
	span.SetAttributes(attribute.Int("chunks", chunkIdx))

	// One pebble commit covers every fragment + the optional hook.
	// Per-fragment keys mean fragment Sets never conflict with each
	// other — concurrent appenders to the same inode/stripe write to
	// distinct keys (distinguished by their unique seq), so the only
	// reason we'd still need the per-inode lock is the hook's
	// read-modify-write (e.g. node.Size = max(...)). Without a hook we
	// skip the lock entirely; with a hook we take it for the duration
	// of the commit so concurrent hooks don't lose each other's
	// updates.
	if txHook != nil {
		cl := idx.commitLockFor(inode)
		cl.Lock()
		defer cl.Unlock()
	}
	batchCtx, commitSpan := obs.Tracer().Start(ctx, "pebble.commit")
	commitSpan.SetAttributes(
		attribute.Int("fragments", len(pendings)),
		attribute.Bool("lock_held", txHook != nil),
	)
	b := idx.db.NewBatch()
	_, fnSpan := obs.Tracer().Start(batchCtx, "pebble.commit_fn")
	for _, p := range pendings {
		if err := b.Set(kv.FragmentKey(inode, p.stripeID, p.seq), encodeFragments([]Fragment{p.frag})); err != nil {
			fnSpan.End()
			commitSpan.End()
			b.Close()
			recordSpanError(span, err)
			return err
		}
	}
	if txHook != nil {
		if err := txHook(b); err != nil {
			fnSpan.End()
			commitSpan.End()
			b.Close()
			recordSpanError(span, err)
			return err
		}
	}
	fnSpan.End()
	err := b.Commit(true)
	commitSpan.End()
	if err != nil {
		recordSpanError(span, err)
	}
	return err
}

// recordSpanError stamps span with the error code + message. Centralised
// so every error path in this file looks identical and we don't accidentally
// leave a span marked OK when something failed.
func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// PlanRead returns the slices required to satisfy a read of [off, off+length)
// for inode. Each slice is either a Locator (caller should pread) or a
// Sparse marker (caller chooses how to fill — block, zero-fill, etc.).
// Slices are sorted by logical offset and non-overlapping.
func (idx *Index) PlanRead(inode uint64, off, length int64) ([]ReadSlice, error) {
	return idx.PlanReadCtx(context.Background(), inode, off, length)
}

// PlanReadCtx is the ctx-aware variant of PlanRead.
func (idx *Index) PlanReadCtx(ctx context.Context, inode uint64, off, length int64) ([]ReadSlice, error) {
	if length <= 0 {
		return nil, nil
	}
	_, span := obs.Tracer().Start(ctx, "index.plan_read")
	span.SetAttributes(
		attribute.Int64("inode", int64(inode)),
		attribute.Int64("off", off),
		attribute.Int64("length", length),
	)
	defer span.End()

	first := idx.stripeOf(off)
	last := idx.stripeOf(off + length - 1)
	allFrags, err := idx.fragmentsInRange(inode, first, last)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	plan := planRead(allFrags, off, length)
	span.SetAttributes(attribute.Int("slices", len(plan)))
	return plan, nil
}

// fragmentsInRange returns every fragment for inode in stripes [first, last]
// inclusive. Iterates a single prefix scan and decodes each per-fragment
// value. Used by PlanRead and friends.
func (idx *Index) fragmentsInRange(inode uint64, first, last uint32) ([]Fragment, error) {
	lo := kv.FragmentStripePrefix(inode, first)
	// Upper bound is "one past the last stripe": same shape as lo, with
	// stripeID = last+1. If last is MaxUint32 we fall back to scanning
	// the whole inode prefix.
	var hi []byte
	if last == ^uint32(0) {
		hi = kv.PrefixUpperBound(kv.StripePrefix(inode))
	} else {
		hi = kv.FragmentStripePrefix(inode, last+1)
	}
	it, err := idx.db.Iter(lo, hi)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []Fragment
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if !kv.IsFragmentKey(k) {
			continue
		}
		out = append(out, decodeFragments(it.Value())...)
	}
	return out, nil
}

// ReadAt pulls bytes from disk for one Locator. Returns the number of bytes
// read. Concurrent-safe.
func (idx *Index) ReadAt(loc Locator, p []byte) (int, error) {
	return idx.ReadAtCtx(context.Background(), loc, p)
}

// ReadAtCtx is the ctx-aware variant of ReadAt.
func (idx *Index) ReadAtCtx(ctx context.Context, loc Locator, p []byte) (int, error) {
	_, span := obs.Tracer().Start(ctx, "segment.read_at")
	span.SetAttributes(
		attribute.String("tier", loc.Tier.String()),
		attribute.Int64("segment_id", int64(loc.SegmentID)),
		attribute.Int64("offset", loc.Offset),
		attribute.Int("bytes", len(p)),
	)
	defer span.End()

	set := idx.setFor(loc.Tier)
	seg := set.Get(loc.SegmentID)
	if seg == nil {
		err := fmt.Errorf("index: missing segment %d in %s", loc.SegmentID, loc.Tier)
		recordSpanError(span, err)
		return 0, err
	}
	if int64(len(p)) > loc.Length {
		p = p[:loc.Length]
	}
	n, err := seg.ReadAt(p, loc.Offset)
	if err != nil {
		recordSpanError(span, err)
	}
	return n, err
}

// BulkSession is a write session that batches segment fsyncs and pebble
// commits across many Write calls into a single Commit. Use it for
// bulk-ingest paths (migration, restore-from-backup, etc.) where the
// per-chunk durability the regular Append path provides is overkill —
// a crash mid-session loses the in-flight bytes as orphans (reclaimable
// by GC) and the caller redoes the work, rather than serialising every
// chunk through fsync.
//
// Lifecycle:
//   sess := idx.NewBulkSession(inode, tier)
//   sess.Write(off1, payload1)
//   sess.Write(off2, payload2)
//   ...
//   if err := sess.Commit(); err != nil { ... }
//
// Commit fsyncs every segment the session touched, then commits all
// queued fragments to pebble in one batch. After Commit returns, the
// bytes are durable and the index sees them.
type BulkSession struct {
	idx     *Index
	inode   uint64
	tier    Tier
	pending []pendingFrag
	touched map[uint32]*segment.Segment
}

type pendingFrag struct {
	stripeID      uint32
	logicalOff    int64
	length        int64
	segmentID     uint32
	segmentOffset int64
}

// NewBulkSession returns a fresh session writing to tier for inode.
// Multiple sessions on the same inode aren't supported and aren't checked
// for — bulk paths are single-writer per file.
func (idx *Index) NewBulkSession(inode uint64, tier Tier) *BulkSession {
	return &BulkSession{
		idx:     idx,
		inode:   inode,
		tier:    tier,
		touched: map[uint32]*segment.Segment{},
	}
}

// Write appends payload at logicalOff. Splits across stripe boundaries
// like Append, but each piece is written to the segment file with no
// fsync; durability is deferred to Commit. The bytes are not visible to
// readers until Commit returns successfully.
func (s *BulkSession) Write(logicalOff int64, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	rem := payload
	off := logicalOff
	for len(rem) > 0 {
		stripeID := s.idx.stripeOf(off)
		stripeEnd := (int64(stripeID) + 1) * s.idx.stripeSize
		chunkLen := int64(len(rem))
		if off+chunkLen > stripeEnd {
			chunkLen = stripeEnd - off
		}
		set := s.idx.setFor(s.tier)
		seg, err := set.Active(segment.FramedSize(int(chunkLen)))
		if err != nil {
			return err
		}
		// Seq=0 in the on-disk record header. The pebble-side Seq is
		// assigned at Commit time. The on-disk Seq is only consulted by
		// the catastrophic-recovery rebuild path, which we'll address
		// separately if it ever matters; index reads always use pebble.
		payloadOff, err := seg.Append(segment.RecordHeader{
			Inode: s.inode, StripeID: stripeID, Seq: 0,
			LogicalOff: off, Length: uint32(chunkLen),
		}, rem[:chunkLen])
		if err != nil {
			return err
		}
		s.touched[seg.ID()] = seg
		s.pending = append(s.pending, pendingFrag{
			stripeID:      stripeID,
			logicalOff:    off,
			length:        chunkLen,
			segmentID:     seg.ID(),
			segmentOffset: payloadOff,
		})
		rem = rem[chunkLen:]
		off += chunkLen
	}
	return nil
}

// Commit fsyncs every touched segment, then commits all queued fragments
// to pebble in a single batch. After return, the index sees the bytes.
func (s *BulkSession) Commit() error {
	s.idx.writeMu.Lock()
	defer s.idx.writeMu.Unlock()

	for _, seg := range s.touched {
		if err := seg.Sync(); err != nil {
			return err
		}
	}
	b := s.idx.db.NewBatch()
	for _, pf := range s.pending {
		seq := s.idx.nextSeq()
		frag := Fragment{
			Seq:           seq,
			LogicalOff:    pf.logicalOff,
			Length:        pf.length,
			Tier:          s.tier,
			SegmentID:     pf.segmentID,
			SegmentOffset: pf.segmentOffset,
		}
		if err := b.Set(kv.FragmentKey(s.inode, pf.stripeID, seq), encodeFragments([]Fragment{frag})); err != nil {
			b.Close()
			return err
		}
	}
	return b.Commit(true)
}

// Abort discards the session. The bytes already written to segments
// remain as orphan records; GC reclaims them on the next pass.
func (s *BulkSession) Abort() {
	s.pending = nil
	s.touched = nil
}

// Truncate drops any fragment data past `size` for inode. Stripes wholly
// past `size` are removed; the stripe containing the truncate point has its
// fragments clipped or removed. Data in segments still on disk remains until
// GC reclaims any segments whose ref-count hits zero.
func (idx *Index) Truncate(inode uint64, size int64) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()

	// Iterate every per-fragment key under inode, decide per fragment
	// whether to keep / clip / delete. Collect ops first, apply in one
	// batch — mutating during iteration is dicey across LSM iterators.
	prefix := kv.StripePrefix(inode)
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	type op struct {
		key []byte
		val []byte // nil => delete
	}
	var ops []op
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if !kv.IsFragmentKey(k) {
			continue
		}
		stripeID := kv.StripeIDFromFragmentKey(k)
		seq := kv.SeqFromFragmentKey(k)
		stripeStart := int64(stripeID) * idx.stripeSize
		keyCopy := append([]byte{}, k...)
		if stripeStart >= size {
			ops = append(ops, op{key: keyCopy})
			continue
		}
		frag := decodeFragments(it.Value())[0]
		_ = seq // seq is in the key; not used here
		if frag.LogicalOff >= size {
			ops = append(ops, op{key: keyCopy})
			continue
		}
		end := frag.LogicalOff + frag.Length
		if end > size {
			frag.Length = size - frag.LogicalOff
			ops = append(ops, op{key: keyCopy, val: encodeFragments([]Fragment{frag})})
		}
	}
	if err := it.Close(); err != nil {
		return err
	}
	b := idx.db.NewBatch()
	for _, o := range ops {
		if o.val == nil {
			if err := b.Delete(o.key); err != nil {
				b.Close()
				return err
			}
		} else {
			if err := b.Set(o.key, o.val); err != nil {
				b.Close()
				return err
			}
		}
	}
	return b.Commit(true)
}

// DeleteInode removes every fragment for inode. Bytes in segments remain
// until GC reclaims them.
func (idx *Index) DeleteInode(inode uint64) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	prefix := kv.StripePrefix(inode)
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		keys = append(keys, append([]byte{}, it.Key()...))
	}
	if err := it.Close(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	b := idx.db.NewBatch()
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			b.Close()
			return err
		}
	}
	return b.Commit(true)
}

// StripeInfo summarises one stripe — used by L3 to pick movement candidates.
type StripeInfo struct {
	Inode    uint64
	StripeID uint32
	HotBytes int64
	ColdBytes int64
	// LiveBytes is the logical byte count after canonical-view shadowing.
	// (Total HotBytes+ColdBytes can be larger if there's overlap.)
	LiveBytes int64
}

// IterStripes calls fn for every stripe that has at least one fragment.
// Stopping (fn returns false) ends iteration. Aggregates per-fragment
// keys into one StripeInfo per (inode, stripeID).
func (idx *Index) IterStripes(fn func(StripeInfo) bool) error {
	prefix := kv.StripeAllPrefix()
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer it.Close()
	// Adjacent fragment keys for the same (inode, stripeID) sort
	// together (same prefix; only seq differs), so we can accumulate
	// per-stripe state and flush on transition.
	var (
		have                    bool
		curInode                uint64
		curStripe               uint32
		curFrags                []Fragment
		curHotBytes, curColdBytes int64
	)
	flush := func() bool {
		if !have {
			return true
		}
		info := StripeInfo{
			Inode:     curInode,
			StripeID:  curStripe,
			HotBytes:  curHotBytes,
			ColdBytes: curColdBytes,
		}
		for _, f := range canonicalView(curFrags) {
			info.LiveBytes += f.Length
		}
		return fn(info)
	}
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if !kv.IsFragmentKey(k) {
			continue
		}
		ino := kv.StripeInodeFromFragmentKey(k)
		sid := kv.StripeIDFromFragmentKey(k)
		if !have || ino != curInode || sid != curStripe {
			if !flush() {
				return nil
			}
			have = true
			curInode = ino
			curStripe = sid
			curFrags = curFrags[:0]
			curHotBytes = 0
			curColdBytes = 0
		}
		f := decodeFragments(it.Value())[0]
		curFrags = append(curFrags, f)
		switch f.Tier {
		case segment.TierHot:
			curHotBytes += f.Length
		case segment.TierCold:
			curColdBytes += f.Length
		}
	}
	if !flush() {
		return nil
	}
	return nil
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
// The bytes to copy are read with no lock held; pebble's batch is only
// opened after the IO completes to make the swap atomic relative to readers.
func (idx *Index) Move(inode uint64, stripeID uint32, target Tier) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()

	// Snapshot the current fragment list + remember the keys so we can
	// delete them in the same batch that writes the rewritten layout.
	prefix := kv.FragmentStripePrefix(inode, stripeID)
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	var (
		frags    []Fragment
		oldKeys  [][]byte
	)
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if !kv.IsFragmentKey(k) {
			continue
		}
		oldKeys = append(oldKeys, append([]byte{}, k...))
		frags = append(frags, decodeFragments(it.Value())...)
	}
	if err := it.Close(); err != nil {
		return err
	}
	if len(frags) == 0 {
		return nil
	}

	view := canonicalView(frags)

	// Walk the canonical view; for fragments not in target tier, copy bytes.
	var newFrags []Fragment
	targetSet := idx.setFor(target)
	for _, f := range view {
		newSeq := idx.nextSeq()
		if f.Tier == target {
			// Re-emit with a new seq so the rewritten list is a clean
			// tail-free canonical layout.
			f.Seq = newSeq
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
			Inode: inode, StripeID: stripeID, Seq: newSeq,
			LogicalOff: f.LogicalOff, Length: uint32(len(buf)),
		}, buf)
		if err != nil {
			return err
		}
		if err := seg.Sync(); err != nil {
			return err
		}
		newFrags = append(newFrags, Fragment{
			Seq:           newSeq,
			LogicalOff:    f.LogicalOff,
			Length:        f.Length,
			Tier:          target,
			SegmentID:     seg.ID(),
			SegmentOffset: payloadOff,
		})
	}

	// Atomically swap: delete every old per-fragment key, write the new
	// ones. writeMu is held the whole time so no Append can race.
	b := idx.db.NewBatch()
	for _, k := range oldKeys {
		if err := b.Delete(k); err != nil {
			b.Close()
			return err
		}
	}
	for _, f := range newFrags {
		if err := b.Set(kv.FragmentKey(inode, stripeID, f.Seq), encodeFragments([]Fragment{f})); err != nil {
			b.Close()
			return err
		}
	}
	return b.Commit(true)
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
	// Build referenced-set by scanning every stripe key.
	referenced := map[segment.Tier]map[uint32]bool{
		segment.TierHot:  {},
		segment.TierCold: {},
	}
	prefix := kv.StripeAllPrefix()
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	for it.First(); it.Valid(); it.Next() {
		if !kv.IsFragmentKey(it.Key()) {
			continue
		}
		for _, f := range decodeFragments(it.Value()) {
			referenced[f.Tier][f.SegmentID] = true
		}
	}
	if err := it.Close(); err != nil {
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

// Sync makes every accumulated write durable on disk (segments + pebble
// WAL). Append already fsyncs the touched segment + commits the WAL, so
// this is only needed if a caller wants to force a global sync barrier.
func (idx *Index) Sync() error {
	return idx.SyncCtx(context.Background())
}

// SyncCtx is the ctx-aware variant of Sync.
func (idx *Index) SyncCtx(ctx context.Context) error {
	_, span := obs.Tracer().Start(ctx, "index.sync")
	defer span.End()
	if err := idx.hot.SyncAll(); err != nil {
		recordSpanError(span, err)
		return err
	}
	if err := idx.cold.SyncAll(); err != nil {
		recordSpanError(span, err)
		return err
	}
	// Every Batch.Commit(true) already fsyncs the WAL, so the DB is
	// already durable. Pebble offers no "fsync the WAL again" primitive —
	// LogData(nil) followed by a Sync commit is the idiomatic full
	// barrier. The cost is one extra WAL append + fsync.
	b := idx.db.NewBatch()
	if err := b.Commit(true); err != nil {
		recordSpanError(span, err)
		return err
	}
	return nil
}

// FragmentsOf returns a snapshot of the fragment list for one stripe.
// Used by tests and by L3 for inspection. Returns nil if absent.
func (idx *Index) FragmentsOf(inode uint64, stripeID uint32) ([]Fragment, error) {
	prefix := kv.FragmentStripePrefix(inode, stripeID)
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []Fragment
	for it.First(); it.Valid(); it.Next() {
		if !kv.IsFragmentKey(it.Key()) {
			continue
		}
		out = append(out, decodeFragments(it.Value())...)
	}
	return out, nil
}

// StripesOf returns every stripe id that has fragments for inode.
func (idx *Index) StripesOf(inode uint64) ([]uint32, error) {
	prefix := kv.StripePrefix(inode)
	it, err := idx.db.Iter(prefix, kv.PrefixUpperBound(prefix))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	seen := map[uint32]struct{}{}
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if !kv.IsFragmentKey(k) {
			continue
		}
		seen[kv.StripeIDFromFragmentKey(k)] = struct{}{}
	}
	out := make([]uint32, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Ensure imports stay referenced when blocks are removed during edits.
var _ = bytes.Equal
