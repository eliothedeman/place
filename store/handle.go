package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/eliothedeman/place/index"
	"github.com/eliothedeman/place/obs"
	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	return h.ReadAtCtx(context.Background(), p, off)
}

// ReadAtCtx is the ctx-aware variant of ReadAt. Tracing spans started
// inside the call attach to ctx.
func (h *Handle) ReadAtCtx(ctx context.Context, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	ctx, span := obs.Tracer().Start(ctx, "store.read_at")
	span.SetAttributes(
		attribute.Int64("inode", int64(h.inode)),
		attribute.Int64("off", off),
		attribute.Int("bytes", len(p)),
	)
	defer span.End()

	node, err := h.store.Stat(h.inode)
	if err != nil {
		recordHandleSpanError(span, err)
		return 0, err
	}
	if off >= node.Size {
		return 0, nil
	}
	if off+int64(len(p)) > node.Size {
		p = p[:node.Size-off]
	}
	plan, err := h.store.idx.PlanReadCtx(ctx, h.inode, off, int64(len(p)))
	if err != nil {
		recordHandleSpanError(span, err)
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
		if _, err := h.store.idx.ReadAtCtx(ctx, sl.Locator, dst); err != nil {
			recordHandleSpanError(span, err)
			return 0, err
		}
	}
	return len(p), nil
}

// WriteAt writes p at off into the default hot tier. The file grows if
// off+len(p) exceeds current size. Mtime/Ctime are bumped.
func (h *Handle) WriteAt(p []byte, off int64) (int, error) {
	return h.WriteAtCtx(context.Background(), p, off)
}

// WriteAtCtx is the ctx-aware variant of WriteAt.
func (h *Handle) WriteAtCtx(ctx context.Context, p []byte, off int64) (int, error) {
	return h.WriteAtTierCtx(ctx, p, off, index.TierHot)
}

// WriteAtTier writes p at off into a specific tier. Used by bulk-ingest
// paths (notably the legacy migrator) that want to land bytes directly in
// cold without going through hot and triggering eviction churn.
func (h *Handle) WriteAtTier(p []byte, off int64, tier index.Tier) (int, error) {
	return h.WriteAtTierCtx(context.Background(), p, off, tier)
}

// WriteAtTierCtx is the ctx-aware variant of WriteAtTier. This is the
// canonical write path for the FUSE adapter; the non-ctx wrappers exist
// for tests + the migrator.
//
// The fragment commit and the node Size/Mtime update share a single
// bbolt write tx via index.AppendToWithTxCtx — one fsync per FUSE
// write instead of two.
func (h *Handle) WriteAtTierCtx(ctx context.Context, p []byte, off int64, tier index.Tier) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	ctx, span := obs.Tracer().Start(ctx, "store.write_at")
	span.SetAttributes(
		attribute.Int64("inode", int64(h.inode)),
		attribute.Int64("off", off),
		attribute.Int("bytes", len(p)),
		attribute.String("tier", tier.String()),
	)
	defer span.End()

	end := off + int64(len(p))
	now := time.Now().UnixNano()
	// nodeUpdate runs inside index's batch tx. It is idempotent: setting
	// Size = max(Size, end) and Mtime = now produces the same result on
	// a Batch retry. (Mtime "moves backward by a few µs across a retry"
	// would be a non-issue and isn't actually possible since `now` is
	// captured before the batch starts.)
	nodeUpdate := func(tx *bolt.Tx) error {
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
	}
	if err := h.store.idx.AppendToWithTxCtx(ctx, h.inode, off, p, tier, nodeUpdate); err != nil {
		recordHandleSpanError(span, err)
		return 0, err
	}
	return len(p), nil
}

// recordHandleSpanError mirrors index.recordSpanError. Defined here so the
// store package doesn't have to import a private helper from index.
func recordHandleSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// NewBulkWriter returns a session that batches segment fsyncs and bbolt
// commits for high-throughput bulk-ingest paths (migrations, restores). All
// the per-chunk durability the regular WriteAt path provides is deferred
// to BulkWriter.Commit, which fsyncs every touched segment exactly once
// and commits every queued fragment in a single bbolt tx. Crash before
// Commit leaves the in-flight bytes as orphan segment records, reclaimed
// by GC; the caller must redo the file.
func (h *Handle) NewBulkWriter(tier index.Tier) *BulkWriter {
	return &BulkWriter{h: h, sess: h.store.idx.NewBulkSession(h.inode, tier)}
}

// BulkWriter is a Handle-level wrapper around index.BulkSession that also
// applies a single Node-size/mtime update at Commit time.
type BulkWriter struct {
	h      *Handle
	sess   *index.BulkSession
	maxEnd int64
}

// Write queues payload at off. Same semantics as Handle.WriteAt but
// without per-chunk durability — Commit makes everything durable.
func (b *BulkWriter) Write(off int64, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if end := off + int64(len(payload)); end > b.maxEnd {
		b.maxEnd = end
	}
	return b.sess.Write(off, payload)
}

// Commit fsyncs touched segments, commits queued fragments, and bumps
// the node's Size/Mtime/Ctime in one tx.
func (b *BulkWriter) Commit() error {
	if err := b.sess.Commit(); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	return b.h.store.db.Update(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketNodes).Get(inodeKey(b.h.inode))
		if v == nil {
			return fmt.Errorf("store: bulk commit on inode %d with no node entry", b.h.inode)
		}
		n, err := decodeNode(v)
		if err != nil {
			return err
		}
		if b.maxEnd > n.Size {
			n.Size = b.maxEnd
		}
		n.Mtime = now
		n.Ctime = now
		return tx.Bucket(bucketNodes).Put(inodeKey(b.h.inode), encodeNode(n))
	})
}

// Abort discards the session without committing. Bytes already on disk
// become orphans that GC reclaims.
func (b *BulkWriter) Abort() { b.sess.Abort() }

// Sync fsyncs the underlying storage to make all preceding writes durable.
func (h *Handle) Sync() error {
	return h.SyncCtx(context.Background())
}

// SyncCtx is the ctx-aware variant of Sync.
func (h *Handle) SyncCtx(ctx context.Context) error {
	ctx, span := obs.Tracer().Start(ctx, "store.sync")
	span.SetAttributes(attribute.Int64("inode", int64(h.inode)))
	defer span.End()
	if err := h.store.idx.SyncCtx(ctx); err != nil {
		recordHandleSpanError(span, err)
		return err
	}
	return nil
}

// Close releases the Handle. The current implementation has no per-handle
// state, but adapters should still call Close so the contract is stable.
func (h *Handle) Close() error {
	return nil
}

// Avoid unused-import lint:
var _ = binary.LittleEndian
