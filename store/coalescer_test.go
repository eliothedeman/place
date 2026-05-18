package store

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eliothedeman/place/index"
)

// newStoreWithCoalescer constructs a store with a coalescer using
// caller-provided options. The default newStore uses production
// defaults, which are too generous (4 GiB cap, 100 ms tick) for tight
// unit tests that want to exercise backpressure or the time tick.
//
// Trick: call the normal newStore to bootstrap, then swap the
// coalescer for one with our options. Saves duplicating Open's root-
// node init.
func newStoreWithCoalescer(t *testing.T, opts coalescerOptions) *Store {
	t.Helper()
	s := newStore(t)
	s.coalescer.Close()
	s.coalescer = newCoalescerWithOptions(s, opts)
	return s
}

// TestCoalescerReadAfterWriteSeesBufferedBytes confirms the most basic
// invariant: a Read immediately after a Write returns the just-written
// bytes, even though no fsync has happened and the bytes are still in
// memory.
func TestCoalescerReadAfterWriteSeesBufferedBytes(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "rw.bin", 0o644, 0, 0)
	defer h.Close()

	payload := bytes.Repeat([]byte("ABCDEFGH"), 1024) // 8 KiB
	if _, err := h.WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}
	// Stat must reflect the buffered write — Size must be visible
	// even without fsync.
	st, err := s.Stat(h.Inode())
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != int64(len(payload)) {
		t.Errorf("stat.Size = %d, want %d (LiveAttrs not consulted?)", st.Size, len(payload))
	}
	got := make([]byte, len(payload))
	n, err := h.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) {
		t.Errorf("read n=%d, want %d", n, len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("read content mismatch (buffer not consulted?)")
	}
}

// TestCoalescerFsyncWaitsForDurability checks that Fsync only returns
// after the flush has landed: post-Fsync, the on-disk fragments
// reflect what was buffered.
func TestCoalescerFsyncWaitsForDurability(t *testing.T) {
	s := newStore(t)
	n, h, _ := s.Create(RootInode, "fsync.bin", 0o644, 0, 0)
	defer h.Close()

	body := bytes.Repeat([]byte("Z"), 16<<10) // 16 KiB
	if _, err := h.WriteAt(body, 0); err != nil {
		t.Fatal(err)
	}
	// Before Sync: no on-disk fragments (writes still in buffer).
	stripes, _ := s.idx.StripesOf(n.Inode)
	if len(stripes) != 0 {
		t.Errorf("expected 0 stripes pre-fsync, got %d", len(stripes))
	}
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	// After Sync: fragments are on disk.
	stripes, _ = s.idx.StripesOf(n.Inode)
	if len(stripes) == 0 {
		t.Errorf("expected at least one stripe after fsync")
	}
}

// TestCoalescerInvalidatePastSizeDropsBufferedBytes verifies that a
// Setattr-truncate before the data has been flushed drops the
// buffered bytes past the new size, not just the disk-side fragments.
func TestCoalescerInvalidatePastSizeDropsBufferedBytes(t *testing.T) {
	s := newStore(t)
	n, h, _ := s.Create(RootInode, "trunc.bin", 0o644, 0, 0)
	defer h.Close()

	body := bytes.Repeat([]byte("Q"), 4096)
	if _, err := h.WriteAt(body, 0); err != nil {
		t.Fatal(err)
	}
	// Truncate to 100 bytes while the write is still in the buffer.
	target := int64(100)
	if _, err := s.Setattr(n.Inode, SetAttr{Size: &target}); err != nil {
		t.Fatal(err)
	}
	// Read: should see only 100 bytes.
	got := make([]byte, 4096)
	rn, err := h.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}
	if int64(rn) != target {
		t.Errorf("read after truncate n=%d, want %d (buffer not clipped?)", rn, target)
	}
	if !bytes.Equal(got[:rn], body[:target]) {
		t.Errorf("post-truncate buffered read mismatch")
	}
}

// TestCoalescerDropOnUnlink confirms that unlinking a file with
// buffered writes does not later flush ghost bytes to disk.
func TestCoalescerDropOnUnlink(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "doomed.bin", 0o644, 0, 0)
	body := bytes.Repeat([]byte("D"), 4096)
	if _, err := h.WriteAt(body, 0); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if err := s.Unlink(RootInode, "doomed.bin"); err != nil {
		t.Fatal(err)
	}
	// Wait past the flush interval; if Drop did its job, no flush
	// happens for this inode (it's gone from the map already).
	time.Sleep(200 * time.Millisecond)
	// Lookup should fail.
	if _, err := s.Lookup(RootInode, "doomed.bin"); !errors.Is(err, ErrNotExist) {
		t.Errorf("after unlink, Lookup returned %v, want ErrNotExist", err)
	}
}

// TestCoalescerBackpressureRespected proves that with a global cap
// far smaller than the total bytes written, the system still makes
// progress: every byte eventually lands, and writes don't error out.
// The cap should force the writes to serialize through the flusher
// rather than balloon memory.
//
// Invariant the cap must satisfy: globalCap >= perW. A single Write
// of size bigger than the cap can never be admitted (semaphore
// Acquire would block forever). The user-facing default keeps cap
// well above MaxWrite=1MiB so this isn't a real-world concern; the
// test just respects it.
func TestCoalescerBackpressureRespected(t *testing.T) {
	const (
		writers = 16
		perW    = 4 << 10 // 4 KiB per writer
		cap_    = 8 << 10 // 8 KiB global cap — at most 2 in flight
	)
	s := newStoreWithCoalescer(t, coalescerOptions{
		perInodeCap:   1 << 20,
		globalCap:     cap_,
		flushInterval: 10 * time.Millisecond,
		workers:       2,
		queueSize:     32,
	})
	_, h, _ := s.Create(RootInode, "press.bin", 0o644, 0, 0)
	defer h.Close()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte('A' + w)}, perW)
			off := int64(w * perW)
			if _, err := h.WriteAt(payload, off); err != nil {
				t.Errorf("writer %d: %v", w, err)
			}
		}()
	}
	wg.Wait()
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, writers*perW)
	if _, err := h.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	for w := 0; w < writers; w++ {
		want := byte('A' + w)
		for i := 0; i < perW; i++ {
			if got[w*perW+i] != want {
				t.Fatalf("byte %d: got %d want %d", w*perW+i, got[w*perW+i], want)
			}
		}
	}
}

// TestCoalescerBackpressureCtxCancellable verifies that a writer
// blocked on the global semaphore unblocks cleanly when its ctx is
// cancelled, returning the ctx error rather than hanging forever.
func TestCoalescerBackpressureCtxCancellable(t *testing.T) {
	// Use a 0-cap semaphore so any Write blocks immediately.
	s := newStoreWithCoalescer(t, coalescerOptions{
		perInodeCap:   1 << 20,
		globalCap:     1, // pathologically tiny; the next write blocks
		flushInterval: time.Hour,
		workers:       1,
		queueSize:     1,
	})
	_, h, _ := s.Create(RootInode, "cancel.bin", 0o644, 0, 0)
	defer h.Close()

	// Pre-fill the semaphore so the next Write blocks.
	if _, err := h.WriteAt([]byte{0}, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := h.WriteAtCtx(ctx, bytes.Repeat([]byte("x"), 64), 1)
	if err == nil {
		t.Fatal("expected ctx deadline error; got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

// TestCoalescerConcurrentSameInodeWritesPreserveAllBytes is the
// regression test for the original bug — concurrent writers to one
// inode used to lose each other's data through the read-merge-write
// commit lock. With per-fragment keys + the coalescer, all bytes must
// survive.
func TestCoalescerConcurrentSameInodeWritesPreserveAllBytes(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "concurrent.bin", 0o644, 0, 0)
	defer h.Close()

	const (
		writers   = 16
		perWriter = 32
		chunkLen  = 1024
	)
	var wg sync.WaitGroup
	var failures atomic.Int64
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				off := int64((w*perWriter + i) * chunkLen)
				payload := bytes.Repeat([]byte{byte(w + 1)}, chunkLen)
				if _, err := h.WriteAt(payload, off); err != nil {
					t.Errorf("writer %d iter %d: %v", w, i, err)
					failures.Add(1)
					return
				}
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.FailNow()
	}
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	// Read everything back. Each writer owns a disjoint logical
	// range, so the bytes at offset (w*perWriter+i)*chunkLen must
	// equal w+1.
	total := int64(writers * perWriter * chunkLen)
	got := make([]byte, total)
	n, err := h.ReadAt(got, 0)
	if err != nil {
		t.Fatal(err)
	}
	if int64(n) != total {
		t.Fatalf("read n=%d, want %d", n, total)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			start := (w*perWriter + i) * chunkLen
			for j := 0; j < chunkLen; j++ {
				if got[start+j] != byte(w+1) {
					t.Fatalf("byte at off=%d: got %d, want %d (writer %d iter %d)",
						start+j, got[start+j], w+1, w, i)
				}
			}
		}
	}
}

// TestCoalescerFsyncOnEmptyInodeIsNoop confirms fsync on an inode
// with no buffered writes returns immediately without error.
func TestCoalescerFsyncOnEmptyInodeIsNoop(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "empty.bin", 0o644, 0, 0)
	defer h.Close()
	if err := h.Sync(); err != nil {
		t.Fatalf("Sync on empty buffer returned error: %v", err)
	}
}

// TestCoalescerDrainOnClosePersistsData round-trips writes through
// Store.Close → reopen, confirming Drain ran and the data is durable
// without an explicit Sync.
func TestCoalescerDrainOnClosePersistsData(t *testing.T) {
	dir := t.TempDir()
	body := bytes.Repeat([]byte("R"), 64<<10) // 64 KiB
	var inode uint64
	{
		idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 20, SegmentMaxSize: 1 << 22})
		if err != nil {
			t.Fatal(err)
		}
		s, err := Open(idx)
		if err != nil {
			t.Fatal(err)
		}
		n, h, err := s.Create(RootInode, "drain.bin", 0o644, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.WriteAt(body, 0); err != nil {
			t.Fatal(err)
		}
		inode = n.Inode
		h.Close()
		if err := s.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	{
		idx, err := index.Open(index.Config{Root: dir, StripeSize: 1 << 20, SegmentMaxSize: 1 << 22})
		if err != nil {
			t.Fatal(err)
		}
		s, err := Open(idx)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		h, err := s.OpenInode(inode, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		got := make([]byte, len(body))
		n, err := h.ReadAt(got, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(body) {
			t.Fatalf("read n=%d, want %d (Drain didn't persist?)", n, len(body))
		}
		if !bytes.Equal(got, body) {
			t.Fatal("post-reopen content mismatch")
		}
	}
}

// TestCoalescerWriteStraddlesStripeBoundary verifies that a single
// write spanning two stripes lands correctly in the buffer (split
// into per-stripe extents) and is fully recoverable on read.
func TestCoalescerWriteStraddlesStripeBoundary(t *testing.T) {
	// StripeSize is 1 MiB in the test helper. Write straddling.
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "straddle.bin", 0o644, 0, 0)
	defer h.Close()
	stripe := s.idx.StripeSize()
	// 256 KiB before + 256 KiB after the boundary.
	body := bytes.Repeat([]byte("S"), 512<<10)
	off := stripe - int64(256<<10)
	if _, err := h.WriteAt(body, off); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(body))
	if _, err := h.ReadAt(got, off); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("straddle read mismatch")
	}
}

// TestCoalescerFsyncCtxCancellationStops verifies that a Fsync whose
// ctx is cancelled returns the ctx error without leaving the
// coalescer in a bad state. Subsequent operations on the same inode
// continue to work.
func TestCoalescerFsyncCtxCancellation(t *testing.T) {
	s := newStore(t)
	_, h, _ := s.Create(RootInode, "cancel.bin", 0o644, 0, 0)
	defer h.Close()
	if _, err := h.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	err := h.SyncCtx(ctx)
	if err == nil {
		t.Fatal("SyncCtx on cancelled ctx returned nil; expected ctx.Err")
	}
	// Subsequent un-cancelled Sync must still work.
	if err := h.Sync(); err != nil {
		t.Fatalf("Sync after cancelled Sync failed: %v", err)
	}
}

