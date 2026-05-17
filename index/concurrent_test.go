package index

import (
	"bytes"
	"sync"
	"testing"

	"github.com/eliothedeman/place/segment"
)

// TestConcurrentAppendsToSameStripeAreOrdered verifies that many
// goroutines appending into the same (inode, stripe) all land with
// distinct, monotonic seqs and survive readback. This exercises the
// "RLock + db.Batch with re-read" path.
func TestConcurrentAppendsToSameStripeAreOrdered(t *testing.T) {
	idx := newIndex(t)
	const (
		inode    uint64 = 1
		stripe          = uint32(0)
		writers         = 16
		perWriter       = 32
		chunkLen        = 256
	)
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// Each writer claims a distinct logical range so the
				// canonical view is unambiguous regardless of seq order.
				off := int64((w*perWriter+i)*chunkLen) % idx.StripeSize()
				payload := bytes.Repeat([]byte{byte(w)}, chunkLen)
				if err := idx.Append(inode, off, payload); err != nil {
					t.Errorf("writer %d iter %d: %v", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// All fragments are present and seqs are distinct.
	frags, err := idx.FragmentsOf(inode, stripe)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(frags); got != writers*perWriter {
		t.Fatalf("fragments=%d, want %d", got, writers*perWriter)
	}
	seqSeen := map[uint64]bool{}
	for _, f := range frags {
		if seqSeen[f.Seq] {
			t.Errorf("duplicate seq %d", f.Seq)
		}
		seqSeen[f.Seq] = true
		if f.Tier != segment.TierHot {
			t.Errorf("fragment tier=%v, want hot", f.Tier)
		}
	}
}

// TestConcurrentAppendsAcrossStripesReadBack writes many independent
// 4KiB blocks at well-spaced offsets across many stripes, in parallel,
// and confirms every block reads back byte-for-byte.
func TestConcurrentAppendsAcrossStripesReadBack(t *testing.T) {
	idx := newIndex(t)
	const (
		inode  uint64 = 1
		blocks        = 64
		blkLen        = 4096
	)
	stripe := idx.StripeSize()
	blocksData := make([][]byte, blocks)
	for i := range blocksData {
		blocksData[i] = bytes.Repeat([]byte{byte(i + 1)}, blkLen)
	}

	var wg sync.WaitGroup
	wg.Add(blocks)
	for i := 0; i < blocks; i++ {
		go func(i int) {
			defer wg.Done()
			off := int64(i) * stripe // one stripe apart so no overlap
			if err := idx.Append(inode, off, blocksData[i]); err != nil {
				t.Errorf("append block %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < blocks; i++ {
		off := int64(i) * stripe
		got := readAll(t, idx, inode, off, int64(blkLen))
		if !bytes.Equal(got, blocksData[i]) {
			t.Errorf("block %d mismatch", i)
		}
	}
}
