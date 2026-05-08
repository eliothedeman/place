// Property tests against fragment list maintenance and read planning.
//
// These tests drive the public Writer + Reader APIs with random write
// patterns and check that subsequent reads return the byte sequence the
// "shadow buffer" predicts (sparse, zero-filled). Any mismatch implies
// a bug in mergeFragment, planRead, planColdForGaps, uncoveredRanges,
// or truncateFragments.
package audit_tests

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/eliothedeman/place"
)

// helper: create a fresh meta+writer+reader trio.
func newMWR(t *testing.T) (*place.Meta, *place.Writer, *place.Reader, func()) {
	t.Helper()
	hot, cold := dirs(t)
	m := openMeta(t, hot)
	hs := newHot(t, hot, 1<<30)
	cs := newCold(t, cold, 1<<30)
	w := place.NewWriter(hs, m, nil)
	r := place.NewReader(hs, cs, m)
	cleanup := func() {
		w.Close()
		hs.CloseAll()
		cs.CloseAll()
		m.Close()
	}
	return m, w, r, cleanup
}

// createFile creates an empty regular file at rel in m (no fragments).
func createFile(t *testing.T, m *place.Meta, rel string) {
	t.Helper()
	if err := m.PutFile(makeReg(rel)); err != nil {
		t.Fatal(err)
	}
}

// TestFragmentMath_RandomOverwrites issues random overlapping writes to a
// single file and verifies every read returns the shadow buffer's contents.
// A divergence means mergeFragment is mis-splitting a displaced range or
// planRead is emitting wrong segment offsets.
func TestFragmentMath_RandomOverwrites(t *testing.T) {
	if !hasCreateRegular() {
		t.Skip("place.Meta.CreateRegular not available; cannot stage files for property test")
	}
	const fileSize = 8192
	m, w, r, cleanup := newMWR(t)
	defer cleanup()

	rel := "f"
	createFile(t, m, rel)
	shadow := make([]byte, fileSize)

	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 500; iter++ {
		off := int64(rng.Intn(fileSize))
		maxLen := int64(fileSize) - off
		if maxLen < 1 {
			continue
		}
		ln := int64(1 + rng.Intn(int(maxLen)))
		buf := make([]byte, ln)
		for i := range buf {
			buf[i] = byte(iter)
		}
		if err := w.Submit(rel, off, buf); err != nil {
			t.Fatalf("submit: %v", err)
		}
		copy(shadow[off:off+ln], buf)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Spot-check several random aligned and unaligned reads against the shadow.
	for k := 0; k < 200; k++ {
		off := int64(rng.Intn(fileSize))
		maxLen := int64(fileSize) - off
		if maxLen < 1 {
			continue
		}
		ln := int64(1 + rng.Intn(int(maxLen)))
		got := make([]byte, ln)
		n, err := r.ReadAt(rel, got, off)
		if err != nil {
			t.Fatalf("read off=%d ln=%d: %v", off, ln, err)
		}
		if int64(n) != ln {
			t.Fatalf("short read off=%d ln=%d: got %d", off, ln, n)
		}
		want := shadow[off : off+ln]
		if !bytes.Equal(got, want) {
			t.Fatalf("byte mismatch off=%d ln=%d: got %x want %x", off, ln, got, want)
		}
	}
}

// hasCreateRegular returns true since createFile uses Meta.PutFile +
// helpers_test.makeReg as a substitute for the absent CreateRegular API.
func hasCreateRegular() bool {
	return true
}

// TestFragmentMath_StraddleByteCorrectness exercises the mergeFragment
// straddle case (existing fragment fully encloses an incoming write):
//   1. Big write covers [0, 1000).
//   2. Small write covers [400, 500), splitting the big one into a left
//      kept_left at [0,400), dead at [400,500), kept_right at [500,1000).
// Reads of [0,1000) must reproduce the byte-by-byte expected pattern.
func TestFragmentMath_StraddleByteCorrectness(t *testing.T) {
	w, r, _, rel, cleanup := newRig2(t)
	defer cleanup()

	big := make([]byte, 1000)
	for i := range big {
		big[i] = byte(0xA0 + (i % 16))
	}
	if err := w.Submit(rel, 0, big); err != nil {
		t.Fatal(err)
	}
	small := make([]byte, 100)
	for i := range small {
		small[i] = byte(0x50 + (i % 16))
	}
	if err := w.Submit(rel, 400, small); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, 1000)
	if _, err := r.ReadAt(rel, got, 0); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 1000)
	copy(want, big)
	copy(want[400:500], small)
	if !bytes.Equal(got, want) {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("first mismatch at off=%d: got=%02x want=%02x", i, got[i], want[i])
			}
		}
	}
	t.Log("straddle case bytes-correct over [0,400)+[400,500)+[500,1000)")
}

// newRig2 is a sibling of newMWR that returns the writer/reader/rel directly,
// in the order the body of new straddle-test wants.
func newRig2(t *testing.T) (*place.Writer, *place.Reader, *place.Meta, string, func()) {
	t.Helper()
	m, w, r, cleanup := newMWR(t)
	rel := "f"
	createFile(t, m, rel)
	return w, r, m, rel, cleanup
}

// TestFragmentMath_ZeroLengthWriteAccumulates is the regression test for the
// metadata-bloat bug where Writer.Submit accepted 0-length data and the fast
// path of mergeFragment appended the 0-length Fragment unconditionally —
// repeated 0-length writes grew HotFragments without bound. Post-fix:
// Submit(rel, off, nil) returns nil and adds nothing to HotFragments, so
// after 50 zero-length submits only the priming real-byte fragment remains.
func TestFragmentMath_ZeroLengthWriteAccumulates(t *testing.T) {
	m, w, _, cleanup := newMWR(t)
	defer cleanup()
	rel := "z"
	createFile(t, m, rel)

	// Prime with one real byte at offset 100.
	if err := w.Submit(rel, 100, []byte{0xAB}); err != nil {
		t.Fatal(err)
	}
	// Each of these must be a no-op return-nil and must not change
	// HotFragments. Both nil and explicit empty slices are common
	// in real callers (FUSE write of len 0, replicate of an empty
	// frame, etc.) — exercise both shapes.
	for i := 0; i < 50; i++ {
		if err := w.Submit(rel, 50, nil); err != nil {
			t.Fatalf("nil submit: %v", err)
		}
		if err := w.Submit(rel, 50, []byte{}); err != nil {
			t.Fatalf("empty-slice submit: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	fm, err := m.GetFile(rel)
	if err != nil {
		t.Fatal(err)
	}
	if fm == nil {
		t.Fatal("file vanished")
	}
	for _, f := range fm.HotFragments {
		if f.Length == 0 {
			t.Errorf("zero-length Fragment leaked into HotFragments: %+v", f)
		}
	}
	// The list must hold exactly the one priming write — nothing else
	// was a real byte. Anything more is bloat.
	if len(fm.HotFragments) != 1 {
		t.Fatalf("HotFragments=%d, want 1 (the priming write); zero-length writes leaked",
			len(fm.HotFragments))
	}
	if fm.Size != 101 {
		t.Errorf("Size=%d, want 101 (priming write at off=100, len=1)", fm.Size)
	}
}
