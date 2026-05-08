package audit_tests

import (
	"bytes"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/eliothedeman/place"
)

// TestReaderReadAtEOFContract pins the io.ReaderAt contract for Reader.ReadAt:
// when n < len(dst), the returned error must be non-nil (io.EOF for past-EOF
// short reads). Pre-fix, ReadAt returned (n, nil) on a short read past fm.Size,
// which would silently truncate any external consumer wrapping Reader as
// io.ReaderAt — they'd see "successful zero-fill" instead of EOF and either
// loop forever or stop at the wrong place.
//
// The compactor's replicateOne (compact.go:replicateOne) was masked from this
// bug by sizing each chunk to min(replicateChunkSize, remaining), so it never
// requested past EOF in the first place — but a future caller (or replicate
// after a mid-flight Size shrink) would silently truncate.
func TestReaderReadAtEOFContract(t *testing.T) {
	hot, cold := dirs(t)
	meta := openMeta(t, hot)
	defer meta.Close()
	hotSegs := newHot(t, hot, 1<<30)
	defer hotSegs.CloseAll()
	coldSegs := newCold(t, cold, 1<<30)
	defer coldSegs.CloseAll()

	w := place.NewWriter(hotSegs, meta, nil)
	defer w.Close()

	now := time.Now().UnixNano()
	if err := meta.PutFile(&place.FileMeta{
		Rel: "f", Mode: syscall.S_IFREG | 0o644,
		Mtime: now, Ctime: now, Atime: now, Nlink: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// 100 bytes of payload.
	payload := bytes.Repeat([]byte("X"), 100)
	if err := w.Submit("f", 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	r := place.NewReader(hotSegs, coldSegs, meta)

	// Case 1: read 200 bytes from offset 0 → (100, io.EOF).
	{
		dst := make([]byte, 200)
		n, err := r.ReadAt("f", dst, 0)
		if n != 100 {
			t.Errorf("read 200 @0: n=%d want 100", n)
		}
		if err != io.EOF {
			t.Errorf("read 200 @0: err=%v want io.EOF", err)
		}
		if !bytes.Equal(dst[:n], payload) {
			t.Errorf("read 200 @0: payload mismatch")
		}
	}

	// Case 2: short read straddling EOF → (5, io.EOF).
	{
		dst := make([]byte, 10)
		n, err := r.ReadAt("f", dst, 95)
		if n != 5 {
			t.Errorf("read 10 @95: n=%d want 5", n)
		}
		if err != io.EOF {
			t.Errorf("read 10 @95: err=%v want io.EOF", err)
		}
		if !bytes.Equal(dst[:n], payload[95:]) {
			t.Errorf("read 10 @95: payload mismatch got %q", dst[:n])
		}
	}

	// Case 3: full read inside the file → (10, nil).
	{
		dst := make([]byte, 10)
		n, err := r.ReadAt("f", dst, 0)
		if n != 10 {
			t.Errorf("read 10 @0: n=%d want 10", n)
		}
		if err != nil {
			t.Errorf("read 10 @0: err=%v want nil", err)
		}
		if !bytes.Equal(dst[:n], payload[:10]) {
			t.Errorf("read 10 @0: payload mismatch")
		}
	}

	// Case 4: read entirely past EOF → (0, io.EOF). This case is already
	// handled by the off >= fm.Size early-return in ReadAt, but we pin it
	// here so a future refactor can't drop the EOF signal.
	{
		dst := make([]byte, 10)
		n, err := r.ReadAt("f", dst, 200)
		if n != 0 {
			t.Errorf("read 10 @200: n=%d want 0", n)
		}
		if err != io.EOF {
			t.Errorf("read 10 @200: err=%v want io.EOF", err)
		}
	}
}
