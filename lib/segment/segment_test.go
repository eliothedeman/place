package segment

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	set, err := OpenSet(dir, TierHot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer set.CloseAll()

	payload := []byte("hello, world")
	seg, err := set.Active(int64(headerSize + len(payload) + trailerSize))
	if err != nil {
		t.Fatal(err)
	}
	off, err := seg.Append(RecordHeader{
		Inode: 1, StripeID: 0, Seq: 1, LogicalOff: 0, Length: uint32(len(payload)),
	}, payload)
	if err != nil {
		t.Fatal(err)
	}

	got := make([]byte, len(payload))
	if _, err := seg.ReadAt(got, off); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q want %q", got, payload)
	}
}

func TestScanRecoversTornTail(t *testing.T) {
	dir := t.TempDir()
	set, err := OpenSet(dir, TierHot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("x"), 100)
	seg, err := set.Active(int64(headerSize + len(payload) + trailerSize))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := seg.Append(RecordHeader{
			Inode: 1, StripeID: 0, Seq: uint64(i + 1), LogicalOff: int64(i) * 100, Length: uint32(len(payload)),
		}, payload); err != nil {
			t.Fatal(err)
		}
	}
	good := seg.Size()
	// Corrupt the tail by appending raw garbage.
	f, err := os.OpenFile(filepath.Join(dir, segFileName(seg.id)), os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("junkjunkjunkjunk"), good); err != nil {
		t.Fatal(err)
	}
	f.Close()
	_ = set.CloseAll()

	// Reopen and repair.
	set, err = OpenSet(dir, TierHot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer set.CloseAll()
	if err := set.RepairTail(); err != nil {
		t.Fatal(err)
	}
	seg = set.Get(0)
	if seg.Size() != good {
		t.Fatalf("after repair size %d want %d", seg.Size(), good)
	}

	var count int
	_, err = seg.Scan(func(h RecordHeader, off int64) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("scan found %d records, want 3", count)
	}
}

func TestRotateOnFull(t *testing.T) {
	dir := t.TempDir()
	// maxSize tiny so each append rotates.
	set, err := OpenSet(dir, TierHot, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer set.CloseAll()

	payload := bytes.Repeat([]byte("p"), 80)
	for i := 0; i < 3; i++ {
		recSize := int64(headerSize + len(payload) + trailerSize)
		seg, err := set.Active(recSize)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := seg.Append(RecordHeader{
			Inode: 1, StripeID: 0, Seq: uint64(i + 1), LogicalOff: int64(i) * int64(len(payload)), Length: uint32(len(payload)),
		}, payload); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(set.All()); got != 3 {
		t.Fatalf("got %d segments, want 3 (one per append)", got)
	}
}

func TestScanCarriesHeaderFields(t *testing.T) {
	dir := t.TempDir()
	set, err := OpenSet(dir, TierCold, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer set.CloseAll()

	payload := []byte("data")
	hdr := RecordHeader{Inode: 42, StripeID: 7, Seq: 123, LogicalOff: 4096, Length: 4}
	seg, err := set.Active(int64(headerSize + len(payload) + trailerSize))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seg.Append(hdr, payload); err != nil {
		t.Fatal(err)
	}

	var got []RecordHeader
	if _, err := seg.Scan(func(h RecordHeader, off int64) error {
		got = append(got, h)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != hdr {
		t.Fatalf("scan returned %v, want exactly [%v]", got, hdr)
	}
}
