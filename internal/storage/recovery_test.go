package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mdstream/internal/domain"
)

// writeLog creates a closed log in dir containing events 1..n.
func writeLog(t *testing.T, dir string, opts Options, n uint64) {
	t.Helper()
	l := openLog(t, dir, opts)
	appendRange(t, l, 1, n)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func activeSegment(t *testing.T, dir string) segmentInfo {
	t.Helper()
	segs := segmentFiles(t, dir)
	return segs[len(segs)-1]
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

// TestRecoverTornTail: the process died in the middle of writing record 101.
func TestRecoverTornTail(t *testing.T) {
	ev := sampleEvent(101)
	partial := appendRecord(nil, &ev)
	cuts := map[string]int{
		"inside header": recordHeaderSize / 2,
		"inside body":   recordHeaderSize + 3,
		"one byte left": len(partial) - 1,
	}
	for name, cut := range cuts {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeLog(t, dir, Options{}, 100)
			seg := activeSegment(t, dir)
			cleanSize := fileSize(t, seg.path)
			appendBytes(t, seg.path, partial[:cut])

			l := openLog(t, dir, Options{})
			defer l.Close()

			rec := l.Recovery()
			if rec.LastSequence != 100 || rec.TruncatedBytes != int64(cut) || !errors.Is(rec.TruncateReason, errTornRecord) {
				t.Fatalf("Recovery = %+v, want last=100 truncated=%d (torn record)", rec, cut)
			}
			if got := fileSize(t, seg.path); got != cleanSize {
				t.Fatalf("segment size after recovery = %d, want %d", got, cleanSize)
			}

			// Operation continues normally from the next sequence.
			appendRange(t, l, 101, 150)
			assertRange(t, collect(t, l, 1), 1, 150)
		})
	}
}

// TestRecoverCorruptLastRecord: the last record is complete but damaged.
func TestRecoverCorruptLastRecord(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, Options{}, 100)
	seg := activeSegment(t, dir)

	ev := sampleEvent(100)
	recSize := int64(len(appendRecord(nil, &ev)))
	size := fileSize(t, seg.path)
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte in the symbol of the last record.
	if _, err := f.WriteAt([]byte{'#'}, size-2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l := openLog(t, dir, Options{})
	defer l.Close()
	rec := l.Recovery()
	if rec.LastSequence != 99 || rec.TruncatedBytes != recSize || !errors.Is(rec.TruncateReason, ErrCorrupt) {
		t.Fatalf("Recovery = %+v, want last=99, %d bytes truncated (corrupt)", rec, recSize)
	}
	appendRange(t, l, 100, 100)
	assertRange(t, collect(t, l, 1), 1, 100)
}

// TestRecoverGarbageTail: random bytes after the last record (e.g. a
// preallocated or partially written block).
func TestRecoverGarbageTail(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, Options{}, 10)
	seg := activeSegment(t, dir)
	appendBytes(t, seg.path, make([]byte, 4096)) // zeros: length field 0 is invalid

	l := openLog(t, dir, Options{})
	defer l.Close()
	if rec := l.Recovery(); rec.LastSequence != 10 || rec.TruncatedBytes != 4096 {
		t.Fatalf("Recovery = %+v", rec)
	}
	appendRange(t, l, 11, 11)
}

// TestRecoverEmptySegmentAfterRoll: the process died right after creating a
// new segment, before writing its first record.
func TestRecoverEmptySegmentAfterRoll(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, Options{}, 50)
	empty := filepath.Join(dir, segmentName(51))
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	l := openLog(t, dir, Options{})
	defer l.Close()
	if rec := l.Recovery(); rec.LastSequence != 50 || rec.ActiveSegment != empty || rec.Segments != 2 {
		t.Fatalf("Recovery = %+v, want last=50 with the empty segment active", rec)
	}
	appendRange(t, l, 51, 60)
	assertRange(t, collect(t, l, 1), 1, 60)
	if got := fileSize(t, empty); got == 0 {
		t.Fatal("new events were not written to the empty active segment")
	}
}

// TestRecoverEverythingTorn: the only record in the log is torn.
func TestRecoverEverythingTorn(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, Options{}, 0) // creates the empty first segment
	ev := sampleEvent(1)
	rec := appendRecord(nil, &ev)
	appendBytes(t, activeSegment(t, dir).path, rec[:len(rec)/2])

	l := openLog(t, dir, Options{})
	defer l.Close()
	if l.LastSequence() != 0 {
		t.Fatalf("LastSequence = %d, want 0", l.LastSequence())
	}
	appendRange(t, l, 1, 5)
	assertRange(t, collect(t, l, 1), 1, 5)
}

// TestScanReportsCorruptSealedSegment: sealed segments are not repaired; a
// damaged one is reported to the reader.
func TestScanReportsCorruptSealedSegment(t *testing.T) {
	dir := t.TempDir()
	ev := sampleEvent(1)
	recSize := int64(len(appendRecord(nil, &ev)))
	opts := Options{SegmentSize: 10 * recSize}
	writeLog(t, dir, opts, 30)

	first := segmentFiles(t, dir)[0]
	f, err := os.OpenFile(first.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, recSize*5+recordHeaderSize); err != nil { // record 6
		t.Fatal(err)
	}
	f.Close()

	l := openLog(t, dir, opts)
	defer l.Close()
	if l.LastSequence() != 30 {
		t.Fatalf("LastSequence = %d, want 30 (only the active segment is scanned on open)", l.LastSequence())
	}
	var seen int
	err = l.Scan(1, func(domain.Event) error { seen++; return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Scan = %v, want ErrCorrupt", err)
	}
	if seen != 5 {
		t.Fatalf("Scan delivered %d events before the corrupt record, want 5", seen)
	}
	// Reading after the damaged segment still works.
	assertRange(t, collect(t, l, 11), 11, 30)
}
