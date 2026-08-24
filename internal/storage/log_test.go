package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mdstream/internal/domain"
)

func openLog(t *testing.T, dir string, opts Options) *Log {
	t.Helper()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l
}

func appendRange(t *testing.T, l *Log, from, to uint64) {
	t.Helper()
	for seq := from; seq <= to; seq++ {
		if err := l.Append(sampleEvent(seq)); err != nil {
			t.Fatalf("Append(%d): %v", seq, err)
		}
	}
}

// collect returns the sequences Scan yields from 'from'.
func collect(t *testing.T, l *Log, from uint64) []uint64 {
	t.Helper()
	var seqs []uint64
	err := l.Scan(from, func(ev domain.Event) error {
		if want := sampleEvent(ev.Sequence); ev != want {
			t.Fatalf("event %d: got %+v, want %+v", ev.Sequence, ev, want)
		}
		seqs = append(seqs, ev.Sequence)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan(%d): %v", from, err)
	}
	return seqs
}

func assertRange(t *testing.T, got []uint64, from, to uint64) {
	t.Helper()
	if want := int(to - from + 1); len(got) != want {
		t.Fatalf("got %d events, want %d (%d..%d)", len(got), want, from, to)
	}
	for i, seq := range got {
		if seq != from+uint64(i) {
			t.Fatalf("event %d has sequence %d, want %d", i, seq, from+uint64(i))
		}
	}
}

func segmentFiles(t *testing.T, dir string) []segmentInfo {
	t.Helper()
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	return segs
}

func TestOpenEmpty(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, Options{})
	defer l.Close()

	if l.LastSequence() != 0 {
		t.Fatalf("LastSequence = %d, want 0", l.LastSequence())
	}
	if st := l.Stats(); st.FirstSequence != 0 || st.LastSequence != 0 || st.Segments != 1 {
		t.Fatalf("Stats = %+v", st)
	}
	if segs := segmentFiles(t, dir); len(segs) != 1 || segs[0].base != 1 {
		t.Fatalf("segments = %+v, want one segment with base 1", segs)
	}
	if got := collect(t, l, 1); len(got) != 0 {
		t.Fatalf("Scan of empty log returned %v", got)
	}
}

func TestAppendScanReopen(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, Options{})
	appendRange(t, l, 1, 1000)

	assertRange(t, collect(t, l, 0), 1, 1000)
	assertRange(t, collect(t, l, 500), 500, 1000)
	if got := collect(t, l, 1001); len(got) != 0 {
		t.Fatalf("Scan past the end returned %d events", len(got))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Append(sampleEvent(1001)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}

	l = openLog(t, dir, Options{})
	defer l.Close()
	if rec := l.Recovery(); rec.LastSequence != 1000 || rec.TruncatedBytes != 0 {
		t.Fatalf("Recovery = %+v, want clean log ending at 1000", rec)
	}
	appendRange(t, l, 1001, 1100)
	assertRange(t, collect(t, l, 1), 1, 1100)
}

func TestAppendRejectsOutOfOrder(t *testing.T) {
	l := openLog(t, t.TempDir(), Options{})
	defer l.Close()
	appendRange(t, l, 1, 3)
	for _, seq := range []uint64{3, 5, 0} {
		if err := l.Append(sampleEvent(seq)); !errors.Is(err, ErrOutOfOrder) {
			t.Fatalf("Append(%d) = %v, want ErrOutOfOrder", seq, err)
		}
	}
	appendRange(t, l, 4, 4) // the log is still usable
}

func TestSegmentRoll(t *testing.T) {
	dir := t.TempDir()
	ev := sampleEvent(1)
	recSize := int64(len(appendRecord(nil, &ev)))
	opts := Options{SegmentSize: 10 * recSize} // exactly 10 records per segment

	l := openLog(t, dir, opts)
	appendRange(t, l, 1, 95)

	segs := segmentFiles(t, dir)
	if len(segs) != 10 {
		t.Fatalf("got %d segments, want 10", len(segs))
	}
	for i, s := range segs {
		if want := uint64(i*10 + 1); s.base != want {
			t.Fatalf("segment %d base = %d, want %d", i, s.base, want)
		}
		if i < len(segs)-1 && s.size != 10*recSize {
			t.Fatalf("sealed segment %d size = %d, want %d", i, s.size, 10*recSize)
		}
	}
	if st := l.Stats(); st.Segments != 10 || st.FirstSequence != 1 || st.LastSequence != 95 || st.Bytes != 95*recSize {
		t.Fatalf("Stats = %+v", st)
	}

	// Scans that start in the middle of, and at the boundary of, segments.
	assertRange(t, collect(t, l, 1), 1, 95)
	assertRange(t, collect(t, l, 37), 37, 95)
	assertRange(t, collect(t, l, 41), 41, 95)
	assertRange(t, collect(t, l, 95), 95, 95)

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openLog(t, dir, opts)
	defer l.Close()
	appendRange(t, l, 96, 120)
	assertRange(t, collect(t, l, 1), 1, 120)
	if n := len(segmentFiles(t, dir)); n != 12 {
		t.Fatalf("got %d segments after reopen, want 12", n)
	}
}

func TestDurabilityModesPersistAcrossReopen(t *testing.T) {
	for _, d := range []Durability{DurabilityWriteThrough, DurabilityFsync, DurabilityBuffered} {
		t.Run(d.String(), func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Durability: d, SegmentSize: 4096}
			l := openLog(t, dir, opts)
			appendRange(t, l, 1, 500)
			assertRange(t, collect(t, l, 1), 1, 500) // Scan sees buffered data too
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			l = openLog(t, dir, opts)
			defer l.Close()
			if l.LastSequence() != 500 {
				t.Fatalf("LastSequence after reopen = %d, want 500", l.LastSequence())
			}
			assertRange(t, collect(t, l, 1), 1, 500)
		})
	}
}

// TestCrashWithoutClose simulates a process kill: the log is never closed and
// never explicitly synced, and a new instance opens the same directory.
// In write-through mode every appended event must survive.
func TestCrashWithoutClose(t *testing.T) {
	dir := t.TempDir()
	crashed := openLog(t, dir, Options{Durability: DurabilityWriteThrough, SyncInterval: time.Hour, SegmentSize: 4096})
	defer crashed.Close() // release the handle at the end of the test (Windows)
	appendRange(t, crashed, 1, 300)

	recovered := openLog(t, dir, Options{SegmentSize: 4096})
	defer recovered.Close()
	if got := recovered.LastSequence(); got != 300 {
		t.Fatalf("LastSequence after crash = %d, want 300", got)
	}
	assertRange(t, collect(t, recovered, 1), 1, 300)
}

func TestSyncerFlushesBufferedData(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, Options{Durability: DurabilityBuffered, SyncInterval: 5 * time.Millisecond})
	defer l.Close()
	appendRange(t, l, 1, 10)

	// Without Close or Scan, the background syncer must push data to the file.
	path := filepath.Join(dir, segmentName(1))
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("buffered data never reached the file")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestScanStopsOnCallbackError(t *testing.T) {
	l := openLog(t, t.TempDir(), Options{})
	defer l.Close()
	appendRange(t, l, 1, 100)

	stop := errors.New("enough")
	var n int
	err := l.Scan(1, func(domain.Event) error {
		n++
		if n == 10 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || n != 10 {
		t.Fatalf("Scan = %v after %d events, want stop after 10", err, n)
	}
}

func TestScanConcurrentWithAppend(t *testing.T) {
	l := openLog(t, t.TempDir(), Options{SegmentSize: 4096})
	defer l.Close()
	appendRange(t, l, 1, 100)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := uint64(101); seq <= 3000; seq++ {
			if err := l.Append(sampleEvent(seq)); err != nil {
				t.Errorf("Append: %v", err)
				return
			}
		}
	}()
	for range 20 {
		var last uint64
		err := l.Scan(1, func(ev domain.Event) error {
			if ev.Sequence != last+1 {
				t.Fatalf("scan jumped from %d to %d", last, ev.Sequence)
			}
			last = ev.Sequence
			return nil
		})
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if last < 100 {
			t.Fatalf("scan ended at %d, want >= 100", last)
		}
	}
	<-done
	assertRange(t, collect(t, l, 1), 1, 3000)
}

func TestOpenRejectsTinySegments(t *testing.T) {
	if _, err := Open(t.TempDir(), Options{SegmentSize: MaxRecordSize - 1}); err == nil {
		t.Fatal("Open accepted a segment size smaller than one record")
	}
}

func TestParseDurability(t *testing.T) {
	for _, d := range []Durability{DurabilityWriteThrough, DurabilityFsync, DurabilityBuffered} {
		got, err := ParseDurability(d.String())
		if err != nil || got != d {
			t.Fatalf("ParseDurability(%q) = %v, %v", d.String(), got, err)
		}
	}
	if _, err := ParseDurability("sometimes"); err == nil {
		t.Fatal("ParseDurability accepted garbage")
	}
}
