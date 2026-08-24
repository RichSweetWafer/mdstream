package storage

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"mdstream/internal/domain"
)

func sampleEvent(seq uint64) domain.Event {
	return domain.Event{
		Sequence:  seq,
		Timestamp: 1_700_000_000_000_000_000 + int64(seq),
		Symbol:    "AAPL",
		Price:     domain.FixedFromFloat(187.25),
		Quantity:  domain.FixedFromFloat(100),
		Type:      domain.EventTypeQuote,
		Side:      domain.SideAsk,
	}
}

func TestRecordRoundTrip(t *testing.T) {
	events := []domain.Event{
		sampleEvent(1),
		{Sequence: 2, Timestamp: -5, Symbol: strings.Repeat("Z", domain.MaxSymbolLen), Price: -1, Quantity: 0, Type: domain.EventTypeTrade},
	}
	var buf []byte
	for i := range events {
		buf = appendRecord(buf, &events[i])
	}

	rr := newRecordReader(bytes.NewReader(buf))
	for i, want := range events {
		got, n, err := rr.next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("record %d: got %+v, want %+v", i, got, want)
		}
		if wantN := recordHeaderSize + bodyFixedSize + len(want.Symbol); n != wantN {
			t.Fatalf("record %d: size %d, want %d", i, n, wantN)
		}
	}
	if _, _, err := rr.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after last record: %v, want io.EOF", err)
	}
}

func TestRecordDetectsTornData(t *testing.T) {
	ev := sampleEvent(1)
	rec := appendRecord(nil, &ev)
	for _, cut := range []int{1, recordHeaderSize - 1, recordHeaderSize, len(rec) - 1} {
		_, _, err := newRecordReader(bytes.NewReader(rec[:cut])).next()
		if !errors.Is(err, errTornRecord) {
			t.Errorf("cut at %d: got %v, want errTornRecord", cut, err)
		}
	}
}

func TestRecordDetectsCorruption(t *testing.T) {
	ev := sampleEvent(1)
	rec := appendRecord(nil, &ev)
	// Flip one bit in every byte position; each must be detected.
	for i := range rec {
		bad := bytes.Clone(rec)
		bad[i] ^= 0x01
		_, _, err := newRecordReader(bytes.NewReader(bad)).next()
		if err == nil {
			t.Fatalf("bit flip at byte %d not detected", i)
		}
		// A flipped length can also make the record look torn; both are tail damage.
		if !isTailDamage(err) {
			t.Fatalf("bit flip at byte %d: unexpected error %v", i, err)
		}
	}
}

func TestScanRecordsChecksSequence(t *testing.T) {
	var buf []byte
	for _, seq := range []uint64{5, 6, 8} {
		ev := sampleEvent(seq)
		buf = appendRecord(buf, &ev)
	}
	end, last, err := scanRecords(bytes.NewReader(buf), 5, nil)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt for the sequence gap", err)
	}
	if last != 6 {
		t.Fatalf("last valid sequence = %d, want 6", last)
	}
	ev := sampleEvent(1)
	if want := int64(2 * len(appendRecord(nil, &ev))); end != want {
		t.Fatalf("validEnd = %d, want %d", end, want)
	}
}

func TestSegmentNames(t *testing.T) {
	name := segmentName(1_048_577)
	if name != "segment-00000000000001048577.log" {
		t.Fatalf("segmentName = %q", name)
	}
	if base, ok := parseSegmentName(name); !ok || base != 1_048_577 {
		t.Fatalf("parseSegmentName(%q) = %d, %v", name, base, ok)
	}
	for _, bad := range []string{"segment-1.log", "segment-00000000000000000000.log", "other.log", "segment-0000000000000000000x.log"} {
		if _, ok := parseSegmentName(bad); ok {
			t.Errorf("parseSegmentName(%q) accepted", bad)
		}
	}
}
