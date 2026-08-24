package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"

	"mdstream/internal/domain"
)

// On-disk record layout (little-endian):
//
//	offset size field
//	0      4    crc32c    CRC-32C (Castagnoli) of bytes [4, 24+N)
//	4      4    length    N, size of the body in bytes
//	8      8    sequence
//	16     8    timestamp Unix nanoseconds
//	24     N    body
//
// Body:
//
//	0      8    price     domain.Fixed
//	8      8    quantity  domain.Fixed
//	16     1    type
//	17     1    side
//	18     2    symbol length L (must equal N-20)
//	20     L    symbol
//
// The checksum covers the length field, so a corrupted length is detected
// rather than used to read garbage.
const (
	recordHeaderSize = 24
	bodyFixedSize    = 20
	maxBodySize      = bodyFixedSize + domain.MaxSymbolLen

	// MaxRecordSize is the largest possible encoded event.
	MaxRecordSize = recordHeaderSize + maxBodySize
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrCorrupt reports a record that fails validation (bad checksum, bad
	// length, or a sequence that does not follow its predecessor).
	ErrCorrupt = errors.New("storage: corrupt record")

	// errTornRecord reports data that ends in the middle of a record, which is
	// what a crash during a write leaves behind.
	errTornRecord = errors.New("storage: torn record")
)

// appendRecord appends the encoding of ev to dst. ev.Symbol must not exceed
// domain.MaxSymbolLen.
func appendRecord(dst []byte, ev *domain.Event) []byte {
	n := bodyFixedSize + len(ev.Symbol)
	start := len(dst)
	dst = slices.Grow(dst, recordHeaderSize+n)[:start+recordHeaderSize+n]
	b := dst[start:]

	binary.LittleEndian.PutUint32(b[4:], uint32(n))
	binary.LittleEndian.PutUint64(b[8:], ev.Sequence)
	binary.LittleEndian.PutUint64(b[16:], uint64(ev.Timestamp))

	body := b[recordHeaderSize:]
	binary.LittleEndian.PutUint64(body[0:], uint64(ev.Price))
	binary.LittleEndian.PutUint64(body[8:], uint64(ev.Quantity))
	body[16] = byte(ev.Type)
	body[17] = byte(ev.Side)
	binary.LittleEndian.PutUint16(body[18:], uint16(len(ev.Symbol)))
	copy(body[bodyFixedSize:], ev.Symbol)

	binary.LittleEndian.PutUint32(b[0:], crc32.Checksum(b[4:], crcTable))
	return dst
}

// recordReader decodes consecutive records from a stream.
type recordReader struct {
	r    *bufio.Reader
	hdr  [recordHeaderSize]byte
	body [maxBodySize]byte
}

func newRecordReader(r io.Reader) *recordReader {
	return &recordReader{r: bufio.NewReaderSize(r, 64<<10)}
}

// next decodes the next record and returns it with its encoded size.
// It returns io.EOF at a clean end of data, errTornRecord if the data ends
// inside a record, and an error wrapping ErrCorrupt for an invalid record.
func (rr *recordReader) next() (domain.Event, int, error) {
	if _, err := io.ReadFull(rr.r, rr.hdr[:]); err != nil {
		switch {
		case errors.Is(err, io.EOF):
			return domain.Event{}, 0, io.EOF
		case errors.Is(err, io.ErrUnexpectedEOF):
			return domain.Event{}, 0, errTornRecord
		default:
			return domain.Event{}, 0, err
		}
	}

	n := binary.LittleEndian.Uint32(rr.hdr[4:])
	if n < bodyFixedSize || n > maxBodySize {
		return domain.Event{}, 0, fmt.Errorf("%w: invalid body length %d", ErrCorrupt, n)
	}
	body := rr.body[:n]
	if _, err := io.ReadFull(rr.r, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return domain.Event{}, 0, errTornRecord
		}
		return domain.Event{}, 0, err
	}

	crc := crc32.Update(crc32.Checksum(rr.hdr[4:], crcTable), crcTable, body)
	if crc != binary.LittleEndian.Uint32(rr.hdr[0:]) {
		return domain.Event{}, 0, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	symLen := int(binary.LittleEndian.Uint16(body[18:]))
	if symLen != int(n)-bodyFixedSize {
		return domain.Event{}, 0, fmt.Errorf("%w: symbol length %d does not match body length %d", ErrCorrupt, symLen, n)
	}

	ev := domain.Event{
		Sequence:  binary.LittleEndian.Uint64(rr.hdr[8:]),
		Timestamp: int64(binary.LittleEndian.Uint64(rr.hdr[16:])),
		Price:     domain.Fixed(binary.LittleEndian.Uint64(body[0:])),
		Quantity:  domain.Fixed(binary.LittleEndian.Uint64(body[8:])),
		Type:      domain.EventType(body[16]),
		Side:      domain.Side(body[17]),
		Symbol:    string(body[bodyFixedSize:]),
	}
	return ev, recordHeaderSize + int(n), nil
}

// scanRecords reads consecutive records from r. The first record must have
// sequence base, and every following one the next sequence.
//
// It returns the byte offset just past the last valid record and that
// record's sequence (0 if there was none). err is nil at a clean end of data,
// errTornRecord if the data ends inside a record, an error wrapping ErrCorrupt
// for an invalid or out-of-sequence record, or the first error returned by fn.
func scanRecords(r io.Reader, base uint64, fn func(domain.Event) error) (validEnd int64, lastSeq uint64, err error) {
	rr := newRecordReader(r)
	expected := base
	for {
		ev, n, err := rr.next()
		if errors.Is(err, io.EOF) {
			return validEnd, lastSeq, nil
		}
		if err != nil {
			return validEnd, lastSeq, err
		}
		if ev.Sequence != expected {
			return validEnd, lastSeq, fmt.Errorf("%w: sequence %d at offset %d, expected %d", ErrCorrupt, ev.Sequence, validEnd, expected)
		}
		if fn != nil {
			if err := fn(ev); err != nil {
				return validEnd, lastSeq, err
			}
		}
		validEnd += int64(n)
		lastSeq = ev.Sequence
		expected++
	}
}

// isTailDamage reports whether err describes damage that recovery repairs by
// truncating the log: a torn or corrupt record.
func isTailDamage(err error) bool {
	return errors.Is(err, errTornRecord) || errors.Is(err, ErrCorrupt)
}
