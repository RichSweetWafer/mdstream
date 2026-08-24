// Package storage implements mdstream's persistent, append-only event log.
//
// The log is a directory of segment files. Records are appended to the last
// ("active") segment; when it would exceed Options.SegmentSize it is fsynced,
// sealed, and a new segment is started. Sealed segments are never modified.
//
// On Open, the active segment is scanned from the start. A torn or corrupt
// record at the tail, which a crash during a write leaves behind, is truncated
// away, and appending continues at the next sequence.
package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"mdstream/internal/domain"
)

// Durability selects when appended records reach the OS and stable storage.
type Durability int

const (
	// DurabilityWriteThrough writes every record to the OS before Append
	// returns and fsyncs every SyncInterval. A process crash loses nothing;
	// a power failure loses at most SyncInterval of events.
	DurabilityWriteThrough Durability = iota

	// DurabilityFsync fsyncs after every record. Nothing acknowledged is lost
	// even on power failure, at the cost of throughput.
	DurabilityFsync

	// DurabilityBuffered buffers records in memory and flushes and fsyncs
	// every SyncInterval. Fastest; a process crash can lose up to
	// SyncInterval of events.
	DurabilityBuffered
)

func (d Durability) String() string {
	switch d {
	case DurabilityWriteThrough:
		return "write"
	case DurabilityFsync:
		return "fsync"
	case DurabilityBuffered:
		return "buffered"
	default:
		return fmt.Sprintf("Durability(%d)", int(d))
	}
}

// ParseDurability parses "write", "fsync" or "buffered".
func ParseDurability(s string) (Durability, error) {
	for _, d := range []Durability{DurabilityWriteThrough, DurabilityFsync, DurabilityBuffered} {
		if s == d.String() {
			return d, nil
		}
	}
	return 0, fmt.Errorf("unknown durability %q (want write, fsync or buffered)", s)
}

const (
	DefaultSegmentSize  = 256 << 20
	DefaultSyncInterval = 100 * time.Millisecond

	bufferedWriterSize = 1 << 20
)

// Options configures a Log. Zero values select defaults.
type Options struct {
	SegmentSize  int64
	Durability   Durability
	SyncInterval time.Duration
}

var (
	// ErrClosed is returned by operations on a closed Log.
	ErrClosed = errors.New("storage: event log closed")

	// ErrOutOfOrder is returned by Append for an event whose sequence does not
	// directly follow the last appended one.
	ErrOutOfOrder = errors.New("storage: out-of-order sequence")
)

// RecoveryInfo describes what Open found and repaired.
type RecoveryInfo struct {
	Segments       int
	ActiveSegment  string
	LastSequence   uint64
	TruncatedBytes int64 // bytes removed from the tail of the active segment
	TruncateReason error // why they were removed; nil if the tail was clean
}

// Stats is a point-in-time snapshot of the log.
type Stats struct {
	Segments      int
	Bytes         int64
	FirstSequence uint64 // 0 if the log is empty
	LastSequence  uint64 // 0 if the log is empty
}

// Log is a segmented, append-only event log. It is safe for concurrent use.
type Log struct {
	dir  string
	opts Options

	mu         sync.Mutex
	sealed     []segmentInfo
	activeBase uint64
	activePath string
	active     *os.File
	bw         *bufio.Writer // DurabilityBuffered only
	size       int64         // logical size of the active segment, including buffered bytes
	lastSeq    uint64
	buf        []byte // encode buffer, reused
	dirty      bool   // data written to the OS since the last fsync
	err        error  // sticky: after a failed write/fsync the log refuses appends
	closed     bool
	recovery   RecoveryInfo

	stopSyncer chan struct{}
	syncerDone chan struct{}
}

// Open opens (creating if necessary) the log in dir and recovers it.
func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultSegmentSize
	}
	if opts.SegmentSize < MaxRecordSize {
		return nil, fmt.Errorf("storage: segment size %d is smaller than the max record size %d", opts.SegmentSize, MaxRecordSize)
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = DefaultSyncInterval
	}
	if _, err := ParseDurability(opts.Durability.String()); err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create %s: %w", dir, err)
	}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, fmt.Errorf("storage: list segments: %w", err)
	}

	if len(segs) == 0 {
		path := filepath.Join(dir, segmentName(1))
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return nil, fmt.Errorf("storage: create first segment: %w", err)
		}
		f.Close()
		if err := syncDir(dir); err != nil {
			return nil, fmt.Errorf("storage: sync dir: %w", err)
		}
		segs = []segmentInfo{{base: 1, path: path}}
	}

	l := &Log{dir: dir, opts: opts, sealed: segs[:len(segs)-1]}
	if err := l.recoverActive(segs[len(segs)-1]); err != nil {
		return nil, err
	}
	l.recovery.Segments = len(segs)

	if opts.Durability == DurabilityBuffered {
		l.bw = bufio.NewWriterSize(l.active, bufferedWriterSize)
	}
	if opts.Durability != DurabilityFsync {
		l.stopSyncer = make(chan struct{})
		l.syncerDone = make(chan struct{})
		go l.runSyncer()
	}
	return l, nil
}

// recoverActive opens the last segment, scans it and truncates any damaged
// tail so that the segment ends with a complete, valid record.
func (l *Log) recoverActive(seg segmentInfo) error {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("storage: open active segment: %w", err)
	}
	fail := func(err error) error {
		f.Close()
		return err
	}

	info, err := f.Stat()
	if err != nil {
		return fail(fmt.Errorf("storage: stat %s: %w", seg.path, err))
	}
	validEnd, lastSeq, scanErr := scanRecords(f, seg.base, nil)
	if scanErr != nil && !isTailDamage(scanErr) {
		return fail(fmt.Errorf("storage: scan %s: %w", seg.path, scanErr))
	}
	if lastSeq == 0 { // empty segment: the previous one ended at base-1
		lastSeq = seg.base - 1
	}

	if truncated := info.Size() - validEnd; truncated > 0 {
		if err := f.Truncate(validEnd); err != nil {
			return fail(fmt.Errorf("storage: truncate %s: %w", seg.path, err))
		}
		if err := f.Sync(); err != nil {
			return fail(fmt.Errorf("storage: sync %s: %w", seg.path, err))
		}
		l.recovery.TruncatedBytes = truncated
		l.recovery.TruncateReason = scanErr
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		return fail(fmt.Errorf("storage: seek %s: %w", seg.path, err))
	}

	l.active = f
	l.activeBase = seg.base
	l.activePath = seg.path
	l.size = validEnd
	l.lastSeq = lastSeq
	l.recovery.ActiveSegment = seg.path
	l.recovery.LastSequence = lastSeq
	return nil
}

// Append writes ev to the log. ev.Sequence must be exactly LastSequence()+1.
//
// When Append returns nil the record has reached the OS (write-through),
// stable storage (fsync), or the in-memory buffer (buffered). After a failed
// write or fsync the log is unusable and every call returns the same error.
func (l *Log) Append(ev domain.Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}
	if l.err != nil {
		return l.err
	}
	if ev.Sequence != l.lastSeq+1 {
		return fmt.Errorf("%w: got %d, want %d", ErrOutOfOrder, ev.Sequence, l.lastSeq+1)
	}
	if len(ev.Symbol) > domain.MaxSymbolLen {
		return fmt.Errorf("storage: symbol longer than %d bytes", domain.MaxSymbolLen)
	}

	l.buf = appendRecord(l.buf[:0], &ev)
	if l.size > 0 && l.size+int64(len(l.buf)) > l.opts.SegmentSize {
		if err := l.rollLocked(ev.Sequence); err != nil {
			l.err = fmt.Errorf("storage: roll segment: %w", err)
			return l.err
		}
	}
	if err := l.writeLocked(l.buf); err != nil {
		return err
	}
	l.lastSeq = ev.Sequence
	return nil
}

// writeLocked writes one encoded record to the active segment.
func (l *Log) writeLocked(rec []byte) error {
	if l.bw != nil {
		if _, err := l.bw.Write(rec); err != nil {
			l.err = fmt.Errorf("storage: write: %w", err)
			return l.err
		}
		l.size += int64(len(rec))
		l.dirty = true
		return nil
	}

	n, err := l.active.Write(rec)
	if err != nil {
		// Undo a partial write so a torn record never sits in the middle of
		// the segment. If that is impossible, stop accepting writes.
		if n > 0 {
			if terr := l.truncateActiveLocked(l.size); terr != nil {
				l.err = fmt.Errorf("storage: write failed (%v) and rollback failed: %w", err, terr)
				return l.err
			}
		}
		return fmt.Errorf("storage: write: %w", err)
	}
	l.size += int64(n)
	l.dirty = true

	if l.opts.Durability == DurabilityFsync {
		if err := l.active.Sync(); err != nil {
			// The record may or may not be durable; the log cannot tell, so it
			// stops rather than risk acknowledging a sequence twice.
			l.err = fmt.Errorf("storage: fsync: %w", err)
			return l.err
		}
		l.dirty = false
	}
	return nil
}

func (l *Log) truncateActiveLocked(size int64) error {
	if err := l.active.Truncate(size); err != nil {
		return err
	}
	_, err := l.active.Seek(size, io.SeekStart)
	return err
}

// rollLocked seals the active segment and starts a new one whose first record
// will have sequence nextBase.
func (l *Log) rollLocked(nextBase uint64) error {
	if err := l.flushLocked(); err != nil {
		return err
	}
	if err := l.active.Sync(); err != nil {
		return err
	}
	if err := l.active.Close(); err != nil {
		return err
	}
	l.sealed = append(l.sealed, segmentInfo{base: l.activeBase, path: l.activePath, size: l.size})

	path := filepath.Join(l.dir, segmentName(nextBase))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		f.Close()
		return err
	}

	l.active = f
	l.activeBase = nextBase
	l.activePath = path
	l.size = 0
	l.dirty = false
	if l.bw != nil {
		l.bw.Reset(f)
	}
	return nil
}

// flushLocked hands buffered bytes to the OS (DurabilityBuffered only).
func (l *Log) flushLocked() error {
	if l.bw == nil || l.bw.Buffered() == 0 {
		return nil
	}
	if err := l.bw.Flush(); err != nil {
		l.err = fmt.Errorf("storage: flush: %w", err)
		return l.err
	}
	return nil
}

// Sync flushes buffered records and fsyncs the active segment.
//
// The fsync runs outside the lock so appends are not stalled behind the disk.
// If a concurrent roll closes the file first, Sync returns nil: rolling
// fsyncs the segment before closing it.
func (l *Log) Sync() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	if l.err != nil {
		err := l.err
		l.mu.Unlock()
		return err
	}
	if err := l.flushLocked(); err != nil {
		l.mu.Unlock()
		return err
	}
	if !l.dirty {
		l.mu.Unlock()
		return nil
	}
	f := l.active
	l.dirty = false
	l.mu.Unlock()

	if err := f.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		l.mu.Lock()
		if l.err == nil {
			l.err = fmt.Errorf("storage: fsync: %w", err)
		}
		l.mu.Unlock()
		return err
	}
	return nil
}

func (l *Log) runSyncer() {
	defer close(l.syncerDone)
	t := time.NewTicker(l.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-l.stopSyncer:
			return
		case <-t.C:
			_ = l.Sync() // failures are sticky and surface on the next Append
		}
	}
}

// Close flushes, fsyncs and closes the log. It is idempotent.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	if l.stopSyncer != nil {
		close(l.stopSyncer)
		<-l.syncerDone
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	flushErr := l.flushLocked()
	syncErr := l.active.Sync()
	closeErr := l.active.Close()
	return errors.Join(flushErr, syncErr, closeErr)
}

// LastSequence returns the sequence of the last appended event, or 0 if the
// log is empty.
func (l *Log) LastSequence() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastSeq
}

// Recovery reports what Open found and repaired.
func (l *Log) Recovery() RecoveryInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.recovery
}

// Stats returns a snapshot of the log's size and sequence range.
func (l *Log) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := Stats{Segments: len(l.sealed) + 1, Bytes: l.size, LastSequence: l.lastSeq}
	for _, s := range l.sealed {
		st.Bytes += s.size
	}
	first := l.activeBase
	if len(l.sealed) > 0 {
		first = l.sealed[0].base
	}
	if l.lastSeq >= first {
		st.FirstSequence = first
	}
	return st
}

// errStopScan ends a scan early without reporting an error.
var errStopScan = errors.New("stop scan")

// Scan calls fn, in sequence order, for every event with Sequence >= from
// that was appended before Scan was called. A non-nil error from fn stops the
// scan and is returned. Scan may run concurrently with Append.
//
// Finding the start position currently means decoding the start of the
// segment that contains from; a sparse index can replace that later.
func (l *Log) Scan(from uint64, fn func(domain.Event) error) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	if err := l.flushLocked(); err != nil {
		l.mu.Unlock()
		return err
	}
	segs := make([]segmentInfo, 0, len(l.sealed)+1)
	segs = append(segs, l.sealed...)
	segs = append(segs, segmentInfo{base: l.activeBase, path: l.activePath, size: l.size})
	last := l.lastSeq
	l.mu.Unlock()

	if from == 0 {
		from = 1
	}
	if from > last {
		return nil
	}

	// The segment containing from is the last one whose base is <= from.
	i := sort.Search(len(segs), func(i int) bool { return segs[i].base > from }) - 1
	if i < 0 {
		i = 0
	}
	for _, seg := range segs[i:] {
		if seg.base > last {
			break
		}
		if err := scanSegment(seg, from, last, fn); err != nil {
			return err
		}
	}
	return nil
}

func scanSegment(seg segmentInfo, from, last uint64, fn func(domain.Event) error) error {
	f, err := os.Open(seg.path)
	if err != nil {
		return fmt.Errorf("storage: open %s: %w", seg.path, err)
	}
	defer f.Close()

	// Read only the bytes that existed when the scan started; the writer may
	// be appending to the active segment concurrently.
	_, _, err = scanRecords(io.LimitReader(f, seg.size), seg.base, func(ev domain.Event) error {
		if ev.Sequence < from {
			return nil
		}
		if ev.Sequence > last {
			return errStopScan
		}
		return fn(ev)
	})
	switch {
	case err == nil, errors.Is(err, errStopScan):
		return nil
	case errors.Is(err, errTornRecord):
		// Within the snapshot size every record is complete, so a torn
		// record here means the file changed under us.
		return fmt.Errorf("%w: %s: truncated record", ErrCorrupt, seg.path)
	case errors.Is(err, ErrCorrupt):
		return fmt.Errorf("%s: %w", seg.path, err)
	default:
		return err
	}
}
