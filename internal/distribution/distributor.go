// Package distribution fans market-data events out to subscribers.
//
// Design:
//
//   - Publish assigns the sequence number and delivers to every subscriber
//     queue under a single mutex. That makes the sequence total-ordered: every
//     subscriber sees strictly increasing sequence numbers, even with many
//     concurrent publishers.
//   - Every subscriber has its own bounded queue (a buffered channel), so memory
//     is bounded by QueueSize × subscribers.
//   - Delivery is a non-blocking send. If a subscriber's queue is full, that
//     subscriber is disconnected with ErrSlowConsumer instead of blocking the
//     publisher. A slow consumer therefore never delays the others.
//   - If an EventLog is configured, the event is appended to it inside the same
//     critical section, before delivery. The log therefore holds exactly the
//     stream subscribers see, in the same order, and no subscriber ever
//     receives an event that is not in the log.
package distribution

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"mdstream/internal/domain"
)

var (
	// ErrClosed is returned by Publish after StopPublishing or Close, and by
	// Subscribe after Close.
	ErrClosed = errors.New("distribution: distributor closed")

	// ErrSlowConsumer terminates a subscription whose queue overflowed.
	ErrSlowConsumer = errors.New("distribution: slow consumer, subscriber queue overflowed")

	// ErrUnsubscribed terminates a subscription closed by its owner.
	ErrUnsubscribed = errors.New("distribution: unsubscribed")

	// ErrShutdown terminates every subscription when the distributor is closed.
	ErrShutdown = errors.New("distribution: distributor shut down")
)

// DefaultQueueSize is used when Config.QueueSize is not positive.
const DefaultQueueSize = 1024

// Config configures a Distributor.
type Config struct {
	// QueueSize is the capacity of each subscriber's queue, in events.
	QueueSize int

	// Now returns the timestamp assigned to events published with a zero
	// Timestamp. Defaults to time.Now; overridable for tests.
	Now func() time.Time

	// Log, if set, receives every event before it is delivered. A failed
	// append fails the Publish and the sequence number is not consumed.
	Log EventLog

	// StartSequence is the last sequence already used (e.g. the event log's
	// last sequence after a restart). The first published event gets
	// StartSequence+1.
	StartSequence uint64
}

// EventLog persists published events. *storage.Log implements it.
type EventLog interface {
	Append(domain.Event) error
}

// Stats is a point-in-time snapshot of distributor counters.
type Stats struct {
	LastSequence            uint64 // sequence of the last published event
	Delivered               uint64 // successful enqueues, summed over subscribers
	Subscribers             int    // currently active subscriptions
	SlowConsumerDisconnects uint64
}

// Distributor delivers every published event to every active subscriber.
// It is safe for concurrent use.
type Distributor struct {
	queueSize int
	now       func() time.Time
	log       EventLog

	mu         sync.Mutex
	seq        uint64
	nextID     uint64
	subs       []*Subscription
	publishing bool
	closed     bool
	delivered  uint64
	slowDrops  uint64
}

// New returns a Distributor that accepts events immediately.
func New(cfg Config) *Distributor {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Distributor{
		queueSize:  cfg.QueueSize,
		now:        cfg.Now,
		log:        cfg.Log,
		seq:        cfg.StartSequence,
		publishing: true,
	}
}

// Subscribe registers a new subscriber that will receive every event published
// from now on. It returns ErrClosed after Close.
func (d *Distributor) Subscribe() (*Subscription, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrClosed
	}
	d.nextID++
	s := &Subscription{
		id:   d.nextID,
		d:    d,
		ch:   make(chan domain.Event, d.queueSize),
		done: make(chan struct{}),
	}
	d.subs = append(d.subs, s)
	return s, nil
}

// Publish validates ev, assigns it the next sequence number (and a timestamp if
// it has none) and enqueues it for every subscriber. It never blocks on
// subscribers: any subscriber whose queue is full is disconnected with
// ErrSlowConsumer.
//
// It returns the assigned sequence number, ErrClosed if publishing has
// stopped, an error wrapping domain.ErrInvalidEvent, or the event log's error.
func (d *Distributor) Publish(ev domain.Event) (uint64, error) {
	if err := ev.Validate(); err != nil {
		return 0, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.publishing {
		return 0, ErrClosed
	}

	ev.Sequence = d.seq + 1
	if ev.Timestamp == 0 {
		ev.Timestamp = d.now().UnixNano()
	}
	if d.log != nil {
		if err := d.log.Append(ev); err != nil {
			return 0, fmt.Errorf("distribution: event log: %w", err)
		}
	}
	d.seq = ev.Sequence

	for i := 0; i < len(d.subs); {
		s := d.subs[i]
		select {
		case s.ch <- ev:
			d.delivered++
			i++
		default:
			// Queue full: disconnect. removeAt moves the last subscriber into
			// slot i, so i is not advanced.
			d.removeAt(i)
			s.terminateLocked(ErrSlowConsumer)
			d.slowDrops++
		}
	}
	return ev.Sequence, nil
}

// StopPublishing makes every later Publish fail with ErrClosed. Existing
// subscribers stay connected and can drain their queues.
func (d *Distributor) StopPublishing() {
	d.mu.Lock()
	d.publishing = false
	d.mu.Unlock()
}

// Close stops publishing and terminates every subscription with ErrShutdown.
// Events already queued remain readable. Close is idempotent.
func (d *Distributor) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.publishing = false
	if d.closed {
		return
	}
	d.closed = true
	for _, s := range d.subs {
		s.terminateLocked(ErrShutdown)
	}
	d.subs = nil
}

// Stats returns a snapshot of the distributor's counters.
func (d *Distributor) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return Stats{
		LastSequence:            d.seq,
		Delivered:               d.delivered,
		Subscribers:             len(d.subs),
		SlowConsumerDisconnects: d.slowDrops,
	}
}

// removeAt removes d.subs[i] in O(1) by swapping in the last element.
// Delivery order between subscribers is irrelevant, so this is safe.
func (d *Distributor) removeAt(i int) {
	last := len(d.subs) - 1
	d.subs[i] = d.subs[last]
	d.subs[last] = nil
	d.subs = d.subs[:last]
}

func (d *Distributor) remove(s *Subscription) {
	for i, cur := range d.subs {
		if cur == s {
			d.removeAt(i)
			return
		}
	}
}

// Subscription is one subscriber's view of the stream.
type Subscription struct {
	id   uint64
	d    *Distributor
	ch   chan domain.Event
	done chan struct{}

	// Guarded by d.mu. err is written before done is closed, so it may be read
	// without the lock once done is closed.
	terminated bool
	err        error
}

// ID returns the subscription's unique identifier.
func (s *Subscription) ID() uint64 { return s.id }

// Events returns the subscriber's queue. The channel is closed when the
// subscription terminates; events queued before termination can still be read.
func (s *Subscription) Events() <-chan domain.Event { return s.ch }

// Done is closed when the subscription terminates.
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Err returns nil while the subscription is active, and afterwards the reason
// it ended: ErrSlowConsumer, ErrUnsubscribed or ErrShutdown.
func (s *Subscription) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

// Close unsubscribes. It is idempotent and safe to call concurrently with
// Publish and with the distributor's Close.
func (s *Subscription) Close() {
	s.d.mu.Lock()
	defer s.d.mu.Unlock()
	if s.terminated {
		return
	}
	s.d.remove(s)
	s.terminateLocked(ErrUnsubscribed)
}

// terminateLocked must be called with d.mu held, and the caller must remove the
// subscription from d.subs before releasing the lock, so no send can ever hit
// the closed channel.
func (s *Subscription) terminateLocked(reason error) {
	s.terminated = true
	s.err = reason
	close(s.ch)
	close(s.done)
}
