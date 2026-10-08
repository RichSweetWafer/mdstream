package distribution_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"mdstream/internal/distribution"
	"mdstream/internal/domain"
)

const waitTimeout = 5 * time.Second

func trade(symbol string) domain.Event {
	return domain.Event{
		Symbol:   symbol,
		Type:     domain.EventTypeTrade,
		Price:    100 * domain.FixedScale,
		Quantity: 10 * domain.FixedScale,
	}
}

func mustSubscribe(t *testing.T, d *distribution.Distributor) *distribution.Subscription {
	t.Helper()
	s, err := d.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return s
}

func mustPublish(t *testing.T, d *distribution.Distributor, ev domain.Event) uint64 {
	t.Helper()
	seq, err := d.Publish(ev)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return seq
}

// recv reads one event, failing if the subscription closes or nothing arrives in time.
func recv(t *testing.T, s *distribution.Subscription) domain.Event {
	t.Helper()
	select {
	case ev, ok := <-s.Events():
		if !ok {
			t.Fatalf("subscription %d closed unexpectedly: %v", s.ID(), s.Err())
		}
		return ev
	case <-time.After(waitTimeout):
		t.Fatalf("subscription %d: timed out waiting for event", s.ID())
	}
	return domain.Event{}
}

// drainUntilClosed reads the remaining queued events, expects the channel to be
// closed with reason want, and returns the drained events.
func drainUntilClosed(t *testing.T, s *distribution.Subscription, want error) []domain.Event {
	t.Helper()
	var got []domain.Event
	deadline := time.After(waitTimeout)
	for {
		select {
		case ev, ok := <-s.Events():
			if !ok {
				if !errors.Is(s.Err(), want) {
					t.Fatalf("subscription %d ended with %v, want %v", s.ID(), s.Err(), want)
				}
				return got
			}
			got = append(got, ev)
		case <-deadline:
			t.Fatalf("subscription %d: not closed within %v", s.ID(), waitTimeout)
		}
	}
}

func assertNoEvent(t *testing.T, s *distribution.Subscription) {
	t.Helper()
	select {
	case ev, ok := <-s.Events():
		if ok {
			t.Fatalf("unexpected event %v", ev)
		}
		t.Fatalf("subscription closed unexpectedly: %v", s.Err())
	default:
	}
}

func TestPublishSubscribe(t *testing.T) {
	fixed := time.Unix(1_700_000_000, 0)
	d := distribution.New(distribution.Config{QueueSize: 8, Now: func() time.Time { return fixed }})
	s := mustSubscribe(t, d)

	if seq := mustPublish(t, d, trade("AAPL")); seq != 1 {
		t.Fatalf("first sequence = %d, want 1", seq)
	}

	ev := recv(t, s)
	if ev.Sequence != 1 || ev.Symbol != "AAPL" || ev.Type != domain.EventTypeTrade {
		t.Fatalf("unexpected event %+v", ev)
	}
	if ev.Timestamp != fixed.UnixNano() {
		t.Fatalf("timestamp = %d, want %d", ev.Timestamp, fixed.UnixNano())
	}
	if s.Err() != nil {
		t.Fatalf("active subscription has Err %v", s.Err())
	}
}

func TestPublishKeepsExplicitTimestamp(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 1})
	s := mustSubscribe(t, d)
	ev := trade("AAPL")
	ev.Timestamp = 42
	mustPublish(t, d, ev)
	if got := recv(t, s).Timestamp; got != 42 {
		t.Fatalf("timestamp = %d, want 42", got)
	}
}

func TestPublishRejectsInvalidEvent(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 8})
	s := mustSubscribe(t, d)

	if _, err := d.Publish(domain.Event{}); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatalf("Publish(invalid) = %v, want ErrInvalidEvent", err)
	}
	assertNoEvent(t, s)

	// An invalid event must not consume a sequence number.
	if seq := mustPublish(t, d, trade("AAPL")); seq != 1 {
		t.Fatalf("sequence after rejected event = %d, want 1", seq)
	}
}

func TestPublishWithoutSubscribers(t *testing.T) {
	d := distribution.New(distribution.Config{})
	for i := 1; i <= 10; i++ {
		if seq := mustPublish(t, d, trade("AAPL")); seq != uint64(i) {
			t.Fatalf("sequence = %d, want %d", seq, i)
		}
	}
}

func TestSubscriberOnlySeesEventsAfterSubscribing(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 8})
	mustPublish(t, d, trade("AAPL"))
	s := mustSubscribe(t, d)
	mustPublish(t, d, trade("AAPL"))
	if ev := recv(t, s); ev.Sequence != 2 {
		t.Fatalf("first event seen = %d, want 2", ev.Sequence)
	}
}

func TestMultipleSubscribersReceiveSameStream(t *testing.T) {
	const subscribers, events = 5, 100
	d := distribution.New(distribution.Config{QueueSize: events})

	subs := make([]*distribution.Subscription, subscribers)
	for i := range subs {
		subs[i] = mustSubscribe(t, d)
	}
	for i := range events {
		mustPublish(t, d, trade(fmt.Sprintf("S%d", i%7)))
	}

	var reference []domain.Event
	for i, s := range subs {
		got := make([]domain.Event, events)
		for j := range got {
			got[j] = recv(t, s)
		}
		for j, ev := range got {
			if ev.Sequence != uint64(j+1) {
				t.Fatalf("subscriber %d: event %d has sequence %d", i, j, ev.Sequence)
			}
		}
		if reference == nil {
			reference = got
			continue
		}
		for j := range got {
			if got[j] != reference[j] {
				t.Fatalf("subscriber %d: event %d = %+v, want %+v", i, j, got[j], reference[j])
			}
		}
	}
}

func TestConcurrentPublishing(t *testing.T) {
	const publishers, perPublisher = 8, 1000
	d := distribution.New(distribution.Config{QueueSize: publishers * perPublisher})
	s := mustSubscribe(t, d)

	var wg sync.WaitGroup
	for p := range publishers {
		wg.Go(func() {
			ev := trade(fmt.Sprintf("P%d", p))
			for range perPublisher {
				if _, err := d.Publish(ev); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()

	perSymbol := make(map[string]int)
	for i := 1; i <= publishers*perPublisher; i++ {
		ev := recv(t, s)
		if ev.Sequence != uint64(i) {
			t.Fatalf("event %d has sequence %d: stream not totally ordered", i, ev.Sequence)
		}
		perSymbol[ev.Symbol]++
	}
	for p := range publishers {
		if n := perSymbol[fmt.Sprintf("P%d", p)]; n != perPublisher {
			t.Fatalf("publisher %d: received %d events, want %d", p, n, perPublisher)
		}
	}
	if st := d.Stats(); st.LastSequence != publishers*perPublisher {
		t.Fatalf("LastSequence = %d", st.LastSequence)
	}
}

// TestConcurrentSubscribeUnsubscribe churns subscriptions while publishing.
// Run with -race. Every subscriber must see strictly increasing sequences.
func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 1 << 16})
	stop := make(chan struct{})

	var pubWG sync.WaitGroup
	for range 4 {
		pubWG.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := d.Publish(trade("AAPL")); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		})
	}

	var subWG sync.WaitGroup
	for range 16 {
		subWG.Go(func() {
			for range 20 {
				s, err := d.Subscribe()
				if err != nil {
					t.Errorf("Subscribe: %v", err)
					return
				}
				var last uint64
				for range 50 {
					ev, ok := <-s.Events()
					if !ok {
						break
					}
					if ev.Sequence <= last {
						t.Errorf("sequence went from %d to %d", last, ev.Sequence)
					}
					last = ev.Sequence
				}
				s.Close()
			}
		})
	}

	subWG.Wait()
	close(stop)
	pubWG.Wait()
	if n := d.Stats().Subscribers; n != 0 {
		t.Fatalf("Subscribers = %d after all unsubscribed, want 0", n)
	}
}

func TestUnsubscribe(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 8})
	gone := mustSubscribe(t, d)
	stay := mustSubscribe(t, d)

	gone.Close()
	gone.Close() // idempotent

	select {
	case <-gone.Done():
	default:
		t.Fatal("Done not closed after Close")
	}
	drainUntilClosed(t, gone, distribution.ErrUnsubscribed)

	mustPublish(t, d, trade("AAPL"))
	if ev := recv(t, stay); ev.Sequence != 1 {
		t.Fatalf("remaining subscriber got sequence %d", ev.Sequence)
	}
	if n := d.Stats().Subscribers; n != 1 {
		t.Fatalf("Subscribers = %d, want 1", n)
	}
}

func TestSlowConsumerIsDisconnected(t *testing.T) {
	const queueSize, events = 4, 20
	d := distribution.New(distribution.Config{QueueSize: queueSize})
	slow := mustSubscribe(t, d) // never reads
	fast := mustSubscribe(t, d)

	for i := 1; i <= events; i++ {
		mustPublish(t, d, trade("AAPL"))
		if ev := recv(t, fast); ev.Sequence != uint64(i) {
			t.Fatalf("fast subscriber got sequence %d, want %d", ev.Sequence, i)
		}
	}

	// The slow subscriber keeps exactly what fit in its queue, then is closed.
	got := drainUntilClosed(t, slow, distribution.ErrSlowConsumer)
	if len(got) != queueSize {
		t.Fatalf("slow subscriber drained %d events, want %d", len(got), queueSize)
	}
	for i, ev := range got {
		if ev.Sequence != uint64(i+1) {
			t.Fatalf("slow subscriber event %d has sequence %d", i, ev.Sequence)
		}
	}

	if fast.Err() != nil {
		t.Fatalf("fast subscriber terminated: %v", fast.Err())
	}
	st := d.Stats()
	if st.SlowConsumerDisconnects != 1 || st.Subscribers != 1 {
		t.Fatalf("stats = %+v, want 1 slow disconnect and 1 subscriber", st)
	}
}

func TestQueueOverflowBoundary(t *testing.T) {
	const queueSize = 8
	d := distribution.New(distribution.Config{QueueSize: queueSize})
	s := mustSubscribe(t, d)

	for range queueSize {
		mustPublish(t, d, trade("AAPL"))
	}
	if s.Err() != nil {
		t.Fatalf("subscriber with a full (not overflowed) queue was terminated: %v", s.Err())
	}

	mustPublish(t, d, trade("AAPL")) // one more than fits
	if !errors.Is(s.Err(), distribution.ErrSlowConsumer) {
		t.Fatalf("Err after overflow = %v, want ErrSlowConsumer", s.Err())
	}
	if got := drainUntilClosed(t, s, distribution.ErrSlowConsumer); len(got) != queueSize {
		t.Fatalf("drained %d events, want %d (memory must be bounded by the queue)", len(got), queueSize)
	}
}

func TestPublisherNeverBlocksOnStalledSubscribers(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 16})
	subs := make([]*distribution.Subscription, 10)
	for i := range subs {
		subs[i] = mustSubscribe(t, d) // none of them ever read
	}

	done := make(chan error, 1)
	go func() {
		for range 100_000 {
			if _, err := d.Publish(trade("AAPL")); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("publisher blocked by stalled subscribers")
	}

	for _, s := range subs {
		if !errors.Is(s.Err(), distribution.ErrSlowConsumer) {
			t.Fatalf("stalled subscriber %d: Err = %v, want ErrSlowConsumer", s.ID(), s.Err())
		}
	}
}

func TestStopPublishing(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 8})
	s := mustSubscribe(t, d)
	mustPublish(t, d, trade("AAPL"))

	d.StopPublishing()
	if _, err := d.Publish(trade("AAPL")); !errors.Is(err, distribution.ErrClosed) {
		t.Fatalf("Publish after StopPublishing = %v, want ErrClosed", err)
	}

	// Existing subscribers stay connected and keep their queued events.
	if ev := recv(t, s); ev.Sequence != 1 {
		t.Fatalf("got sequence %d", ev.Sequence)
	}
	if s.Err() != nil {
		t.Fatalf("subscriber terminated by StopPublishing: %v", s.Err())
	}
}

func TestCloseTerminatesSubscribers(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 8})
	a := mustSubscribe(t, d)
	b := mustSubscribe(t, d)
	for range 3 {
		mustPublish(t, d, trade("AAPL"))
	}

	d.Close()
	d.Close() // idempotent

	// Events queued before shutdown are still delivered.
	for _, s := range []*distribution.Subscription{a, b} {
		if got := drainUntilClosed(t, s, distribution.ErrShutdown); len(got) != 3 {
			t.Fatalf("subscriber %d drained %d events, want 3", s.ID(), len(got))
		}
		s.Close() // must be a no-op after shutdown
	}

	if _, err := d.Publish(trade("AAPL")); !errors.Is(err, distribution.ErrClosed) {
		t.Fatalf("Publish after Close = %v, want ErrClosed", err)
	}
	if _, err := d.Subscribe(); !errors.Is(err, distribution.ErrClosed) {
		t.Fatalf("Subscribe after Close = %v, want ErrClosed", err)
	}
	if n := d.Stats().Subscribers; n != 0 {
		t.Fatalf("Subscribers = %d after Close", n)
	}
}

func TestCloseWhilePublishing(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 1 << 10})
	s := mustSubscribe(t, d)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				_, err := d.Publish(trade("AAPL"))
				if errors.Is(err, distribution.ErrClosed) {
					return
				}
				if err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range s.Events() {
		}
	})

	time.Sleep(10 * time.Millisecond)
	d.Close()

	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(waitTimeout):
		t.Fatal("publishers or reader did not stop after Close")
	}
}
