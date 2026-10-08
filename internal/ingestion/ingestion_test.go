package ingestion_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mdstream/internal/distribution"
	"mdstream/internal/domain"
	"mdstream/internal/ingestion"
)

func TestGeneratorIsDeterministicAndValid(t *testing.T) {
	a := ingestion.NewGenerator(nil, 7)
	b := ingestion.NewGenerator(nil, 7)
	var trades, quotes int
	for i := range 10_000 {
		ea, eb := a.Next(), b.Next()
		if ea != eb {
			t.Fatalf("event %d differs for the same seed: %+v vs %+v", i, ea, eb)
		}
		if err := ea.Validate(); err != nil {
			t.Fatalf("event %d invalid: %v (%+v)", i, err, ea)
		}
		switch ea.Type {
		case domain.EventTypeTrade:
			trades++
		case domain.EventTypeQuote:
			quotes++
		}
	}
	if trades == 0 || quotes == 0 {
		t.Fatalf("expected a mix of trades (%d) and quotes (%d)", trades, quotes)
	}
}

func TestGeneratorUsesConfiguredSymbols(t *testing.T) {
	g := ingestion.NewGenerator([]string{"X", "Y"}, 1)
	for range 1000 {
		if s := g.Next().Symbol; s != "X" && s != "Y" {
			t.Fatalf("unexpected symbol %q", s)
		}
	}
}

// countingPublisher counts events and can cancel a context after a limit.
type countingPublisher struct {
	n      atomic.Uint64
	limit  uint64
	cancel context.CancelFunc
	err    error
}

func (p *countingPublisher) Publish(domain.Event) (uint64, error) {
	if p.err != nil {
		return 0, p.err
	}
	n := p.n.Add(1)
	if p.limit > 0 && n == p.limit && p.cancel != nil {
		p.cancel()
	}
	return n, nil
}

func runWithTimeout(t *testing.T, p *ingestion.Producer, ctx context.Context) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx) }()
	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not stop")
		return nil
	}
}

func TestProducerUnpacedStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub := &countingPublisher{limit: 10_000, cancel: cancel}
	p := ingestion.NewProducer(ingestion.ProducerConfig{Rate: 0}, pub)

	if err := runWithTimeout(t, p, ctx); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if p.Sent() < 10_000 {
		t.Fatalf("Sent = %d, want >= 10000", p.Sent())
	}
}

func TestProducerPacedRate(t *testing.T) {
	const rate, duration = 2_000, 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	pub := &countingPublisher{}
	p := ingestion.NewProducer(ingestion.ProducerConfig{Rate: rate}, pub)

	if err := runWithTimeout(t, p, ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	// Expect ~1000 events; bounds are loose to tolerate slow CI machines.
	want := float64(rate) * duration.Seconds()
	if got := float64(p.Sent()); got < want*0.5 || got > want*1.5 {
		t.Fatalf("Sent = %v, want about %v", got, want)
	}
}

func TestProducerStopsWhenPublisherCloses(t *testing.T) {
	d := distribution.New(distribution.Config{})
	p := ingestion.NewProducer(ingestion.ProducerConfig{Rate: 0}, d)

	var wg sync.WaitGroup
	var runErr error
	wg.Go(func() { runErr = p.Run(context.Background()) })

	time.Sleep(10 * time.Millisecond)
	d.StopPublishing()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not stop after the distributor closed")
	}
	if runErr != nil {
		t.Fatalf("Run = %v, want nil", runErr)
	}
	if got, want := p.Sent(), d.Stats().LastSequence; got != want {
		t.Fatalf("Sent = %d, distributor LastSequence = %d", got, want)
	}
}

func TestProducerReturnsUnexpectedErrors(t *testing.T) {
	boom := errors.New("boom")
	p := ingestion.NewProducer(ingestion.ProducerConfig{Rate: 0}, &countingPublisher{err: boom})
	if err := runWithTimeout(t, p, context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want wrapped boom", err)
	}
}
