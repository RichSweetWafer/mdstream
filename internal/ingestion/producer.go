package ingestion

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"mdstream/internal/distribution"
	"mdstream/internal/domain"
)

// Publisher accepts events. *distribution.Distributor implements it.
type Publisher interface {
	Publish(domain.Event) (uint64, error)
}

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	// Rate is the target number of events per second. Zero or negative means
	// publish as fast as possible.
	Rate int

	// Symbols to generate events for; DefaultSymbols if empty.
	Symbols []string

	// Seed for the deterministic generator.
	Seed uint64

	// Tick is the pacing granularity for rate-limited production.
	// Defaults to 1ms.
	Tick time.Duration
}

// Producer is a synthetic market-data source that publishes generated events
// at a configurable rate.
type Producer struct {
	cfg  ProducerConfig
	pub  Publisher
	gen  *Generator
	sent atomic.Uint64
}

// NewProducer returns a Producer that publishes to pub.
func NewProducer(cfg ProducerConfig, pub Publisher) *Producer {
	if cfg.Tick <= 0 {
		cfg.Tick = time.Millisecond
	}
	return &Producer{
		cfg: cfg,
		pub: pub,
		gen: NewGenerator(cfg.Symbols, cfg.Seed),
	}
}

// Sent returns the number of events published successfully so far.
func (p *Producer) Sent() uint64 { return p.sent.Load() }

// ctxCheckInterval is how many events are published between context checks.
const ctxCheckInterval = 256

// Run publishes events until ctx is cancelled or the publisher stops accepting
// events (distribution.ErrClosed); both are a normal stop and return nil.
// Any other publish error stops the producer and is returned.
func (p *Producer) Run(ctx context.Context) error {
	if p.cfg.Rate <= 0 {
		return p.runUnpaced(ctx)
	}
	return p.runPaced(ctx)
}

func (p *Producer) runUnpaced(ctx context.Context) error {
	for {
		for range ctxCheckInterval {
			if stop, err := p.publishOne(); stop {
				return err
			}
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// runPaced publishes in small bursts: on every tick it emits however many
// events are due according to elapsed time × rate. This reaches high rates
// (1M/s and more) without needing a timer per event, and it does not drift.
//
// If the producer falls behind (e.g. a GC pause), at most 100ms worth of
// backlog is caught up; the rest is skipped rather than released as one burst.
func (p *Producer) runPaced(ctx context.Context) error {
	rate := float64(p.cfg.Rate)
	maxBurst := uint64(max(p.cfg.Rate/10, 1))

	ticker := time.NewTicker(p.cfg.Tick)
	defer ticker.Stop()

	start := time.Now()
	var emitted uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		due := uint64(time.Since(start).Seconds() * rate)
		if due > emitted+maxBurst {
			emitted = due - maxBurst
		}
		for emitted < due {
			if stop, err := p.publishOne(); stop {
				return err
			}
			emitted++
			if emitted%ctxCheckInterval == 0 && ctx.Err() != nil {
				return nil
			}
		}
	}
}

// publishOne publishes the next generated event. stop is true when the
// producer must stop; err is non-nil only for unexpected failures.
func (p *Producer) publishOne() (stop bool, err error) {
	_, err = p.pub.Publish(p.gen.Next())
	switch {
	case err == nil:
		p.sent.Add(1)
		return false, nil
	case errors.Is(err, distribution.ErrClosed):
		return true, nil
	default:
		return true, fmt.Errorf("ingestion: publish: %w", err)
	}
}
