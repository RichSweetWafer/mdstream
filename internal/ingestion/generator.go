// Package ingestion produces market-data events and feeds them to a publisher.
package ingestion

import (
	"math/rand/v2"
	"slices"

	"mdstream/internal/domain"
)

// DefaultSymbols is used when no symbols are configured.
var DefaultSymbols = []string{"AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA", "JPM"}

// Generator produces a deterministic pseudo-random stream of trades and
// quotes: each symbol's mid price follows a random walk in 1-basis-point steps,
// roughly 25% of events are trades and the rest are top-of-book quotes.
//
// The same symbols and seed always yield the same sequence of events.
// A Generator is not safe for concurrent use.
type Generator struct {
	rng     *rand.Rand
	symbols []string
	mids    []domain.Fixed
}

// NewGenerator returns a Generator for symbols (DefaultSymbols if empty).
func NewGenerator(symbols []string, seed uint64) *Generator {
	if len(symbols) == 0 {
		symbols = DefaultSymbols
	}
	g := &Generator{
		rng:     rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		symbols: slices.Clone(symbols),
		mids:    make([]domain.Fixed, len(symbols)),
	}
	for i := range g.mids {
		g.mids[i] = domain.Fixed(50+g.rng.IntN(450)) * domain.FixedScale // 50..499
	}
	return g
}

// Next returns the next event. Sequence and Timestamp are left zero; they are
// assigned when the event is published.
func (g *Generator) Next() domain.Event {
	i := g.rng.IntN(len(g.symbols))

	mid := g.mids[i]
	tick := max(mid/10_000, 1) // 1 bp
	mid += tick * domain.Fixed(g.rng.IntN(3)-1)
	mid = max(mid, domain.FixedScale) // keep prices at or above 1.0
	g.mids[i] = mid

	ev := domain.Event{Symbol: g.symbols[i]}
	if g.rng.IntN(4) == 0 {
		ev.Type = domain.EventTypeTrade
		ev.Price = mid
		ev.Quantity = domain.Fixed(1+g.rng.IntN(500)) * domain.FixedScale
		return ev
	}

	ev.Type = domain.EventTypeQuote
	if g.rng.IntN(2) == 0 {
		ev.Side = domain.SideBid
		ev.Price = mid - tick
	} else {
		ev.Side = domain.SideAsk
		ev.Price = mid + tick
	}
	ev.Quantity = domain.Fixed(1+g.rng.IntN(5_000)) * domain.FixedScale
	return ev
}
