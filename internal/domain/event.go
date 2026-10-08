// Package domain defines the market-data types shared by every mdstream component.
package domain

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// EventType identifies the kind of market-data event.
type EventType uint8

const (
	EventTypeUnknown EventType = iota
	EventTypeTrade
	EventTypeQuote
)

func (t EventType) String() string {
	switch t {
	case EventTypeTrade:
		return "TRADE"
	case EventTypeQuote:
		return "QUOTE"
	default:
		return "UNKNOWN"
	}
}

// Side is the book side of a quote. Trades carry SideNone.
type Side uint8

const (
	SideNone Side = iota
	SideBid
	SideAsk
)

func (s Side) String() string {
	switch s {
	case SideBid:
		return "BID"
	case SideAsk:
		return "ASK"
	default:
		return "-"
	}
}

// FixedScale is the number of Fixed units in 1.0 (8 fractional digits).
const FixedScale = 100_000_000

// Fixed is a fixed-point decimal with 8 fractional digits.
//
// Prices and quantities use fixed-point rather than float64 so that values are
// exact, comparable and cheap to copy and serialize.
type Fixed int64

// FixedFromFloat converts f to the nearest Fixed value.
func FixedFromFloat(f float64) Fixed {
	return Fixed(math.Round(f * FixedScale))
}

// Float64 returns f as a float64. It may lose precision for very large values.
func (f Fixed) Float64() float64 {
	return float64(f) / FixedScale
}

// String formats f exactly, without trailing zeros: 1.5, 100, -0.00000001.
func (f Fixed) String() string {
	u := uint64(f)
	neg := f < 0
	if neg {
		u = -u // two's-complement negation; correct for math.MinInt64 too
	}
	s := strconv.FormatUint(u/FixedScale, 10)
	if frac := u % FixedScale; frac != 0 {
		fs := strconv.FormatUint(frac, 10)
		fs = strings.Repeat("0", 8-len(fs)) + fs
		s += "." + strings.TrimRight(fs, "0")
	}
	if neg {
		s = "-" + s
	}
	return s
}

// Event is a single market-data event.
//
// Sequence is assigned by the distributor when the event is published and is
// strictly increasing across the whole stream, starting at 1.
type Event struct {
	Sequence  uint64
	Timestamp int64 // Unix nanoseconds; set at publish time if zero
	Symbol    string
	Price     Fixed
	Quantity  Fixed
	Type      EventType
	Side      Side
}

// ErrInvalidEvent is returned (wrapped) by Validate.
var ErrInvalidEvent = errors.New("invalid event")

// Validate checks the event's business fields. Sequence and Timestamp are not
// checked because they are assigned during publishing.
func (e *Event) Validate() error {
	if e.Symbol == "" {
		return fmt.Errorf("%w: empty symbol", ErrInvalidEvent)
	}
	if e.Price <= 0 {
		return fmt.Errorf("%w: non-positive price %s", ErrInvalidEvent, e.Price)
	}
	switch e.Type {
	case EventTypeTrade:
		if e.Side != SideNone {
			return fmt.Errorf("%w: trade must not have a side", ErrInvalidEvent)
		}
		if e.Quantity <= 0 {
			return fmt.Errorf("%w: non-positive trade quantity %s", ErrInvalidEvent, e.Quantity)
		}
	case EventTypeQuote:
		if e.Side != SideBid && e.Side != SideAsk {
			return fmt.Errorf("%w: quote side must be BID or ASK", ErrInvalidEvent)
		}
		if e.Quantity < 0 {
			return fmt.Errorf("%w: negative quote quantity %s", ErrInvalidEvent, e.Quantity)
		}
	default:
		return fmt.Errorf("%w: unknown type %d", ErrInvalidEvent, e.Type)
	}
	return nil
}

// String renders the event for logs and the example client.
func (e Event) String() string {
	ts := time.Unix(0, e.Timestamp).UTC().Format("15:04:05.000000")
	if e.Type == EventTypeQuote {
		return fmt.Sprintf("#%d %s %s %-6s %s %s@%s", e.Sequence, ts, e.Type, e.Symbol, e.Side, e.Quantity, e.Price)
	}
	return fmt.Sprintf("#%d %s %s %-6s %s@%s", e.Sequence, ts, e.Type, e.Symbol, e.Quantity, e.Price)
}
