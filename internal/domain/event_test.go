package domain

import (
	"errors"
	"math"
	"testing"
)

func TestFixedString(t *testing.T) {
	tests := []struct {
		in   Fixed
		want string
	}{
		{0, "0"},
		{FixedScale, "1"},
		{150_000_000, "1.5"},
		{1, "0.00000001"},
		{-1, "-0.00000001"},
		{-250_000_000, "-2.5"},
		{12_345_678_900, "123.456789"},
		{math.MinInt64, "-92233720368.54775808"},
	}
	for _, tt := range tests {
		if got := tt.in.String(); got != tt.want {
			t.Errorf("Fixed(%d).String() = %q, want %q", int64(tt.in), got, tt.want)
		}
	}
}

func TestFixedFromFloat(t *testing.T) {
	if got := FixedFromFloat(101.25); got != 10_125_000_000 {
		t.Fatalf("FixedFromFloat(101.25) = %d", got)
	}
	if got := FixedFromFloat(0.1).Float64(); got != 0.1 {
		t.Fatalf("round trip 0.1 = %v", got)
	}
}

func TestEventValidate(t *testing.T) {
	valid := []Event{
		{Symbol: "AAPL", Type: EventTypeTrade, Price: FixedScale, Quantity: FixedScale},
		{Symbol: "AAPL", Type: EventTypeQuote, Side: SideBid, Price: FixedScale, Quantity: 0},
		{Symbol: "AAPL", Type: EventTypeQuote, Side: SideAsk, Price: FixedScale, Quantity: FixedScale},
	}
	for i, e := range valid {
		if err := e.Validate(); err != nil {
			t.Errorf("valid[%d]: unexpected error %v", i, err)
		}
	}

	invalid := []Event{
		{Type: EventTypeTrade, Price: FixedScale, Quantity: FixedScale},                                // no symbol
		{Symbol: "AAPL", Type: EventTypeTrade, Price: 0, Quantity: FixedScale},                         // zero price
		{Symbol: "AAPL", Type: EventTypeTrade, Price: FixedScale, Quantity: 0},                         // zero trade qty
		{Symbol: "AAPL", Type: EventTypeTrade, Side: SideBid, Price: FixedScale, Quantity: FixedScale}, // trade with side
		{Symbol: "AAPL", Type: EventTypeQuote, Price: FixedScale, Quantity: FixedScale},                // quote without side
		{Symbol: "AAPL", Type: EventTypeQuote, Side: SideAsk, Price: FixedScale, Quantity: -1},         // negative qty
		{Symbol: "AAPL", Type: EventTypeUnknown, Price: FixedScale, Quantity: FixedScale},              // unknown type
	}
	for i, e := range invalid {
		if err := e.Validate(); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("invalid[%d]: got %v, want ErrInvalidEvent", i, err)
		}
	}
}
