// Package grpcapi exposes the distributor over gRPC.
package grpcapi

import (
	mdstreamv1 "mdstream/api/gen/mdstream/v1"
	"mdstream/internal/domain"
)

// The protobuf enums use the same numeric values as the domain enums, so the
// conversions below are plain casts (checked in convert_test.go).

// ToProto copies ev into dst. dst is reused by the server to avoid one
// allocation per sent event.
func ToProto(ev domain.Event, dst *mdstreamv1.Event) {
	dst.Sequence = ev.Sequence
	dst.TimestampUnixNano = ev.Timestamp
	dst.Symbol = ev.Symbol
	dst.Type = mdstreamv1.EventType(ev.Type)
	dst.Side = mdstreamv1.Side(ev.Side)
	dst.Price = int64(ev.Price)
	dst.Quantity = int64(ev.Quantity)
}

// FromProto converts a received protobuf event to the domain model.
func FromProto(m *mdstreamv1.Event) domain.Event {
	return domain.Event{
		Sequence:  m.GetSequence(),
		Timestamp: m.GetTimestampUnixNano(),
		Symbol:    m.GetSymbol(),
		Type:      domain.EventType(m.GetType()),
		Side:      domain.Side(m.GetSide()),
		Price:     domain.Fixed(m.GetPrice()),
		Quantity:  domain.Fixed(m.GetQuantity()),
	}
}
