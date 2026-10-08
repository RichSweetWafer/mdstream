package grpcapi_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	mdstreamv1 "mdstream/api/gen/mdstream/v1"
	"mdstream/internal/distribution"
	"mdstream/internal/domain"
	"mdstream/internal/grpcapi"
)

func startServer(t *testing.T, d *distribution.Distributor) mdstreamv1.MarketDataClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	grpcapi.Register(gs, grpcapi.NewServer(d, slog.New(slog.DiscardHandler)))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return mdstreamv1.NewMarketDataClient(conn)
}

func subscribe(t *testing.T, ctx context.Context, c mdstreamv1.MarketDataClient, id string) mdstreamv1.MarketData_SubscribeClient {
	t.Helper()
	stream, err := c.Subscribe(ctx, &mdstreamv1.SubscribeRequest{ClientId: id})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return stream
}

// waitSubscribers waits until the server has registered n subscriptions, so
// that events published afterwards are guaranteed to reach them.
func waitSubscribers(t *testing.T, d *distribution.Distributor, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for d.Stats().Subscribers != n {
		if time.Now().After(deadline) {
			t.Fatalf("subscribers = %d, want %d", d.Stats().Subscribers, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func quote(symbol string, side domain.Side) domain.Event {
	return domain.Event{
		Symbol:   symbol,
		Type:     domain.EventTypeQuote,
		Side:     side,
		Price:    domain.FixedFromFloat(101.25),
		Quantity: domain.FixedFromFloat(3.5),
	}
}

func TestEnumValuesMatchDomain(t *testing.T) {
	pairs := []struct{ proto, domain int32 }{
		{int32(mdstreamv1.EventType_EVENT_TYPE_UNSPECIFIED), int32(domain.EventTypeUnknown)},
		{int32(mdstreamv1.EventType_EVENT_TYPE_TRADE), int32(domain.EventTypeTrade)},
		{int32(mdstreamv1.EventType_EVENT_TYPE_QUOTE), int32(domain.EventTypeQuote)},
		{int32(mdstreamv1.Side_SIDE_UNSPECIFIED), int32(domain.SideNone)},
		{int32(mdstreamv1.Side_SIDE_BID), int32(domain.SideBid)},
		{int32(mdstreamv1.Side_SIDE_ASK), int32(domain.SideAsk)},
	}
	for _, p := range pairs {
		if p.proto != p.domain {
			t.Errorf("proto enum %d != domain enum %d", p.proto, p.domain)
		}
	}
}

func TestConvertRoundTrip(t *testing.T) {
	in := quote("AAPL", domain.SideAsk)
	in.Sequence, in.Timestamp = 7, 123456789
	var m mdstreamv1.Event
	grpcapi.ToProto(in, &m)
	if out := grpcapi.FromProto(&m); out != in {
		t.Fatalf("round trip: got %+v, want %+v", out, in)
	}
}

func TestSubscribeStreamsSameEventsToAllClients(t *testing.T) {
	const clients, events = 3, 50
	d := distribution.New(distribution.Config{QueueSize: events})
	c := startServer(t, d)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	streams := make([]mdstreamv1.MarketData_SubscribeClient, clients)
	for i := range streams {
		streams[i] = subscribe(t, ctx, c, "client")
	}
	waitSubscribers(t, d, clients)

	for i := range events {
		side := domain.SideBid
		if i%2 == 1 {
			side = domain.SideAsk
		}
		if _, err := d.Publish(quote("MSFT", side)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	for ci, s := range streams {
		for i := 1; i <= events; i++ {
			m, err := s.Recv()
			if err != nil {
				t.Fatalf("client %d: Recv: %v", ci, err)
			}
			ev := grpcapi.FromProto(m)
			if ev.Sequence != uint64(i) || ev.Symbol != "MSFT" || ev.Type != domain.EventTypeQuote {
				t.Fatalf("client %d: unexpected event %+v", ci, ev)
			}
			if ev.Price != domain.FixedFromFloat(101.25) || ev.Quantity != domain.FixedFromFloat(3.5) {
				t.Fatalf("client %d: price/qty mismatch %+v", ci, ev)
			}
		}
	}
}

func TestClientCancelUnsubscribes(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 8})
	c := startServer(t, d)

	ctx, cancel := context.WithCancel(context.Background())
	subscribe(t, ctx, c, "leaver")
	waitSubscribers(t, d, 1)

	cancel()
	waitSubscribers(t, d, 0)
}

func TestShutdownEndsStreamWithUnavailable(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 8})
	c := startServer(t, d)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := subscribe(t, ctx, c, "client")
	waitSubscribers(t, d, 1)
	for range 3 {
		if _, err := d.Publish(quote("AAPL", domain.SideBid)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	d.Close()

	// Events published before shutdown are still delivered, then the stream ends.
	for i := 1; i <= 3; i++ {
		m, err := s.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if m.GetSequence() != uint64(i) {
			t.Fatalf("sequence = %d, want %d", m.GetSequence(), i)
		}
	}
	if _, err := s.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv after shutdown = %v, want Unavailable", err)
	}
}

func TestSubscribeAfterShutdownIsUnavailable(t *testing.T) {
	d := distribution.New(distribution.Config{})
	c := startServer(t, d)
	d.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := subscribe(t, ctx, c, "late")
	if _, err := s.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv = %v, want Unavailable", err)
	}
}

// TestSlowClientIsDisconnected: a client that never reads eventually fills the
// HTTP/2 flow-control windows, then its server-side queue, and is cut off with
// RESOURCE_EXHAUSTED while publishing continues unblocked.
func TestSlowClientIsDisconnected(t *testing.T) {
	d := distribution.New(distribution.Config{QueueSize: 16})
	c := startServer(t, d)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s := subscribe(t, ctx, c, "sleepy")
	waitSubscribers(t, d, 1)

	const maxEvents = 5_000_000
	for i := 0; d.Stats().SlowConsumerDisconnects == 0; i++ {
		if i == maxEvents {
			t.Fatalf("client not disconnected after %d events", maxEvents)
		}
		if _, err := d.Publish(quote("AAPL", domain.SideBid)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// The client now drains what was already in flight and then sees the error.
	for {
		_, err := s.Recv()
		if err == nil {
			continue
		}
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("stream ended with %v, want ResourceExhausted", err)
		}
		return
	}
}
