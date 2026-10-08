package app_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mdstreamv1 "mdstream/api/gen/mdstream/v1"
	"mdstream/internal/app"
	"mdstream/internal/distribution"
	"mdstream/internal/domain"
)

func validTrade() domain.Event {
	return domain.Event{Symbol: "AAPL", Type: domain.EventTypeTrade, Price: domain.FixedScale, Quantity: domain.FixedScale}
}

func testConfig() app.Config {
	cfg := app.DefaultConfig()
	cfg.GRPCAddr = "127.0.0.1:0"
	cfg.Producer.Rate = 5_000
	cfg.ShutdownTimeout = 5 * time.Second
	return cfg
}

func TestConfigValidate(t *testing.T) {
	if err := app.DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	bad := app.DefaultConfig()
	bad.QueueSize = 0
	bad.Producer.Rate = -1
	if err := bad.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := app.New(bad, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("New accepted an invalid config")
	}
}

// TestGracefulShutdown starts the full server with the built-in producer,
// connects a client, then cancels the context (as SIGINT/SIGTERM would).
func TestGracefulShutdown(t *testing.T) {
	a, err := app.New(testConfig(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	conn, err := grpc.NewClient(a.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	clientCtx, clientCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer clientCancel()
	stream, err := mdstreamv1.NewMarketDataClient(conn).Subscribe(clientCtx, &mdstreamv1.SubscribeRequest{ClientId: "test"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Receive some live events from the producer; sequences must be contiguous.
	var last uint64
	for range 50 {
		m, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if last != 0 && m.GetSequence() != last+1 {
			t.Fatalf("gap: %d -> %d", last, m.GetSequence())
		}
		last = m.GetSequence()
	}

	cancel() // simulate SIGTERM

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown was requested")
	}

	// The client drains the remaining events and then sees UNAVAILABLE.
	for {
		m, err := stream.Recv()
		if err != nil {
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("stream ended with %v, want Unavailable", err)
			}
			break
		}
		if m.GetSequence() != last+1 {
			t.Fatalf("gap during shutdown: %d -> %d", last, m.GetSequence())
		}
		last = m.GetSequence()
	}

	if got := a.Distributor().Stats().LastSequence; got != last {
		t.Fatalf("client received up to %d, but %d events were published", last, got)
	}
	if _, err := a.Distributor().Publish(validTrade()); !errors.Is(err, distribution.ErrClosed) {
		t.Fatalf("Publish after shutdown = %v, want ErrClosed", err)
	}
}

func TestRunWithoutProducer(t *testing.T) {
	cfg := testConfig()
	cfg.Producer.Enabled = false
	a, err := app.New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	if _, err := a.Distributor().Publish(validTrade()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}
