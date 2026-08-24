package app_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
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
	"mdstream/internal/storage"
)

func validTrade() domain.Event {
	return domain.Event{Symbol: "AAPL", Type: domain.EventTypeTrade, Price: domain.FixedScale, Quantity: domain.FixedScale}
}

func testConfig(t *testing.T) app.Config {
	cfg := app.DefaultConfig()
	cfg.GRPCAddr = "127.0.0.1:0"
	cfg.Storage.DataDir = t.TempDir()
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
	a, err := app.New(testConfig(t), slog.New(slog.DiscardHandler))
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
	if got := a.EventLog().LastSequence(); got != last {
		t.Fatalf("event log ends at %d, but clients received up to %d", got, last)
	}
}

func TestRunWithoutProducer(t *testing.T) {
	cfg := testConfig(t)
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

// runAndStop starts an app on dir without the producer, publishes n events,
// shuts it down and returns the sequences assigned.
func runAndStop(t *testing.T, dir string, n int) []uint64 {
	t.Helper()
	cfg := testConfig(t)
	cfg.Producer.Enabled = false
	cfg.Storage.DataDir = dir
	a, err := app.New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	seqs := make([]uint64, n)
	for i := range seqs {
		seq, err := a.Distributor().Publish(validTrade())
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		seqs[i] = seq
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
	return seqs
}

func readLog(t *testing.T, dir string) []uint64 {
	t.Helper()
	l, err := storage.Open(dir, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer l.Close()
	var seqs []uint64
	if err := l.Scan(1, func(ev domain.Event) error {
		seqs = append(seqs, ev.Sequence)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return seqs
}

func seqRange(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

// TestRestartContinuesSequence: stop the server, start it again on the same
// data directory; the sequence continues where it left off and the log holds
// both runs.
func TestRestartContinuesSequence(t *testing.T) {
	dir := t.TempDir()
	if got := runAndStop(t, dir, 10); !slices.Equal(got, seqRange(1, 10)) {
		t.Fatalf("first run sequences = %v", got)
	}
	if got := runAndStop(t, dir, 5); !slices.Equal(got, seqRange(11, 15)) {
		t.Fatalf("second run sequences = %v, want 11..15", got)
	}
	if got := readLog(t, dir); !slices.Equal(got, seqRange(1, 15)) {
		t.Fatalf("log = %v, want 1..15", got)
	}
}

// TestRestartAfterTornWrite: the previous process died mid-write, leaving a
// partial record. The server truncates it and continues.
func TestRestartAfterTornWrite(t *testing.T) {
	dir := t.TempDir()
	runAndStop(t, dir, 10)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(dir, entries[len(entries)-1].Name())
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if got := runAndStop(t, dir, 3); !slices.Equal(got, seqRange(11, 13)) {
		t.Fatalf("sequences after recovery = %v, want 11..13", got)
	}
	if got := readLog(t, dir); !slices.Equal(got, seqRange(1, 13)) {
		t.Fatalf("log = %v, want 1..13", got)
	}
}
