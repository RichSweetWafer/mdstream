// Command mdstream runs the market-data distribution server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"mdstream/internal/app"
	"mdstream/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mdstream:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := app.DefaultConfig()

	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.IntVar(&cfg.QueueSize, "queue-size", cfg.QueueSize, "per-subscriber queue capacity (events)")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "max time to wait for graceful gRPC shutdown")
	flag.BoolVar(&cfg.Producer.Enabled, "producer", cfg.Producer.Enabled, "run the built-in synthetic producer")
	flag.IntVar(&cfg.Producer.Rate, "rate", cfg.Producer.Rate, "producer rate in events/sec (0 = unlimited)")
	flag.Uint64Var(&cfg.Producer.Seed, "seed", cfg.Producer.Seed, "producer random seed")

	flag.StringVar(&cfg.Storage.DataDir, "data-dir", cfg.Storage.DataDir, "event log directory (empty = no persistence)")
	flag.Func("segment-size", "event log segment size, e.g. 256MB, 64KB or bytes (default 256MB)", func(s string) error {
		n, err := parseSize(s)
		cfg.Storage.SegmentSize = n
		return err
	})
	flag.Func("durability", "event log durability: write (write-through + periodic fsync), fsync (every event) or buffered (default write)", func(s string) error {
		d, err := storage.ParseDurability(s)
		cfg.Storage.Durability = d
		return err
	})
	flag.DurationVar(&cfg.Storage.SyncInterval, "sync-interval", cfg.Storage.SyncInterval, "event log fsync interval for write and buffered durability")

	symbols := flag.String("symbols", strings.Join(cfg.Producer.Symbols, ","), "comma-separated producer symbols")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat := flag.String("log-format", "text", "log format: text or json")

	flag.Parse()

	cfg.Producer.Symbols = splitSymbols(*symbols)

	log, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}

	a, err := app.New(cfg, log)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return a.Run(ctx)
}

// parseSize parses a byte size: plain bytes or a KB/MB/GB suffix (powers of 1024).
func parseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1}} {
		if rest, ok := strings.CutSuffix(s, u.suffix); ok {
			s, mult = strings.TrimSpace(rest), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}

func splitSymbols(s string) []string {
	var out []string
	for _, sym := range strings.Split(s, ",") {
		if sym = strings.TrimSpace(sym); sym != "" {
			out = append(out, sym)
		}
	}
	return out
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("invalid -log-format %q (want text or json)", format)
	}
}
