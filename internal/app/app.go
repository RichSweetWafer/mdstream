// Package app wires the mdstream components together and owns their lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"mdstream/internal/distribution"
	"mdstream/internal/grpcapi"
	"mdstream/internal/ingestion"
	"mdstream/internal/storage"
)

// Config is the full server configuration.
type Config struct {
	GRPCAddr        string
	QueueSize       int
	ShutdownTimeout time.Duration
	Producer        ProducerConfig
	Storage         StorageConfig
}

// StorageConfig configures the persistent event log.
type StorageConfig struct {
	// DataDir holds the event log segments. Empty disables persistence.
	DataDir      string
	SegmentSize  int64
	Durability   storage.Durability
	SyncInterval time.Duration
}

// ProducerConfig configures the built-in synthetic producer.
type ProducerConfig struct {
	Enabled bool
	Rate    int // events/sec; 0 = unlimited
	Symbols []string
	Seed    uint64
}

// DefaultConfig returns the configuration used when no flags are given.
func DefaultConfig() Config {
	return Config{
		GRPCAddr:        ":50051",
		QueueSize:       distribution.DefaultQueueSize,
		ShutdownTimeout: 10 * time.Second,
		Producer: ProducerConfig{
			Enabled: true,
			Rate:    10_000,
			Symbols: ingestion.DefaultSymbols,
			Seed:    1,
		},
		Storage: StorageConfig{
			DataDir:      "data",
			SegmentSize:  storage.DefaultSegmentSize,
			Durability:   storage.DurabilityWriteThrough,
			SyncInterval: storage.DefaultSyncInterval,
		},
	}
}

// Validate reports configuration errors.
func (c Config) Validate() error {
	var errs []error
	if c.GRPCAddr == "" {
		errs = append(errs, errors.New("grpc address is empty"))
	}
	if c.QueueSize <= 0 {
		errs = append(errs, fmt.Errorf("queue size must be positive, got %d", c.QueueSize))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("shutdown timeout must be positive, got %v", c.ShutdownTimeout))
	}
	if c.Producer.Rate < 0 {
		errs = append(errs, fmt.Errorf("producer rate must not be negative, got %d", c.Producer.Rate))
	}
	if c.Storage.DataDir != "" {
		if c.Storage.SegmentSize < storage.MaxRecordSize {
			errs = append(errs, fmt.Errorf("segment size must be at least %d bytes, got %d", storage.MaxRecordSize, c.Storage.SegmentSize))
		}
		if c.Storage.SyncInterval <= 0 {
			errs = append(errs, fmt.Errorf("sync interval must be positive, got %v", c.Storage.SyncInterval))
		}
		if _, err := storage.ParseDurability(c.Storage.Durability.String()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// App is a running mdstream server:
//
//	producer → distributor ─┬─► event log (disk)
//	                        └─► gRPC subscribers
type App struct {
	cfg      Config
	log      *slog.Logger
	eventLog *storage.Log // nil when persistence is disabled
	dist     *distribution.Distributor
	producer *ingestion.Producer // nil when disabled
	grpc     *grpc.Server
	lis      net.Listener
}

// New builds the application: it opens and recovers the event log, then
// binds the gRPC listener, so Addr is valid before Run is called.
func New(cfg Config, log *slog.Logger) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	distCfg := distribution.Config{QueueSize: cfg.QueueSize}
	var eventLog *storage.Log
	if cfg.Storage.DataDir != "" {
		var err error
		eventLog, err = openEventLog(cfg.Storage, log)
		if err != nil {
			return nil, err
		}
		distCfg.Log = eventLog
		distCfg.StartSequence = eventLog.LastSequence()
	} else {
		log.Warn("persistence disabled: events are not written to disk")
	}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		if eventLog != nil {
			eventLog.Close()
		}
		return nil, fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	dist := distribution.New(distCfg)

	gs := grpc.NewServer(
		// Detect dead client connections so their handlers do not linger.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	grpcapi.Register(gs, grpcapi.NewServer(dist, log))

	a := &App{cfg: cfg, log: log, eventLog: eventLog, dist: dist, grpc: gs, lis: lis}
	if cfg.Producer.Enabled {
		a.producer = ingestion.NewProducer(ingestion.ProducerConfig{
			Rate:    cfg.Producer.Rate,
			Symbols: cfg.Producer.Symbols,
			Seed:    cfg.Producer.Seed,
		}, dist)
	}
	return a, nil
}

func openEventLog(cfg StorageConfig, log *slog.Logger) (*storage.Log, error) {
	l, err := storage.Open(cfg.DataDir, storage.Options{
		SegmentSize:  cfg.SegmentSize,
		Durability:   cfg.Durability,
		SyncInterval: cfg.SyncInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("open event log: %w", err)
	}

	rec := l.Recovery()
	attrs := []any{
		"data_dir", cfg.DataDir,
		"segments", rec.Segments,
		"active_segment", rec.ActiveSegment,
		"last_sequence", rec.LastSequence,
		"durability", cfg.Durability.String(),
	}
	if rec.TruncatedBytes > 0 {
		log.Warn("event log recovered: damaged tail truncated",
			append(attrs, "truncated_bytes", rec.TruncatedBytes, "reason", rec.TruncateReason)...)
	} else {
		log.Info("event log opened", attrs...)
	}
	return l, nil
}

// Addr returns the address the gRPC server listens on.
func (a *App) Addr() net.Addr { return a.lis.Addr() }

// Distributor returns the event distributor, e.g. to publish from Go code.
func (a *App) Distributor() *distribution.Distributor { return a.dist }

// EventLog returns the persistent event log, or nil if persistence is disabled.
func (a *App) EventLog() *storage.Log { return a.eventLog }

// Run serves until ctx is cancelled (or a component fails), then shuts down
// gracefully. It returns nil after a clean, signal-initiated shutdown.
func (a *App) Run(ctx context.Context) error {
	failed := make(chan error, 2)
	var workers sync.WaitGroup

	workers.Go(func() {
		if err := a.grpc.Serve(a.lis); err != nil {
			failed <- fmt.Errorf("grpc server: %w", err)
		}
	})

	producerCtx, stopProducer := context.WithCancel(context.Background())
	defer stopProducer()
	producerDone := make(chan struct{})
	if a.producer != nil {
		go func() {
			defer close(producerDone)
			if err := a.producer.Run(producerCtx); err != nil {
				failed <- fmt.Errorf("producer: %w", err)
			}
		}()
	} else {
		close(producerDone)
	}

	a.log.Info("mdstream started",
		"grpc_addr", a.Addr().String(),
		"queue_size", a.cfg.QueueSize,
		"producer", a.cfg.Producer.Enabled,
		"rate", a.cfg.Producer.Rate,
	)

	var runErr error
	select {
	case <-ctx.Done():
		a.log.Info("shutdown requested", "reason", context.Cause(ctx))
	case runErr = <-failed:
		a.log.Error("component failed, shutting down", "error", runErr)
	}

	a.shutdown(stopProducer, producerDone, &workers)
	return runErr
}

// shutdown runs the graceful-shutdown sequence:
//
//  1. stop accepting new events;
//  2. stop the producer;
//  3. close subscribers (queued events are still flushed to clients);
//  4. shut down gRPC (forcefully after ShutdownTimeout);
//  5. wait for worker goroutines;
//  6. flush, fsync and close the event log.
func (a *App) shutdown(stopProducer context.CancelFunc, producerDone <-chan struct{}, workers *sync.WaitGroup) {
	start := time.Now()

	a.log.Info("shutdown: stop accepting events")
	a.dist.StopPublishing()

	a.log.Info("shutdown: stopping producer")
	stopProducer()
	<-producerDone
	if a.producer != nil {
		a.log.Info("shutdown: producer stopped", "events_sent", a.producer.Sent())
	}

	st := a.dist.Stats()
	a.log.Info("shutdown: closing subscribers",
		"subscribers", st.Subscribers,
		"last_sequence", st.LastSequence,
		"slow_consumer_disconnects", st.SlowConsumerDisconnects,
	)
	a.dist.Close()

	a.log.Info("shutdown: stopping gRPC server")
	stopped := make(chan struct{})
	go func() {
		a.grpc.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(a.cfg.ShutdownTimeout):
		a.log.Warn("shutdown: graceful stop timed out, forcing", "timeout", a.cfg.ShutdownTimeout)
		a.grpc.Stop()
		<-stopped
	}

	a.log.Info("shutdown: waiting for workers")
	workers.Wait()

	if a.eventLog != nil {
		st := a.eventLog.Stats()
		if err := a.eventLog.Close(); err != nil {
			a.log.Error("shutdown: closing event log failed", "error", err)
		} else {
			a.log.Info("shutdown: event log closed", "last_sequence", st.LastSequence, "segments", st.Segments, "bytes", st.Bytes)
		}
	}
	a.log.Info("shutdown complete", "took", time.Since(start).Round(time.Millisecond))
}
