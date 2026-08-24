// Command mdclient is an example subscriber: it connects to mdstream, consumes
// the event stream and reports throughput, the last sequence and any gaps.
//
// Use -delay to simulate a slow consumer and watch the server disconnect it.
// Use -offset-file to persist the last processed sequence across restarts.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mdstreamv1 "mdstream/api/gen/mdstream/v1"
	"mdstream/internal/grpcapi"
	"mdstream/internal/offset"
)

type options struct {
	addr        string
	id          string
	delay       time.Duration
	printEvents bool
	offsetFile  string
}

func main() {
	var o options
	flag.StringVar(&o.addr, "addr", "localhost:50051", "mdstream gRPC address")
	flag.StringVar(&o.id, "id", fmt.Sprintf("mdclient-%d", os.Getpid()), "client id (shown in server logs)")
	flag.DurationVar(&o.delay, "delay", 0, "artificial processing delay per event (simulates a slow consumer)")
	flag.BoolVar(&o.printEvents, "print", false, "print every event")
	flag.StringVar(&o.offsetFile, "offset-file", "", "file to persist the last processed sequence in (optional)")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "mdclient:", err)
		os.Exit(1)
	}
}

// consumer tracks this client's position in the stream.
type consumer struct {
	received atomic.Uint64
	gaps     atomic.Uint64
	lastSeq  atomic.Uint64 // last processed sequence
	store    *offset.FileStore

	saveMu sync.Mutex // serializes checkpoints so an older one never overwrites a newer one
	saved  uint64
}

func run(o options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := &consumer{}
	if o.offsetFile != "" {
		c.store = offset.NewFileStore(o.offsetFile)
		prev, err := c.store.Load()
		if err != nil {
			return err
		}
		if prev > 0 {
			fmt.Printf("last processed sequence from the previous run: %d\n", prev)
			c.lastSeq.Store(prev)
		}
	}

	conn, err := grpc.NewClient(o.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	stream, err := mdstreamv1.NewMarketDataClient(conn).Subscribe(ctx, &mdstreamv1.SubscribeRequest{ClientId: o.id})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	fmt.Printf("subscribed to %s as %q\n", o.addr, o.id)

	go c.report(ctx)
	defer c.saveOffset()

	for {
		m, err := stream.Recv()
		if err != nil {
			fmt.Printf("received=%d last_seq=%d gaps=%d\n", c.received.Load(), c.lastSeq.Load(), c.gaps.Load())
			switch {
			case ctx.Err() != nil:
				fmt.Println("stopped")
				return nil
			case errors.Is(err, io.EOF):
				fmt.Println("stream closed by server")
				return nil
			default:
				st := status.Convert(err)
				return fmt.Errorf("stream ended: %s: %s", st.Code(), st.Message())
			}
		}

		ev := grpcapi.FromProto(m)
		prev := c.lastSeq.Load()
		switch {
		case prev == 0:
			// First event ever: nothing to compare against.
		case ev.Sequence <= prev:
			// Already processed in a previous run (possible once resume exists).
			continue
		case ev.Sequence != prev+1:
			missing := ev.Sequence - prev - 1
			c.gaps.Add(missing)
			fmt.Printf("GAP: expected %d, got %d (%d missing; recoverable from the event log once resume is implemented)\n",
				prev+1, ev.Sequence, missing)
		}

		if o.printEvents {
			fmt.Println(ev)
		}
		if o.delay > 0 {
			time.Sleep(o.delay)
		}

		// Only now is the event fully processed.
		c.lastSeq.Store(ev.Sequence)
		c.received.Add(1)
	}
}

func (c *consumer) saveOffset() {
	if c.store == nil {
		return
	}
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	seq := c.lastSeq.Load()
	if seq == 0 || seq == c.saved {
		return
	}
	if err := c.store.Save(seq); err != nil {
		fmt.Fprintln(os.Stderr, "mdclient: save offset:", err)
		return
	}
	c.saved = seq
}

func (c *consumer) report(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var prev uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n := c.received.Load()
			fmt.Printf("rate=%d ev/s received=%d last_seq=%d gaps=%d\n", n-prev, n, c.lastSeq.Load(), c.gaps.Load())
			prev = n
			c.saveOffset() // periodic checkpoint
		}
	}
}
