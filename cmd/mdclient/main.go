// Command mdclient is an example subscriber: it connects to mdstream, consumes
// the event stream and reports throughput, the last sequence and any gaps.
//
// Use -delay to simulate a slow consumer and watch the server disconnect it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mdstreamv1 "mdstream/api/gen/mdstream/v1"
	"mdstream/internal/grpcapi"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "mdstream gRPC address")
	id := flag.String("id", fmt.Sprintf("mdclient-%d", os.Getpid()), "client id (shown in server logs)")
	delay := flag.Duration("delay", 0, "artificial processing delay per event (simulates a slow consumer)")
	printEvents := flag.Bool("print", false, "print every event")
	flag.Parse()

	if err := run(*addr, *id, *delay, *printEvents); err != nil {
		fmt.Fprintln(os.Stderr, "mdclient:", err)
		os.Exit(1)
	}
}

func run(addr, id string, delay time.Duration, printEvents bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	stream, err := mdstreamv1.NewMarketDataClient(conn).Subscribe(ctx, &mdstreamv1.SubscribeRequest{ClientId: id})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	fmt.Printf("subscribed to %s as %q\n", addr, id)

	var received, gaps, lastSeq atomic.Uint64
	go report(ctx, &received, &gaps, &lastSeq)

	for {
		m, err := stream.Recv()
		if err != nil {
			fmt.Printf("received=%d last_seq=%d gaps=%d\n", received.Load(), lastSeq.Load(), gaps.Load())
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
		if prev := lastSeq.Load(); prev != 0 && ev.Sequence != prev+1 {
			missing := ev.Sequence - prev - 1
			gaps.Add(missing)
			fmt.Printf("GAP: expected %d, got %d (%d missing)\n", prev+1, ev.Sequence, missing)
		}
		lastSeq.Store(ev.Sequence)
		received.Add(1)

		if printEvents {
			fmt.Println(ev)
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func report(ctx context.Context, received, gaps, lastSeq *atomic.Uint64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var prev uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n := received.Load()
			fmt.Printf("rate=%d ev/s received=%d last_seq=%d gaps=%d\n", n-prev, n, lastSeq.Load(), gaps.Load())
			prev = n
		}
	}
}
