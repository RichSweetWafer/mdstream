package grpcapi

import (
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mdstreamv1 "mdstream/api/gen/mdstream/v1"
	"mdstream/internal/distribution"
)

// Server implements the MarketData gRPC service on top of a Distributor.
type Server struct {
	mdstreamv1.UnimplementedMarketDataServer

	dist *distribution.Distributor
	log  *slog.Logger
}

// NewServer returns a Server that streams events from dist.
func NewServer(dist *distribution.Distributor, log *slog.Logger) *Server {
	return &Server{dist: dist, log: log}
}

// Register registers s on gs.
func Register(gs *grpc.Server, s *Server) {
	mdstreamv1.RegisterMarketDataServer(gs, s)
}

// Subscribe streams live events to the client until the client goes away, the
// client falls behind (RESOURCE_EXHAUSTED) or the server shuts down
// (UNAVAILABLE).
//
// The handler goroutine drains the subscriber's bounded queue into the gRPC
// stream. When the client reads slower than events arrive, HTTP/2 flow control
// blocks stream.Send, the queue fills up and the distributor disconnects the
// subscriber — without ever blocking the publisher or other clients.
func (s *Server) Subscribe(req *mdstreamv1.SubscribeRequest, stream mdstreamv1.MarketData_SubscribeServer) error {
	sub, err := s.dist.Subscribe()
	if err != nil {
		return status.Error(codes.Unavailable, "server is shutting down")
	}
	defer sub.Close()

	log := s.log.With("subscriber_id", sub.ID(), "client_id", req.GetClientId())
	log.Info("subscriber connected")

	ctx := stream.Context()
	msg := &mdstreamv1.Event{} // reused: Send serializes synchronously
	var sent uint64
	for {
		select {
		case <-ctx.Done():
			log.Info("subscriber disconnected", "reason", ctx.Err(), "sent", sent)
			return status.FromContextError(ctx.Err()).Err()

		case ev, ok := <-sub.Events():
			if !ok {
				return s.ended(log, sub.Err(), sent)
			}
			// A slow consumer is cut off immediately instead of being fed the
			// rest of its (stale) queue. On shutdown the queue is drained so
			// clients receive everything published before the stop.
			if errors.Is(sub.Err(), distribution.ErrSlowConsumer) {
				return s.ended(log, sub.Err(), sent)
			}
			ToProto(ev, msg)
			if err := stream.Send(msg); err != nil {
				log.Info("subscriber disconnected", "reason", err, "sent", sent)
				return err
			}
			sent++
		}
	}
}

func (s *Server) ended(log *slog.Logger, reason error, sent uint64) error {
	switch {
	case errors.Is(reason, distribution.ErrSlowConsumer):
		log.Warn("subscriber disconnected: slow consumer", "sent", sent)
		return status.Error(codes.ResourceExhausted, "slow consumer: subscriber queue overflowed")
	case errors.Is(reason, distribution.ErrShutdown):
		log.Info("subscriber closed: server shutting down", "sent", sent)
		return status.Error(codes.Unavailable, "server is shutting down")
	default:
		log.Info("subscriber closed", "reason", reason, "sent", sent)
		return status.Error(codes.Aborted, "subscription ended")
	}
}
