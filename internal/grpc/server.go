package grpc

import (
	"fmt"
	"net"

	ingestionpb "GoTTDust/internal/genproto/ingestionpb"
	querypb "GoTTDust/internal/genproto/querypb"
	"GoTTDust/internal/ingestion"
	"GoTTDust/internal/query"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps the gRPC server with TTDust service registrations.
type Server struct {
	grpcServer       *grpc.Server
	ingestionHandler *ingestion.IngestionHandler
	queryHandler     *query.QueryHandler
	address          string
}

// NewServer creates a new gRPC server.
// Optional grpc.ServerOption values (e.g. auth interceptors, TLS credentials) can be passed.
func NewServer(
	address string,
	ingestionHandler *ingestion.IngestionHandler,
	queryHandler *query.QueryHandler,
	opts ...grpc.ServerOption,
) *Server {
	grpcServer := grpc.NewServer(opts...)

	ingestionpb.RegisterIngestionServiceServer(grpcServer, ingestionHandler)
	querypb.RegisterQueryServiceServer(grpcServer, queryHandler)

	// Enable reflection for debugging tools like grpcurl
	reflection.Register(grpcServer)

	return &Server{
		grpcServer:       grpcServer,
		ingestionHandler: ingestionHandler,
		queryHandler:     queryHandler,
		address:          address,
	}
}

// Serve starts the gRPC server.
func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("gRPC listen on %s: %w", s.address, err)
	}

	return s.grpcServer.Serve(lis)
}

// GracefulStop gracefully stops the gRPC server.
func (s *Server) GracefulStop() {
	s.grpcServer.GracefulStop()
}

// Stop immediately stops the gRPC server.
func (s *Server) Stop() {
	s.grpcServer.Stop()
}
