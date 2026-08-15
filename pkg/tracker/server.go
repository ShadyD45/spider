package tracker

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
)

// Server implements proto.TrackerServiceServer.
type Server struct {
	proto.UnimplementedTrackerServiceServer
	registry *Registry
	server   *grpc.Server
}

// NewServer creates a new tracker gRPC service wrapper.
func NewServer(registry *Registry) *Server {
	return &Server{
		registry: registry,
	}
}

// RegisterPeer registers a node in the mesh.
func (s *Server) RegisterPeer(ctx context.Context, req *proto.RegisterPeerRequest) (*proto.RegisterPeerResponse, error) {
	if req.GetPeer() == nil || req.GetPeer().GetNodeId() == "" {
		return &proto.RegisterPeerResponse{
			Success: false,
			Message: "missing peer information or node_id",
		}, nil
	}

	s.registry.RegisterPeer(req.GetPeer())
	log.Printf("[Tracker] Registered peer %s (%s) rack=%s zone=%s region=%s",
		req.Peer.NodeId, req.Peer.Address, req.Peer.Rack, req.Peer.Zone, req.Peer.Region)

	return &proto.RegisterPeerResponse{
		Success: true,
		Message: "registered successfully",
	}, nil
}

// Heartbeat refreshes node liveness.
func (s *Server) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	acknowledged := s.registry.Heartbeat(req.GetNodeId())
	return &proto.HeartbeatResponse{Acknowledged: acknowledged}, nil
}

// ReportChunks updates chunk availability on the tracker.
func (s *Server) ReportChunks(ctx context.Context, req *proto.ReportChunksRequest) (*proto.ReportChunksResponse, error) {
	count := s.registry.ReportChunks(req.GetNodeId(), req.GetChunkHashes())
	return &proto.ReportChunksResponse{ChunksRecorded: count}, nil
}

// LocateChunks returns ranked candidate peers for requested chunks.
func (s *Server) LocateChunks(ctx context.Context, req *proto.LocateChunksRequest) (*proto.LocateChunksResponse, error) {
	locations := s.registry.LocateChunks(req.GetRequesterNodeId(), req.GetChunkHashes())
	return &proto.LocateChunksResponse{Locations: locations}, nil
}

// ListPeers returns all active peers in the mesh.
func (s *Server) ListPeers(ctx context.Context, req *proto.ListPeersRequest) (*proto.ListPeersResponse, error) {
	peers := s.registry.ListActivePeers()
	return &proto.ListPeersResponse{Peers: peers}, nil
}

// Start runs the gRPC listener and periodic dead-peer cleanup loop.
func (s *Server) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	s.server = grpc.NewServer()
	proto.RegisterTrackerServiceServer(s.server, s)

	// Background pruning loop
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			pruned := s.registry.PruneDeadPeers()
			if pruned > 0 {
				log.Printf("[Tracker] Pruned %d dead peers", pruned)
			}
		}
	}()

	log.Printf("[Tracker] Server listening on port %d...", port)
	return s.server.Serve(lis)
}

// Stop gracefully stops the tracker server.
func (s *Server) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}
