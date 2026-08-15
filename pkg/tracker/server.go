package tracker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
	"spider/pkg/metrics"
	"spider/pkg/store"
)

// Server implements proto.TrackerServiceServer.
type Server struct {
	proto.UnimplementedTrackerServiceServer
	registry *Registry
	server   *grpc.Server
}

func NewServer(registry *Registry) *Server {
	return &Server{registry: registry}
}

func (s *Server) RegisterPeer(ctx context.Context, req *proto.RegisterPeerRequest) (*proto.RegisterPeerResponse, error) {
	if req.GetPeer() == nil || req.GetPeer().GetNodeId() == "" {
		return &proto.RegisterPeerResponse{Success: false, Message: "missing peer information or node_id"}, nil
	}
	s.registry.RegisterPeer(ctx, req.GetPeer())
	slog.Info("registered peer", "node", req.Peer.NodeId, "addr", req.Peer.Address, "rack", req.Peer.Rack, "zone", req.Peer.Zone)
	return &proto.RegisterPeerResponse{Success: true, Message: "registered successfully"}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	return &proto.HeartbeatResponse{Acknowledged: s.registry.Heartbeat(ctx, req.GetNodeId())}, nil
}

func (s *Server) DeregisterPeer(ctx context.Context, req *proto.DeregisterPeerRequest) (*proto.DeregisterPeerResponse, error) {
	s.registry.DeregisterPeer(ctx, req.GetNodeId())
	slog.Info("deregistered peer", "node", req.GetNodeId())
	return &proto.DeregisterPeerResponse{Success: true}, nil
}

func (s *Server) ReportChunks(ctx context.Context, req *proto.ReportChunksRequest) (*proto.ReportChunksResponse, error) {
	count := s.registry.ReportChunks(ctx, req.GetNodeId(), req.GetChunkHashes())
	return &proto.ReportChunksResponse{ChunksRecorded: count}, nil
}

func (s *Server) LocateChunks(ctx context.Context, req *proto.LocateChunksRequest) (*proto.LocateChunksResponse, error) {
	locations := s.registry.LocateChunks(ctx, req.GetRequesterNodeId(), req.GetChunkHashes())
	return &proto.LocateChunksResponse{Locations: locations}, nil
}

func (s *Server) ListPeers(ctx context.Context, _ *proto.ListPeersRequest) (*proto.ListPeersResponse, error) {
	peers := s.registry.ListActivePeers(ctx)
	metrics.ActivePeers.Set(float64(len(peers)))
	return &proto.ListPeersResponse{Peers: peers}, nil
}

func (s *Server) PutArtifact(ctx context.Context, req *proto.PutArtifactRequest) (*proto.PutArtifactResponse, error) {
	err := s.registry.PutArtifact(ctx, store.ArtifactRecord{
		ArtifactID:   req.GetArtifactId(),
		Name:         req.GetName(),
		Version:      req.GetVersion(),
		ManifestJSON: req.GetManifestJson(),
	})
	if err != nil {
		return &proto.PutArtifactResponse{Success: false}, err
	}
	return &proto.PutArtifactResponse{Success: true}, nil
}

func (s *Server) ReportArtifact(ctx context.Context, req *proto.ReportArtifactRequest) (*proto.ReportArtifactResponse, error) {
	if req.GetComplete() {
		s.registry.ReportArtifact(ctx, req.GetNodeId(), req.GetArtifactId())
	}
	return &proto.ReportArtifactResponse{Success: true}, nil
}

func (s *Server) LocateArtifact(ctx context.Context, req *proto.LocateArtifactRequest) (*proto.LocateArtifactResponse, error) {
	peers := s.registry.LocateArtifact(ctx, req.GetRequesterNodeId(), req.GetArtifactId())
	return &proto.LocateArtifactResponse{ArtifactId: req.GetArtifactId(), SeedPeers: peers}, nil
}

func (s *Server) GetArtifactStatus(ctx context.Context, req *proto.GetArtifactStatusRequest) (*proto.GetArtifactStatusResponse, error) {
	ready := s.registry.ReadyNodes(ctx, req.GetArtifactId())
	return &proto.GetArtifactStatusResponse{
		ArtifactId: req.GetArtifactId(),
		ReadyNodes: ready,
	}, nil
}

func (s *Server) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}
	s.server = grpc.NewServer()
	proto.RegisterTrackerServiceServer(s.server, s)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			pruned := s.registry.PruneDeadPeers(ctx)
			if pruned > 0 {
				slog.Info("pruned dead peers", "count", pruned)
			}
			metrics.ActivePeers.Set(float64(len(s.registry.ListActivePeers(ctx))))
			cancel()
		}
	}()
	slog.Info("tracker listening", "port", port)
	return s.server.Serve(lis)
}

func (s *Server) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

func (s *Server) Ping(ctx context.Context) error {
	return s.registry.Ping(ctx)
}
