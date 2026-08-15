package peer

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"spider/api/v1/proto"
	"spider/pkg/cache"
)

const (
	StreamSliceSize = 64 * 1024 // 64 KiB slice per gRPC streaming frame
)

// SyncHandler allows the engine to be invoked when a SyncArtifact RPC is received.
type SyncHandler interface {
	HandleSync(ctx context.Context, manifestJSON, destPath, originType, originURI string) (string, error)
	GetStatus(jobID string) *proto.GetNodeStatusResponse
}

// Server implements proto.PeerServiceServer.
type Server struct {
	proto.UnimplementedPeerServiceServer
	nodeID      string
	cache       *cache.Cache
	syncHandler SyncHandler
	grpcServer  *grpc.Server
	mu          sync.RWMutex
}

// NewServer creates a new peer chunk streaming server.
func NewServer(nodeID string, c *cache.Cache, syncHandler SyncHandler) *Server {
	return &Server{
		nodeID:      nodeID,
		cache:       c,
		syncHandler: syncHandler,
	}
}

// GetChunk streams a content-addressed chunk to a requesting peer.
func (s *Server) GetChunk(req *proto.GetChunkRequest, stream proto.PeerService_GetChunkServer) error {
	chunkHash := req.GetChunkHash()
	if chunkHash == "" {
		return status.Error(codes.InvalidArgument, "chunk_hash is required")
	}

	reader, totalSize, err := s.cache.GetChunkReader(chunkHash)
	if err != nil {
		return status.Errorf(codes.NotFound, "chunk %s not found in local cache: %v", chunkHash, err)
	}
	defer reader.Close()

	buf := make([]byte, StreamSliceSize)
	var currentOffset int64

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			isEOF := (currentOffset+int64(n)) >= totalSize
			payload := make([]byte, n)
			copy(payload, buf[:n])

			chunkData := &proto.ChunkDataChunk{
				Payload:   payload,
				Offset:    currentOffset,
				TotalSize: totalSize,
				IsEof:     isEOF,
			}

			if err := stream.Send(chunkData); err != nil {
				return status.Errorf(codes.Canceled, "failed to send chunk slice: %v", err)
			}

			currentOffset += int64(n)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "error reading chunk file: %v", err)
		}
	}

	return nil
}

// SyncArtifact triggers an artifact sync on this node.
func (s *Server) SyncArtifact(ctx context.Context, req *proto.SyncArtifactRequest) (*proto.SyncArtifactResponse, error) {
	if s.syncHandler == nil {
		return &proto.SyncArtifactResponse{
			Accepted: false,
			Message:  "sync handler not configured",
		}, nil
	}

	jobID, err := s.syncHandler.HandleSync(ctx, req.GetManifestJson(), req.GetDestinationPath(), req.GetOriginType(), req.GetOriginUri())
	if err != nil {
		return &proto.SyncArtifactResponse{
			Accepted: false,
			Message:  err.Error(),
		}, nil
	}

	return &proto.SyncArtifactResponse{
		Accepted: true,
		JobId:    jobID,
		Message:  "sync started successfully",
	}, nil
}

// GetNodeStatus reports the daemon's local status.
func (s *Server) GetNodeStatus(ctx context.Context, req *proto.GetNodeStatusRequest) (*proto.GetNodeStatusResponse, error) {
	if s.syncHandler != nil {
		return s.syncHandler.GetStatus(req.GetJobId()), nil
	}

	chunks, _ := s.cache.ListChunks()
	totalBytes, _ := s.cache.TotalCachedBytes()

	return &proto.GetNodeStatusResponse{
		NodeId:           s.nodeID,
		CachedChunks:     int64(len(chunks)),
		TotalBytesCached: totalBytes,
	}, nil
}

// Start begins listening on the specified port.
func (s *Server) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	s.grpcServer = grpc.NewServer()
	proto.RegisterPeerServiceServer(s.grpcServer, s)

	log.Printf("[PeerServer] Node %s listening for chunk streams on port %d...", s.nodeID, port)
	return s.grpcServer.Serve(lis)
}

// Stop gracefully stops the peer gRPC server.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}
