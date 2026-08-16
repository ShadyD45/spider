package peer

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/metrics"
	"spider/pkg/netutil"
)

const (
	StreamSliceSize = 64 * 1024 // 64 KiB slice per gRPC streaming frame
)

// SyncHandler allows the engine to be invoked when a SyncArtifact RPC is received.
type SyncHandler interface {
	HandleSync(ctx context.Context, manifestJSON, destPath, originType, originURI string) (string, error)
	GetStatus(jobID string) *proto.GetNodeStatusResponse
}

// UploadLimits bounds concurrent GetChunk work on this node.
type UploadLimits struct {
	MaxConcurrency   int
	MaxQueueSize     int
	MaxBandwidthMbps int
	AfterAcquire     func() // optional hook for tests
}

// Server implements proto.PeerServiceServer.
type Server struct {
	proto.UnimplementedPeerServiceServer
	nodeID        string
	cache         *cache.ChunkStore
	syncHandler   SyncHandler
	grpcServer    *grpc.Server
	slots         chan struct{}
	queued        atomic.Int64
	maxQueue      int
	limiter       *rate.Limiter
	bytesPerSec   float64
	activeStreams atomic.Int64
	afterAcquire  func()
}

// NewServer creates a peer chunk streaming server with default upload limits.
func NewServer(nodeID string, c *cache.ChunkStore, syncHandler SyncHandler) *Server {
	return NewServerWithLimits(nodeID, c, syncHandler, UploadLimits{MaxConcurrency: 16, MaxQueueSize: 100})
}

// NewServerWithLimits creates a peer server with explicit upload backpressure.
func NewServerWithLimits(nodeID string, c *cache.ChunkStore, syncHandler SyncHandler, lim UploadLimits) *Server {
	if lim.MaxConcurrency <= 0 {
		lim.MaxConcurrency = 16
	}
	if lim.MaxQueueSize < 0 {
		lim.MaxQueueSize = 0
	}
	s := &Server{
		nodeID:      nodeID,
		cache:       c,
		syncHandler: syncHandler,
		slots:       make(chan struct{}, lim.MaxConcurrency),
		maxQueue:    lim.MaxQueueSize,
	}
	if lim.MaxBandwidthMbps > 0 {
		bytesPerSec := float64(lim.MaxBandwidthMbps) * 1024 * 1024 / 8
		s.bytesPerSec = bytesPerSec
		s.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), StreamSliceSize)
	}
	metrics.UploadBandwidthLimitMbps.Set(float64(lim.MaxBandwidthMbps))
	if lim.AfterAcquire != nil {
		s.afterAcquire = lim.AfterAcquire
	}
	return s
}

func (s *Server) acquireUpload(ctx context.Context) error {
	if s.slots == nil {
		return nil
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	default:
	}
	if s.maxQueue == 0 {
		return status.Error(codes.ResourceExhausted, "upload concurrency limit reached")
	}
	q := s.queued.Add(1)
	defer s.queued.Add(-1)
	if q > int64(s.maxQueue) {
		return status.Error(codes.ResourceExhausted, "upload queue full")
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (s *Server) releaseUpload() {
	if s.slots == nil {
		return
	}
	select {
	case <-s.slots:
	default:
	}
}

func (s *Server) waitBandwidth(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	if s.bytesPerSec > 0 {
		active := s.activeStreams.Load()
		if active < 1 {
			active = 1
		}
		share := s.bytesPerSec / float64(active)
		if share > 0 {
			delay := time.Duration(float64(n) / share * float64(time.Second))
			if delay > 0 {
				t := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					t.Stop()
					return ctx.Err()
				case <-t.C:
				}
			}
		}
		return nil
	}
	if s.limiter == nil {
		return nil
	}
	return s.limiter.WaitN(ctx, n)
}

// GetChunk streams a content-addressed chunk to a requesting peer.
func (s *Server) GetChunk(req *proto.GetChunkRequest, stream proto.PeerService_GetChunkServer) error {
	if err := s.acquireUpload(stream.Context()); err != nil {
		return err
	}
	defer s.releaseUpload()
	s.activeStreams.Add(1)
	defer s.activeStreams.Add(-1)
	if s.afterAcquire != nil {
		s.afterAcquire()
	}

	chunkHash := req.GetChunkHash()
	if chunkHash == "" {
		return status.Error(codes.InvalidArgument, "chunk_hash is required")
	}

	reader, totalSize, err := s.cache.GetChunkReader(chunkHash)
	if err != nil {
		return status.Errorf(codes.NotFound, "chunk %s not found in local cache: %v", chunkHash, err)
	}
	defer reader.Close()

	offset := req.GetOffset()
	if offset == 0 {
		if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
			if vals := md.Get("x-chunk-offset"); len(vals) > 0 {
				if n, err := strconv.ParseInt(vals[0], 10, 64); err == nil {
					offset = n
				}
			}
		}
	}
	if offset < 0 {
		return status.Error(codes.InvalidArgument, "offset must be >= 0")
	}
	if offset > totalSize {
		return status.Errorf(codes.OutOfRange, "offset %d past chunk size %d", offset, totalSize)
	}
	if offset > 0 {
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			return status.Errorf(codes.Internal, "seek chunk: %v", err)
		}
	}

	buf := make([]byte, StreamSliceSize)
	currentOffset := offset

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := s.waitBandwidth(stream.Context(), n); err != nil {
				return status.FromContextError(err).Err()
			}
			isEOF := (currentOffset + int64(n)) >= totalSize
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
			metrics.PeerBytesUploaded.Add(float64(n))

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
	if !netutil.IsLoopbackListen(lis.Addr().String()) {
		slog.Warn("peer gRPC is listening without TLS; anything that can reach this port has full mesh access", "addr", lis.Addr().String())
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
