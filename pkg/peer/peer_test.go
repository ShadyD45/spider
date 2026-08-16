package peer

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/chunk"
)

func TestPeerStreamingTransfer(t *testing.T) {
	tempDir := t.TempDir()
	c, err := cache.NewCache(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create test chunk (128 KiB to test streaming slicing)
	chunkData := bytes.Repeat([]byte("0123456789ABCDEF"), 8192) // 128 KiB
	chunkHash := chunk.ComputeHash(chunkData)

	if err := c.PutChunk(chunkHash, chunkData); err != nil {
		t.Fatal(err)
	}

	// Start peer server on random available port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverPort := lis.Addr().(*net.TCPAddr).Port

	grpcServer := grpc.NewServer()
	srv := NewServer("node-1", c, nil)
	proto.RegisterPeerServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// Download using ClientPool
	pool := NewClientPool()
	defer pool.Close()

	peerAddr := fmt.Sprintf("127.0.0.1:%d", serverPort)
	ctx := context.Background()

	downloaded, err := pool.DownloadChunk(ctx, peerAddr, chunkHash)
	if err != nil {
		t.Fatalf("DownloadChunk failed: %v", err)
	}

	if !bytes.Equal(downloaded, chunkData) {
		t.Fatal("Downloaded bytes mismatch")
	}

	// Test non-existent chunk
	_, err = pool.DownloadChunk(ctx, peerAddr, "sha256:0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Fatal("Expected error for missing chunk, got nil")
	}
}

type corruptPeerServer struct {
	proto.UnimplementedPeerServiceServer
}

func (s *corruptPeerServer) GetChunk(req *proto.GetChunkRequest, stream proto.PeerService_GetChunkServer) error {
	return stream.Send(&proto.ChunkDataChunk{
		Payload:   []byte("this payload does not match the requested hash"),
		Offset:    0,
		TotalSize: 46,
		IsEof:     true,
	})
}

func TestDownloadChunkRejectsHashMismatch(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterPeerServiceServer(grpcServer, &corruptPeerServer{})
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	pool := NewClientPool()
	defer pool.Close()

	expected := chunk.ComputeHash([]byte("expected honest bytes"))
	_, err = pool.DownloadChunk(context.Background(), fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port), expected)
	if err == nil {
		t.Fatal("expected hash mismatch error from corrupt peer stream")
	}
}

func TestUploadBackpressure(t *testing.T) {
	tempDir := t.TempDir()
	c, err := cache.NewChunkStore(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	chunkData := bytes.Repeat([]byte("ab"), 4096)
	chunkHash := chunk.ComputeHash(chunkData)
	if err := c.PutChunk(chunkHash, chunkData); err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	hold := make(chan struct{})
	started := make(chan struct{})
	srv := NewServerWithLimits("node-1", c, nil, UploadLimits{
		MaxConcurrency: 1,
		MaxQueueSize:   0,
		AfterAcquire: func() {
			select {
			case <-started:
			default:
				close(started)
			}
			<-hold
		},
	})
	proto.RegisterPeerServiceServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	addr := fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port)
	pool := NewClientPool()
	defer pool.Close()

	go func() {
		_, _ = pool.DownloadChunk(context.Background(), addr, chunkHash)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first upload did not start")
	}
	_, err = pool.DownloadChunk(context.Background(), addr, chunkHash)
	if err == nil {
		t.Fatal("expected resource exhausted on second upload")
	}
	close(hold)
}

func TestDownloadResumeFromOffset(t *testing.T) {
	tempDir := t.TempDir()
	c, err := cache.NewChunkStore(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	chunkData := bytes.Repeat([]byte("0123456789ABCDEF"), 256)
	chunkHash := chunk.ComputeHash(chunkData)
	if err := c.PutChunk(chunkHash, chunkData); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterPeerServiceServer(grpcServer, NewServer("n", c, nil))
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	dest, err := cache.NewChunkStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.AppendPartial(chunkHash, bytes.NewReader(chunkData[:100])); err != nil {
		t.Fatal(err)
	}
	pool := NewClientPool()
	defer pool.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port)
	var buf bytes.Buffer
	if _, err := pool.DownloadChunkTo(context.Background(), addr, chunkHash, 100, &buf); err != nil {
		t.Fatal(err)
	}
	if err := dest.AppendPartial(chunkHash, &buf); err != nil {
		t.Fatal(err)
	}
	if err := dest.CommitPartial(chunkHash); err != nil {
		t.Fatal(err)
	}
	if !dest.HasChunk(chunkHash) {
		t.Fatal("resumed chunk missing")
	}
}

func startTestServer(t *testing.T, srv *Server) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterPeerServiceServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	return fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port), func() { grpcServer.Stop() }
}

func TestSharedUploadBandwidthLimit(t *testing.T) {
	tempDir := t.TempDir()
	c, err := cache.NewChunkStore(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	// 1 MiB chunk
	chunkData := bytes.Repeat([]byte("x"), 1024*1024)
	chunkHash := chunk.ComputeHash(chunkData)
	if err := c.PutChunk(chunkHash, chunkData); err != nil {
		t.Fatal(err)
	}

	addr, stop := startTestServer(t, NewServerWithLimits("n", c, nil, UploadLimits{
		MaxConcurrency:   4,
		MaxBandwidthMbps: 1, // ~125 KB/s
	}))
	defer stop()

	pool := NewClientPool()
	defer pool.Close()
	ctx := context.Background()

	start := time.Now()
	if _, err := pool.DownloadChunk(ctx, addr, chunkHash); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	// 1 MiB = 8 Mbit; at 1 Mbps minimum ~8s (allow margin for test overhead)
	if elapsed < 6*time.Second {
		t.Fatalf("single upload too fast with 1 Mbps cap: %v", elapsed)
	}
}

func TestConcurrentUploadsShareBandwidth(t *testing.T) {
	tempDir := t.TempDir()
	c, err := cache.NewChunkStore(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	chunkData := bytes.Repeat([]byte("y"), 512*1024) // 512 KiB each
	chunkHash := chunk.ComputeHash(chunkData)
	if err := c.PutChunk(chunkHash, chunkData); err != nil {
		t.Fatal(err)
	}

	addr, stop := startTestServer(t, NewServerWithLimits("n", c, nil, UploadLimits{
		MaxConcurrency:   4,
		MaxBandwidthMbps: 1,
	}))
	defer stop()

	pool := NewClientPool()
	defer pool.Close()
	ctx := context.Background()

	const workers = 2
	start := time.Now()
	done := make(chan struct{}, workers)
	for i := 0; i < workers; i++ {
		go func() {
			_, err := pool.DownloadChunk(ctx, addr, chunkHash)
			if err != nil {
				t.Error(err)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < workers; i++ {
		<-done
	}
	elapsed := time.Since(start)
	// 2 x 512 KiB = 8 Mbit total; shared 1 Mbps ~8s minimum
	if elapsed < 6*time.Second {
		t.Fatalf("concurrent uploads finished too fast for shared 1 Mbps: %v", elapsed)
	}
}

func TestWaitBandwidthFairShare(t *testing.T) {
	s := &Server{bytesPerSec: 100_000}
	s.activeStreams.Store(2)
	ctx := context.Background()
	start := time.Now()
	if err := s.waitBandwidth(ctx, 50_000); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < 700*time.Millisecond || elapsed > 2500*time.Millisecond {
		t.Fatalf("expected ~1s fair-share wait, got %v", elapsed)
	}
}

func TestClientPoolEvictsIdleConnections(t *testing.T) {
	pool := NewClientPoolWithConfig(PoolConfig{MaxConnections: 4, IdleTimeout: 50 * time.Millisecond})
	defer pool.Close()

	tempDir := t.TempDir()
	c, err := cache.NewChunkStore(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	chunkData := bytes.Repeat([]byte("z"), 1024)
	chunkHash := chunk.ComputeHash(chunkData)
	if err := c.PutChunk(chunkHash, chunkData); err != nil {
		t.Fatal(err)
	}
	addr, stop := startTestServer(t, NewServer("n", c, nil))
	defer stop()

	ctx := context.Background()
	if _, err := pool.DownloadChunk(ctx, addr, chunkHash); err != nil {
		t.Fatal(err)
	}
	if pool.Len() != 1 {
		t.Fatalf("expected 1 cached conn, got %d", pool.Len())
	}
	time.Sleep(150 * time.Millisecond)
	pool.evictIdle()
	if pool.Len() != 0 {
		t.Fatalf("expected idle conn evicted, got %d", pool.Len())
	}
}
