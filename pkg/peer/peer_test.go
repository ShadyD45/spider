package peer

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"

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
