package peer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"spider/api/v1/proto"
)

// Client pool manages gRPC connections to mesh peers.
type ClientPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

// NewClientPool initializes a connection pool.
func NewClientPool() *ClientPool {
	return &ClientPool{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// GetClient retrieves or dials a gRPC connection to peerAddress.
func (p *ClientPool) GetClient(ctx context.Context, peerAddress string) (proto.PeerServiceClient, error) {
	p.mu.RLock()
	if conn, ok := p.conns[peerAddress]; ok {
		p.mu.RUnlock()
		return proto.NewPeerServiceClient(conn), nil
	}
	p.mu.RUnlock()

	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, peerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %s: %w", peerAddress, err)
	}

	p.mu.Lock()
	if existing, ok := p.conns[peerAddress]; ok {
		p.mu.Unlock()
		_ = conn.Close()
		return proto.NewPeerServiceClient(existing), nil
	}
	p.conns[peerAddress] = conn
	p.mu.Unlock()
	return proto.NewPeerServiceClient(conn), nil
}

// Close closes all cached peer connections.
func (p *ClientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.conns = make(map[string]*grpc.ClientConn)
}

// DownloadChunk streams a complete chunk from a peer and verifies its SHA-256 integrity.
func (p *ClientPool) DownloadChunk(ctx context.Context, peerAddress string, chunkHash string) ([]byte, error) {
	client, err := p.GetClient(ctx, peerAddress)
	if err != nil {
		return nil, err
	}

	stream, err := client.GetChunk(ctx, &proto.GetChunkRequest{
		ChunkHash: chunkHash,
	})
	if err != nil {
		return nil, fmt.Errorf("peer GetChunk RPC failed: %w", err)
	}

	hasher := sha256.New()
	var buf bytes.Buffer
	mw := io.MultiWriter(&buf, hasher)

	for {
		chunkData, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading chunk stream from %s: %w", peerAddress, err)
		}

		if len(chunkData.Payload) > 0 {
			if _, err := mw.Write(chunkData.Payload); err != nil {
				return nil, fmt.Errorf("failed to write buffer: %w", err)
			}
		}

		if chunkData.IsEof {
			break
		}
	}

	receivedBytes := buf.Bytes()
	actualHash := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if actualHash != chunkHash {
		return nil, fmt.Errorf("chunk corruption detected from peer %s: expected %s, got %s", peerAddress, chunkHash, actualHash)
	}

	return receivedBytes, nil
}
