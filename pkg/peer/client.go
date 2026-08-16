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
	"google.golang.org/grpc/metadata"
	"spider/api/v1/proto"
)

// ClientPool manages gRPC connections to mesh peers.
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

// DownloadChunkTo streams chunk bytes starting at offset into w. Hash is verified by the chunk store, not here.
func (p *ClientPool) DownloadChunkTo(ctx context.Context, peerAddress, chunkHash string, offset int64, w io.Writer) (int64, error) {
	client, err := p.GetClient(ctx, peerAddress)
	if err != nil {
		return 0, err
	}

	if offset > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-chunk-offset", fmt.Sprintf("%d", offset))
	}
	stream, err := client.GetChunk(ctx, &proto.GetChunkRequest{
		ChunkHash: chunkHash,
		Offset:    offset,
	})
	if err != nil {
		return 0, fmt.Errorf("peer GetChunk RPC failed: %w", err)
	}

	var totalSize int64
	var skipped int64
	for {
		chunkData, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalSize, fmt.Errorf("error reading chunk stream from %s: %w", peerAddress, err)
		}
		if chunkData.TotalSize > 0 {
			totalSize = chunkData.TotalSize
		}
		payload := chunkData.Payload
		if offset > 0 && chunkData.GetOffset() == 0 && skipped < offset {
			need := offset - skipped
			if int64(len(payload)) <= need {
				skipped += int64(len(payload))
				if chunkData.IsEof {
					break
				}
				continue
			}
			payload = payload[need:]
			skipped = offset
		}
		if len(payload) > 0 {
			if _, err := w.Write(payload); err != nil {
				return totalSize, fmt.Errorf("failed to write chunk stream: %w", err)
			}
		}
		if chunkData.IsEof {
			break
		}
	}
	return totalSize, nil
}

// DownloadChunk streams a complete chunk from a peer and verifies SHA-256 (offset 0 only).
func (p *ClientPool) DownloadChunk(ctx context.Context, peerAddress string, chunkHash string) ([]byte, error) {
	var buf bytes.Buffer
	hasher := sha256.New()
	mw := io.MultiWriter(&buf, hasher)
	if _, err := p.DownloadChunkTo(ctx, peerAddress, chunkHash, 0, mw); err != nil {
		return nil, err
	}
	actualHash := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if actualHash != chunkHash {
		return nil, fmt.Errorf("chunk corruption detected from peer %s: expected %s, got %s", peerAddress, chunkHash, actualHash)
	}
	return buf.Bytes(), nil
}
