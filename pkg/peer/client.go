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

// PoolConfig controls gRPC client connection reuse and eviction.
type PoolConfig struct {
	MaxConnections int
	IdleTimeout    time.Duration
}

// DefaultPoolConfig returns production-safe client pool defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{MaxConnections: 64, IdleTimeout: 2 * time.Minute}
}

type pooledConn struct {
	conn     *grpc.ClientConn
	lastUsed time.Time
}

// ClientPool manages gRPC connections to mesh peers.
type ClientPool struct {
	mu          sync.Mutex
	conns       map[string]*pooledConn
	maxConns    int
	idleTimeout time.Duration
	stopCh      chan struct{}
	stopped     bool
}

// NewClientPool initializes a connection pool with defaults.
func NewClientPool() *ClientPool {
	return NewClientPoolWithConfig(DefaultPoolConfig())
}

// NewClientPoolWithConfig initializes a connection pool with explicit limits.
func NewClientPoolWithConfig(cfg PoolConfig) *ClientPool {
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 64
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 2 * time.Minute
	}
	p := &ClientPool{
		conns:       make(map[string]*pooledConn),
		maxConns:    cfg.MaxConnections,
		idleTimeout: cfg.IdleTimeout,
		stopCh:      make(chan struct{}),
	}
	go p.evictLoop()
	return p
}

func (p *ClientPool) evictLoop() {
	t := time.NewTicker(p.idleTimeout / 2)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-t.C:
			p.evictIdle()
		}
	}
}

func (p *ClientPool) evictIdle() {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, pc := range p.conns {
		if now.Sub(pc.lastUsed) >= p.idleTimeout {
			_ = pc.conn.Close()
			delete(p.conns, addr)
		}
	}
}

func (p *ClientPool) evictOldestLocked() {
	var oldestAddr string
	var oldest time.Time
	for addr, pc := range p.conns {
		if oldestAddr == "" || pc.lastUsed.Before(oldest) {
			oldestAddr = addr
			oldest = pc.lastUsed
		}
	}
	if oldestAddr != "" {
		_ = p.conns[oldestAddr].conn.Close()
		delete(p.conns, oldestAddr)
	}
}

// GetClient retrieves or dials a gRPC connection to peerAddress.
func (p *ClientPool) GetClient(ctx context.Context, peerAddress string) (proto.PeerServiceClient, error) {
	p.mu.Lock()
	if pc, ok := p.conns[peerAddress]; ok {
		pc.lastUsed = time.Now()
		p.mu.Unlock()
		return proto.NewPeerServiceClient(pc.conn), nil
	}
	if len(p.conns) >= p.maxConns {
		p.evictOldestLocked()
	}
	p.mu.Unlock()

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
	defer p.mu.Unlock()
	if existing, ok := p.conns[peerAddress]; ok {
		_ = conn.Close()
		existing.lastUsed = time.Now()
		return proto.NewPeerServiceClient(existing.conn), nil
	}
	if len(p.conns) >= p.maxConns {
		p.evictOldestLocked()
	}
	p.conns[peerAddress] = &pooledConn{conn: conn, lastUsed: time.Now()}
	return proto.NewPeerServiceClient(conn), nil
}

// RemovePeer closes and removes a cached connection.
func (p *ClientPool) RemovePeer(peerAddress string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pc, ok := p.conns[peerAddress]; ok {
		_ = pc.conn.Close()
		delete(p.conns, peerAddress)
	}
}

// Len returns the number of cached connections (for tests).
func (p *ClientPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// Close closes all cached peer connections and stops the eviction loop.
func (p *ClientPool) Close() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	close(p.stopCh)
	for _, pc := range p.conns {
		_ = pc.conn.Close()
	}
	p.conns = make(map[string]*pooledConn)
	p.mu.Unlock()
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
	var downloaded int64
	for {
		chunkData, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return downloaded, fmt.Errorf("error reading chunk stream from %s: %w", peerAddress, err)
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
				return downloaded, fmt.Errorf("failed to write chunk stream: %w", err)
			}
			downloaded += int64(len(payload))
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
