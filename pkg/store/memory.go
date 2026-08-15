package store

import (
	"context"
	"sync"
	"time"
)

func init() {
	Register("memory", func(Options) (Store, error) { return NewMemory(), nil })
}

type memSeed struct {
	nodes map[string]time.Time
}

// Memory is an in-process Store used for tests and demos.
type Memory struct {
	mu       sync.RWMutex
	peers    map[string]Peer
	arts     map[string]ArtifactRecord
	seeds    map[string]*memSeed            // artifactID -> nodes
	chunks   map[string]map[string]time.Time // hash -> nodeID -> t
}

func NewMemory() *Memory {
	return &Memory{
		peers:  make(map[string]Peer),
		arts:   make(map[string]ArtifactRecord),
		seeds:  make(map[string]*memSeed),
		chunks: make(map[string]map[string]time.Time),
	}
}

func (m *Memory) Name() string { return "memory" }

func (m *Memory) Ping(context.Context) error { return nil }

func (m *Memory) Close() error { return nil }

func (m *Memory) UpsertPeer(_ context.Context, peer Peer) error {
	if peer.NodeID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if peer.LastHeartbeat.IsZero() {
		peer.LastHeartbeat = time.Now()
	}
	if peer.Status == "" {
		peer.Status = "HEALTHY"
	}
	m.peers[peer.NodeID] = peer
	return nil
}

func (m *Memory) Heartbeat(_ context.Context, nodeID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.peers[nodeID]
	if !ok {
		return false, nil
	}
	p.LastHeartbeat = time.Now()
	m.peers[nodeID] = p
	return true, nil
}

func (m *Memory) GetPeer(_ context.Context, nodeID string) (*Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[nodeID]
	if !ok {
		return nil, nil
	}
	cp := p
	return &cp, nil
}

func (m *Memory) ListPeers(_ context.Context, expiry time.Duration) ([]Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	var out []Peer
	for _, p := range m.peers {
		if expiry <= 0 || now.Sub(p.LastHeartbeat) <= expiry {
			out = append(out, p)
		}
	}
	return out, nil
}

func (m *Memory) DeregisterPeer(_ context.Context, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peers, nodeID)
	for _, s := range m.seeds {
		delete(s.nodes, nodeID)
	}
	for _, nodes := range m.chunks {
		delete(nodes, nodeID)
	}
	return nil
}

func (m *Memory) PruneExpiredPeers(_ context.Context, expiry time.Duration) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	var dead []string
	for id, p := range m.peers {
		if now.Sub(p.LastHeartbeat) > expiry {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		delete(m.peers, id)
		for _, s := range m.seeds {
			delete(s.nodes, id)
		}
		for _, nodes := range m.chunks {
			delete(nodes, id)
		}
	}
	return len(dead), nil
}

func (m *Memory) PutArtifact(_ context.Context, rec ArtifactRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.arts[rec.ArtifactID] = rec
	return nil
}

func (m *Memory) GetArtifact(_ context.Context, artifactID string) (*ArtifactRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.arts[artifactID]
	if !ok {
		return nil, nil
	}
	cp := a
	return &cp, nil
}

func (m *Memory) ReportSeed(_ context.Context, artifactID, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.seeds[artifactID]
	if !ok {
		s = &memSeed{nodes: make(map[string]time.Time)}
		m.seeds[artifactID] = s
	}
	s.nodes[nodeID] = time.Now()
	return nil
}

func (m *Memory) ListSeeds(_ context.Context, artifactID string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.seeds[artifactID]
	if !ok {
		return nil, nil
	}
	var ids []string
	for id := range s.nodes {
		ids = append(ids, id)
	}
	return ids, nil
}

func (m *Memory) ReportChunks(_ context.Context, nodeID string, hashes []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if p, ok := m.peers[nodeID]; ok {
		p.LastHeartbeat = now
		m.peers[nodeID] = p
	}
	var n int64
	for _, h := range hashes {
		if h == "" {
			continue
		}
		nodes, ok := m.chunks[h]
		if !ok {
			nodes = make(map[string]time.Time)
			m.chunks[h] = nodes
		}
		nodes[nodeID] = now
		n++
	}
	return n, nil
}

func (m *Memory) LocateChunkNodes(_ context.Context, hash string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := m.chunks[hash]
	var ids []string
	for id := range nodes {
		ids = append(ids, id)
	}
	return ids, nil
}
