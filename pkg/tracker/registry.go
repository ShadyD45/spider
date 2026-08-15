package tracker

import (
	"sync"
	"time"

	"spider/api/v1/proto"
	"spider/pkg/topology"
)

// PeerEntry tracks an active peer in the mesh.
type PeerEntry struct {
	Info     *proto.PeerInfo
	LastSeen time.Time
}

// Registry maintains in-memory peer and chunk indexes.
type Registry struct {
	mu           sync.RWMutex
	peers        map[string]*PeerEntry
	chunkToNodes map[string]map[string]time.Time // chunkHash -> nodeID -> lastReported
	peerExpiry   time.Duration
}

// NewRegistry creates a new peer and chunk tracking registry.
func NewRegistry(peerExpiry time.Duration) *Registry {
	if peerExpiry <= 0 {
		peerExpiry = 30 * time.Second
	}
	return &Registry{
		peers:        make(map[string]*PeerEntry),
		chunkToNodes: make(map[string]map[string]time.Time),
		peerExpiry:   peerExpiry,
	}
}

// RegisterPeer registers or updates a peer's network and topology information.
func (r *Registry) RegisterPeer(peer *proto.PeerInfo) {
	if peer == nil || peer.NodeId == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	peer.LastSeenUnix = now.Unix()
	r.peers[peer.NodeId] = &PeerEntry{
		Info:     peer,
		LastSeen: now,
	}
}

// Heartbeat updates the last seen timestamp of a node.
func (r *Registry) Heartbeat(nodeID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.peers[nodeID]
	if !ok {
		return false
	}
	now := time.Now()
	entry.LastSeen = now
	entry.Info.LastSeenUnix = now.Unix()
	return true
}

// ReportChunks associates chunk hashes with a node.
func (r *Registry) ReportChunks(nodeID string, chunkHashes []string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	// Update heartbeat if peer exists
	if entry, ok := r.peers[nodeID]; ok {
		entry.LastSeen = now
		entry.Info.LastSeenUnix = now.Unix()
	}

	var recorded int64
	for _, h := range chunkHashes {
		if h == "" {
			continue
		}
		nodes, ok := r.chunkToNodes[h]
		if !ok {
			nodes = make(map[string]time.Time)
			r.chunkToNodes[h] = nodes
		}
		nodes[nodeID] = now
		recorded++
	}

	return recorded
}

// LocateChunks returns candidate peers for each requested chunk, sorted by topology proximity to requester.
func (r *Registry) LocateChunks(requesterNodeID string, chunkHashes []string) []*proto.ChunkLocation {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var requesterLoc topology.Locality
	if entry, ok := r.peers[requesterNodeID]; ok && entry.Info != nil {
		requesterLoc = topology.FromProto(entry.Info)
	}

	now := time.Now()
	var locations []*proto.ChunkLocation

	for _, h := range chunkHashes {
		nodesMap, ok := r.chunkToNodes[h]
		var candidatePeers []*proto.PeerInfo
		if ok {
			for nodeID := range nodesMap {
				// Don't return self as candidate peer
				if nodeID == requesterNodeID {
					continue
				}
				peerEntry, exists := r.peers[nodeID]
				if exists && now.Sub(peerEntry.LastSeen) <= r.peerExpiry {
					candidatePeers = append(candidatePeers, peerEntry.Info)
				}
			}
		}

		// Sort candidate peers topologically
		if len(candidatePeers) > 0 {
			topology.SortPeersByProximity(requesterLoc, candidatePeers)
		}

		locations = append(locations, &proto.ChunkLocation{
			ChunkHash: h,
			Peers:     candidatePeers,
		})
	}

	return locations
}

// ListActivePeers returns all peers that have heartbeated within peerExpiry.
func (r *Registry) ListActivePeers() []*proto.PeerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	var active []*proto.PeerInfo
	for _, entry := range r.peers {
		if now.Sub(entry.LastSeen) <= r.peerExpiry {
			active = append(active, entry.Info)
		}
	}
	return active
}

// PruneDeadPeers removes peers that have expired and removes their chunk advertisements.
func (r *Registry) PruneDeadPeers() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	var deadNodes []string
	for nodeID, entry := range r.peers {
		if now.Sub(entry.LastSeen) > r.peerExpiry {
			deadNodes = append(deadNodes, nodeID)
		}
	}

	for _, nodeID := range deadNodes {
		delete(r.peers, nodeID)
		for _, nodesMap := range r.chunkToNodes {
			delete(nodesMap, nodeID)
		}
	}

	return len(deadNodes)
}
