package tracker

import (
	"context"
	"log/slog"
	"time"

	"spider/api/v1/proto"
	"spider/pkg/store"
	"spider/pkg/topology"
)

// Registry is a Store-backed peer/chunk index (memory, sqlite, or postgres).
type Registry struct {
	store      store.Store
	peerExpiry time.Duration
}

func NewRegistry(peerExpiry time.Duration) *Registry {
	return NewRegistryWithStore(store.NewMemory(), peerExpiry)
}

func NewRegistryWithStore(st store.Store, peerExpiry time.Duration) *Registry {
	if peerExpiry <= 0 {
		peerExpiry = 30 * time.Second
	}
	if st == nil {
		st = store.NewMemory()
	}
	return &Registry{store: st, peerExpiry: peerExpiry}
}

func (r *Registry) Store() store.Store { return r.store }

func (r *Registry) bind(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, 5*time.Second)
}

func (r *Registry) RegisterPeer(ctx context.Context, peer *proto.PeerInfo) {
	if peer == nil || peer.NodeId == "" {
		return
	}
	p := store.PeerFromProto(peer)
	p.LastHeartbeat = time.Now()
	ctx, cancel := r.bind(ctx)
	defer cancel()
	if err := r.store.UpsertPeer(ctx, p); err != nil {
		slog.Error("register peer", "node", p.NodeID, "err", err)
	}
}

func (r *Registry) Heartbeat(ctx context.Context, nodeID string) bool {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	ok, err := r.store.Heartbeat(ctx, nodeID)
	if err != nil {
		slog.Error("heartbeat", "node", nodeID, "err", err)
		return false
	}
	return ok
}

func (r *Registry) DeregisterPeer(ctx context.Context, nodeID string) {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	if err := r.store.DeregisterPeer(ctx, nodeID); err != nil {
		slog.Error("deregister", "node", nodeID, "err", err)
	}
}

func (r *Registry) ReportChunks(ctx context.Context, nodeID string, chunkHashes []string) int64 {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	n, err := r.store.ReportChunks(ctx, nodeID, chunkHashes)
	if err != nil {
		slog.Error("report chunks", "node", nodeID, "err", err)
		return 0
	}
	return n
}

func (r *Registry) ReportArtifact(ctx context.Context, nodeID, artifactID string) {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	if err := r.store.ReportSeed(ctx, artifactID, nodeID); err != nil {
		slog.Error("report artifact seed", "node", nodeID, "artifact", artifactID, "err", err)
	}
}

func (r *Registry) PutArtifact(ctx context.Context, rec store.ArtifactRecord) error {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	return r.store.PutArtifact(ctx, rec)
}

func (r *Registry) LocateChunks(ctx context.Context, requesterNodeID string, chunkHashes []string) []*proto.ChunkLocation {
	ctx, cancel := r.bind(ctx)
	defer cancel()

	requesterLoc := r.requesterLoc(ctx, requesterNodeID)
	now := time.Now()
	var locations []*proto.ChunkLocation
	for _, h := range chunkHashes {
		ids, err := r.store.LocateChunkNodes(ctx, h)
		if err != nil {
			locations = append(locations, &proto.ChunkLocation{ChunkHash: h})
			continue
		}
		peers := r.peersForIDs(ctx, ids, requesterNodeID, now)
		topology.SortPeersByProximity(requesterLoc, peers)
		locations = append(locations, &proto.ChunkLocation{ChunkHash: h, Peers: peers})
	}
	return locations
}

func (r *Registry) LocateArtifact(ctx context.Context, requesterNodeID, artifactID string) []*proto.PeerInfo {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	ids, err := r.store.ListSeeds(ctx, artifactID)
	if err != nil {
		return nil
	}
	peers := r.peersForIDs(ctx, ids, requesterNodeID, time.Now())
	topology.SortPeersByProximity(r.requesterLoc(ctx, requesterNodeID), peers)
	return peers
}

func (r *Registry) requesterLoc(ctx context.Context, nodeID string) topology.Locality {
	p, err := r.store.GetPeer(ctx, nodeID)
	if err != nil || p == nil {
		return topology.Locality{}
	}
	return topology.FromProto(p.Proto())
}

func (r *Registry) peersForIDs(ctx context.Context, ids []string, skip string, now time.Time) []*proto.PeerInfo {
	var out []*proto.PeerInfo
	for _, id := range ids {
		if id == skip {
			continue
		}
		p, err := r.store.GetPeer(ctx, id)
		if err != nil || p == nil {
			continue
		}
		if now.Sub(p.LastHeartbeat) <= r.peerExpiry {
			out = append(out, p.Proto())
		}
	}
	return out
}

func (r *Registry) ListActivePeers(ctx context.Context) []*proto.PeerInfo {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	peers, err := r.store.ListPeers(ctx, r.peerExpiry)
	if err != nil {
		return nil
	}
	out := make([]*proto.PeerInfo, 0, len(peers))
	for _, p := range peers {
		out = append(out, p.Proto())
	}
	return out
}

func (r *Registry) PruneDeadPeers(ctx context.Context) int {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	n, err := r.store.PruneExpiredPeers(ctx, r.peerExpiry)
	if err != nil {
		slog.Error("prune peers", "err", err)
		return 0
	}
	return n
}

func (r *Registry) ReadyNodes(ctx context.Context, artifactID string) []string {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	ids, err := r.store.ListSeeds(ctx, artifactID)
	if err != nil {
		slog.Error("ready nodes", "artifact", artifactID, "err", err)
		return nil
	}
	return ids
}

func (r *Registry) Ping(ctx context.Context) error {
	return r.store.Ping(ctx)
}
