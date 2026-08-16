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
	fleet      *FleetWatch
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
	return &Registry{store: st, peerExpiry: peerExpiry, fleet: NewFleetWatch()}
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
		return
	}
	if r.fleet != nil {
		r.fleet.NodeReady(artifactID, nodeID)
	}
}

func (r *Registry) PutArtifact(ctx context.Context, rec store.ArtifactRecord) error {
	ctx, cancel := r.bind(ctx)
	defer cancel()
	if err := r.store.PutArtifact(ctx, rec); err != nil {
		return err
	}
	if r.fleet != nil {
		expected := len(r.ListActivePeers(ctx))
		if expected <= 0 {
			expected = 1
		}
		r.fleet.BeginDistribution(rec.ArtifactID, expected)
	}
	return nil
}

func (r *Registry) LocateChunks(ctx context.Context, requesterNodeID string, chunkHashes []string) []*proto.ChunkLocation {
	ctx, cancel := r.bind(ctx)
	defer cancel()

	requesterLoc := r.requesterLoc(ctx, requesterNodeID)
	now := time.Now()
	located, err := r.store.LocateChunkNodesBatch(ctx, chunkHashes)
	if err != nil {
		var locations []*proto.ChunkLocation
		for _, h := range chunkHashes {
			locations = append(locations, &proto.ChunkLocation{ChunkHash: h})
		}
		return locations
	}
	idSet := make(map[string]struct{})
	for _, ids := range located {
		for _, id := range ids {
			if id == requesterNodeID {
				continue
			}
			idSet[id] = struct{}{}
		}
	}
	uniq := make([]string, 0, len(idSet))
	for id := range idSet {
		uniq = append(uniq, id)
	}
	peersByID, err := r.store.GetPeersBatch(ctx, uniq)
	if err != nil {
		peersByID = map[string]*store.Peer{}
	}
	var locations []*proto.ChunkLocation
	for _, h := range chunkHashes {
		peers := r.peersFromCache(located[h], requesterNodeID, now, peersByID)
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
	peers, err := r.store.GetPeersBatch(ctx, ids)
	if err != nil {
		return nil
	}
	return r.peersFromCache(ids, skip, now, peers)
}

func (r *Registry) peersFromCache(ids []string, skip string, now time.Time, peers map[string]*store.Peer) []*proto.PeerInfo {
	var out []*proto.PeerInfo
	for _, id := range ids {
		if id == skip {
			continue
		}
		p := peers[id]
		if p == nil {
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
