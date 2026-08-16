package store

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"spider/pkg/metacache"
	"spider/pkg/metrics"
)

const (
	keyPeerPrefix     = "peer:"
	keyPeersList      = "peers:list"
	keyArtifactPrefix = "artifact:"
	keySeedsPrefix    = "seeds:"
	keyLocatePrefix   = "locate:"
	keyChunksPrefix   = "chunks:"
)

// CachedStore is a read-through decorator. Writes hit inner Store first, then invalidate.
type CachedStore struct {
	inner       Store
	cache       metacache.Cache
	ttl         time.Duration
	hbEvery     time.Duration
	mu          sync.Mutex
	lastHBFlush map[string]time.Time
}

// Wrap returns inner unchanged if cache is nil or Nop-like with driver none handled by caller.
func Wrap(inner Store, cache metacache.Cache, ttl time.Duration) Store {
	if inner == nil {
		return inner
	}
	if cache == nil {
		return inner
	}
	if _, ok := cache.(metacache.Nop); ok {
		return inner
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return &CachedStore{
		inner:       inner,
		cache:       cache,
		ttl:         ttl,
		hbEvery:     5 * time.Second,
		lastHBFlush: make(map[string]time.Time),
	}
}

func (c *CachedStore) Name() string { return c.inner.Name() }

func (c *CachedStore) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }

func (c *CachedStore) Close() error {
	_ = c.cache.Close()
	return c.inner.Close()
}

func (c *CachedStore) hit()  { metrics.StoreCacheHits.Inc() }
func (c *CachedStore) miss() { metrics.StoreCacheMisses.Inc() }

func (c *CachedStore) UpsertPeer(ctx context.Context, peer Peer) error {
	if err := c.inner.UpsertPeer(ctx, peer); err != nil {
		return err
	}
	_ = c.cache.Delete(ctx, keyPeerPrefix+peer.NodeID)
	_ = c.cache.Delete(ctx, keyPeersList)
	return nil
}

func (c *CachedStore) Heartbeat(ctx context.Context, nodeID string) (bool, error) {
	c.mu.Lock()
	last := c.lastHBFlush[nodeID]
	flush := time.Since(last) >= c.hbEvery
	if flush {
		c.lastHBFlush[nodeID] = time.Now()
	}
	c.mu.Unlock()

	if !flush {
		if raw, ok, _ := c.cache.Get(ctx, keyPeerPrefix+nodeID); ok {
			if p, err := unmarshalPeer(raw); err == nil && p != nil {
				p.LastHeartbeat = time.Now()
				_ = c.cache.Set(ctx, keyPeerPrefix+nodeID, marshal(p), c.ttl)
				return true, nil
			}
		}
	}
	ok, err := c.inner.Heartbeat(ctx, nodeID)
	if err != nil || !ok {
		return ok, err
	}
	if raw, found, _ := c.cache.Get(ctx, keyPeerPrefix+nodeID); found {
		if p, uerr := unmarshalPeer(raw); uerr == nil && p != nil {
			p.LastHeartbeat = time.Now()
			_ = c.cache.Set(ctx, keyPeerPrefix+nodeID, marshal(p), c.ttl)
		}
	}
	return true, nil
}

func (c *CachedStore) GetPeer(ctx context.Context, nodeID string) (*Peer, error) {
	if raw, ok, err := c.cache.Get(ctx, keyPeerPrefix+nodeID); err == nil && ok {
		c.hit()
		return unmarshalPeer(raw)
	}
	c.miss()
	p, err := c.inner.GetPeer(ctx, nodeID)
	if err != nil || p == nil {
		return p, err
	}
	_ = c.cache.Set(ctx, keyPeerPrefix+nodeID, marshal(p), c.ttl)
	return p, nil
}

func (c *CachedStore) ListPeers(ctx context.Context, expiry time.Duration) ([]Peer, error) {
	if raw, ok, err := c.cache.Get(ctx, keyPeersList); err == nil && ok {
		c.hit()
		var peers []Peer
		_ = json.Unmarshal(raw, &peers)
		return filterExpiry(peers, expiry), nil
	}
	c.miss()
	peers, err := c.inner.ListPeers(ctx, 0)
	if err != nil {
		return nil, err
	}
	_ = c.cache.Set(ctx, keyPeersList, marshal(peers), c.ttl)
	return filterExpiry(peers, expiry), nil
}

func filterExpiry(peers []Peer, expiry time.Duration) []Peer {
	if expiry <= 0 {
		return peers
	}
	now := time.Now()
	var out []Peer
	for _, p := range peers {
		if now.Sub(p.LastHeartbeat) <= expiry {
			out = append(out, p)
		}
	}
	return out
}

func (c *CachedStore) DeregisterPeer(ctx context.Context, nodeID string) error {
	if err := c.inner.DeregisterPeer(ctx, nodeID); err != nil {
		return err
	}
	_ = c.cache.Delete(ctx, keyPeerPrefix+nodeID)
	_ = c.cache.Delete(ctx, keyPeersList)
	_ = c.cache.DeletePrefix(ctx, keySeedsPrefix)
	_ = c.cache.DeletePrefix(ctx, keyLocatePrefix)
	_ = c.cache.DeletePrefix(ctx, keyChunksPrefix)
	return nil
}

func (c *CachedStore) PruneExpiredPeers(ctx context.Context, expiry time.Duration) (int, error) {
	n, err := c.inner.PruneExpiredPeers(ctx, expiry)
	if err != nil || n == 0 {
		return n, err
	}
	_ = c.cache.Delete(ctx, keyPeersList)
	_ = c.cache.DeletePrefix(ctx, keyPeerPrefix)
	_ = c.cache.DeletePrefix(ctx, keySeedsPrefix)
	_ = c.cache.DeletePrefix(ctx, keyLocatePrefix)
	_ = c.cache.DeletePrefix(ctx, keyChunksPrefix)
	return n, nil
}

func (c *CachedStore) PutArtifact(ctx context.Context, rec ArtifactRecord) error {
	if err := c.inner.PutArtifact(ctx, rec); err != nil {
		return err
	}
	_ = c.cache.Delete(ctx, keyArtifactPrefix+rec.ArtifactID)
	return nil
}

func (c *CachedStore) GetArtifact(ctx context.Context, artifactID string) (*ArtifactRecord, error) {
	if raw, ok, err := c.cache.Get(ctx, keyArtifactPrefix+artifactID); err == nil && ok {
		c.hit()
		return unmarshalArtifact(raw)
	}
	c.miss()
	a, err := c.inner.GetArtifact(ctx, artifactID)
	if err != nil || a == nil {
		return a, err
	}
	_ = c.cache.Set(ctx, keyArtifactPrefix+artifactID, marshal(a), c.ttl)
	return a, nil
}

func (c *CachedStore) ReportSeed(ctx context.Context, artifactID, nodeID string) error {
	if err := c.inner.ReportSeed(ctx, artifactID, nodeID); err != nil {
		return err
	}
	_ = c.cache.Delete(ctx, keySeedsPrefix+artifactID)
	_ = c.cache.Delete(ctx, keyLocatePrefix+artifactID)
	return nil
}

func (c *CachedStore) ListSeeds(ctx context.Context, artifactID string) ([]string, error) {
	if raw, ok, err := c.cache.Get(ctx, keySeedsPrefix+artifactID); err == nil && ok {
		c.hit()
		return unmarshalStrings(raw), nil
	}
	c.miss()
	ids, err := c.inner.ListSeeds(ctx, artifactID)
	if err != nil {
		return nil, err
	}
	_ = c.cache.Set(ctx, keySeedsPrefix+artifactID, marshalStrings(ids), c.ttl)
	return ids, nil
}

func (c *CachedStore) ReportChunks(ctx context.Context, nodeID string, hashes []string) (int64, error) {
	n, err := c.inner.ReportChunks(ctx, nodeID, hashes)
	if err != nil {
		return n, err
	}
	for _, h := range hashes {
		_ = c.cache.Delete(ctx, keyChunksPrefix+h)
	}
	return n, nil
}

func (c *CachedStore) LocateChunkNodes(ctx context.Context, hash string) ([]string, error) {
	if raw, ok, err := c.cache.Get(ctx, keyChunksPrefix+hash); err == nil && ok {
		c.hit()
		return unmarshalStrings(raw), nil
	}
	c.miss()
	ids, err := c.inner.LocateChunkNodes(ctx, hash)
	if err != nil {
		return nil, err
	}
	_ = c.cache.Set(ctx, keyChunksPrefix+hash, marshalStrings(ids), c.ttl)
	return ids, nil
}

func (c *CachedStore) LocateChunkNodesBatch(ctx context.Context, hashes []string) (map[string][]string, error) {
	out := make(map[string][]string, len(hashes))
	if len(hashes) == 0 {
		return out, nil
	}
	keys := make([]string, len(hashes))
	for i, h := range hashes {
		keys[i] = keyChunksPrefix + h
	}
	got, err := c.cache.MGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	var misses []string
	for i, h := range hashes {
		raw, ok := got[keys[i]]
		if !ok {
			c.miss()
			misses = append(misses, h)
			continue
		}
		c.hit()
		out[h] = unmarshalStrings(raw)
	}
	if len(misses) == 0 {
		return out, nil
	}
	inner, err := c.inner.LocateChunkNodesBatch(ctx, misses)
	if err != nil {
		return nil, err
	}
	set := make(map[string][]byte, len(inner))
	for h, ids := range inner {
		out[h] = ids
		set[keyChunksPrefix+h] = marshalStrings(ids)
	}
	_ = c.cache.MSet(ctx, set, c.ttl)
	return out, nil
}

func (c *CachedStore) GetPeersBatch(ctx context.Context, nodeIDs []string) (map[string]*Peer, error) {
	out := make(map[string]*Peer, len(nodeIDs))
	if len(nodeIDs) == 0 {
		return out, nil
	}
	keys := make([]string, len(nodeIDs))
	for i, id := range nodeIDs {
		keys[i] = keyPeerPrefix + id
	}
	got, err := c.cache.MGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	var misses []string
	for i, id := range nodeIDs {
		raw, ok := got[keys[i]]
		if !ok {
			c.miss()
			misses = append(misses, id)
			continue
		}
		c.hit()
		p, err := unmarshalPeer(raw)
		if err == nil && p != nil {
			out[id] = p
		}
	}
	if len(misses) == 0 {
		return out, nil
	}
	inner, err := c.inner.GetPeersBatch(ctx, misses)
	if err != nil {
		return nil, err
	}
	set := make(map[string][]byte, len(inner))
	for id, p := range inner {
		if p == nil {
			continue
		}
		out[id] = p
		set[keyPeerPrefix+id] = marshal(p)
	}
	_ = c.cache.MSet(ctx, set, c.ttl)
	return out, nil
}
