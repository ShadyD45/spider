package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"spider/api/v1/proto"
)

// Peer is a tracker-registered mesh node.
type Peer struct {
	NodeID        string    `json:"nodeId"`
	Address       string    `json:"address"`
	Region        string    `json:"region"`
	Zone          string    `json:"zone"`
	Rack          string    `json:"rack"`
	Host          string    `json:"host"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
}

func (p Peer) Proto() *proto.PeerInfo {
	return &proto.PeerInfo{
		NodeId:       p.NodeID,
		Address:      p.Address,
		Region:       p.Region,
		Zone:         p.Zone,
		Rack:         p.Rack,
		Host:         p.Host,
		LastSeenUnix: p.LastHeartbeat.Unix(),
	}
}

func PeerFromProto(info *proto.PeerInfo) Peer {
	if info == nil {
		return Peer{}
	}
	ts := time.Now()
	if info.LastSeenUnix > 0 {
		ts = time.Unix(info.LastSeenUnix, 0)
	}
	return Peer{
		NodeID:        info.NodeId,
		Address:       info.Address,
		Region:        info.Region,
		Zone:          info.Zone,
		Rack:          info.Rack,
		Host:          info.Host,
		Status:        "HEALTHY",
		LastHeartbeat: ts,
	}
}

// ArtifactRecord is immutable artifact metadata.
type ArtifactRecord struct {
	ArtifactID   string `json:"artifactId"`
	Name         string `json:"name"`
	Version      string `json:"version"`
	ManifestJSON []byte `json:"manifestJson"`
}

// Store is the durable tracker metadata interface. Drivers register by name.
type Store interface {
	Name() string
	Ping(ctx context.Context) error
	Close() error

	UpsertPeer(ctx context.Context, peer Peer) error
	Heartbeat(ctx context.Context, nodeID string) (bool, error)
	GetPeer(ctx context.Context, nodeID string) (*Peer, error)
	ListPeers(ctx context.Context, expiry time.Duration) ([]Peer, error)
	DeregisterPeer(ctx context.Context, nodeID string) error
	PruneExpiredPeers(ctx context.Context, expiry time.Duration) (int, error)

	PutArtifact(ctx context.Context, rec ArtifactRecord) error
	GetArtifact(ctx context.Context, artifactID string) (*ArtifactRecord, error)

	ReportSeed(ctx context.Context, artifactID, nodeID string) error
	ListSeeds(ctx context.Context, artifactID string) ([]string, error)

	ReportChunks(ctx context.Context, nodeID string, hashes []string) (int64, error)
	LocateChunkNodes(ctx context.Context, hash string) ([]string, error)
	LocateChunkNodesBatch(ctx context.Context, hashes []string) (map[string][]string, error)
	GetPeersBatch(ctx context.Context, nodeIDs []string) (map[string]*Peer, error)
}

type opener func(opts Options) (Store, error)

var (
	mu      sync.RWMutex
	openers = map[string]opener{}
)

// Options is passed to every Store driver. New backends must honor Pool
// (zero values mean driver defaults).
type Options struct {
	DSN  string
	Pool Pool
}

// Pool is the SQL/connection pool for a store driver.
type Pool struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Register a store driver. New backends must accept Options (DSN + Pool)
// and treat a zero Pool as driver-specific defaults.
func Register(name string, fn opener) {
	mu.Lock()
	defer mu.Unlock()
	openers[name] = fn
}

// Open constructs a Store. Unknown drivers return an error so new backends can be added later.
func Open(driver string, opts Options) (Store, error) {
	if driver == "" {
		driver = "memory"
	}
	mu.RLock()
	fn, ok := openers[driver]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown store driver %q (registered: memory, sqlite, postgres)", driver)
	}
	return fn(opts)
}

func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func unmarshalPeer(b []byte) (*Peer, error) {
	var p Peer
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func unmarshalArtifact(b []byte) (*ArtifactRecord, error) {
	var a ArtifactRecord
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	return &a, nil
}

func marshalStrings(ss []string) []byte {
	b, _ := json.Marshal(ss)
	return b
}

func unmarshalStrings(b []byte) []string {
	var ss []string
	_ = json.Unmarshal(b, &ss)
	return ss
}
