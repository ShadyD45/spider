package tracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"spider/api/v1/proto"
	"spider/pkg/store"
)

func BenchmarkLocateChunks25k(b *testing.B) {
	st := store.NewMemory()
	ctx := context.Background()
	const peers = 8
	for i := 0; i < peers; i++ {
		id := fmt.Sprintf("node-%d", i)
		_ = st.UpsertPeer(ctx, store.Peer{
			NodeID: id, Address: fmt.Sprintf("10.0.0.%d:1", i+1),
			Status: "HEALTHY", LastHeartbeat: time.Now(),
		})
	}
	hashes := make([]string, 25000)
	for i := 0; i < 25000; i++ {
		h := fmt.Sprintf("sha256:%064d", i)
		hashes[i] = h
		_, _ = st.ReportChunks(ctx, fmt.Sprintf("node-%d", i%peers), []string{h})
	}
	reg := NewRegistryWithStore(st, time.Minute)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		locs := reg.LocateChunks(ctx, "req", hashes)
		if len(locs) != len(hashes) {
			b.Fatalf("got %d", len(locs))
		}
	}
}

func TestLocateChunksBatchFindsPeers(t *testing.T) {
	reg := NewRegistry(time.Minute)
	ctx := context.Background()
	p := &proto.PeerInfo{NodeId: "n1", Address: "10.0.0.1:1"}
	reg.RegisterPeer(ctx, p)
	reg.ReportChunks(ctx, "n1", []string{"sha256:aaa", "sha256:bbb"})
	locs := reg.LocateChunks(ctx, "req", []string{"sha256:aaa", "sha256:bbb", "sha256:ccc"})
	if len(locs) != 3 {
		t.Fatalf("locs %d", len(locs))
	}
	if len(locs[0].Peers) != 1 || len(locs[2].Peers) != 0 {
		t.Fatalf("unexpected peers %+v", locs)
	}
}
