package tracker

import (
	"context"
	"testing"
	"time"

	"spider/api/v1/proto"
)

func TestTrackerRegistryAndRanking(t *testing.T) {
	reg := NewRegistry(5 * time.Second)

	node1 := &proto.PeerInfo{
		NodeId:  "node-1",
		Address: "127.0.0.1:5001",
		Region:  "us-east-1",
		Zone:    "zone-a",
		Rack:    "rack-1",
		Host:    "host-1",
	}

	node2 := &proto.PeerInfo{
		NodeId:  "node-2",
		Address: "127.0.0.1:5002",
		Region:  "us-east-1",
		Zone:    "zone-a",
		Rack:    "rack-2",
		Host:    "host-2",
	}

	requester := &proto.PeerInfo{
		NodeId:  "req-node",
		Address: "127.0.0.1:5003",
		Region:  "us-east-1",
		Zone:    "zone-a",
		Rack:    "rack-1", // same rack as node-1
		Host:    "host-3",
	}

	reg.RegisterPeer(node1)
	reg.RegisterPeer(node2)
	reg.RegisterPeer(requester)

	chunkHash := "sha256:1111111111111111111111111111111111111111111111111111111111111111"

	// Both node1 and node2 have chunkHash
	reg.ReportChunks("node-1", []string{chunkHash})
	reg.ReportChunks("node-2", []string{chunkHash})

	locs := reg.LocateChunks("req-node", []string{chunkHash})
	if len(locs) != 1 {
		t.Fatalf("Expected 1 location entry, got %d", len(locs))
	}

	peers := locs[0].Peers
	if len(peers) != 2 {
		t.Fatalf("Expected 2 candidate peers, got %d", len(peers))
	}

	// node-1 is same rack as req-node, so it must be ranked first before node-2 (different rack)
	if peers[0].NodeId != "node-1" {
		t.Fatalf("Expected node-1 ranked first due to rack affinity, got %s", peers[0].NodeId)
	}

	// Test heartbeats and pruning
	shortReg := NewRegistry(50 * time.Millisecond)
	shortReg.RegisterPeer(node1)
	time.Sleep(100 * time.Millisecond)
	pruned := shortReg.PruneDeadPeers()
	if pruned != 1 {
		t.Fatalf("Expected 1 pruned peer, got %d", pruned)
	}
	if len(shortReg.ListActivePeers()) != 0 {
		t.Fatal("Expected 0 active peers after pruning")
	}
}

func TestTrackerServerRPC(t *testing.T) {
	reg := NewRegistry(10 * time.Second)
	srv := NewServer(reg)
	ctx := context.Background()

	regResp, err := srv.RegisterPeer(ctx, &proto.RegisterPeerRequest{
		Peer: &proto.PeerInfo{
			NodeId:  "test-worker",
			Address: "10.0.0.2:50052",
		},
	})
	if err != nil || !regResp.Success {
		t.Fatalf("RegisterPeer RPC failed: %v, resp: %+v", err, regResp)
	}

	hbResp, err := srv.Heartbeat(ctx, &proto.HeartbeatRequest{NodeId: "test-worker"})
	if err != nil || !hbResp.Acknowledged {
		t.Fatalf("Heartbeat RPC failed: %v, resp: %+v", err, hbResp)
	}

	repResp, err := srv.ReportChunks(ctx, &proto.ReportChunksRequest{
		NodeId:      "test-worker",
		ChunkHashes: []string{"sha256:abc"},
	})
	if err != nil || repResp.ChunksRecorded != 1 {
		t.Fatalf("ReportChunks RPC failed: %v, resp: %+v", err, repResp)
	}

	peersResp, err := srv.ListPeers(ctx, &proto.ListPeersRequest{})
	if err != nil || len(peersResp.Peers) != 1 {
		t.Fatalf("ListPeers RPC failed: %v, resp: %+v", err, peersResp)
	}
}
