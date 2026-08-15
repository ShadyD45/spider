package topology

import (
	"testing"

	"spider/api/v1/proto"
)

func TestTopologyScoringAndSorting(t *testing.T) {
	self := Locality{
		Region: "us-east-1",
		Zone:   "us-east-1a",
		Rack:   "rack-1",
		Host:   "node-01",
	}

	peerSameHost := &proto.PeerInfo{NodeId: "p1", Region: "us-east-1", Zone: "us-east-1a", Rack: "rack-1", Host: "node-01"}
	peerSameRack := &proto.PeerInfo{NodeId: "p2", Region: "us-east-1", Zone: "us-east-1a", Rack: "rack-1", Host: "node-02"}
	peerSameZone := &proto.PeerInfo{NodeId: "p3", Region: "us-east-1", Zone: "us-east-1a", Rack: "rack-2", Host: "node-03"}
	peerSameRegion := &proto.PeerInfo{NodeId: "p4", Region: "us-east-1", Zone: "us-east-1b", Rack: "rack-3", Host: "node-04"}
	peerRemote := &proto.PeerInfo{NodeId: "p5", Region: "eu-west-1", Zone: "eu-west-1a", Rack: "rack-9", Host: "node-99"}

	peers := []*proto.PeerInfo{peerRemote, peerSameZone, peerSameHost, peerSameRegion, peerSameRack}

	SortPeersByProximity(self, peers)

	expectedOrder := []string{"p1", "p2", "p3", "p4", "p5"}
	for i, expectedID := range expectedOrder {
		if peers[i].NodeId != expectedID {
			t.Errorf("At index %d: expected %s, got %s", i, expectedID, peers[i].NodeId)
		}
	}
}
