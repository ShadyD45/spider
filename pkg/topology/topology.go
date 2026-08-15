package topology

import (
	"sort"
	"strings"

	"spider/api/v1/proto"
)

const (
	ScoreSameHost   = 0
	ScoreSameRack   = 10
	ScoreSameZone   = 20
	ScoreSameRegion = 30
	ScoreRemote     = 40
)

// Locality holds topological domain identifiers.
type Locality struct {
	Region string
	Zone   string
	Rack   string
	Host   string
}

// FromProto extracts Locality from protobuf PeerInfo.
func FromProto(p *proto.PeerInfo) Locality {
	if p == nil {
		return Locality{}
	}
	return Locality{
		Region: strings.TrimSpace(p.Region),
		Zone:   strings.TrimSpace(p.Zone),
		Rack:   strings.TrimSpace(p.Rack),
		Host:   strings.TrimSpace(p.Host),
	}
}

// Distance calculates a numerical topology penalty score between two localities.
// Lower score = closer proximity = faster transfer bandwidth & lower latency.
func Distance(a, b Locality) int {
	if a.Host != "" && b.Host != "" && strings.EqualFold(a.Host, b.Host) {
		return ScoreSameHost
	}
	if a.Rack != "" && b.Rack != "" && strings.EqualFold(a.Rack, b.Rack) {
		return ScoreSameRack
	}
	if a.Zone != "" && b.Zone != "" && strings.EqualFold(a.Zone, b.Zone) {
		return ScoreSameZone
	}
	if a.Region != "" && b.Region != "" && strings.EqualFold(a.Region, b.Region) {
		return ScoreSameRegion
	}
	return ScoreRemote
}

// SortPeersByProximity sorts a slice of proto.PeerInfo in-place from closest to furthest relative to self.
func SortPeersByProximity(self Locality, peers []*proto.PeerInfo) {
	sort.SliceStable(peers, func(i, j int) bool {
		distI := Distance(self, FromProto(peers[i]))
		distJ := Distance(self, FromProto(peers[j]))
		if distI == distJ {
			// Deterministic secondary sort on NodeId
			return peers[i].NodeId < peers[j].NodeId
		}
		return distI < distJ
	})
}
