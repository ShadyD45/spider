package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"spider/api/v1/proto"
	"spider/pkg/topology"
)

const (
	untrustedAfter = 3
	untrustedFor   = 15 * time.Minute
	ewmaAlpha      = 1.0 / 8.0
)

// Stats records EWMA latency, throughput, and success for a peer address.
type Stats struct {
	RTT         time.Duration
	Throughput  float64 // bytes per second EWMA
	Inflight    int
	Failures    int
	UntrustedAt time.Time
}

// Scheduler ranks peers and tracks circuit breakers / inflight caps.
type Scheduler struct {
	mu          sync.Mutex
	stats       map[string]*Stats
	maxInflight int
}

func New(maxInflight int) *Scheduler {
	if maxInflight <= 0 {
		maxInflight = 4
	}
	return &Scheduler{stats: make(map[string]*Stats), maxInflight: maxInflight}
}

func (s *Scheduler) stat(addr string) *Stats {
	st, ok := s.stats[addr]
	if !ok {
		st = &Stats{}
		s.stats[addr] = st
	}
	return st
}

// RankPeers orders candidates: locality, inflight, throughput, RTT. Untrusted peers are omitted unless none remain.
func (s *Scheduler) RankPeers(self topology.Locality, peers []*proto.PeerInfo) []*proto.PeerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var trusted, untrusted []*proto.PeerInfo
	for _, p := range peers {
		if p == nil || p.Address == "" {
			continue
		}
		st := s.stat(p.Address)
		if !st.UntrustedAt.IsZero() && now.Sub(st.UntrustedAt) < untrustedFor {
			untrusted = append(untrusted, p)
			continue
		}
		if !st.UntrustedAt.IsZero() && now.Sub(st.UntrustedAt) >= untrustedFor {
			st.UntrustedAt = time.Time{}
			st.Failures = 0
		}
		trusted = append(trusted, p)
	}
	pool := trusted
	if len(pool) == 0 {
		pool = untrusted
	}
	s.sortLoad(self, pool)
	return pool
}

func (s *Scheduler) sortLoad(self topology.Locality, peers []*proto.PeerInfo) {
	sort.SliceStable(peers, func(i, j int) bool {
		di := topology.Distance(self, topology.FromProto(peers[i]))
		dj := topology.Distance(self, topology.FromProto(peers[j]))
		if di != dj {
			return di < dj
		}
		si := s.stat(peers[i].Address)
		sj := s.stat(peers[j].Address)
		if si.Inflight != sj.Inflight {
			return si.Inflight < sj.Inflight
		}
		if si.Throughput != sj.Throughput {
			return si.Throughput > sj.Throughput
		}
		if si.RTT != sj.RTT {
			return si.RTT < sj.RTT
		}
		return peers[i].NodeId < peers[j].NodeId
	})
}

func (s *Scheduler) Begin(addr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stat(addr)
	if st.Inflight >= s.maxInflight {
		return false
	}
	st.Inflight++
	return true
}

// WaitBegin acquires an inflight slot, blocking until one is free or ctx is cancelled.
func (s *Scheduler) WaitBegin(ctx context.Context, addr string) bool {
	for {
		if s.Begin(addr) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (s *Scheduler) End(addr string, nbytes int64, d time.Duration, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stat(addr)
	if st.Inflight > 0 {
		st.Inflight--
	}
	if d > 0 {
		if st.RTT == 0 {
			st.RTT = d
		} else {
			st.RTT = time.Duration(float64(st.RTT)*(1-ewmaAlpha) + float64(d)*ewmaAlpha)
		}
		if nbytes > 0 {
			sample := float64(nbytes) / d.Seconds()
			if st.Throughput == 0 {
				st.Throughput = sample
			} else {
				st.Throughput = st.Throughput*(1-ewmaAlpha) + sample*ewmaAlpha
			}
		}
	}
	if ok {
		st.Failures = 0
		return
	}
	st.Failures++
	if st.Failures >= untrustedAfter {
		st.UntrustedAt = time.Now()
	}
}

func (s *Scheduler) IsUntrusted(addr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stat(addr)
	return !st.UntrustedAt.IsZero() && time.Since(st.UntrustedAt) < untrustedFor
}

// RarestFirst orders work hashes by fewest known peers (missing with 0 peers last so origin can fill).
func RarestFirst(hashes []string, locations map[string][]*proto.PeerInfo) []string {
	type item struct {
		h string
		n int
	}
	items := make([]item, 0, len(hashes))
	for _, h := range hashes {
		items = append(items, item{h: h, n: len(locations[h])})
	}
	for i := 1; i < len(items); i++ {
		j := i
		for j > 0 && items[j].n < items[j-1].n {
			items[j], items[j-1] = items[j-1], items[j]
			j--
		}
	}
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.h
	}
	return out
}
