package scheduler

import (
	"context"
	"testing"
	"time"

	"spider/api/v1/proto"
	"spider/pkg/topology"
)

func TestRarestFirst(t *testing.T) {
	locs := map[string][]*proto.PeerInfo{
		"a": {{NodeId: "1"}, {NodeId: "2"}},
		"b": {{NodeId: "1"}},
		"c": {},
	}
	got := RarestFirst([]string{"a", "b", "c"}, locs)
	if got[0] != "b" || got[1] != "a" || got[2] != "c" {
		t.Fatalf("got %v want [b a c] (rarest with peers first, 0-peer last)", got)
	}
}

func TestRarestFirstTable(t *testing.T) {
	p := func(n int) []*proto.PeerInfo {
		out := make([]*proto.PeerInfo, n)
		for i := 0; i < n; i++ {
			out[i] = &proto.PeerInfo{NodeId: string(rune('A' + i))}
		}
		return out
	}
	got := RarestFirst([]string{"zero", "one", "three", "alsoZero"}, map[string][]*proto.PeerInfo{
		"zero":     {},
		"one":      p(1),
		"three":    p(3),
		"alsoZero": nil,
	})
	if got[0] != "one" || got[1] != "three" {
		t.Fatalf("expected 1-peer then 3-peer first, got %v", got)
	}
	if got[2] != "zero" && got[2] != "alsoZero" {
		t.Fatalf("expected 0-peer hashes last, got %v", got)
	}
	if got[3] != "zero" && got[3] != "alsoZero" {
		t.Fatalf("expected 0-peer hashes last, got %v", got)
	}
}

func TestCircuitBreaker(t *testing.T) {
	s := New(2)
	addr := "10.0.0.1:1"
	for i := 0; i < 3; i++ {
		s.End(addr, 0, time.Millisecond, false)
	}
	if !s.IsUntrusted(addr) {
		t.Fatal("expected untrusted after 3 failures")
	}
	ranked := s.RankPeers(topology.Locality{Rack: "r1"}, []*proto.PeerInfo{
		{NodeId: "bad", Address: addr, Rack: "r1"},
		{NodeId: "good", Address: "10.0.0.2:1", Rack: "r1"},
	})
	if ranked[0].NodeId != "good" {
		t.Fatalf("expected good first, got %s", ranked[0].NodeId)
	}
}

func TestWaitBegin(t *testing.T) {
	s := New(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !s.WaitBegin(ctx, "a") {
		t.Fatal("first wait begin")
	}
	acquired := make(chan struct{})
	go func() {
		if s.WaitBegin(ctx, "a") {
			close(acquired)
		}
	}()
	select {
	case <-acquired:
		t.Fatal("should block while inflight full")
	case <-time.After(50 * time.Millisecond):
	}
	s.End("a", 0, time.Millisecond, true)
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("expected second acquire after end")
	}
}

func TestInflightCap(t *testing.T) {
	s := New(1)
	if !s.Begin("a") {
		t.Fatal("first begin")
	}
	if s.Begin("a") {
		t.Fatal("second begin should fail")
	}
	s.End("a", 0, time.Millisecond, true)
	if !s.Begin("a") {
		t.Fatal("after end")
	}
}

func TestRankPeersPrefersLowerInflight(t *testing.T) {
	s := New(8)
	busy := "10.0.0.1:1"
	idle := "10.0.0.2:1"
	s.Begin(busy)
	ranked := s.RankPeers(topology.Locality{Rack: "r1"}, []*proto.PeerInfo{
		{NodeId: "busy", Address: busy, Rack: "r1"},
		{NodeId: "idle", Address: idle, Rack: "r1"},
	})
	if ranked[0].NodeId != "idle" {
		t.Fatalf("expected idle first, got %s", ranked[0].NodeId)
	}
}

func TestRankPeersPrefersHigherThroughput(t *testing.T) {
	s := New(8)
	fast := "10.0.0.1:1"
	slow := "10.0.0.2:1"
	for i := 0; i < 5; i++ {
		s.End(fast, 10*1024*1024, 100*time.Millisecond, true)
		s.End(slow, 100*1024, 100*time.Millisecond, true)
	}
	ranked := s.RankPeers(topology.Locality{Rack: "r1"}, []*proto.PeerInfo{
		{NodeId: "slow", Address: slow, Rack: "r1"},
		{NodeId: "fast", Address: fast, Rack: "r1"},
	})
	if ranked[0].NodeId != "fast" {
		t.Fatalf("expected fast peer first, got %s", ranked[0].NodeId)
	}
}

func TestThroughputEWMAConverges(t *testing.T) {
	s := New(8)
	addr := "10.0.0.1:1"
	for i := 0; i < 10; i++ {
		s.End(addr, 1024*1024, 100*time.Millisecond, true)
	}
	s.mu.Lock()
	tp := s.stats[addr].Throughput
	s.mu.Unlock()
	want := float64(1024*1024) / 0.1
	if tp < want*0.5 || tp > want*1.5 {
		t.Fatalf("EWMA throughput %f, want near %f", tp, want)
	}
}
