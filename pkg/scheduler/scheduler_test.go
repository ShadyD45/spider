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
	if got[0] != "c" || got[1] != "b" || got[2] != "a" {
		t.Fatalf("got %v", got)
	}
}

func TestCircuitBreaker(t *testing.T) {
	s := New(2)
	addr := "10.0.0.1:1"
	for i := 0; i < 3; i++ {
		s.End(addr, time.Millisecond, false)
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
	s.End("a", time.Millisecond, true)
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
	s.End("a", time.Millisecond, true)
	if !s.Begin("a") {
		t.Fatal("after end")
	}
}
