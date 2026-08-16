package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"spider/pkg/metacache"
)

func TestMemoryAndCachedInvalidation(t *testing.T) {
	ctx := context.Background()
	inner := NewMemory()
	mc := metacache.NewMemory(time.Minute)
	st := Wrap(inner, mc, time.Minute)

	peer := Peer{NodeID: "n1", Address: "127.0.0.1:1", Rack: "r1", Status: "HEALTHY", LastHeartbeat: time.Now()}
	if err := st.UpsertPeer(ctx, peer); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetPeer(ctx, "n1")
	if err != nil || got == nil || got.Address != "127.0.0.1:1" {
		t.Fatalf("get: %v %+v", err, got)
	}
	// mutate inner without going through cache to prove cache hit would be stale if we skipped invalidation
	peer.Address = "127.0.0.1:2"
	if err := st.UpsertPeer(ctx, peer); err != nil {
		t.Fatal(err)
	}
	got, err = st.GetPeer(ctx, "n1")
	if err != nil || got == nil || got.Address != "127.0.0.1:2" {
		t.Fatalf("expected invalidation, got %+v err=%v", got, err)
	}

	if err := st.PutArtifact(ctx, ArtifactRecord{ArtifactID: "sha256:aa", Name: "m", Version: "1", ManifestJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReportSeed(ctx, "sha256:aa", "n1"); err != nil {
		t.Fatal(err)
	}
	seeds, _ := st.ListSeeds(ctx, "sha256:aa")
	if len(seeds) != 1 {
		t.Fatalf("seeds %v", seeds)
	}
	if err := st.DeregisterPeer(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	seeds, _ = st.ListSeeds(ctx, "sha256:aa")
	for _, id := range seeds {
		if id == "n1" {
			t.Fatal("seed should be gone after deregister")
		}
	}
}

func TestSQLitePersistence(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/t.db"
	st, err := Open("sqlite", Options{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPeer(ctx, Peer{NodeID: "n1", Address: "a:1", LastHeartbeat: time.Now(), Status: "HEALTHY"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReportChunks(ctx, "n1", []string{"sha256:abc"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	st2, err := Open("sqlite", Options{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	p, err := st2.GetPeer(ctx, "n1")
	if err != nil || p == nil {
		t.Fatalf("peer missing after reopen: %v", err)
	}
	ids, err := st2.LocateChunkNodes(ctx, "sha256:abc")
	if err != nil || len(ids) != 1 {
		t.Fatalf("chunks %v %v", ids, err)
	}
	batch, err := st2.LocateChunkNodesBatch(ctx, []string{"sha256:abc", "sha256:missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch["sha256:abc"]) != 1 {
		t.Fatalf("batch locate %v", batch)
	}
	peers, err := st2.GetPeersBatch(ctx, []string{"n1", "missing"})
	if err != nil || peers["n1"] == nil {
		t.Fatalf("batch peers %v %v", peers, err)
	}
}

func TestPostgresStoreOptional(t *testing.T) {
	dsn := os.Getenv("SPIDER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SPIDER_TEST_POSTGRES_DSN to run postgres store tests")
	}
	ctx := context.Background()
	st, err := Open("postgres", Options{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("pg-test-%d", time.Now().UnixNano())
	if err := st.UpsertPeer(ctx, Peer{NodeID: id, Address: "a:1", LastHeartbeat: time.Now(), Status: "HEALTHY"}); err != nil {
		t.Fatal(err)
	}
	p, err := st.GetPeer(ctx, id)
	if err != nil || p == nil {
		t.Fatalf("postgres peer: %v %+v", err, p)
	}
	_ = st.DeregisterPeer(ctx, id)
}
