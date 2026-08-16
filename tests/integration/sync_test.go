package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/engine"
	"spider/pkg/materializer"
	"spider/pkg/peer"
	"spider/pkg/source"
	"spider/pkg/tracker"
)

func TestCachedOwnershipReconciledForPeerFetch(t *testing.T) {
	ctx := context.Background()
	trReg := tracker.NewRegistry(10 * time.Second)
	trSrv := tracker.NewServer(trReg)
	trLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcTracker := grpc.NewServer()
	proto.RegisterTrackerServiceServer(grpcTracker, trSrv)
	go func() { _ = grpcTracker.Serve(trLis) }()
	defer grpcTracker.Stop()

	trConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", trLis.Addr().(*net.TCPAddr).Port), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer trConn.Close()
	trClient := proto.NewTrackerServiceClient(trConn)

	originDir := t.TempDir()
	data := []byte("integration-test-payload-1234567890")
	if err := os.WriteFile(filepath.Join(originDir, "model.bin"), data, 0644); err != nil {
		t.Fatal(err)
	}
	originSrc, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}

	seederCache, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pub := engine.NewPublisher(seederCache, 32)
	manifest, err := pub.Publish(ctx, originSrc, "", "int-model", "1.0")
	if err != nil {
		t.Fatal(err)
	}
	hashes := manifest.AllChunkHashes()

	seederLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSeeder := grpc.NewServer()
	proto.RegisterPeerServiceServer(grpcSeeder, peer.NewServer("seeder", seederCache, nil))
	go func() { _ = grpcSeeder.Serve(seederLis) }()
	defer grpcSeeder.Stop()
	seederAddr := fmt.Sprintf("127.0.0.1:%d", seederLis.Addr().(*net.TCPAddr).Port)

	_, _ = trClient.RegisterPeer(ctx, &proto.RegisterPeerRequest{Peer: &proto.PeerInfo{NodeId: "seeder", Address: seederAddr}})
	_, _ = trClient.RegisterPeer(ctx, &proto.RegisterPeerRequest{Peer: &proto.PeerInfo{NodeId: "leecher", Address: "127.0.0.1:9"}})

	leecherCache, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hashes {
		r, size, err := seederCache.GetChunkReader(h)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, size)
		_, _ = r.Read(buf)
		_ = r.Close()
		if err := leecherCache.PutChunk(h, buf); err != nil {
			t.Fatal(err)
		}
	}

	leecherEng := engine.NewEngine(engine.Config{
		NodeID:        "leecher",
		Cache:         leecherCache,
		TrackerClient: trClient,
		Materializer:  materializer.NewMaterializer(materializer.DefaultOptions()),
	})

	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := leecherEng.Sync(ctx, "job-int", manifest, dest, nil); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)
	loc, err := trClient.LocateChunks(ctx, &proto.LocateChunksRequest{
		RequesterNodeId: "other",
		ChunkHashes:     hashes,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range loc.GetLocations() {
		foundLeecher := false
		for _, p := range l.GetPeers() {
			if p.GetNodeId() == "leecher" {
				foundLeecher = true
			}
		}
		if !foundLeecher {
			t.Fatalf("expected leecher advertised for chunk %s", l.GetChunkHash())
		}
	}
}

func TestPeerFailureFallsBackToOrigin(t *testing.T) {
	ctx := context.Background()
	trReg := tracker.NewRegistry(10 * time.Second)
	trSrv := tracker.NewServer(trReg)
	trLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcTracker := grpc.NewServer()
	proto.RegisterTrackerServiceServer(grpcTracker, trSrv)
	go func() { _ = grpcTracker.Serve(trLis) }()
	defer grpcTracker.Stop()

	trConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", trLis.Addr().(*net.TCPAddr).Port), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer trConn.Close()
	trClient := proto.NewTrackerServiceClient(trConn)

	originDir := t.TempDir()
	data := []byte("origin-fallback-payload-1234567890")
	if err := os.WriteFile(filepath.Join(originDir, "model.bin"), data, 0644); err != nil {
		t.Fatal(err)
	}
	originSrc, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}

	pubCache, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := engine.NewPublisher(pubCache, 32).Publish(ctx, originSrc, "", "fb-model", "1.0")
	if err != nil {
		t.Fatal(err)
	}
	hashes := manifest.AllChunkHashes()

	// Register a bad peer that does not serve chunks.
	_, _ = trClient.RegisterPeer(ctx, &proto.RegisterPeerRequest{Peer: &proto.PeerInfo{NodeId: "bad-peer", Address: "127.0.0.1:1"}})
	_, _ = trClient.ReportChunks(ctx, &proto.ReportChunksRequest{NodeId: "bad-peer", ChunkHashes: hashes})

	leecherCache, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.NewEngine(engine.Config{
		NodeID:        "leecher",
		Cache:         leecherCache,
		TrackerClient: trClient,
		Materializer:  materializer.NewMaterializer(materializer.DefaultOptions()),
	})

	dest := filepath.Join(t.TempDir(), "dest")
	metrics, err := eng.Sync(ctx, "job-fb", manifest, dest, originSrc)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.OriginChunks == 0 {
		t.Fatal("expected origin fallback after peer failure")
	}
}

func TestCrashRecoveryResumesPartial(t *testing.T) {
	ctx := context.Background()
	originDir := t.TempDir()
	data := []byte("0123456789ABCDEF" + "GHIJKLMNOPQRSTUV")
	if err := os.WriteFile(filepath.Join(originDir, "model.bin"), data, 0644); err != nil {
		t.Fatal(err)
	}
	originSrc, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}

	c, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := engine.NewPublisher(c, 16).Publish(ctx, originSrc, "", "resume", "1.0")
	if err != nil {
		t.Fatal(err)
	}
	hashes := manifest.AllChunkHashes()
	if err := c.AppendPartial(hashes[0], bytes.NewReader(data[:16])); err != nil {
		t.Fatal(err)
	}

	eng := engine.NewEngine(engine.Config{
		NodeID:       "resume-node",
		Cache:        c,
		Materializer: materializer.NewMaterializer(materializer.DefaultOptions()),
	})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := eng.Sync(ctx, "job-resume", manifest, dest, originSrc); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "model.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("recovered artifact mismatch")
	}
}

// Ensure unused import guard for sync in file compiles.
var _ = sync.Map{}
