package engine

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/materializer"
	"spider/pkg/peer"
	"spider/pkg/source"
	"spider/pkg/tracker"
)

func TestEngineE2E_P2PFanoutAndFallback(t *testing.T) {
	ctx := context.Background()

	// 1. Setup Tracker Server
	trReg := tracker.NewRegistry(10 * time.Second)
	trSrv := tracker.NewServer(trReg)

	trLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	trPort := trLis.Addr().(*net.TCPAddr).Port
	grpcTracker := grpc.NewServer()
	proto.RegisterTrackerServiceServer(grpcTracker, trSrv)
	go func() { _ = grpcTracker.Serve(trLis) }()
	defer grpcTracker.Stop()

	trackerAddr := fmt.Sprintf("127.0.0.1:%d", trPort)
	trConn, err := grpc.Dial(trackerAddr, grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer trConn.Close()
	trClient := proto.NewTrackerServiceClient(trConn)

	// 2. Setup Origin Filesystem
	originDir := t.TempDir()
	testFileData := []byte("1234567890ABCDEF" + "GHIJKLMNOPQRSTUV") // 32 bytes with 2 distinct chunks
	if err := os.WriteFile(filepath.Join(originDir, "model.bin"), testFileData, 0644); err != nil {
		t.Fatal(err)
	}
	originSrc, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Worker 1 (Seeder): Publishes artifact, caches chunks, starts peer server, registers with Tracker
	w1Dir := t.TempDir()
	w1Cache, err := cache.NewCache(filepath.Join(w1Dir, "cache"))
	if err != nil {
		t.Fatal(err)
	}

	pub := NewPublisher(w1Cache, 16) // 16-byte chunks (total 2 chunks for 32 bytes)
	manifest, err := pub.Publish(ctx, originSrc, "", "test-model", "1.0")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	w1Lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w1Port := w1Lis.Addr().(*net.TCPAddr).Port
	w1PeerSrv := peer.NewServer("worker-1", w1Cache, nil)
	grpcW1 := grpc.NewServer()
	proto.RegisterPeerServiceServer(grpcW1, w1PeerSrv)
	go func() { _ = grpcW1.Serve(w1Lis) }()
	defer grpcW1.Stop()

	w1Addr := fmt.Sprintf("127.0.0.1:%d", w1Port)
	_, err = trClient.RegisterPeer(ctx, &proto.RegisterPeerRequest{
		Peer: &proto.PeerInfo{
			NodeId:  "worker-1",
			Address: w1Addr,
			Rack:    "rack-1",
			Zone:    "zone-a",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Worker 1 reports chunks to Tracker
	_, err = trClient.ReportChunks(ctx, &proto.ReportChunksRequest{
		NodeId:      "worker-1",
		ChunkHashes: manifest.AllChunkHashes(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// 4. Worker 2 (Leecher): Syncs artifact from Worker 1 via P2P
	w2Dir := t.TempDir()
	w2Cache, err := cache.NewCache(filepath.Join(w2Dir, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	w2Dest := filepath.Join(w2Dir, "materialized")

	w2Engine := NewEngine(Config{
		NodeID:        "worker-2",
		Cache:         w2Cache,
		TrackerClient: trClient,
		Materializer:  materializer.NewMaterializer(materializer.DefaultOptions()),
	})

	// Sync with origin provided as fallback (should pull 100% from worker-1 peer!)
	metrics, err := w2Engine.Sync(ctx, "job-1", manifest, w2Dest, originSrc)
	if err != nil {
		t.Fatalf("Worker 2 sync failed: %v", err)
	}

	if metrics.PeerChunks != 2 {
		t.Fatalf("Expected 2 chunks from peer, got %d", metrics.PeerChunks)
	}
	if metrics.OriginChunks != 0 {
		t.Fatalf("Expected 0 chunks from origin, got %d", metrics.OriginChunks)
	}

	// Verify materialized file in worker 2
	w2Content, err := os.ReadFile(filepath.Join(w2Dest, "model.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(w2Content) != string(testFileData) {
		t.Fatalf("Materialized content mismatch: got %q, expected %q", string(w2Content), string(testFileData))
	}

	// 5. Worker 3 (Leecher with Worker 1 stopped -> Fallback to Origin)
	grpcW1.Stop() // Terminate worker 1 to test fallback!

	w3Dir := t.TempDir()
	w3Cache, err := cache.NewCache(filepath.Join(w3Dir, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	w3Dest := filepath.Join(w3Dir, "materialized")

	w3Engine := NewEngine(Config{
		NodeID:        "worker-3",
		Cache:         w3Cache,
		TrackerClient: trClient,
	})

	metrics3, err := w3Engine.Sync(ctx, "job-2", manifest, w3Dest, originSrc)
	if err != nil {
		t.Fatalf("Worker 3 sync failed during fallback: %v", err)
	}

	if metrics3.OriginChunks != 2 {
		t.Fatalf("Expected 2 chunks from origin fallback, got %d", metrics3.OriginChunks)
	}

	w3Content, err := os.ReadFile(filepath.Join(w3Dest, "model.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(w3Content) != string(testFileData) {
		t.Fatalf("Fallback materialized content mismatch: got %q, expected %q", string(w3Content), string(testFileData))
	}
}

// corruptPeerServer simulates a degraded peer that serves corrupted byte payloads
type corruptPeerServer struct {
	proto.UnimplementedPeerServiceServer
}

func (s *corruptPeerServer) GetChunk(req *proto.GetChunkRequest, stream proto.PeerService_GetChunkServer) error {
	// Return intentionally corrupted payload
	badData := []byte("CORRUPTED_BYTES_INJECTED_FOR_TESTING!!")
	return stream.Send(&proto.ChunkDataChunk{
		Payload:   badData,
		Offset:    0,
		TotalSize: int64(len(badData)),
		IsEof:     true,
	})
}

func TestEngineCorruptChunkRejectionAndRecovery(t *testing.T) {
	ctx := context.Background()

	// 1. Setup Tracker
	trReg := tracker.NewRegistry(10 * time.Second)
	trSrv := tracker.NewServer(trReg)
	trLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	trPort := trLis.Addr().(*net.TCPAddr).Port
	grpcTracker := grpc.NewServer()
	proto.RegisterTrackerServiceServer(grpcTracker, trSrv)
	go func() { _ = grpcTracker.Serve(trLis) }()
	defer grpcTracker.Stop()

	trConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", trPort), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer trConn.Close()
	trClient := proto.NewTrackerServiceClient(trConn)

	// 2. Setup Origin
	originDir := t.TempDir()
	cleanData := []byte("CleanVerifiedBytesFromOriginStore!!")
	if err := os.WriteFile(filepath.Join(originDir, "data.bin"), cleanData, 0644); err != nil {
		t.Fatal(err)
	}
	originSrc, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create manifest
	tempCache, _ := cache.NewCache(filepath.Join(t.TempDir(), "cache"))
	pub := NewPublisher(tempCache, int64(len(cleanData)))
	manifest, err := pub.Publish(ctx, originSrc, "", "corrupt-test", "1.0")
	if err != nil {
		t.Fatal(err)
	}

	chunkHash := manifest.AllChunkHashes()[0]

	// 3. Start Corrupt Peer
	badLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	badPort := badLis.Addr().(*net.TCPAddr).Port
	grpcBad := grpc.NewServer()
	proto.RegisterPeerServiceServer(grpcBad, &corruptPeerServer{})
	go func() { _ = grpcBad.Serve(badLis) }()
	defer grpcBad.Stop()

	// Register corrupt peer with Tracker claiming it has chunkHash
	_, _ = trClient.RegisterPeer(ctx, &proto.RegisterPeerRequest{
		Peer: &proto.PeerInfo{
			NodeId:  "bad-peer",
			Address: fmt.Sprintf("127.0.0.1:%d", badPort),
		},
	})
	_, _ = trClient.ReportChunks(ctx, &proto.ReportChunksRequest{
		NodeId:      "bad-peer",
		ChunkHashes: []string{chunkHash},
	})

	// 4. Download on worker with origin fallback enabled
	workerDir := t.TempDir()
	workerCache, err := cache.NewCache(filepath.Join(workerDir, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	workerDest := filepath.Join(workerDir, "materialized")

	workerEng := NewEngine(Config{
		NodeID:        "honest-worker",
		Cache:         workerCache,
		TrackerClient: trClient,
	})

	// Sync should reject bad-peer payload, log warning, and fetch clean chunk from originSrc!
	metrics, err := workerEng.Sync(ctx, "job-corrupt-test", manifest, workerDest, originSrc)
	if err != nil {
		t.Fatalf("Sync failed to recover from corrupt peer: %v", err)
	}

	if metrics.PeerChunks != 0 {
		t.Fatalf("Expected 0 accepted chunks from corrupt peer, got %d", metrics.PeerChunks)
	}
	if metrics.OriginChunks != 1 {
		t.Fatalf("Expected 1 chunk recovered from origin, got %d", metrics.OriginChunks)
	}

	// Verify materialized file integrity
	matContent, err := os.ReadFile(filepath.Join(workerDest, "data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(matContent) != string(cleanData) {
		t.Fatalf("Materialized content corrupted: got %q, expected %q", string(matContent), string(cleanData))
	}
}

type tamperedOrigin struct {
	inner source.Source
}

func (t *tamperedOrigin) ListFiles(ctx context.Context, prefix string) ([]source.FileInfo, error) {
	return t.inner.ListFiles(ctx, prefix)
}

func (t *tamperedOrigin) ReadChunk(ctx context.Context, path string, offset int64, size int64) ([]byte, error) {
	return []byte("ORIGIN_RETURNED_TAMPERED_BYTES!!"), nil
}

func (t *tamperedOrigin) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	return t.inner.Open(ctx, path)
}

func TestEngineRejectsCorruptOriginBytes(t *testing.T) {
	ctx := context.Background()

	originDir := t.TempDir()
	cleanData := []byte("CleanVerifiedBytesFromOriginStore!!")
	if err := os.WriteFile(filepath.Join(originDir, "data.bin"), cleanData, 0644); err != nil {
		t.Fatal(err)
	}
	honestOrigin, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}

	tempCache, err := cache.NewCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	pub := NewPublisher(tempCache, int64(len(cleanData)))
	manifest, err := pub.Publish(ctx, honestOrigin, "", "origin-tamper", "1.0")
	if err != nil {
		t.Fatal(err)
	}

	workerCache, err := cache.NewCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(Config{
		NodeID: "worker-origin-check",
		Cache:  workerCache,
	})

	_, err = eng.Sync(ctx, "job-origin-tamper", manifest, filepath.Join(t.TempDir(), "dest"), &tamperedOrigin{inner: honestOrigin})
	if err == nil {
		t.Fatal("expected sync to fail when origin returns bytes that do not match chunk hash")
	}
}

