package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/config"
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

func (t *tamperedOrigin) ReadChunkTo(ctx context.Context, path string, offset int64, size int64, w io.Writer) (int64, error) {
	data := []byte("ORIGIN_RETURNED_TAMPERED_BYTES!!")
	n, err := w.Write(data)
	return int64(n), err
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

func TestEngineCancelsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	originDir := t.TempDir()
	data := []byte("0123456789abcdef0123456789abcdef")
	if err := os.WriteFile(filepath.Join(originDir, "f.bin"), data, 0644); err != nil {
		t.Fatal(err)
	}
	src, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pub := NewPublisher(c, 16)
	manifest, err := pub.Publish(context.Background(), src, "", "cancel", "1")
	if err != nil {
		t.Fatal(err)
	}
	empty, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(Config{NodeID: "c", Cache: empty})
	_, err = eng.Sync(ctx, "job-cancel", manifest, filepath.Join(t.TempDir(), "d"), src)
	if err == nil {
		t.Fatal("expected canceled sync")
	}
}

func TestAdvertiserFlushesBeforeStop(t *testing.T) {
	var mu sync.Mutex
	var reports [][]string
	client := &reportSpy{onReport: func(hashes []string) {
		mu.Lock()
		reports = append(reports, append([]string(nil), hashes...))
		mu.Unlock()
	}}
	ad := newAdvertiser(client, "node-a", config.AdvertisementConfig{
		BatchSize:    4,
		Interval:     50 * time.Millisecond,
		MaxRetries:   5,
		RetryBackoff: 10 * time.Millisecond,
	}, 2*time.Second)
	ad.enqueue("sha256:aa")
	ad.enqueue("sha256:bb")
	ad.stop()

	mu.Lock()
	defer mu.Unlock()
	if len(reports) == 0 {
		t.Fatal("expected at least one ReportChunks flush before stop")
	}
	var all []string
	for _, batch := range reports {
		all = append(all, batch...)
	}
	if len(all) < 2 {
		t.Fatalf("expected both hashes reported, got %v", all)
	}
}

func TestEngineResumesPartialFromPeer(t *testing.T) {
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
	data := []byte("0123456789ABCDEF" + "GHIJKLMNOPQRSTUV")
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
	pub := NewPublisher(seederCache, 16)
	manifest, err := pub.Publish(ctx, originSrc, "", "resume-model", "1.0")
	if err != nil {
		t.Fatal(err)
	}
	hashes := manifest.AllChunkHashes()
	if len(hashes) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(hashes))
	}

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
	_, _ = trClient.ReportChunks(ctx, &proto.ReportChunksRequest{NodeId: "seeder", ChunkHashes: hashes})

	leecherCache, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate crash after first half of chunk 0.
	if err := leecherCache.AppendPartial(hashes[0], bytes.NewReader(data[:16])); err != nil {
		t.Fatal(err)
	}

	eng := NewEngine(Config{
		NodeID:        "leecher",
		Cache:         leecherCache,
		TrackerClient: trClient,
	})
	metrics, err := eng.Sync(ctx, "resume-job", manifest, filepath.Join(t.TempDir(), "dest"), originSrc)
	if err != nil {
		t.Fatalf("resume sync failed: %v", err)
	}
	if metrics.PeerChunks < 1 {
		t.Fatalf("expected peer chunks, got %+v", metrics)
	}
	if !leecherCache.HasChunk(hashes[0]) || !leecherCache.HasChunk(hashes[1]) {
		t.Fatal("expected all chunks after resume sync")
	}
}

func TestPendingQueuePicksRarestFirst(t *testing.T) {
	locs := &locationMap{locs: map[string][]*proto.PeerInfo{
		"common": {{NodeId: "p1", Address: "a"}, {NodeId: "p2", Address: "b"}, {NodeId: "p3", Address: "c"}},
		"rare":   {{NodeId: "p1", Address: "a"}},
	}}
	q := newPendingQueue([]chunkWorkItem{
		{hash: "common"},
		{hash: "rare"},
	})
	work, ok := q.next(locs)
	if !ok {
		t.Fatal("expected work item")
	}
	if work.hash != "rare" {
		t.Fatalf("expected rare chunk first, got %s", work.hash)
	}
	work, ok = q.next(locs)
	if !ok || work.hash != "common" {
		t.Fatalf("expected common chunk second, got %v ok=%v", work.hash, ok)
	}
	if _, ok = q.next(locs); ok {
		t.Fatal("expected queue empty")
	}
}

type countingOrigin struct {
	source.Source
	mu       sync.Mutex
	inflight int
	max      int
	delay    time.Duration
}

func (c *countingOrigin) ReadChunkTo(ctx context.Context, path string, offset int64, size int64, w io.Writer) (int64, error) {
	c.mu.Lock()
	c.inflight++
	if c.inflight > c.max {
		c.max = c.inflight
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.inflight--
		c.mu.Unlock()
	}()
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return c.Source.ReadChunkTo(ctx, path, offset, size, w)
}

func TestOriginConcurrencyLimit(t *testing.T) {
	ctx := context.Background()
	originDir := t.TempDir()
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(filepath.Join(originDir, "payload.bin"), data, 0644); err != nil {
		t.Fatal(err)
	}
	inner, err := source.NewFilesystemSource(originDir)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &countingOrigin{Source: inner, delay: 25 * time.Millisecond}

	c, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pub := NewPublisher(c, 16)
	manifest, err := pub.Publish(ctx, inner, "", "origin-cap", "1")
	if err != nil {
		t.Fatal(err)
	}

	leecherCache, err := cache.NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(Config{
		Cache:                leecherCache,
		MaxPeerConcurrency:   4,
		MaxOriginConcurrency: 1,
	})
	metrics, err := eng.Sync(ctx, "origin-cap-job", manifest, t.TempDir(), wrapped)
	if err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if metrics.OriginChunks != 4 {
		t.Fatalf("expected 4 origin chunks, got %d", metrics.OriginChunks)
	}
	if wrapped.max > 1 {
		t.Fatalf("expected at most 1 concurrent origin fetch, saw %d", wrapped.max)
	}
}

func TestLocationMapReconcilesStalePeers(t *testing.T) {
	locs := &locationMap{locs: map[string][]*proto.PeerInfo{
		"chunk-x": {{NodeId: "a", Address: "10.0.0.1:1"}},
	}}
	locs.replace("chunk-x", []*proto.PeerInfo{
		{NodeId: "b", Address: "10.0.0.2:1"},
		{NodeId: "c", Address: "10.0.0.3:1"},
	})
	peers := locs.get("chunk-x")
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers after replace, got %d", len(peers))
	}
	for _, p := range peers {
		if p.NodeId == "a" {
			t.Fatal("stale peer A should be removed")
		}
	}
}

func TestAdvertiserRetriesOnFailure(t *testing.T) {
	var calls atomic.Int32
	failClient := &failingReporter{failUntil: 2, calls: &calls}
	ad := newAdvertiser(failClient, "node-a", config.AdvertisementConfig{
		BatchSize:    1,
		Interval:     20 * time.Millisecond,
		MaxRetries:   5,
		RetryBackoff: 10 * time.Millisecond,
	}, 200*time.Millisecond)
	ad.enqueue("sha256:retry")
	time.Sleep(300 * time.Millisecond)
	ad.stop()
	if calls.Load() < 2 {
		t.Fatalf("expected retries, got %d calls", calls.Load())
	}
}

type failingReporter struct {
	failUntil int32
	calls     *atomic.Int32
}

func (f *failingReporter) ReportChunks(_ context.Context, _ *proto.ReportChunksRequest, _ ...grpc.CallOption) (*proto.ReportChunksResponse, error) {
	n := f.calls.Add(1)
	if n <= f.failUntil {
		return nil, fmt.Errorf("tracker unavailable")
	}
	return &proto.ReportChunksResponse{ChunksRecorded: 1}, nil
}

func TestSortByAssignedPrefersLessLoadedPeer(t *testing.T) {
	var assigned sync.Map
	aCtr, bCtr := new(int64), new(int64)
	assigned.Store("a", aCtr)
	assigned.Store("b", bCtr)
	atomic.StoreInt64(aCtr, 3)
	atomic.StoreInt64(bCtr, 1)

	peers := []*proto.PeerInfo{
		{NodeId: "heavy", Address: "a"},
		{NodeId: "light", Address: "b"},
	}
	sortByAssigned(peers, &assigned)
	if peers[0].NodeId != "light" {
		t.Fatalf("expected lighter peer first, got %s", peers[0].NodeId)
	}
}

type reportSpy struct {
	onReport func([]string)
}

func (r *reportSpy) ReportChunks(_ context.Context, req *proto.ReportChunksRequest, _ ...grpc.CallOption) (*proto.ReportChunksResponse, error) {
	if r.onReport != nil {
		r.onReport(req.GetChunkHashes())
	}
	return &proto.ReportChunksResponse{ChunksRecorded: int64(len(req.GetChunkHashes()))}, nil
}
