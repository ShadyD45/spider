package benchmark

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
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

// ScenarioResult records metrics for a benchmark scenario.
type ScenarioResult struct {
	Name                 string        `json:"name"`
	TotalArtifactSize    int64         `json:"totalArtifactSizeBytes"`
	WorkerCount          int           `json:"workerCount"`
	Duration             time.Duration `json:"duration"`
	TotalThroughputMBs   float64       `json:"totalThroughputMBs"`
	OriginBytesTransferred int64       `json:"originBytesTransferred"`
	PeerBytesTransferred   int64       `json:"peerBytesTransferred"`
	OriginBandwidthSavedPct float64    `json:"originBandwidthSavedPct"`
	SpeedupFactor        float64       `json:"speedupFactor"`
}

// Suite runs comparative benchmarks between Direct Origin and P2P Mesh.
type Suite struct {
	baseTempDir string
}

// NewSuite creates a benchmark suite. Work caches go under baseTempDir (default tmp/work).
func NewSuite(baseTempDir string) *Suite {
	if baseTempDir == "" {
		baseTempDir = DefaultWorkRel
	}
	return &Suite{baseTempDir: baseTempDir}
}

// RunComparison executes both Direct Origin and P2P Mesh transfers for workerCount nodes.
// originPath is a file or directory used as the seed; it is not deleted.
func (s *Suite) RunComparison(ctx context.Context, originPath string, workerCount int, chunkSize int64) (*ScenarioResult, *ScenarioResult, error) {
	if err := os.MkdirAll(s.baseTempDir, 0755); err != nil {
		return nil, nil, err
	}
	benchDir, err := os.MkdirTemp(s.baseTempDir, "run-*")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(benchDir)

	originSrc, err := source.NewPathSource(originPath)
	if err != nil {
		return nil, nil, err
	}

	// Pre-create manifest
	seederCacheDir := filepath.Join(benchDir, "seeder-cache")
	seederCache, err := cache.NewCache(seederCacheDir)
	if err != nil {
		return nil, nil, err
	}
	pub := engine.NewPublisher(seederCache, chunkSize)
	manifest, err := pub.Publish(ctx, originSrc, "", "bench-model", "1.0")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to publish benchmark manifest: %w", err)
	}

	actualArtifactSize := manifest.TotalSize

	// -------------------------------------------------------------
	// SCENARIO 1: Direct Origin Download (Baseline: No P2P Mesh)
	// -------------------------------------------------------------
	originStart := time.Now()
	var originTotalBytes int64
	var wgOrigin sync.WaitGroup
	var originErr error
	var originMu sync.Mutex

	for w := 0; w < workerCount; w++ {
		wgOrigin.Add(1)
		go func(workerIdx int) {
			defer wgOrigin.Done()
			wDir := filepath.Join(benchDir, fmt.Sprintf("origin-worker-%d", workerIdx))
			wCache, err := cache.NewCache(filepath.Join(wDir, "cache"))
			if err != nil {
				originMu.Lock()
				originErr = err
				originMu.Unlock()
				return
			}

			eng := engine.NewEngine(engine.Config{
				NodeID: fmt.Sprintf("origin-worker-%d", workerIdx),
				Cache:  wCache,
			})

			metrics, err := eng.Sync(ctx, fmt.Sprintf("job-origin-%d", workerIdx), manifest, filepath.Join(wDir, "dest"), originSrc)
			if err != nil {
				originMu.Lock()
				originErr = err
				originMu.Unlock()
				return
			}

			originMu.Lock()
			originTotalBytes += metrics.OriginBytes
			originMu.Unlock()
		}(w)
	}
	wgOrigin.Wait()
	if originErr != nil {
		return nil, nil, fmt.Errorf("baseline origin benchmark failed: %w", originErr)
	}
	originDuration := time.Since(originStart)

	originResult := &ScenarioResult{
		Name:                   "Direct Origin (Baseline)",
		TotalArtifactSize:      actualArtifactSize,
		WorkerCount:            workerCount,
		Duration:               originDuration,
		TotalThroughputMBs:     (float64(actualArtifactSize*int64(workerCount)) / (1024 * 1024)) / originDuration.Seconds(),
		OriginBytesTransferred: originTotalBytes,
		PeerBytesTransferred:   0,
		OriginBandwidthSavedPct: 0.0,
		SpeedupFactor:          1.0,
	}

	// -------------------------------------------------------------
	// SCENARIO 2: P2P Mesh Distribution (1 Seeder + N Workers)
	// -------------------------------------------------------------
	// 1. Start Tracker
	trReg := tracker.NewRegistry(10 * time.Second)
	trSrv := tracker.NewServer(trReg)
	trLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	trPort := trLis.Addr().(*net.TCPAddr).Port
	grpcTr := grpc.NewServer()
	proto.RegisterTrackerServiceServer(grpcTr, trSrv)
	go func() { _ = grpcTr.Serve(trLis) }()
	defer grpcTr.Stop()

	trConn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", trPort), grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	defer trConn.Close()
	trClient := proto.NewTrackerServiceClient(trConn)

	// 2. Start Seeder Node
	seederLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	seederPort := seederLis.Addr().(*net.TCPAddr).Port
	seederPeerSrv := peer.NewServer("seeder-node", seederCache, nil)
	grpcSeeder := grpc.NewServer()
	proto.RegisterPeerServiceServer(grpcSeeder, seederPeerSrv)
	go func() { _ = grpcSeeder.Serve(seederLis) }()
	defer grpcSeeder.Stop()

	_, _ = trClient.RegisterPeer(ctx, &proto.RegisterPeerRequest{
		Peer: &proto.PeerInfo{
			NodeId:  "seeder-node",
			Address: fmt.Sprintf("127.0.0.1:%d", seederPort),
			Rack:    "rack-1",
			Zone:    "zone-a",
		},
	})
	_, _ = trClient.ReportChunks(ctx, &proto.ReportChunksRequest{
		NodeId:      "seeder-node",
		ChunkHashes: manifest.AllChunkHashes(),
	})

	// 3. Concurrently run worker mesh downloads
	meshStart := time.Now()
	var meshOriginBytes int64
	var meshPeerBytes int64
	var wgMesh sync.WaitGroup
	var meshErr error
	var meshMu sync.Mutex

	for w := 0; w < workerCount; w++ {
		wgMesh.Add(1)
		go func(workerIdx int) {
			defer wgMesh.Done()
			wDir := filepath.Join(benchDir, fmt.Sprintf("mesh-worker-%d", workerIdx))
			wCache, err := cache.NewCache(filepath.Join(wDir, "cache"))
			if err != nil {
				meshMu.Lock()
				meshErr = err
				meshMu.Unlock()
				return
			}

			// Start peer server for this worker so it also becomes a seeder for others
			wLis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				meshMu.Lock()
				meshErr = err
				meshMu.Unlock()
				return
			}
			wPort := wLis.Addr().(*net.TCPAddr).Port
			wPeerSrv := peer.NewServer(fmt.Sprintf("worker-%d", workerIdx), wCache, nil)
			grpcW := grpc.NewServer()
			proto.RegisterPeerServiceServer(grpcW, wPeerSrv)
			go func() { _ = grpcW.Serve(wLis) }()
			defer grpcW.Stop()

			_, _ = trClient.RegisterPeer(ctx, &proto.RegisterPeerRequest{
				Peer: &proto.PeerInfo{
					NodeId:  fmt.Sprintf("worker-%d", workerIdx),
					Address: fmt.Sprintf("127.0.0.1:%d", wPort),
					Rack:    "rack-1",
					Zone:    "zone-a",
				},
			})

			eng := engine.NewEngine(engine.Config{
				NodeID:        fmt.Sprintf("worker-%d", workerIdx),
				Cache:         wCache,
				TrackerClient: trClient,
				Materializer:  materializer.NewMaterializer(materializer.DefaultOptions()),
			})

			metrics, err := eng.Sync(ctx, fmt.Sprintf("job-mesh-%d", workerIdx), manifest, filepath.Join(wDir, "dest"), originSrc)
			if err != nil {
				meshMu.Lock()
				meshErr = err
				meshMu.Unlock()
				return
			}

			meshMu.Lock()
			meshOriginBytes += metrics.OriginBytes
			meshPeerBytes += metrics.PeerBytes
			meshMu.Unlock()
		}(w)
	}
	wgMesh.Wait()
	if meshErr != nil {
		return nil, nil, fmt.Errorf("mesh benchmark failed: %w", meshErr)
	}
	meshDuration := time.Since(meshStart)

	var savedPct float64
	if originTotalBytes > 0 {
		savedPct = (float64(originTotalBytes-meshOriginBytes) / float64(originTotalBytes)) * 100.0
	}

	speedup := float64(originDuration) / float64(meshDuration)

	meshResult := &ScenarioResult{
		Name:                   "Spider P2P Mesh (Distributed)",
		TotalArtifactSize:      actualArtifactSize,
		WorkerCount:            workerCount,
		Duration:               meshDuration,
		TotalThroughputMBs:     (float64(actualArtifactSize*int64(workerCount)) / (1024 * 1024)) / meshDuration.Seconds(),
		OriginBytesTransferred: meshOriginBytes,
		PeerBytesTransferred:   meshPeerBytes,
		OriginBandwidthSavedPct: savedPct,
		SpeedupFactor:          speedup,
	}

	return originResult, meshResult, nil
}
