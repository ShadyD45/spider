package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "spider/api/v1"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/materializer"
	"spider/pkg/metrics"
	"spider/pkg/peer"
	"spider/pkg/scheduler"
	"spider/pkg/source"
	"spider/pkg/topology"
)

type SyncMetrics struct {
	TotalChunks      int64
	DownloadedChunks int64
	PeerChunks       int64
	OriginChunks     int64
	SkippedChunks    int64
	PeerBytes        int64
	OriginBytes      int64
	Duration         time.Duration
}

func (m *SyncMetrics) OriginBytesSaved() int64 {
	if m == nil {
		return 0
	}
	return m.PeerBytes
}

func (m *SyncMetrics) FormatSummary() string {
	saved := m.PeerBytes
	return fmt.Sprintf("reused_chunks=%d peer_chunks=%d peer_bytes=%d origin_chunks=%d origin_bytes=%d origin_bytes_saved=%d duration=%s",
		m.SkippedChunks, m.PeerChunks, m.PeerBytes, m.OriginChunks, m.OriginBytes, saved, m.Duration.Round(time.Millisecond))
}

type SyncJob struct {
	JobID            string
	ArtifactID       string
	Status           string
	TotalChunks      int64
	DownloadedChunks int64
	PeerChunks       int64
	OriginChunks     int64
	SkippedChunks    int64
	PeerBytes        int64
	OriginBytes      int64
	ErrorMessage     string
	StartTime        time.Time
	EndTime          time.Time
}

type Engine struct {
	nodeID           string
	locality         topology.Locality
	cache            *cache.Cache
	trackerClient    proto.TrackerServiceClient
	clientPool       *peer.ClientPool
	materializer     *materializer.Materializer
	scheduler        *scheduler.Scheduler
	maxPeerWorkers   int
	maxOriginWorkers int

	mu   sync.RWMutex
	jobs map[string]*SyncJob
}

type Config struct {
	NodeID               string
	Locality             topology.Locality
	Cache                *cache.Cache
	TrackerClient        proto.TrackerServiceClient
	ClientPool           *peer.ClientPool
	Materializer         *materializer.Materializer
	Scheduler            *scheduler.Scheduler
	MaxPeerConcurrency   int
	MaxOriginConcurrency int
}

func NewEngine(cfg Config) *Engine {
	if cfg.MaxPeerConcurrency <= 0 {
		cfg.MaxPeerConcurrency = 8
	}
	if cfg.MaxOriginConcurrency <= 0 {
		cfg.MaxOriginConcurrency = 4
	}
	if cfg.ClientPool == nil {
		cfg.ClientPool = peer.NewClientPool()
	}
	if cfg.Materializer == nil {
		cfg.Materializer = materializer.NewMaterializer(materializer.DefaultOptions())
	}
	if cfg.Scheduler == nil {
		cfg.Scheduler = scheduler.New(cfg.MaxPeerConcurrency)
	}
	return &Engine{
		nodeID:           cfg.NodeID,
		locality:         cfg.Locality,
		cache:            cfg.Cache,
		trackerClient:    cfg.TrackerClient,
		clientPool:       cfg.ClientPool,
		materializer:     cfg.Materializer,
		scheduler:        cfg.Scheduler,
		maxPeerWorkers:   cfg.MaxPeerConcurrency,
		maxOriginWorkers: cfg.MaxOriginConcurrency,
		jobs:             make(map[string]*SyncJob),
	}
}

type chunkWorkItem struct {
	hash     string
	filePath string
	offset   int64
	size     int64
}

func (e *Engine) Sync(ctx context.Context, jobID string, manifest *v1.ArtifactManifest, destDir string, origin source.Source) (*SyncMetrics, error) {
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}

	startTime := time.Now()
	allHashes := manifest.AllChunkHashes()

	job := &SyncJob{
		JobID:       jobID,
		ArtifactID:  manifest.ArtifactID,
		Status:      "RUNNING",
		TotalChunks: int64(len(allHashes)),
		StartTime:   startTime,
	}
	e.mu.Lock()
	e.jobs[jobID] = job
	e.mu.Unlock()
	defer func() { job.EndTime = time.Now() }()

	var missingItems []chunkWorkItem
	var missingHashes []string
	seenMissing := make(map[string]struct{})
	hashToWork := make(map[string]chunkWorkItem)

	for _, fileEntry := range manifest.Files {
		for _, chunkRef := range fileEntry.Chunks {
			if e.cache.HasChunk(chunkRef.Hash) {
				atomic.AddInt64(&job.SkippedChunks, 1)
				metrics.CacheHits.Inc()
				continue
			}
			if _, exists := seenMissing[chunkRef.Hash]; exists {
				continue
			}
			seenMissing[chunkRef.Hash] = struct{}{}
			item := chunkWorkItem{hash: chunkRef.Hash, filePath: fileEntry.Path, offset: chunkRef.Offset, size: chunkRef.Size}
			missingHashes = append(missingHashes, chunkRef.Hash)
			missingItems = append(missingItems, item)
			hashToWork[chunkRef.Hash] = item
		}
	}

	slog.Info("sync start", "artifact", manifest.Name, "version", manifest.Version, "total", len(allHashes), "missing", len(missingItems), "reused", job.SkippedChunks)

	peerLocations := make(map[string][]*proto.PeerInfo)
	if e.trackerClient != nil && len(missingHashes) > 0 {
		locateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if art, err := e.trackerClient.LocateArtifact(locateCtx, &proto.LocateArtifactRequest{
			RequesterNodeId: e.nodeID,
			ArtifactId:      manifest.ArtifactID,
		}); err == nil && len(art.GetSeedPeers()) > 0 {
			for _, h := range missingHashes {
				peerLocations[h] = art.GetSeedPeers()
			}
		}
		resp, err := e.trackerClient.LocateChunks(locateCtx, &proto.LocateChunksRequest{
			RequesterNodeId: e.nodeID,
			ChunkHashes:     missingHashes,
		})
		cancel()
		if err != nil {
			slog.Warn("LocateChunks failed; origin fallback", "err", err)
		} else {
			for _, loc := range resp.GetLocations() {
				if len(loc.GetPeers()) > 0 {
					peerLocations[loc.GetChunkHash()] = loc.GetPeers()
				}
			}
		}
	}

	if len(missingItems) > 0 {
		order := scheduler.RarestFirst(missingHashes, peerLocations)
		sem := make(chan struct{}, e.maxPeerWorkers)
		var wg sync.WaitGroup
		var downloadErr atomic.Value
		var newlyAcquiredHashes []string
		var reportMu sync.Mutex

		for _, hash := range order {
			if ctx.Err() != nil {
				downloadErr.Store(ctx.Err())
				break
			}
			if downloadErr.Load() != nil {
				break
			}
			work := hashToWork[hash]
			wg.Add(1)
			sem <- struct{}{}
			go func(work chunkWorkItem) {
				defer wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					downloadErr.Store(ctx.Err())
					return
				}
				if downloadErr.Load() != nil {
					return
				}

				candidates := e.scheduler.RankPeers(e.locality, peerLocations[work.hash])
				var downloadedBytes []byte
				var isFromPeer bool

				for _, p := range candidates {
					addr := p.GetAddress()
					if addr == "" || e.scheduler.IsUntrusted(addr) {
						continue
					}
					if !e.scheduler.WaitBegin(ctx, addr) {
						continue
					}
					peerCtx, peerCancel := context.WithTimeout(ctx, 10*time.Second)
					start := time.Now()
					data, fetchErr := e.clientPool.DownloadChunk(peerCtx, addr, work.hash)
					peerCancel()
					ok := fetchErr == nil
					e.scheduler.End(addr, time.Since(start), ok)
					if !ok {
						slog.Warn("p2p download failed", "peer", p.GetNodeId(), "addr", addr, "chunk", work.hash, "err", fetchErr)
						if fetchErr != nil && (errors.Is(fetchErr, context.DeadlineExceeded) || containsCorrupt(fetchErr)) {
							metrics.ChunkVerifyFailures.Inc()
						}
						continue
					}
					downloadedBytes = data
					isFromPeer = true
					break
				}

				if !isFromPeer || len(downloadedBytes) == 0 {
					if origin == nil {
						downloadErr.Store(fmt.Errorf("no peer available for chunk %s and no origin configured", work.hash))
						return
					}
					originCtx, originCancel := context.WithTimeout(ctx, 30*time.Second)
					data, fetchErr := origin.ReadChunk(originCtx, work.filePath, work.offset, work.size)
					originCancel()
					if fetchErr != nil {
						downloadErr.Store(fmt.Errorf("failed to fetch chunk %s from origin (%s offset %d): %w", work.hash, work.filePath, work.offset, fetchErr))
						return
					}
					downloadedBytes = data
				}

				if err := e.cache.PutChunk(work.hash, downloadedBytes); err != nil {
					if errors.Is(err, cache.ErrHashMismatch) {
						metrics.ChunkVerifyFailures.Inc()
						downloadErr.Store(fmt.Errorf("integrity check failed for chunk %s: %w", work.hash, err))
						return
					}
					downloadErr.Store(fmt.Errorf("failed to commit chunk %s to cache: %w", work.hash, err))
					return
				}

				chunkSize := int64(len(downloadedBytes))
				atomic.AddInt64(&job.DownloadedChunks, 1)
				if isFromPeer {
					atomic.AddInt64(&job.PeerChunks, 1)
					atomic.AddInt64(&job.PeerBytes, chunkSize)
					metrics.PeerBytesTransferred.Add(float64(chunkSize))
					metrics.OriginBytesSaved.Add(float64(chunkSize))
				} else {
					atomic.AddInt64(&job.OriginChunks, 1)
					atomic.AddInt64(&job.OriginBytes, chunkSize)
					metrics.OriginBytesDownloaded.Add(float64(chunkSize))
				}
				reportMu.Lock()
				newlyAcquiredHashes = append(newlyAcquiredHashes, work.hash)
				reportMu.Unlock()
			}(work)
		}
		wg.Wait()

		if errVal := downloadErr.Load(); errVal != nil {
			job.Status = "FAILED"
			job.ErrorMessage = fmt.Sprintf("%v", errVal)
			return nil, errVal.(error)
		}

		if e.trackerClient != nil && len(newlyAcquiredHashes) > 0 {
			reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = e.trackerClient.ReportChunks(reportCtx, &proto.ReportChunksRequest{
				NodeId:      e.nodeID,
				ChunkHashes: newlyAcquiredHashes,
			})
			cancel()
		}
	}

	_ = e.cache.SaveManifest(manifest)
	if err := e.materializer.Materialize(ctx, manifest, e.cache, destDir); err != nil {
		job.Status = "FAILED"
		job.ErrorMessage = fmt.Sprintf("materialization failed: %v", err)
		return nil, fmt.Errorf("failed to materialize artifact files into %s: %w", destDir, err)
	}

	if e.trackerClient != nil {
		repCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = e.trackerClient.ReportArtifact(repCtx, &proto.ReportArtifactRequest{
			NodeId:     e.nodeID,
			ArtifactId: manifest.ArtifactID,
			Complete:   true,
		})
		cancel()
	}

	job.Status = "COMPLETED"
	duration := time.Since(startTime)
	metrics.SyncDuration.Observe(duration.Seconds())

	m := &SyncMetrics{
		TotalChunks:      job.TotalChunks,
		DownloadedChunks: job.DownloadedChunks,
		PeerChunks:       job.PeerChunks,
		OriginChunks:     job.OriginChunks,
		SkippedChunks:    job.SkippedChunks,
		PeerBytes:        job.PeerBytes,
		OriginBytes:      job.OriginBytes,
		Duration:         duration,
	}
	slog.Info("sync completed", "artifact", manifest.Name, "version", manifest.Version, "summary", m.FormatSummary())
	return m, nil
}

func containsCorrupt(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, cache.ErrHashMismatch) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "corrupt") || strings.Contains(msg, "integrity")
}

func (e *Engine) GetJob(jobID string) (*SyncJob, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	j, ok := e.jobs[jobID]
	return j, ok
}

func (e *Engine) AllJobs() []*SyncJob {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var list []*SyncJob
	for _, j := range e.jobs {
		list = append(list, j)
	}
	return list
}
