package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	v1 "spider/api/v1"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/materializer"
	"spider/pkg/peer"
	"spider/pkg/source"
	"spider/pkg/topology"
)

// SyncMetrics details transfer statistics for a sync operation.
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

// SyncJob tracks an active or completed download job.
type SyncJob struct {
	JobID            string
	ArtifactID       string
	Status           string // RUNNING, COMPLETED, FAILED
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

// Engine coordinates chunk location, P2P streaming, origin fallback, and materialization.
type Engine struct {
	nodeID           string
	locality         topology.Locality
	cache            *cache.Cache
	trackerClient    proto.TrackerServiceClient
	clientPool       *peer.ClientPool
	materializer     *materializer.Materializer
	maxPeerWorkers   int
	maxOriginWorkers int

	mu   sync.RWMutex
	jobs map[string]*SyncJob
}

// Config holds Engine initialization parameters.
type Config struct {
	NodeID                 string
	Locality               topology.Locality
	Cache                  *cache.Cache
	TrackerClient          proto.TrackerServiceClient
	ClientPool             *peer.ClientPool
	Materializer           *materializer.Materializer
	MaxPeerConcurrency     int
	MaxOriginConcurrency   int
}

// NewEngine creates an Engine instance.
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

	return &Engine{
		nodeID:           cfg.NodeID,
		locality:         cfg.Locality,
		cache:            cfg.Cache,
		trackerClient:    cfg.TrackerClient,
		clientPool:       cfg.ClientPool,
		materializer:     cfg.Materializer,
		maxPeerWorkers:   cfg.MaxPeerConcurrency,
		maxOriginWorkers: cfg.MaxOriginConcurrency,
		jobs:             make(map[string]*SyncJob),
	}
}

// chunkWorkItem describes a specific chunk to download.
type chunkWorkItem struct {
	hash     string
	filePath string
	offset   int64
	size     int64
}

// Sync downloads all required chunks for an artifact and materializes the target directory.
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

	defer func() {
		job.EndTime = time.Now()
	}()

	// 1. Identify missing chunks
	var missingItems []chunkWorkItem
	var missingHashes []string
	seenMissing := make(map[string]struct{})

	for _, fileEntry := range manifest.Files {
		for _, chunkRef := range fileEntry.Chunks {
			if !e.cache.HasChunk(chunkRef.Hash) {
				if _, exists := seenMissing[chunkRef.Hash]; !exists {
					seenMissing[chunkRef.Hash] = struct{}{}
					missingHashes = append(missingHashes, chunkRef.Hash)
					missingItems = append(missingItems, chunkWorkItem{
						hash:     chunkRef.Hash,
						filePath: fileEntry.Path,
						offset:   chunkRef.Offset,
						size:     chunkRef.Size,
					})
				}
			} else {
				atomic.AddInt64(&job.SkippedChunks, 1)
			}
		}
	}

	log.Printf("[Engine] Syncing %s@%s (%d total unique chunks, %d missing, %d cached)",
		manifest.Name, manifest.Version, len(allHashes), len(missingItems), job.SkippedChunks)

	// 2. Query Tracker for candidate peers if tracker client is available
	peerLocations := make(map[string][]*proto.PeerInfo)
	if e.trackerClient != nil && len(missingHashes) > 0 {
		locateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := e.trackerClient.LocateChunks(locateCtx, &proto.LocateChunksRequest{
			RequesterNodeId: e.nodeID,
			ChunkHashes:     missingHashes,
		})
		cancel()

		if err != nil {
			log.Printf("[Engine] Tracker LocateChunks warning: %v (falling back to origin for missing chunks)", err)
		} else {
			for _, loc := range resp.GetLocations() {
				if len(loc.GetPeers()) > 0 {
					peerLocations[loc.GetChunkHash()] = loc.GetPeers()
				}
			}
		}
	}

	// 3. Concurrently download missing chunks
	if len(missingItems) > 0 {
		sem := make(chan struct{}, e.maxPeerWorkers)
		var wg sync.WaitGroup
		var downloadErr atomic.Value
		var newlyAcquiredHashes []string
		var reportMu sync.Mutex

		for _, item := range missingItems {
			if downloadErr.Load() != nil {
				break
			}

			wg.Add(1)
			sem <- struct{}{}

			go func(work chunkWorkItem) {
				defer wg.Done()
				defer func() { <-sem }()

				if downloadErr.Load() != nil {
					return
				}

				candidatePeers := peerLocations[work.hash]
				var downloadedBytes []byte
				var isFromPeer bool
				var fetchErr error

				// Attempt P2P streaming from candidate peers first
				for _, p := range candidatePeers {
					if p.GetAddress() == "" {
						continue
					}
					peerCtx, peerCancel := context.WithTimeout(ctx, 10*time.Second)
					downloadedBytes, fetchErr = e.clientPool.DownloadChunk(peerCtx, p.GetAddress(), work.hash)
					peerCancel()

					if fetchErr == nil {
						isFromPeer = true
						break
					}
					log.Printf("[Engine] P2P download failed from peer %s (%s) for chunk %s: %v (trying next candidate)",
						p.GetNodeId(), p.GetAddress(), work.hash, fetchErr)
				}

				// If P2P failed or no peers had chunk, fall back to origin
				if !isFromPeer || len(downloadedBytes) == 0 {
					if origin == nil {
						err := fmt.Errorf("no peer available for chunk %s and no origin configured", work.hash)
						downloadErr.Store(err)
						return
					}

					originCtx, originCancel := context.WithTimeout(ctx, 30*time.Second)
					downloadedBytes, fetchErr = origin.ReadChunk(originCtx, work.filePath, work.offset, work.size)
					originCancel()

					if fetchErr != nil {
						err := fmt.Errorf("failed to fetch chunk %s from origin (%s offset %d): %w", work.hash, work.filePath, work.offset, fetchErr)
						downloadErr.Store(err)
						return
					}
				}

				// Store chunk atomically in cache (re-hashes durable bytes; rejects origin/peer mismatch)
				if err := e.cache.PutChunk(work.hash, downloadedBytes); err != nil {
					if errors.Is(err, cache.ErrHashMismatch) {
						downloadErr.Store(fmt.Errorf("integrity check failed for chunk %s: %w", work.hash, err))
						return
					}
					downloadErr.Store(fmt.Errorf("failed to commit chunk %s to cache: %w", work.hash, err))
					return
				}

				// Update metrics
				chunkSize := int64(len(downloadedBytes))
				atomic.AddInt64(&job.DownloadedChunks, 1)
				if isFromPeer {
					atomic.AddInt64(&job.PeerChunks, 1)
					atomic.AddInt64(&job.PeerBytes, chunkSize)
				} else {
					atomic.AddInt64(&job.OriginChunks, 1)
					atomic.AddInt64(&job.OriginBytes, chunkSize)
				}

				// Collect for reporting to tracker
				reportMu.Lock()
				newlyAcquiredHashes = append(newlyAcquiredHashes, work.hash)
				reportMu.Unlock()
			}(item)
		}

		wg.Wait()

		if errVal := downloadErr.Load(); errVal != nil {
			job.Status = "FAILED"
			job.ErrorMessage = fmt.Sprintf("%v", errVal)
			return nil, errVal.(error)
		}

		// Report newly acquired chunks to central tracker
		if e.trackerClient != nil && len(newlyAcquiredHashes) > 0 {
			reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = e.trackerClient.ReportChunks(reportCtx, &proto.ReportChunksRequest{
				NodeId:      e.nodeID,
				ChunkHashes: newlyAcquiredHashes,
			})
			cancel()
		}
	}

	// 4. Save manifest locally
	_ = e.cache.SaveManifest(manifest)

	// 5. Materialize file tree into destDir
	if err := e.materializer.Materialize(ctx, manifest, e.cache, destDir); err != nil {
		job.Status = "FAILED"
		job.ErrorMessage = fmt.Sprintf("materialization failed: %v", err)
		return nil, fmt.Errorf("failed to materialize artifact files into %s: %w", destDir, err)
	}

	job.Status = "COMPLETED"
	duration := time.Since(startTime)

	metrics := &SyncMetrics{
		TotalChunks:      job.TotalChunks,
		DownloadedChunks: job.DownloadedChunks,
		PeerChunks:       job.PeerChunks,
		OriginChunks:     job.OriginChunks,
		SkippedChunks:    job.SkippedChunks,
		PeerBytes:        job.PeerBytes,
		OriginBytes:      job.OriginBytes,
		Duration:         duration,
	}

	log.Printf("[Engine] Sync COMPLETED for %s@%s in %v (Peer Chunks: %d [%d bytes], Origin Chunks: %d [%d bytes], Skipped: %d)",
		manifest.Name, manifest.Version, duration, metrics.PeerChunks, metrics.PeerBytes, metrics.OriginChunks, metrics.OriginBytes, metrics.SkippedChunks)

	return metrics, nil
}

// GetJob returns status of a sync job by ID.
func (e *Engine) GetJob(jobID string) (*SyncJob, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	j, ok := e.jobs[jobID]
	return j, ok
}

// AllJobs returns a list of all jobs.
func (e *Engine) AllJobs() []*SyncJob {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var list []*SyncJob
	for _, j := range e.jobs {
		list = append(list, j)
	}
	return list
}
