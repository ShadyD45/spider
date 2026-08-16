package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	v1 "spider/api/v1"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/config"
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
	cache            *cache.ChunkStore
	trackerClient    proto.TrackerServiceClient
	clientPool       *peer.ClientPool
	materializer     *materializer.Materializer
	scheduler        *scheduler.Scheduler
	maxPeerWorkers   int
	maxOriginWorkers int
	advertiseBatch   int
	advertiseEvery   time.Duration
	discoverEvery    time.Duration
	retryAttempts    int
	retryInitial     time.Duration
	retryMax         time.Duration

	mu   sync.RWMutex
	jobs map[string]*SyncJob
}

type Config struct {
	NodeID               string
	Locality             topology.Locality
	Cache                *cache.ChunkStore
	TrackerClient        proto.TrackerServiceClient
	ClientPool           *peer.ClientPool
	Materializer         *materializer.Materializer
	Scheduler            *scheduler.Scheduler
	MaxPeerConcurrency   int
	MaxOriginConcurrency int
	Advertisement        config.AdvertisementConfig
	PeerDiscovery        config.PeerDiscoveryConfig
	Retry                config.RetryConfig
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
	if cfg.Advertisement.BatchSize <= 0 {
		cfg.Advertisement.BatchSize = 16
	}
	if cfg.Advertisement.Interval <= 0 {
		cfg.Advertisement.Interval = 100 * time.Millisecond
	}
	if cfg.PeerDiscovery.RefreshInterval <= 0 {
		cfg.PeerDiscovery.RefreshInterval = 500 * time.Millisecond
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 3
	}
	if cfg.Retry.Backoff.Initial <= 0 {
		cfg.Retry.Backoff.Initial = 100 * time.Millisecond
	}
	if cfg.Retry.Backoff.Max <= 0 {
		cfg.Retry.Backoff.Max = 2 * time.Second
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
		advertiseBatch:   cfg.Advertisement.BatchSize,
		advertiseEvery:   cfg.Advertisement.Interval,
		discoverEvery:    cfg.PeerDiscovery.RefreshInterval,
		retryAttempts:    cfg.Retry.MaxAttempts,
		retryInitial:     cfg.Retry.Backoff.Initial,
		retryMax:         cfg.Retry.Backoff.Max,
		jobs:             make(map[string]*SyncJob),
	}
}

type chunkWorkItem struct {
	hash     string
	filePath string
	offset   int64
	size     int64
}

type locationMap struct {
	mu   sync.Mutex
	locs map[string][]*proto.PeerInfo
}

func (m *locationMap) get(hash string) []*proto.PeerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*proto.PeerInfo(nil), m.locs[hash]...)
}

func (m *locationMap) merge(hash string, peers []*proto.PeerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{})
	var out []*proto.PeerInfo
	for _, p := range m.locs[hash] {
		if p == nil || p.Address == "" {
			continue
		}
		if _, ok := seen[p.Address]; ok {
			continue
		}
		seen[p.Address] = struct{}{}
		out = append(out, p)
	}
	for _, p := range peers {
		if p == nil || p.Address == "" {
			continue
		}
		if _, ok := seen[p.Address]; ok {
			continue
		}
		seen[p.Address] = struct{}{}
		out = append(out, p)
	}
	m.locs[hash] = out
}

func (m *locationMap) snapshot() map[string][]*proto.PeerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]*proto.PeerInfo, len(m.locs))
	for k, v := range m.locs {
		out[k] = append([]*proto.PeerInfo(nil), v...)
	}
	return out
}

type chunkReporter interface {
	ReportChunks(ctx context.Context, in *proto.ReportChunksRequest, opts ...grpc.CallOption) (*proto.ReportChunksResponse, error)
}

type advertiser struct {
	client    chunkReporter
	nodeID    string
	batchSize int
	interval  time.Duration
	ch        chan string
	done      chan struct{}
	finished  chan struct{}
}

func newAdvertiser(client chunkReporter, nodeID string, batchSize int, interval time.Duration) *advertiser {
	a := &advertiser{
		client:    client,
		nodeID:    nodeID,
		batchSize: batchSize,
		interval:  interval,
		ch:        make(chan string, 1024),
		done:      make(chan struct{}),
		finished:  make(chan struct{}),
	}
	go a.loop()
	return a
}

func (a *advertiser) enqueue(hash string) {
	if a == nil || a.client == nil || hash == "" {
		return
	}
	select {
	case a.ch <- hash:
	default:
		a.flush([]string{hash})
	}
}

func (a *advertiser) loop() {
	defer close(a.finished)
	t := time.NewTicker(a.interval)
	defer t.Stop()
	var buf []string
	flush := func() {
		if len(buf) == 0 {
			return
		}
		a.flush(buf)
		buf = buf[:0]
	}
	for {
		select {
		case <-a.done:
			for {
				select {
				case h := <-a.ch:
					buf = append(buf, h)
				default:
					flush()
					return
				}
			}
		case h := <-a.ch:
			buf = append(buf, h)
			if len(buf) >= a.batchSize {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

func (a *advertiser) flush(hashes []string) {
	if a.client == nil || len(hashes) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, _ = a.client.ReportChunks(ctx, &proto.ReportChunksRequest{
		NodeId:      a.nodeID,
		ChunkHashes: hashes,
	})
	cancel()
}

func (a *advertiser) stop() {
	if a == nil {
		return
	}
	close(a.done)
	<-a.finished
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

	locs := &locationMap{locs: make(map[string][]*proto.PeerInfo)}
	e.refreshLocations(ctx, manifest.ArtifactID, missingHashes, locs)

	refreshCtx, stopRefresh := context.WithCancel(ctx)
	defer stopRefresh()
	if e.trackerClient != nil && len(missingHashes) > 0 {
		go func() {
			t := time.NewTicker(e.discoverEvery)
			defer t.Stop()
			for {
				select {
				case <-refreshCtx.Done():
					return
				case <-t.C:
					remaining := make([]string, 0, len(missingHashes))
					for _, h := range missingHashes {
						if !e.cache.HasChunk(h) {
							remaining = append(remaining, h)
						}
					}
					if len(remaining) == 0 {
						return
					}
					e.refreshLocations(refreshCtx, manifest.ArtifactID, remaining, locs)
				}
			}
		}()
	}

	ad := newAdvertiser(e.trackerClient, e.nodeID, e.advertiseBatch, e.advertiseEvery)
	defer ad.stop()

	if len(missingItems) > 0 {
		var wg sync.WaitGroup
		var downloadErr atomic.Value
		var assigned sync.Map // addr -> *int64

		workCh := make(chan chunkWorkItem, len(missingItems))
		order := scheduler.RarestFirst(missingHashes, locs.snapshot())
		for _, hash := range order {
			workCh <- hashToWork[hash]
		}
		close(workCh)

		for i := 0; i < e.maxPeerWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for work := range workCh {
					if ctx.Err() != nil {
						downloadErr.Store(ctx.Err())
						return
					}
					if downloadErr.Load() != nil {
						return
					}
					fromPeer, n, err := e.fetchChunk(ctx, work, origin, locs, &assigned)
					if err != nil {
						downloadErr.Store(err)
						return
					}
					atomic.AddInt64(&job.DownloadedChunks, 1)
					if fromPeer {
						atomic.AddInt64(&job.PeerChunks, 1)
						atomic.AddInt64(&job.PeerBytes, n)
						metrics.PeerBytesTransferred.Add(float64(n))
						metrics.OriginBytesSaved.Add(float64(n))
					} else {
						atomic.AddInt64(&job.OriginChunks, 1)
						atomic.AddInt64(&job.OriginBytes, n)
						metrics.OriginBytesDownloaded.Add(float64(n))
					}
					ad.enqueue(work.hash)
				}
			}()
		}
		wg.Wait()

		if errVal := downloadErr.Load(); errVal != nil {
			job.Status = "FAILED"
			job.ErrorMessage = fmt.Sprintf("%v", errVal)
			return nil, errVal.(error)
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

func (e *Engine) refreshLocations(ctx context.Context, artifactID string, hashes []string, locs *locationMap) {
	if e.trackerClient == nil || len(hashes) == 0 {
		return
	}
	locateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if art, err := e.trackerClient.LocateArtifact(locateCtx, &proto.LocateArtifactRequest{
		RequesterNodeId: e.nodeID,
		ArtifactId:      artifactID,
	}); err == nil && len(art.GetSeedPeers()) > 0 {
		for _, h := range hashes {
			locs.merge(h, art.GetSeedPeers())
		}
	}
	resp, err := e.trackerClient.LocateChunks(locateCtx, &proto.LocateChunksRequest{
		RequesterNodeId: e.nodeID,
		ChunkHashes:     hashes,
	})
	if err != nil {
		slog.Warn("LocateChunks failed; origin fallback", "err", err)
		return
	}
	for _, loc := range resp.GetLocations() {
		if len(loc.GetPeers()) > 0 {
			locs.merge(loc.GetChunkHash(), loc.GetPeers())
		}
	}
}

func loadAssigned(m *sync.Map, addr string) int64 {
	v, _ := m.LoadOrStore(addr, new(int64))
	return atomic.LoadInt64(v.(*int64))
}

func addAssigned(m *sync.Map, addr string, delta int64) {
	v, _ := m.LoadOrStore(addr, new(int64))
	atomic.AddInt64(v.(*int64), delta)
}

func (e *Engine) fetchChunk(ctx context.Context, work chunkWorkItem, origin source.Source, locs *locationMap, assigned *sync.Map) (fromPeer bool, bytes int64, err error) {
	backoff := e.retryInitial
	for attempt := 0; attempt < e.retryAttempts; attempt++ {
		candidates := e.scheduler.RankPeers(e.locality, locs.get(work.hash))
		sortByAssigned(candidates, assigned)
		for _, p := range candidates {
			addr := p.GetAddress()
			if addr == "" || e.scheduler.IsUntrusted(addr) {
				continue
			}
			if !e.scheduler.Begin(addr) {
				continue
			}
			addAssigned(assigned, addr, 1)
			peerCtx, peerCancel := context.WithTimeout(ctx, 30*time.Second)
			start := time.Now()
			n, fetchErr := e.downloadFromPeer(peerCtx, addr, work.hash)
			peerCancel()
			addAssigned(assigned, addr, -1)
			ok := fetchErr == nil
			e.scheduler.End(addr, time.Since(start), ok)
			if !ok {
				slog.Warn("p2p download failed", "peer", p.GetNodeId(), "addr", addr, "chunk", work.hash, "err", fetchErr)
				if fetchErr != nil && (errors.Is(fetchErr, context.DeadlineExceeded) || containsCorrupt(fetchErr)) {
					metrics.ChunkVerifyFailures.Inc()
				}
				continue
			}
			return true, n, nil
		}
		if attempt+1 < e.retryAttempts {
			select {
			case <-ctx.Done():
				return false, 0, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < e.retryMax {
				backoff *= 2
				if backoff > e.retryMax {
					backoff = e.retryMax
				}
			}
		}
	}

	if origin == nil {
		return false, 0, fmt.Errorf("no peer available for chunk %s and no origin configured", work.hash)
	}
	n, err := e.downloadFromOrigin(ctx, work, origin)
	if err != nil {
		return false, 0, err
	}
	return false, n, nil
}

func sortByAssigned(peers []*proto.PeerInfo, assigned *sync.Map) {
	if len(peers) < 2 {
		return
	}
	// insertion sort to keep RankPeers order for ties
	for i := 1; i < len(peers); i++ {
		j := i
		for j > 0 && loadAssigned(assigned, peers[j].Address) < loadAssigned(assigned, peers[j-1].Address) {
			peers[j], peers[j-1] = peers[j-1], peers[j]
			j--
		}
	}
}

func (e *Engine) downloadFromPeer(ctx context.Context, addr, hash string) (int64, error) {
	offset := e.cache.PartialSize(hash)
	pr, pw := io.Pipe()
	var total int64
	var dlErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		total, dlErr = e.clientPool.DownloadChunkTo(ctx, addr, hash, offset, pw)
		if dlErr != nil {
			_ = pw.CloseWithError(dlErr)
			return
		}
		_ = pw.Close()
	}()
	appendErr := e.cache.AppendPartial(hash, pr)
	<-done
	if dlErr != nil {
		if containsCorrupt(dlErr) || errors.Is(dlErr, cache.ErrHashMismatch) {
			_ = e.cache.DiscardPartial(hash)
		}
		return 0, dlErr
	}
	if appendErr != nil {
		return 0, appendErr
	}
	got := e.cache.PartialSize(hash)
	if total > 0 && got < total {
		return 0, fmt.Errorf("%w: have %d want %d", cache.ErrIncomplete, got, total)
	}
	if err := e.cache.CommitPartial(hash); err != nil {
		if errors.Is(err, cache.ErrHashMismatch) {
			metrics.ChunkVerifyFailures.Inc()
			_ = e.cache.DiscardPartial(hash)
		}
		return 0, err
	}
	if total <= 0 {
		total = got
	}
	return total, nil
}

func (e *Engine) downloadFromOrigin(ctx context.Context, work chunkWorkItem, origin source.Source) (int64, error) {
	partial := e.cache.PartialSize(work.hash)
	remain := work.size - partial
	if remain < 0 {
		_ = e.cache.DiscardPartial(work.hash)
		partial = 0
		remain = work.size
	}
	originCtx, originCancel := context.WithTimeout(ctx, 30*time.Second)
	data, fetchErr := origin.ReadChunk(originCtx, work.filePath, work.offset+partial, remain)
	originCancel()
	if fetchErr != nil {
		return 0, fmt.Errorf("failed to fetch chunk %s from origin (%s offset %d): %w", work.hash, work.filePath, work.offset, fetchErr)
	}
	if err := e.cache.AppendPartial(work.hash, bytes.NewReader(data)); err != nil {
		return 0, err
	}
	if err := e.cache.CommitPartial(work.hash); err != nil {
		if errors.Is(err, cache.ErrHashMismatch) {
			metrics.ChunkVerifyFailures.Inc()
			_ = e.cache.DiscardPartial(work.hash)
			return 0, fmt.Errorf("integrity check failed for chunk %s: %w", work.hash, err)
		}
		return 0, fmt.Errorf("failed to commit chunk %s to cache: %w", work.hash, err)
	}
	return work.size, nil
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
