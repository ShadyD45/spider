package advertise

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
	"spider/pkg/config"
	"spider/pkg/metrics"
)

// ChunkReporter reports chunk ownership to the tracker.
type ChunkReporter interface {
	ReportChunks(ctx context.Context, in *proto.ReportChunksRequest, opts ...grpc.CallOption) (*proto.ReportChunksResponse, error)
}

// Advertiser batches and retries chunk ownership reports without blocking downloads.
type Advertiser struct {
	client       ChunkReporter
	nodeID       string
	batchSize    int
	interval     time.Duration
	maxRetries   int
	retryInitial time.Duration
	retryMax     time.Duration
	ch           chan string
	retry        []retryEntry
	retryMu      sync.Mutex
	done         chan struct{}
	finished     chan struct{}
}

type retryEntry struct {
	hash    string
	attempt int
	nextAt  time.Time
}

// New creates a background advertiser. Call Stop before process exit.
func New(client ChunkReporter, nodeID string, cfg config.AdvertisementConfig) *Advertiser {
	retryMax := cfg.MaxRetryBackoff
	if retryMax <= 0 {
		retryMax = 5 * time.Second
	}
	a := &Advertiser{
		client:       client,
		nodeID:       nodeID,
		batchSize:    cfg.BatchSize,
		interval:     cfg.Interval,
		maxRetries:   cfg.MaxRetries,
		retryInitial: cfg.RetryBackoff,
		retryMax:     retryMax,
		ch:           make(chan string, 1024),
		done:         make(chan struct{}),
		finished:     make(chan struct{}),
	}
	go a.loop()
	return a
}

// Enqueue schedules a single chunk hash for advertisement.
func (a *Advertiser) Enqueue(hash string) {
	if a == nil || a.client == nil || hash == "" {
		return
	}
	select {
	case a.ch <- hash:
		metrics.AdvertisementQueueDepth.Inc()
	default:
		a.scheduleRetry([]string{hash}, 0)
	}
}

// Reconcile enqueues manifest-owned chunk hashes, deduplicated.
func (a *Advertiser) Reconcile(hashes []string) {
	if a == nil || len(hashes) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		a.Enqueue(h)
	}
}

func (a *Advertiser) loop() {
	defer close(a.finished)
	t := time.NewTicker(a.interval)
	defer t.Stop()
	var buf []string
	flush := func() {
		if len(buf) == 0 {
			return
		}
		_ = a.flush(buf, 0)
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
					a.flushRetries(true)
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
			a.flushRetries(false)
		}
	}
}

func (a *Advertiser) scheduleRetry(hashes []string, attempt int) {
	if len(hashes) == 0 {
		return
	}
	backoff := a.retryInitial
	if attempt > 0 {
		for i := 1; i < attempt; i++ {
			backoff *= 2
			if backoff > a.retryMax {
				backoff = a.retryMax
				break
			}
		}
	}
	nextAt := time.Now().Add(backoff)
	a.retryMu.Lock()
	defer a.retryMu.Unlock()
	const maxRetryQueue = 4096
	for _, h := range hashes {
		if len(a.retry) >= maxRetryQueue {
			slog.Warn("advertisement retry queue full, dropping hash", "hash", h)
			metrics.AdvertisementFailures.Inc()
			continue
		}
		a.retry = append(a.retry, retryEntry{hash: h, attempt: attempt, nextAt: nextAt})
		metrics.AdvertisementQueueDepth.Inc()
		if attempt > 0 {
			metrics.AdvertisementRetries.Inc()
		}
	}
}

func (a *Advertiser) flushRetries(force bool) {
	now := time.Now()
	a.retryMu.Lock()
	var ready []string
	var attempts []int
	var rest []retryEntry
	for _, e := range a.retry {
		if force || !e.nextAt.After(now) {
			ready = append(ready, e.hash)
			attempts = append(attempts, e.attempt)
		} else {
			rest = append(rest, e)
		}
	}
	a.retry = rest
	a.retryMu.Unlock()
	for i, h := range ready {
		_ = a.flush([]string{h}, attempts[i])
	}
}

func (a *Advertiser) flush(hashes []string, attempt int) error {
	if a.client == nil || len(hashes) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := a.client.ReportChunks(ctx, &proto.ReportChunksRequest{
		NodeId:      a.nodeID,
		ChunkHashes: hashes,
	})
	if err != nil {
		if attempt+1 < a.maxRetries {
			a.scheduleRetry(hashes, attempt+1)
		} else {
			metrics.AdvertisementFailures.Add(float64(len(hashes)))
			slog.Warn("chunk advertisement failed after retries", "count", len(hashes), "err", err)
		}
		return err
	}
	metrics.AdvertisementSuccess.Add(float64(len(hashes)))
	metrics.AdvertisementQueueDepth.Sub(float64(len(hashes)))
	return nil
}

// Stop drains pending work and shuts down the background loop.
func (a *Advertiser) Stop() {
	if a == nil {
		return
	}
	close(a.done)
	<-a.finished
}
