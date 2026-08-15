package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OriginBytesSaved = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_origin_bytes_saved_total",
		Help: "Bytes served from peers or local cache instead of origin",
	})
	OriginBytesDownloaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_origin_bytes_downloaded_total",
		Help: "Bytes pulled from origin storage",
	})
	PeerBytesTransferred = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_peer_bytes_transferred_total",
		Help: "Bytes streamed from peer nodes",
	})
	SyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "spider_sync_duration_seconds",
		Help:    "Artifact sync duration",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
	})
	ChunkVerifyFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_chunk_verify_failures_total",
		Help: "SHA-256 validation failures",
	})
	CacheUsedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_cache_used_bytes",
		Help: "Local chunk cache size in bytes",
	})
	CacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_cache_hits_total",
		Help: "Chunks found in local disk cache",
	})
	ActivePeers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_active_peers_count",
		Help: "Healthy peers registered with the tracker",
	})
	StoreCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_store_cache_hits_total",
		Help: "Tracker metadata cache hits",
	})
	StoreCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_store_cache_misses_total",
		Help: "Tracker metadata cache misses",
	})
	StoreOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "spider_store_ops_total",
		Help: "Tracker store operations",
	}, []string{"op", "driver"})
)
