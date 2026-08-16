package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OriginBytesSaved = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_origin_bytes_saved_total",
		Help: "Deprecated alias for spider_origin_bytes_avoided_total",
	})
	OriginBytesAvoided = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_origin_bytes_avoided_total",
		Help: "Bytes not read from origin (peer transfers + local cache reuse)",
	})
	OriginBytesDownloaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_origin_bytes_downloaded_total",
		Help: "Bytes pulled from origin storage",
	})
	PeerBytesTransferred = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_peer_bytes_transferred_total",
		Help: "Bytes received from peer nodes",
	})
	PeerBytesUploaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_peer_bytes_uploaded_total",
		Help: "Bytes sent to peer nodes from this node",
	})
	PeerBytesDownloaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_peer_bytes_downloaded_total",
		Help: "Bytes received from peer nodes on this node",
	})
	CacheBytesReused = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_cache_bytes_reused_total",
		Help: "Bytes of chunks already present in local cache at sync start",
	})
	SwarmUniqueSources = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_swarm_unique_sources",
		Help: "Distinct peer sources seen during the current artifact sync",
	})
	SwarmChunksWithPeers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_swarm_chunks_with_peers",
		Help: "Missing chunks with at least one known peer during sync",
	})
	SwarmAvgChunkReplication = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_swarm_avg_chunk_replication",
		Help: "Average peer source count per missing chunk during sync",
	})
	SwarmAmplificationRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_swarm_amplification_ratio",
		Help: "Peer bytes transferred divided by origin bytes downloaded for the last completed sync",
	})
	AdvertisementSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_advertisement_success_total",
		Help: "Chunk hashes successfully reported to the tracker",
	})
	AdvertisementFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_advertisement_failures_total",
		Help: "Chunk advertisement attempts that failed after retries",
	})
	AdvertisementRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "spider_advertisement_retries_total",
		Help: "Chunk advertisement retry attempts scheduled after transient failures",
	})
	AdvertisementQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_advertisement_queue_depth",
		Help: "Pending chunk hashes waiting to be advertised",
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
	UploadBandwidthLimitMbps = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_upload_bandwidth_limit_mbps",
		Help: "Configured node-wide P2P upload bandwidth cap in Mbps (0 = unlimited)",
	})
	FleetReadyNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "spider_fleet_ready_nodes",
		Help: "Nodes that reported artifact completion for the latest published artifact",
	})
	FleetTimeToReadySeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spider_fleet_time_to_ready_seconds",
		Help: "Seconds from artifact publish to fleet readiness threshold",
	}, []string{"threshold"})
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
