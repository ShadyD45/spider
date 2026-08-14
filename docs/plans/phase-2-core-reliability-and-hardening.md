# Phase 2: Core Reliability & Framework Hardening Plan

**Document Status:** Approved Specification  
**Phase:** 2 of 6  
**Focus:** Persistent Storage, Adaptive Scheduler, Cache Management, and Production Observability

---

## 1. Overview & Objectives

Phase 2 transitions the Artifact Mesh prototype into a production-grade daemon framework by replacing in-memory metadata with persistent storage, introducing intelligent peer scheduling, implementing cache eviction policies, and exposing standard Prometheus observability metrics.

### Key Objectives
1. **Persistent Metadata Engine**: Replace in-memory Tracker state with persistent SQLite (WAL mode) / PostgreSQL backend.
2. **Adaptive Peer & Download Scheduler**: Dynamic peer selection based on measured transfer latency, active connections, dynamic bandwidth throttling, and exponential backoff retries.
3. **Cache Eviction & Garbage Collection**: Implement LRU (Least Recently Used) cache eviction, max storage disk quotas (`maxBytes`), and artifact pinning.
4. **Resilience & Fault Tolerance**: Health probes, peer blacklisting for bad data/timeouts, and graceful daemon teardown.
5. **Observability Suite**: Prometheus metrics endpoint (`/metrics`) exposing `origin_bytes_saved`, peer throughput, cache hit ratios, and structured JSON audit logging.

---

## 2. Component Design & Technical Specifications

### 2.1 Persistent Tracker Storage (`pkg/tracker/db`)
Replace temporary map storage with an interface supporting SQLite (local dev) and PostgreSQL (HA production).

#### Schema Definition (`schema.sql`)
```sql
CREATE TABLE IF NOT EXISTS peers (
    node_id VARCHAR(64) PRIMARY KEY,
    ip_address VARCHAR(45) NOT NULL,
    grpc_port INT NOT NULL,
    region VARCHAR(32) NOT NULL,
    zone VARCHAR(32) NOT NULL,
    rack VARCHAR(32) NOT NULL,
    host VARCHAR(64) NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(16) NOT NULL -- 'HEALTHY', 'DEGRADED', 'STALE'
);

CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_hash VARCHAR(64) NOT NULL,
    node_id VARCHAR(64) REFERENCES peers(node_id) ON DELETE CASCADE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (chunk_hash, node_id)
);

CREATE INDEX idx_chunk_locations_hash ON chunk_locations(chunk_hash);
CREATE INDEX idx_peers_heartbeat ON peers(last_heartbeat);
```

- **Stale Peer Expiration**: Background worker runs every 10 seconds:
  ```sql
  DELETE FROM peers WHERE last_heartbeat < NOW() - INTERVAL '30 seconds';
  ```

---

### 2.2 Adaptive Download Scheduler (`pkg/scheduler`)

The Phase 2 scheduler dynamic balances load across candidate peers:

```text
Candidates for Chunk X: [NodeA, NodeB, NodeC, Origin]
         |
         v
  Scoring Engine
    - Locality Weight (Same host: 100, Same rack: 80, Same zone: 50)
    - Latency Penalty (Historical round-trip time)
    - Active Transfer Penalty (Current active streams)
         |
         v
  Top Candidate Selected -> Stream Chunk
         |
         v
  Record Stats (Bytes, Duration, Success/Failure)
```

- **Peer Rate-Limiting & Backpressure**: Token bucket rate limiters per peer connection to prevent saturating seed node NICs.
- **Peer Blacklisting**: If a peer returns 3 consecutive SHA-256 verification failures or network timeouts within 5 minutes, mark the peer as `UNTRUSTED` for 15 minutes.

---

### 2.3 Cache Manager & Garbage Collector (`pkg/cache/manager.go`)

Each `artifactd` node manages local disk usage against configured limits:

```yaml
cache:
  maxBytes: 500GB
  lowWatermark: 0.80  # Trigger eviction when disk exceeds 80% of maxBytes
  highWatermark: 0.90 # Emergency eviction threshold
  policy: lru
  pinnedArtifacts:
    - "gpt-x@2.0"
```

- **Eviction Protocol**:
  1. Monitor local `/var/lib/artifactd` storage utilization.
  2. If disk usage > `highWatermark * maxBytes`, select non-pinned artifacts sorted by `last_accessed_at` ascending.
  3. Evict unreferenced chunks until storage usage drops to `lowWatermark * maxBytes`.
  4. Ensure pinned artifacts are explicitly exempt from eviction.

---

### 2.4 Observability & Metrics (`pkg/metrics`)

Expose standard Prometheus metrics on `artifactd` and `tracker` via HTTP `/metrics`.

#### Core Metric Catalog
| Metric Name | Type | Description |
|---|---|---|
| `daf_origin_bytes_saved_total` | Counter | Cumulative bytes downloaded via P2P mesh instead of origin |
| `daf_origin_bytes_downloaded_total` | Counter | Cumulative bytes pulled directly from origin storage (S3/MinIO) |
| `daf_peer_bytes_transferred_total` | Counter | Total bytes streamed to/from peer nodes |
| `daf_sync_duration_seconds` | Histogram | Time taken to complete artifact synchronization |
| `daf_chunk_verify_failures_total` | Counter | Total SHA-256 validation errors detected |
| `daf_cache_used_bytes` | Gauge | Current size of local chunk cache in bytes |
| `daf_cache_hits_total` | Counter | Total chunks retrieved from local disk cache |
| `daf_active_peers_count` | Gauge | Total active healthy peers registered with tracker |

#### Structured Audit Logging
JSON output format containing:
```json
{
  "timestamp": "2026-08-14T20:40:00Z",
  "level": "INFO",
  "component": "transfer_engine",
  "event": "chunk_download_complete",
  "artifact_id": "sha256:abc123",
  "chunk_hash": "sha256:789xyz",
  "source_peer": "worker-1",
  "bytes": 4194304,
  "duration_ms": 42,
  "throughput_mbps": 800.0
}
```

---

## 3. Implementation Checklist

- [ ] Add GORM / SQL driver support for SQLite and PostgreSQL.
- [ ] Implement persistent DB migrations for Tracker peer and chunk location tables.
- [ ] Upgrade `pkg/scheduler` with dynamic scoring, peer load balancing, and rate limiting.
- [ ] Implement peer blacklisting and failure backoff mechanics.
- [ ] Implement `pkg/cache/manager.go` with LRU eviction and pinned artifact rules.
- [ ] Integrate Prometheus exporter (`github.com/prometheus/client_model`) into `artifactd` and `tracker`.
- [ ] Update `podman-compose.yml` to include Prometheus & Grafana dashboard service.
- [ ] Add integration test verifying eviction logic under artificial disk space pressure.
