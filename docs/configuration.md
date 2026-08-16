# Spider configuration and tuning guide

Spider is configured primarily through **`spider.yaml`**, passed to both `tracker` and `spiderd` via `--config`. CLI flags and environment variables override or supplement YAML where noted.

**Example file:** [`spider.yaml`](../spider.yaml) at the repository root.

---

## Quick reference

| Area | YAML keys | Used by | Purpose |
|------|-----------|---------|---------|
| Tracker store | `store.*` | `tracker` | Durable peer/chunk index (SQLite, Postgres, memory) |
| Tracker meta cache | `metaCache.*` (`cache` alias) | `tracker` | Hot metadata cache (memory, Redis, none) |
| Chunk disk cache | `chunkCache.*` (`diskCache` alias) | `spiderd` | On-disk content-addressed chunks |
| Peer downloads | `download.*` | `spiderd` | Max concurrent chunk fetch workers |
| Origin fallback | `origin.*` | `spiderd` | Max concurrent origin reads |
| Peer uploads | `upload.*` | `spiderd` | Upload concurrency, **node-wide** bandwidth cap, queue |
| Outbound gRPC | `peerClient.*` | `spiderd` | Connection pool size and idle eviction |
| Chunk ads | `advertisement.*` | `spiderd` | Batched `ReportChunks` + retry on tracker failure |
| Live discovery | `peerDiscovery.*` | `spiderd` | How often to refresh peer locations mid-sync |
| Fetch retries | `retry.*` | `spiderd` | Peer/origin attempt count and backoff |
| HTTP / logs | `httpAddr`, `logFormat` | both | Metrics, health, log format |

---

## Environment variables

| Variable | Overrides | Example |
|----------|-----------|---------|
| `SPIDER_STORE_DRIVER` | `store.driver` | `postgres` |
| `SPIDER_STORE_DSN` | `store.dsn` | `postgres://user:pass@db:5432/spider` |
| `SPIDER_CACHE_DRIVER` | `metaCache.driver` | `redis` |
| `SPIDER_CACHE_REDIS_URL` | `metaCache.redis.url` | `redis://:pass@cache:6379/0` |
| `SPIDER_CACHE_REDIS_ADDR` | `metaCache.redis.addr` | `cache.example:6379` |
| `SPIDER_CACHE_REDIS_PASSWORD` | `metaCache.redis.password` | — |
| `NODE_ID`, `TRACKER_ADDR`, `CACHE_DIR`, `REGION`, `ZONE`, `RACK`, `HOST` | `spiderd` CLI defaults | compose / k8s |
| `S3_BUCKET`, `S3_ENDPOINT`, `AWS_*`, `MINIO_*` | S3 origin fallback on `spiderd` | MinIO in compose |

`${VAR}` expansion is applied inside YAML strings before env overlay.

---

## Tracker backends (`store`, `metaCache`)

### `store` — durable metadata

```yaml
store:
  driver: sqlite          # memory | sqlite | postgres
  dsn: /var/lib/spider/tracker.db
  pool:
    maxOpenConns: 8
    maxIdleConns: 8
    connMaxLifetime: 0s
    connMaxIdleTime: 0s
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `driver` | `sqlite` | Use `postgres` for HA tracker; `memory` for tests only |
| `dsn` | `/var/lib/spider/tracker.db` | Postgres: full connection URL with `sslmode` |
| `pool.*` | driver defaults | Raise `maxOpenConns` for large fleets hitting tracker |

**CLI overrides:** `--store-driver`, `--store-dsn`

### `metaCache` — tracker read cache (not chunk bytes)

```yaml
metaCache:
  driver: memory    # none | memory | redis
  ttl: 10s
  redis:
    url: ${SPIDER_CACHE_REDIS_URL}
    addr: 127.0.0.1:6379
    prefix: "spider:"
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `driver` | `memory` | `none` if Postgres is fast enough; `redis` for shared tracker cache |
| `ttl` | `10s` | Lower = fresher locate results, more store reads |
| `redis.url` | — | Preferred over `addr`+`password` when using managed Redis |

**CLI overrides:** `--cache-driver`, `--cache-redis-url`, `--cache-redis-addr`

---

## Worker chunk cache (`chunkCache`)

```yaml
chunkCache:
  dir: /var/lib/spider
  maxBytes: 536870912000   # 500 GiB
  lowWatermark: 0.80
  highWatermark: 0.90
  pinnedArtifacts: []
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `dir` | `/var/lib/spider` | Fast local SSD; separate from materialized model path |
| `maxBytes` | 500 GiB | Set to ~80% of available disk for chunk shard dir |
| `lowWatermark` / `highWatermark` | 0.80 / 0.90 | LRU eviction runs between these thresholds |
| `pinnedArtifacts` | `[]` | Artifact IDs never evicted (seed nodes) |

**CLI override:** `--cache-dir` on `spiderd`

---

## Data plane tuning (`spiderd`)

### `download` — parallel chunk fetch workers

```yaml
download:
  maxConcurrency: 8
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `maxConcurrency` | `8` | Goroutines pulling missing chunks. Raise on fast LAN + many peers; lower if disk-bound |

### `origin` — origin storage concurrency cap

```yaml
origin:
  maxConcurrency: 4
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `maxConcurrency` | `4` | Limits simultaneous origin reads **per node**. Prevents thundering herd on S3/MinIO |

### `upload` — serving chunks to peers

```yaml
upload:
  maxConcurrency: 16
  maxBandwidthMbps: 0    # 0 = unlimited
  maxQueueSize: 100
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `maxConcurrency` | `16` | Max simultaneous `GetChunk` streams **out** of this node |
| `maxBandwidthMbps` | `0` (unlimited) | **Node-wide** cap shared by all uploads (`golang.org/x/time/rate`). Set to NIC budget (e.g. `5000` for ~5 Gbps) |
| `maxQueueSize` | `100` | Waiters when concurrency slots full; `0` = fail fast |

### `peerClient` — outbound gRPC to peers

```yaml
peerClient:
  maxConnections: 64
  idleTimeout: 2m
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `maxConnections` | `64` | Max cached `*grpc.ClientConn` per spiderd process |
| `idleTimeout` | `2m` | Close unused peer connections after idle period |

### `advertisement` — reporting verified chunks to tracker

```yaml
advertisement:
  batchSize: 16
  interval: 100ms
  maxRetries: 5
  retryBackoff: 100ms
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `batchSize` | `16` | Hashes per `ReportChunks` RPC |
| `interval` | `100ms` | Max delay before flushing a partial batch |
| `maxRetries` | `5` | Retries if tracker is temporarily unavailable |
| `retryBackoff` | `100ms` | Initial backoff; doubles up to `retry.backoff.max` |

Chunks are **never** advertised until SHA-256 verified and atomically committed to disk.

### `peerDiscovery` — mid-sync location refresh

```yaml
peerDiscovery:
  refreshInterval: 500ms
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `refreshInterval` | `500ms` | Poll tracker for updated peer locations during active sync. Lower = faster swarm reaction; more tracker load |

Tracker responses replace stale per-chunk peer lists (snapshot reconciliation).

### `retry` — chunk fetch retries

```yaml
retry:
  maxAttempts: 3
  backoff:
    initial: 100ms
    max: 2s
```

| Knob | Default | Tuning notes |
|------|---------|--------------|
| `maxAttempts` | `3` | Per-chunk peer attempts before origin fallback |
| `backoff.initial` / `backoff.max` | `100ms` / `2s` | Exponential delay between peer rounds |

---

## Scheduler behavior (not YAML — internal)

The engine uses `pkg/scheduler` with these policies (tuned via code defaults today):

| Signal | Effect |
|--------|--------|
| Topology distance | Prefer same host → rack → zone → region |
| Inflight per peer | Skip saturated peers (`Begin` returns false) |
| EWMA throughput | Prefer peers with higher observed bytes/sec |
| EWMA RTT | Tie-break toward lower latency |
| Circuit breaker | 3 verify/timeout failures → untrusted for 15 min |
| Rarest-first | Dynamic dequeue: fetch chunks with fewest known peers first |
| `sortByAssigned` | Spread load across peers during a sync |

---

## Prometheus metrics

Exposed on `httpAddr` (default `:9090`) at `/metrics`.

| Metric | Meaning |
|--------|---------|
| `spider_origin_bytes_downloaded_total` | Bytes read from origin |
| `spider_peer_bytes_transferred_total` | Bytes received from peers (legacy name) |
| `spider_peer_bytes_downloaded_total` | Bytes received from peers on this node |
| `spider_peer_bytes_uploaded_total` | Bytes sent to peers from this node |
| `spider_cache_bytes_reused_total` | Bytes served from local cache at sync start |
| `spider_origin_bytes_avoided_total` | Peer + cache reuse (primary savings metric) |
| `spider_swarm_unique_sources` | Distinct peer sources during active sync |
| `spider_swarm_chunks_with_peers` | Missing chunks with ≥1 known peer |
| `spider_swarm_amplification_ratio` | Peer bytes ÷ origin bytes (last sync) |
| `spider_advertisement_success_total` | Chunk ads acknowledged by tracker |
| `spider_advertisement_failures_total` | Ads failed after all retries |
| `spider_advertisement_queue_depth` | Pending hashes waiting to advertise |

**Swarm amplification (PromQL):**

```promql
rate(spider_peer_bytes_transferred_total[5m])
/
rate(spider_origin_bytes_downloaded_total[5m])
```

See [benchmarks.md](benchmarks.md) for interpretation caveats.

---

## Health endpoints

| Path | Binary | Checks |
|------|--------|--------|
| `/healthz` | tracker, spiderd | Process alive |
| `/readyz` | tracker, spiderd | Store/cache writable |
| `/metrics` | both | Prometheus scrape |

---

## Recommended profiles

### Local dev / PoC

```yaml
store: { driver: sqlite, dsn: ./tracker.db }
metaCache: { driver: memory, ttl: 10s }
chunkCache: { dir: ./tmp/spider-cache, maxBytes: 10737418240 }
download: { maxConcurrency: 4 }
upload: { maxConcurrency: 8, maxBandwidthMbps: 0 }
```

### Compose testbed (3 workers)

Use defaults from [`spider.yaml`](../spider.yaml). Redis meta cache + SQLite store.

### Production-oriented seed node

```yaml
chunkCache:
  maxBytes: <80% disk>
  pinnedArtifacts: ["sha256:<artifact-id>"]
upload:
  maxConcurrency: 32
  maxBandwidthMbps: 10000   # 10 Gbps node budget
download: { maxConcurrency: 16 }
origin: { maxConcurrency: 8 }
advertisement: { batchSize: 32, interval: 50ms }
```

### Production-oriented leaf worker

```yaml
download: { maxConcurrency: 16 }
origin: { maxConcurrency: 2 }    # rarely hit origin
upload: { maxConcurrency: 8, maxBandwidthMbps: 5000 }
peerDiscovery: { refreshInterval: 250ms }
```

---

## CLI quick reference

### `tracker`

```bash
./bin/tracker --config=spider.yaml --port=50051
```

### `spiderd`

```bash
./bin/spiderd \
  --config=spider.yaml \
  --node-id=worker-1 \
  --port=50052 \
  --tracker=127.0.0.1:50051 \
  --rack=rack-1 --zone=zone-a --region=us-east-1
```

S3 origin fallback activates only when `S3_BUCKET` (or `--s3-bucket`) is set.

### `spiderctl`

```bash
./bin/spiderctl publish --source=/data/model --name=my-model --version=1.0
./bin/spiderctl sync --manifest=manifest.json --dest=/models/my-model
./bin/spiderctl benchmark --size=500 --workers=6 --chunk-size=4
```

---

## Related docs

- [benchmarks.md](benchmarks.md) — performance methodology and recorded numbers
- [plans/phase-2-core-reliability-and-hardening.md](plans/phase-2-core-reliability-and-hardening.md) — Phase 2 design notes
- [plans/00-overview.md](plans/00-overview.md) — roadmap index
