# Phase 2: Core Reliability & Framework Hardening Plan

**Document Status:** Approved Specification  
**Phase:** 2 of 6  
**Product name:** Spider  
**Focus:** Pluggable Store/Cache, compact seed advertisements, swarm scheduler, refcounted disk eviction, YAML config, Prometheus/health

---

## 1. Overview & Objectives

Phase 2 makes the PoC viable as a production daemon without claiming Redis, SQL, or Prometheus as differentiators (those are catch-up). Product bets: integrity-by-default (already in Phase 1), version-delta sync UX, topology + origin SLA, seed-based locate for 100GB+ trees, and desired-state pinning without Kubernetes.

### Key Objectives
1. **Pluggable `Store` + metadata `Cache`**: SQLite (default) / Postgres / memory stores; memory / Redis / none caches; write-path invalidation.
2. **Compact advertisements**: `ReportArtifact` / `LocateArtifact` for complete seeds; per-chunk rows only for partial nodes.
3. **Swarm scheduler**: locality + EWMA + rarest-first + per-peer inflight caps + circuit breaker.
4. **Refcounted disk LRU**: pins, watermarks; `spiderd` reconciles `cache.pinnedArtifacts`.
5. **Ops**: `spider.yaml`, slog, `/metrics` (`client_golang`), `/healthz`, `/readyz`, graceful drain, path-join guard.

**Not in Phase 2:** mTLS/signing/RBAC (Phase 3), CRDs (Phase 4), FastCDC/splice/regional trackers (Phase 5), RDMA/GDS (Phase 6). GORM is forbidden.

---

## 2. Catch-up vs better than alternatives

**Catch-up:** durable tracker, metadata cache, disk quota, health/metrics, YAML, rarest-first.

**Bets (document and CLI must encode these):**
1. Integrity as the default path (`spiderctl verify`).
2. `sync` always prints reused / peer / origin / `origin_bytes_saved`.
3. Topology ranking + origin fallback without Kubernetes.
4. Seed advertisements instead of a SQL row per chunk per node.
5. `pinnedArtifacts` reconciled by `spiderd` from YAML.

---

## 3. Pluggable Store and Cache

Drivers register by name (`RegisterStore`, `RegisterCache`). Callers never import a concrete driver.

### 3.1 `Store` (`pkg/store`)

| Driver | Use |
|---|---|
| `memory` | Tests and single-process demos |
| `sqlite` | Default local/dev (WAL) |
| `postgres` | HA production, same SQL subset |

Methods: peers (register, heartbeat, get, list, deregister), artifacts (put, get), artifact seeds, sparse chunk locations, stale-peer sweep, ping.

### 3.2 Metadata `Cache` (`pkg/metacache`) — not the on-disk chunk store

| Driver | Use |
|---|---|
| `none` | Every read hits Store |
| `memory` | Default; single tracker replica |
| `redis` | Shared cache for multiple tracker processes |

`CachedStore` implements `Store`: reads fill cache; **writes hit Store first, then invalidate**.

**Invalidation:** peer keys + `peers:list` on register/status/deregister; artifact keys on put; `seeds:{id}` / `locate:{id}` on seed reports; `chunks:{hash}` on chunk reports. Heartbeats persist to Store on an interval (~5s), not on every beat. Cache TTL shorter than stale expiry (5–10s).

### 3.3 SQL schema

```sql
CREATE TABLE IF NOT EXISTS peers (
    node_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    region TEXT NOT NULL,
    zone TEXT NOT NULL,
    rack TEXT NOT NULL,
    host TEXT NOT NULL,
    last_heartbeat INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    manifest_json BLOB NOT NULL,
    UNIQUE(name, version)
);
CREATE TABLE IF NOT EXISTS artifact_seeds (
    artifact_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (artifact_id, node_id)
);
CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_hash TEXT NOT NULL,
    node_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (chunk_hash, node_id)
);
```

Default data dir: `/var/lib/spider`.

### 3.4 Config

```yaml
store:
  driver: sqlite          # memory | sqlite | postgres
  dsn: /var/lib/spider/tracker.db
cache:
  driver: memory          # none | memory | redis
  ttl: 10s
  redis:
    addr: 127.0.0.1:6379
    prefix: spider:
```

Metrics: `spider_store_cache_hits_total`, `spider_store_cache_misses_total`, `spider_store_ops_total`.

---

## 4. Swarm scheduler (`pkg/scheduler`)

Score = locality + EWMA RTT/throughput − inflight − blacklist. Rarest-first among missing chunks. Per-peer token bucket and max inflight. Circuit breaker: 3 verify/timeout failures → `UNTRUSTED` for 15 minutes. Origin concurrency cap unchanged.

---

## 5. Disk cache manager

Chunk index: `hash → size, last_access, refcount`. Pin holds refs for an artifact id. Evict unreferenced, unpinned, oldest until low watermark. `HasChunk` must not hold a global mutex across disk I/O.

---

## 6. Ops surface

- YAML + flags override (`pkg/config`)
- `log/slog` (`--log-format json|text`)
- HTTP `/metrics`, `/healthz`, `/readyz` (ready = Store ping)
- Graceful shutdown: cancel sync context, `DeregisterPeer`, close pools
- Path-join guard in materializer (Phase 3 still owns mTLS and signed manifests)
- Compose: Prometheus + Grafana; optional Redis sidecar
- Copy remains default materialization; hardlink optional

---

## 7. Implementation Checklist

- [x] `pkg/config` YAML including `store.driver` / `cache.driver`
- [x] `Store` + `Cache` interfaces, sqlite/postgres/memory, memory/redis/none, `CachedStore` invalidation tests
- [x] Tracker RPCs `ReportArtifact` / `LocateArtifact` / `DeregisterPeer`
- [x] Swarm scheduler + cancelable engine jobs; sync prints reused/peer/origin/saved
- [x] Refcounted LRU, pin/unpin, `spiderd` pin reconcile
- [x] Path sanitization, drain, Prometheus/Grafana, persistence/eviction/blacklist tests
- [x] Immediate chunk advertisement, live peer discovery, upload backpressure, streaming/resumable chunk ingest
- [x] Bring-your-own Postgres/Redis/`none` via YAML, flags, and `SPIDER_*` env
