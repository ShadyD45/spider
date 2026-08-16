# Spider architecture

Spider is a content-addressed P2P artifact distribution mesh. This document describes how the components fit together, how data flows, and the design principles behind the implementation.

For YAML knobs and tuning, see **[configuration.md](configuration.md)**. For performance numbers, see **[benchmarks.md](benchmarks.md)**.

---

## Control plane vs data plane

The **tracker** (`cmd/tracker`) is the control plane: peer registry, artifact seeds, and sparse chunk location index. It never stores or proxies artifact bytes.

The **data plane** (`cmd/spiderd`) streams 4 MiB content-addressed chunks directly between workers over gRPC, with origin storage (S3 / MinIO / local FS) as a verified fallback.

```mermaid
flowchart TB
  subgraph origin_layer [Origin Layer]
    Origin["Origin Storage\nS3 / MinIO / Local FS"]
  end

  subgraph control_plane [Control Plane — metadata only]
    Tracker["tracker\npkg/tracker"]
    Store[("store\nSQLite / Postgres")]
    MetaCache[("metaCache\nmemory / Redis")]
    Tracker --> Store
    Tracker --> MetaCache
  end

  subgraph data_plane [Data Plane — bytes never through tracker]
    SpiderdA["spiderd\nworker A"]
    SpiderdB["spiderd\nworker B"]
    SpiderdC["spiderd\nworker C"]
    ChunkStoreA[("chunkCache\ncontent-addressed disk")]
    ChunkStoreB[("chunkCache")]
    ChunkStoreC[("chunkCache")]
    SpiderdA --- ChunkStoreA
    SpiderdB --- ChunkStoreB
    SpiderdC --- ChunkStoreC
    SpiderdA <-->|"gRPC GetChunk\nstreaming 64KiB frames"| SpiderdB
    SpiderdB <-->|"P2P mesh"| SpiderdC
    SpiderdA <-->|"P2P mesh"| SpiderdC
  end

  subgraph clients [Operators]
    Spiderctl["spiderctl\npublish / sync / verify"]
  end

  Origin -->|"fallback read\norigin.maxConcurrency"| SpiderdA
  Origin --> SpiderdB
  Spiderctl -->|"Register / Locate / ReportChunks"| Tracker
  SpiderdA -->|"heartbeat + chunk ads"| Tracker
  SpiderdB --> Tracker
  SpiderdC --> Tracker
  Spiderctl --> SpiderdA
```

---

## Sync lifecycle

A typical artifact distribution follows publish → locate → fetch → advertise → materialize.

```mermaid
sequenceDiagram
  participant Op as spiderctl
  participant Tr as tracker
  participant Seed as spiderd seed
  participant Leech as spiderd leecher
  participant Org as origin

  Op->>Seed: publish manifest + cache chunks
  Seed->>Tr: ReportChunks + ReportArtifact
  Op->>Leech: sync manifest
  Leech->>Tr: LocateArtifact + LocateChunks
  Tr-->>Leech: peer list per chunk
  loop missing chunks rarest-first
    Leech->>Seed: GetChunk stream
    Seed-->>Leech: verified bytes
    Leech->>Leech: AppendPartial + CommitPartial
    Leech->>Tr: ReportChunks batch
  end
  Note over Leech,Org: If no peer available
  Leech->>Org: ReadChunkTo stream
  Leech->>Leech: materialize to dest path
```

### Engine sync phases

1. **Inventory** — Walk manifest; skip chunks already in local `chunkCache` (count as cache reuse).
2. **Discover** — `LocateArtifact` + `LocateChunks` from tracker; periodic refresh replaces stale peer lists mid-sync.
3. **Fetch** — Worker pool dequeues missing chunks **rarest-first** (dynamic reorder as discovery updates).
4. **Verify & persist** — Stream into partial files; incremental hash; atomic rename into `chunks/sha256/...`.
5. **Advertise** — Batched `ReportChunks` with retry/backoff (never before verify + commit).
6. **Materialize** — Assemble verified chunks into destination directory tree.

---

## Component map

```mermaid
flowchart LR
  subgraph binaries [Binaries]
    T[tracker]
    D[spiderd]
    C[spiderctl]
  end

  subgraph engine_pkg [pkg/engine]
    Sync[Sync scheduler]
    Pub[Publisher]
    Ad[Advertiser batch+retry]
  end

  subgraph peer_pkg [pkg/peer]
    Srv[GetChunk server\nupload limits + bandwidth]
    Pool[ClientPool\neviction]
  end

  subgraph support [Supporting packages]
    Sch[scheduler\nEWMA RTT + throughput]
    Cache[cache ChunkStore]
    Met[metrics Prometheus]
  end

  C --> D
  D --> Sync
  Sync --> Sch
  Sync --> Pool
  Sync --> Cache
  Sync --> Ad
  D --> Srv
  T --> Met
  D --> Met
  Ad --> T
  Pool --> Srv
```

---

## Chunk data path

```mermaid
flowchart LR
  subgraph ingest [Download path]
    PeerOrOrigin["Peer GetChunk\nor origin ReadChunkTo"]
    Partial["tmp/partial/hash"]
    Shard["chunks/sha256/xx/..."]
    PeerOrOrigin -->|"io.Pipe stream"| Partial
    Partial -->|"CommitPartial\nSHA-256 verify"| Shard
  end

  subgraph serve [Upload path]
    Shard2["chunks/sha256/xx/..."]
    Limiter["node-wide\nrate.Limiter"]
  end

  Shard --> Shard2
  Shard2 -->|"64 KiB frames"| Limiter
  Limiter -->|"gRPC stream"| Requester["peer leecher"]
```

---

## Scheduler and peer selection

When fetching a chunk, the engine ranks peers using signals from `pkg/scheduler`:

```mermaid
flowchart TD
  Candidates["Tracker peer list\nsnapshot per chunk"]
  Rank["RankPeers"]
  Topo["Topology distance\nhost > rack > zone > region"]
  Inflight["Inflight cap per peer"]
  TP["EWMA throughput\ndesc"]
  RTT["EWMA RTT\nasc"]
  CB["Circuit breaker\n3 failures → 15 min untrusted"]
  Pick["Begin slot → GetChunk"]
  Fallback["Origin fallback\norigin.maxConcurrency"]

  Candidates --> Rank
  Rank --> Topo --> Inflight --> TP --> RTT
  Rank --> CB
  TP --> Pick
  Pick -->|"fail / exhausted"| Fallback
```

Work ordering across chunks uses **rarest-first**: chunks with the fewest known peers are fetched first, re-ranked on each dequeue as discovery refreshes.

---

## Layer reference (Phase 2 + 2.5)

| Layer | Package / binary | Role |
| :--- | :--- | :--- |
| **Config** | `pkg/config`, [`spider.yaml`](../spider.yaml), [configuration.md](configuration.md) | All knobs: store, caches, download/upload, ads, retries, peer client pool |
| **Tracker store** | `pkg/store` | Durable peers, artifact seeds, sparse chunk index (SQLite WAL default; Postgres via DSN) |
| **Tracker meta cache** | `pkg/metacache` | Optional Redis/memory/`none` fronting store reads |
| **Scheduler** | `pkg/scheduler` | Locality rank, EWMA RTT + throughput, rarest-first, inflight caps, circuit breaker |
| **Engine** | `pkg/engine` | Batched chunk ads with retry, live peer refresh + stale reconciliation, streaming ingest, origin fallback |
| **Chunk store** | `pkg/cache` (`ChunkStore`, `QuotaManager`) | Content-addressed files, resumable partials, refcounted LRU pins |
| **Peer transport** | `pkg/peer` | gRPC streaming, node-wide upload bandwidth (`golang.org/x/time/rate`), connection pool lifecycle |
| **Observability** | `pkg/metrics`, `pkg/httpserver` | Origin/peer/cache/swarm metrics; health/readiness on tracker and workers |
| **Build / deploy** | `scripts/build-binaries.*`, `Containerfile` | Cross-compile on host → slim Alpine runtime image (`localhost/spider:local`) |

### Tracker backends

Bring-your-own store and meta cache (YAML, flags, or `SPIDER_*` env). Combinations are independent:

```yaml
store:
  driver: postgres
  dsn: ${SPIDER_STORE_DSN}
metaCache:
  driver: redis
  redis:
    url: ${SPIDER_CACHE_REDIS_URL}
```

`cache:` / `diskCache:` remain valid aliases for `metaCache` / `chunkCache`. The tracker never stores artifact bytes — each `spiderd` keeps chunks on local disk.

S3/MinIO origin fallback on workers is enabled only when `S3_BUCKET` is explicitly set.

---

## Design principles

1. **Artifact-first** — Models, checkpoints, datasets, and release trees are arbitrary multi-file manifests of immutable content-addressed chunks.
2. **Control / data plane separation** — Tracker tracks metadata and topology only; bytes flow peer-to-peer or from origin.
3. **Content addressing & verification** — Every chunk is `sha256:<hex>`. Chunks are verified before cache commit and before advertisement.
4. **Topology-aware proximity** — Scheduler prefers `Host` > `Rack` > `Zone` > `Region` > remote.
5. **Standalone with extension points** — Runs on bare metal, VMs, or containers; clean hooks for future Kubernetes operator integration.

---

## Integrity layers

Spider verifies integrity at five points (see README for CLI commands):

| Layer | Location | What is checked |
|-------|----------|-----------------|
| Wire transfer | `pkg/peer/client` | Stream hash on complete download |
| Atomic commit | `pkg/cache` | On-disk re-hash before rename into shard store |
| Manifest | `api/v1` | Canonical artifact ID and chunk layout |
| Materialize | `pkg/materializer` | Per-chunk hash while assembling files |
| Offline audit | `pkg/verifier` | Directory tree vs manifest |

---

## Related docs

- [configuration.md](configuration.md) — YAML reference and tuning profiles
- [benchmarks.md](benchmarks.md) — performance methodology
- [plans/phase-2-core-reliability-and-hardening.md](plans/phase-2-core-reliability-and-hardening.md) — Phase 2 design notes
