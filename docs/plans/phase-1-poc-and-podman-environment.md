# Phase 1: Proof-of-Concept (PoC) & Podman Testbed Plan

**Document Status:** Approved Specification  
**Phase:** 1 of 6  
**Focus:** Core Engine Primitives, Local P2P Distribution, and Podman Container Testing Environment

---

## 1. Overview & Objectives

Phase 1 establishes a fully functional end-to-end Proof-of-Concept (PoC) for the Distributed Artifact Fabric (DAF). It provides a clean, modular Go codebase that isolates the core distribution engine from complex orchestrators like Kubernetes.

### Key Objectives
1. Implement the **Artifact Manifest**, **Chunker**, and **SHA-256 Content Addressing**.
2. Build the **Content-Addressed Local Cache** (`/var/lib/artifactd/chunks`) with atomic verification (`tmp` -> rename).
3. Implement **Source Adapters** for Local Filesystem and S3/MinIO origin storage.
4. Implement the **Central Tracker** service for peer registration, heartbeat tracking, and chunk availability matching.
5. Implement the **`artifactd` Node Daemon** with a gRPC chunk streaming server and concurrent download engine.
6. Implement the **`artifactctl` CLI** for publishing, inspecting, syncing, and querying nodes.
7. Create a **Podman containerized multi-node test environment** (`podman-compose.yml`) simulating MinIO storage, tracker, and 3+ worker nodes across simulated racks/zones.
8. Execute **6 core validation experiments** proving origin bandwidth reduction, crash recovery, chunk corruption rejection, and deduplication.

---

## 2. Go Module & Component Architecture

### Package Structure
```text
artifact-mesh/
├── Containerfile                  # Multi-stage Containerfile for Go binaries
├── podman-compose.yml             # Podman multi-node cluster composition
├── go.mod                         # Go 1.22 module definition
├── go.sum
├── api/
│   └── v1/
│       ├── manifest.go            # Manifest JSON schema, generator, validator
│       └── proto/
│           ├── tracker.proto      # Tracker gRPC protocol definition
│           └── peer.proto         # Peer gRPC chunk streaming protocol definition
├── cmd/
│   ├── artifactctl/               # Publisher & node management CLI
│   ├── artifactd/                 # Local node daemon process
│   └── tracker/                   # Central metadata tracker daemon
├── pkg/
│   ├── chunk/                     # 4 MiB fixed chunker & SHA-256 calculator
│   ├── cache/                     # Disk-backed content-addressed store
│   ├── materializer/              # Reconstructs directory files from chunks
│   ├── source/                    # Storage interface + FS & S3/MinIO implementations
│   ├── tracker/                   # Tracker registry & in-memory peer map
│   ├── peer/                      # gRPC stream chunk server & client helper
│   ├── engine/                    # Concurrent download scheduler & fallback manager
│   ├── topology/                  # Locality scoring (Host > Rack > Zone > Region)
│   └── verifier/                  # SHA-256 cache audit & materialized-directory verifier
└── scripts/
    ├── generate-test-data.sh      # Synthetic data generator for benchmarks
    └── podman-poc-test.sh         # E2E test runner executing 6 validation experiments
```

---

## 3. Detailed Component Specifications

### 3.1 Chunker & Manifest (`pkg/chunk`, `api/v1/manifest.go`)
- **Default Chunk Size**: 4 MiB (4,194,304 bytes), configurable via flag.
- **Content Identity**: Hash = SHA-256 of raw chunk bytes.
- **Manifest Struct**:
  - `ArtifactID`: `sha256(manifest_canonical_json)`
  - `Files`: Array of relative paths, sizes, file permissions (`0644`), and chunk references (`hash`, `offset`, `size`).
  - Supports arbitrary multi-file directory structures.

### 3.2 Content-Addressed Local Cache (`pkg/cache`)
- Root directory: `/var/lib/artifactd/`
- Storage layout:
  ```text
  /var/lib/artifactd/
    chunks/sha256/aa/aabbcc...
    tmp/sha256-...tmp
    manifests/sha256-abc.json
    artifacts/<name>/<version>/
  ```
- **Atomic Persistence**:
  1. Download chunk to `/var/lib/artifactd/tmp/<hash>.tmp`.
  2. `fsync` the temp file, then compute SHA-256 of the **on-disk** bytes.
  3. If hash matches expected hash, rename atomically (`os.Rename`) to `/var/lib/artifactd/chunks/sha256/xx/xxxx...`.
  4. If hash does NOT match, delete temporary file and return `ErrHashMismatch`.

### 3.3 Materializer (`pkg/materializer`)
- Construct physical target directory from verified chunk store.
- Re-hashes every chunk while copying (or before hardlinking) so a bit-rotten cache cannot produce a READY artifact.
- Supports **file copying** or **hardlinking** (configurable, default copying for cross-device safety).
- Validates total file sizes and mode permissions upon completion.

### 3.4 Storage Source Adapters (`pkg/source`)
Interface:
```go
type Source interface {
    ListFiles(ctx context.Context, prefix string) ([]FileInfo, error)
    ReadChunk(ctx context.Context, path string, offset int64, size int64) ([]byte, error)
}
```
- **Filesystem Adapter**: Reads direct from local disk paths.
- **S3 / MinIO Adapter**: Uses AWS SDK for Go v2 (`s3.GetObject` with `Range: bytes=offset-end`).

### 3.5 Tracker Service (`cmd/tracker`, `pkg/tracker`)
- In-memory thread-safe registry tracking:
  - Active nodes: `NodeID`, IP/Port, Topology (`Region`, `Zone`, `Rack`, `Host`), Last Heartbeat.
  - Chunk Availability: `map[chunk_hash][]NodeID`.
- gRPC API (`api/v1/proto/tracker.proto`):
  - `RegisterPeer(PeerInfo) returns (RegisterResponse)`
  - `Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)`
  - `ReportChunks(ReportChunksRequest) returns (ReportChunksResponse)`
  - `LocateChunks(LocateChunksRequest) returns (LocateChunksResponse)` (returns candidate nodes ordered by topology proximity).

### 3.6 Node Daemon (`cmd/artifactd`, `pkg/engine`, `pkg/peer`)
- **Peer Server**: gRPC `GetChunk(ChunkRequest) returns (stream ChunkDataResponse)`.
  - Streams 64 KiB chunks over gRPC connection until 4 MiB chunk transfer completes.
- **Download Engine**:
  1. Check local cache; identify missing chunks.
  2. Query Tracker `LocateChunks` for missing chunk hashes.
  3. Rank candidate peers using `pkg/topology` locality score.
  4. Concurrently pull missing chunks from top-ranked peers (up to `max_concurrent_peer_downloads`).
  5. If no valid peers exist or peer transfers fail, fall back to Origin Source (MinIO/S3).
  6. Verify SHA-256, move to cache atomically, and notify Tracker via `ReportChunks`.
  7. Once all chunks are ready, invoke `Materializer` to instantiate local artifact folder and set status `READY`.

### 3.7 Publisher & Admin CLI (`cmd/spiderctl`, `cmd/artifactctl`)
- `spiderctl publish --source s3://bucket/gpt-x/v2 --name gpt-x --version 2.0`
- `spiderctl sync --manifest manifest.json --dest /models/gpt-x/2.0`
- `spiderctl inspect --manifest manifest.json`
- `spiderctl status`
- `spiderctl cache`
- `spiderctl benchmark --size 100 --workers 6`
- `spiderctl verify artifact --manifest manifest.json --dest /models/gpt-x/2.0`
- `spiderctl verify cache --cache-dir /var/lib/artifactd`

### 3.8 Cryptographic Verification & Integrity Auditing (`pkg/verifier`)
- **Per-Chunk SHA-256 Audit**: Re-verifies every chunk on disk against its hash name to detect bit-rot or silent data corruption.
- **Materialized File Validation**: Inspects entire materialized directories against canonical manifest (verifying file presence, sizes, modes, and computing SHA-256 hashes of every slice).
- **Corrupt Stream Rejection & Recovery**: Rejects corrupted peer stream frames and automatically recovers clean chunks from origin storage without workflow interruption.
- **Unit coverage**: cache mismatch, peer stream mismatch, origin mismatch, materializer abort on cache bit-rot, directory size/tamper/missing-file reports.

**Status (Phase 1 PoC):** Implemented in `pkg/cache`, `pkg/peer`, `pkg/engine`, `pkg/materializer`, `pkg/verifier`, and `spiderctl verify`.

---

## 4. Local Podman Test Environment (`deploy/podman`)

### 4.1 Podman Compose Setup (`podman-compose.yml`)
```yaml
version: '3.8'

networks:
  artifact-net:
    driver: bridge

services:
  minio:
    image: minio/minio:latest
    container_name: minio-origin
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    networks:
      - artifact-net

  tracker:
    build:
      context: .
      dockerfile: Containerfile
    container_name: Central-Tracker
    command: ["/app/tracker", "--port=50051"]
    ports:
      - "50051:50051"
    networks:
      - artifact-net

  worker-1:
    build:
      context: .
      dockerfile: Containerfile
    container_name: worker-node-1
    command: ["/app/artifactd", "--node-id=worker-1", "--rack=rack-1", "--zone=zone-a", "--tracker=tracker:50051", "--port=50052"]
    environment:
      - MINIO_ENDPOINT=minio:9000
    networks:
      - artifact-net
    depends_on:
      - tracker
      - minio

  worker-2:
    build:
      context: .
      dockerfile: Containerfile
    container_name: worker-node-2
    command: ["/app/artifactd", "--node-id=worker-2", "--rack=rack-1", "--zone=zone-a", "--tracker=tracker:50051", "--port=50052"]
    environment:
      - MINIO_ENDPOINT=minio:9000
    networks:
      - artifact-net
    depends_on:
      - tracker
      - minio

  worker-3:
    build:
      context: .
      dockerfile: Containerfile
    container_name: worker-node-3
    command: ["/app/artifactd", "--node-id=worker-3", "--rack=rack-2", "--zone=zone-b", "--tracker=tracker:50051", "--port=50052"]
    environment:
      - MINIO_ENDPOINT=minio:9000
    networks:
      - artifact-net
    depends_on:
      - tracker
      - minio
```

---

## 5. Phase 1 Validation Experiments

The script `scripts/podman-poc-test.sh` executes the following benchmark test battery automatically:

### Experiment 1: Baseline Origin Downloads
- 3 workers independently download a 1 GB synthetic artifact from MinIO without P2P enabled.
- Measure total origin network traffic (3.0 GB) and baseline completion time.

### Experiment 2: P2P Fan-Out Efficiency
- Worker 1 downloads artifact from MinIO.
- Workers 2 & 3 sync the artifact with P2P enabled.
- Verify origin traffic drops from 3.0 GB to ~1.0 GB (`origin_bytes_saved >= 66%`).

### Experiment 3: Peer Disruption / Fallback
- Kill Worker 1 (`podman stop worker-node-1`) midway through Worker 2's sync.
- Verify Worker 2 automatically detects missing peer stream and seamlessly falls back to MinIO without sync failure.

### Experiment 4: Restart & Resumability
- Stop Worker 2 at 50% download completion (`podman stop worker-node-2`).
- Restart Worker 2 (`podman start worker-node-2`).
- Verify Worker 2 skips previously verified chunks and completes sync downloading only the remaining 50%.

### Experiment 5: Corrupt Chunk Rejection
- Inject corrupt chunk data into a peer's stream response.
- Verify `artifactd` SHA-256 verification fails, corrupt temporary chunk is deleted, peer is marked degraded, and clean chunk is re-fetched from origin.

### Experiment 6: Version Deduplication
- Publish Version 1.0 (1 GB) and Version 2.0 (1 GB, with 800 MB identical chunks and 200 MB modified chunks).
- Sync Version 2.0 to a worker holding Version 1.0.
- Verify worker transfers only the 200 MB changed chunks.

---

## 6. Phase 1 Implementation Checklist

- [ ] Create repository skeleton and `go.mod` (Go 1.22+).
- [ ] Implement `api/v1/manifest.go` (JSON canonicalization, SHA-256 identity).
- [ ] Implement `pkg/chunk` (4 MiB fixed chunking & hash generation).
- [x] Implement `pkg/cache` (Atomic tmp-to-store move, directory layout, on-disk SHA-256 before rename).
- [x] Implement `pkg/materializer` (File tree builder with per-chunk re-hash).
- [x] Implement `pkg/verifier` (cache audit + materialized directory verification).
- [x] Implement `spiderctl verify artifact|cache` CLI.
- [x] Unit tests for hash mismatch (cache, peer, origin, materializer, verifier).
- [ ] Implement `pkg/source` (Filesystem & S3/MinIO drivers).
- [ ] Define Protobuf contracts (`tracker.proto`, `peer.proto`).
- [ ] Implement `pkg/tracker` & `cmd/tracker` (Central registration & location server).
- [ ] Implement `pkg/peer` & `pkg/engine` (gRPC chunk streaming & concurrent scheduler).
- [ ] Implement `cmd/artifactd` (Daemon runtime & HTTP/gRPC management API).
- [ ] Implement `cmd/artifactctl` (CLI tool).
- [ ] Create `Containerfile` and `podman-compose.yml`.
- [ ] Write `scripts/generate-test-data.sh` and `scripts/podman-poc-test.sh`.
- [ ] Run Podman automated experiment battery and verify Phase 1 Success Criteria.
