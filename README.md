# Spider: Distributed Artifact Fabric (DAF)

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Architecture](https://img.shields.io/badge/Architecture-P2P%20Mesh-orange.svg)](#architecture)
[![Status](https://img.shields.io/badge/Status-Phase%201%20PoC%20Complete-brightgreen.svg)](#roadmap)

**Spider** is a high-throughput, content-addressed, topology-aware P2P distribution mesh designed to distribute massive immutable artifacts (LLM/ML models, datasets, binaries, containers, and directory trees) across large compute fleets while drastically reducing origin storage (S3/MinIO) network traffic.

---

## ⚡ Key Highlights & Benchmark Results

### 1. Storage & Engine Microbenchmarks
*Measurements taken on 11th Gen Intel(R) Core(TM) i5-11320H @ 3.20GHz:*

| Primitive / Benchmark | Latency / Op | Effective Throughput | Memory Allocs |
| :--- | :--- | :--- | :--- |
| **SHA-256 Content-Addressing Hash** | 2.75 ms/op | **1,522.20 MB/s (1.52 GB/s)** | 3 allocs/op |
| **Fixed 4 MiB Stream Chunking** | 23.2 ms/op | **722.41 MB/s** | 21 allocs/op |
| **Atomic Cache Put (`tmp` $\to$ verify $\to$ rename)** | 10.4 ms/op | **402.72 MB/s** | 17 allocs/op |

### 2. Live Fleet Distribution Benchmark (`spiderctl benchmark`)

#### 100 MB Model across 6 Concurrent Worker Nodes:
```
========================================================================
  Spider Artifact Mesh — Automated Distribution Benchmark
  Model Size: 100 MB | Workers: 6 | Chunk Size: 4 MB
========================================================================

METRIC                    DIRECT ORIGIN (BASELINE)   SPIDER P2P MESH   IMPROVEMENT
------                    ------------------------   ---------------   -----------
Origin Data Transferred   600.00 MB                  0.00 MB           100.0% bandwidth saved
Peer Data Transferred     0.00 MB                    600.00 MB         Offloaded to Mesh
Fleet Chunks Resolved     174 / 174 chunks           174 / 174 chunks  100% SHA-256 Verified
```
* **Origin Storage Traffic Saved:** **100.0%** (Origin traffic dropped from 600 MB to 0 MB across workers).
* **Self-Healing Fallback:** Workers seamlessly fall back to origin (S3/MinIO/FS) upon peer failure or network disruption without job failure.

---

## 🏗️ System Architecture

```text
       External Origin Storage (S3 / MinIO / Local FS)
                             │
                             ▼
                 Spider Canonical Manifest
                  (JSON + SHA-256 Identity)
                             │
                             ▼
               Fixed Content Chunks (4 MiB)
                             │
                             ▼
         Content-Addressed Local Cache (/var/lib/spider/chunks)
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
   Distributed Peer Mesh           Materialized Directory Tree
   (gRPC Chunk Streaming)            (POSIX View for Apps/Inference)
```

### Core Design Principles
1. **Artifact-First, Not Model-First**: Treats models, checkpoints, datasets, or software releases as arbitrary multi-file trees of immutable content-addressed chunks.
2. **Strict Control / Data Plane Separation**: Central Tracker only tracks metadata, locations, and topology. Data plane streams bytes directly peer-to-peer or from origin. Tracker **never proxies payload bytes**.
3. **Content Addressing & Verification**: Every chunk is addressed by cryptographic hash (`sha256:<hex>`). Chunks are atomically verified before committing to cache or advertising to the mesh.
4. **Topology-Aware Proximity**: Peering scheduler prefers candidate nodes ranked by network distance (`Host` > `Rack` > `Zone` > `Region` > `Remote`).
5. **Standalone Engine with Optional Orchestration**: Runs standalone on bare metal, containers, or VMs with zero external dependencies, while providing clean extension points for Kubernetes CRDs and operators.

---

## 📦 Repository Structure

```text
spider/
├── Containerfile                  # Multi-stage container build (Alpine runtime)
├── podman-compose.yml             # Local multi-node testbed (MinIO + Tracker + 3 Workers)
├── docker-compose.yml             # Docker-compatible testbed definition
├── buf.yaml / buf.gen.yaml        # Protobuf compiler configuration
├── api/
│   └── v1/
│       ├── manifest.go            # Manifest JSON schema, generator & SHA-256 ID calculator
│       └── proto/
│           ├── tracker.proto      # Tracker gRPC protocol
│           └── peer.proto         # Peer gRPC streaming protocol
├── cmd/
│   ├── spiderctl/ / artifactctl/  # Publisher & node management CLI
│   ├── spiderd/ / artifactd/      # Node daemon process
│   └── tracker/                   # Central metadata tracker daemon
├── pkg/
│   ├── benchmark/                 # Automated benchmark suite and scenario runner
│   ├── cache/                     # Disk-backed atomic verified chunk store
│   ├── chunk/                     # 4 MiB chunker and SHA-256 hash calculator
│   ├── engine/                    # Concurrent P2P scheduler & fallback manager
│   ├── materializer/              # Reconstructs directory trees from chunks
│   ├── peer/                      # gRPC stream chunk server & client
│   ├── source/                    # Storage adapters (Local FS, AWS S3, MinIO)
│   ├── topology/                  # Locality scoring (Host/Rack/Zone/Region)
│   ├── tracker/                   # Central in-memory registry & peer matcher
│   └── verifier/                  # Cryptographic chunk audit & directory verifier
└── scripts/
    ├── generate-test-data.sh      # Synthetic model generator
    ├── podman-poc-test.sh         # 6 E2E validation experiments
    ├── run-benchmarks.sh          # Linux/macOS benchmark runner
    └── run-benchmarks.ps1         # Windows PowerShell benchmark runner
```

---

## 🔒 Multi-Layer Data Integrity & Verification

Spider enforces end-to-end cryptographic integrity verification across 5 distinct layers to guarantee zero bit-rot, corruption rejection, and tamper-proofing:

1. **Wire Transfer Verification (`pkg/peer/client.go`)**:
   - Every 4 MiB chunk streamed over gRPC is cryptographically verified against its declared SHA-256 hash as bytes arrive over the network.
   - If a peer sends a single altered byte, the stream is rejected, the peer is marked degraded, and the engine automatically falls back to clean origin storage.
2. **Atomic Staging & Commit (`pkg/cache/cache.go`)**:
   - Chunks are downloaded to an isolated `tmp/` staging directory and `fsync`'d.
   - SHA-256 is re-computed from the **written on-disk bytes**. Only matching files are atomically renamed (`os.Rename`) into the shard store `chunks/sha256/xx/xxxx...`.
   - Corrupted or incomplete chunks are immediately purged from disk (`ErrHashMismatch`).
3. **Canonical Manifest Validation (`api/v1/manifest.go`)**:
   - Artifact identity is canonically computed as `sha256(canonical_manifest_json)`.
   - Every file's chunk offsets, lengths, and permissions must sum exactly to total file size. Tampered `artifactId` values are rejected.
4. **Materialize-Time Re-Hash (`pkg/materializer`)**:
   - Each cached chunk is hashed again while assembling the destination file tree. A bit-rotten cache entry aborts materialization and removes the partial file.
5. **Offline Bit-Rot Audit & Directory Verifier (`pkg/verifier`)**:
   - Audits materialized physical folders on disk against the canonical manifest (presence, size, per-chunk SHA-256).
   - Scans and detects bit-rot across local cache shards.

### Tests covering integrity:
- `pkg/cache`: hash mismatch on `PutChunk` / `PutChunkFromReader`, on-disk re-hash before rename.
- `pkg/peer`: corrupt gRPC payload rejected before cache commit.
- `pkg/engine`: corrupt peer recovery from origin; origin bytes that fail SHA-256 rejected.
- `pkg/materializer`: corrupt cache bytes cannot produce a destination file.
- `pkg/verifier`: valid audit, injected bit-rot, missing files, size mismatch.

### Verification CLI Commands:
```bash
# 1. Audit materialized folder on disk (validates all files, sizes, modes, and chunk SHA-256 hashes)
./bin/spiderctl verify artifact --manifest=manifest.json --dest=/models/llama-3-8b/1.0.0

# 2. Audit local chunk cache against silent disk corruption / bit-rot
./bin/spiderctl verify cache --cache-dir=/var/lib/artifactd
```

---

## 🚀 Quickstart

### Prerequisites
- Go 1.22+
- (Optional) Podman / Docker for containerized cluster simulation

### 1. Build Binaries
```bash
go build -o bin/tracker ./cmd/tracker
go build -o bin/spiderd ./cmd/spiderd
go build -o bin/spiderctl ./cmd/spiderctl
```

### 2. Run Benchmarks
Run the built-in automated benchmark suite to compare Direct Origin vs. Spider P2P Mesh:
```bash
# Run 100 MB model across 6 worker nodes
./bin/spiderctl benchmark --size=100 --workers=6 --chunk-size=4

# Run Go engine microbenchmarks
go test -bench=Benchmark -benchmem ./pkg/chunk ./pkg/cache
```

### 3. Publishing, Syncing, and Verifying Artifacts

#### A. Start the Central Tracker
```bash
./bin/tracker --port=50051
```

#### B. Start Node Daemons (`spiderd`)
```bash
# Node 1 (Seeder)
./bin/spiderd --node-id=worker-1 --port=50052 --tracker=127.0.0.1:50051 --rack=rack-1 --zone=zone-a

# Node 2 (Peer)
./bin/spiderd --node-id=worker-2 --port=50053 --tracker=127.0.0.1:50051 --rack=rack-1 --zone=zone-a
```

#### C. Publish an Artifact (`spiderctl publish`)
```bash
./bin/spiderctl publish \
  --source=/path/to/model-weights \
  --name=llama-3-8b \
  --version=1.0.0 \
  --output=manifest.json \
  --tracker=127.0.0.1:50051
```

#### D. Sync Artifact across Mesh (`spiderctl sync`)
```bash
./bin/spiderctl sync \
  --manifest=manifest.json \
  --dest=/models/llama-3-8b/1.0.0 \
  --daemon=127.0.0.1:50053
```

#### E. Verify Data Integrity on Destination Disk (`spiderctl verify`)
```bash
./bin/spiderctl verify artifact \
  --manifest=manifest.json \
  --dest=/models/llama-3-8b/1.0.0
```

#### F. Query Status & Mesh Peers
```bash
./bin/spiderctl status --daemon=127.0.0.1:50053
./bin/spiderctl peers --tracker=127.0.0.1:50051
./bin/spiderctl cache --cache-dir=/var/lib/artifactd
```

---

## 🐳 Containerized Multi-Node Testbed

Spider includes a complete containerized simulation environment with MinIO origin storage, central tracker, and 3 worker nodes located in different simulated racks and zones.

```bash
# Start cluster
podman-compose up -d --build
# or
docker-compose up -d --build

# Run automated validation battery (6 E2E experiments)
./scripts/podman-poc-test.sh
```

---

## 🗺️ Roadmap & Phased Architecture

| Phase | Plan Document | Status | Focus |
|---|---|---|---|
| **Phase 1** | [`01-poc-and-podman`](docs/plans/phase-1-poc-and-podman-environment.md) | ✅ **Complete** | Core Go primitives, gRPC chunk streaming, tracker, atomic cache, CLI, and benchmark harness. |
| **Phase 2** | [`02-core-reliability`](docs/plans/phase-2-core-reliability-and-hardening.md) | 📋 Planned | Persistent SQLite/Postgres tracker DB, adaptive download scheduler, LRU cache eviction & pinning, Prometheus metrics. |
| **Phase 3** | [`03-security-auth`](docs/plans/phase-3-security-and-authorization.md) | 📋 Planned | Mutual TLS (mTLS), Ed25519 signed manifests, RBAC, path traversal policies. |
| **Phase 4** | [`04-k8s-operator`](docs/plans/phase-4-kubernetes-operator-and-crds.md) | 📋 Planned | `ArtifactDeployment` CRD, Kubernetes Operator (`cmd/controller`), `spiderd` DaemonSet manifests. |
| **Phase 5** | [`05-scale-transports`](docs/plans/phase-5-multi-region-scale-and-transports.md) | 📋 Planned | Multi-region hierarchical control plane, zero-copy `splice` streaming, FastCDC chunking. |
| **Phase 6** | [`06-ml-gpu-extensions`](docs/plans/phase-6-ml-and-gpu-acceleration-extensions.md) | 📋 Planned | RDMA / GPUDirect Storage (GDS) transports, vLLM / HuggingFace runtime adapters. |

---

## 📄 License
MIT License. See [LICENSE](LICENSE) for details.