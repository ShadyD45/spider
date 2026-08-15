# Distributed Artifact Fabric — Product & Technical Specification

**Status:** Draft for implementation planning
**Version:** 0.1
**Purpose:** Feed this specification to a coding agent to plan and implement a PoC, validate the architecture, and evolve the system toward production readiness.

---

## 1. Executive Summary

Distributed Artifact Fabric (DAF) is a general-purpose system for efficiently distributing very large artifacts across a fleet of machines.

The primary motivating use case is ML/LLM model distribution:

* A model may consist of many files.
* Individual files may be hundreds of GB.
* A model release may need to become available on hundreds or thousands of GPU workers.
* Downloading the complete artifact independently from S3/GCS/Azure Blob on every worker creates unnecessary network traffic, slow rollout, and object-store bottlenecks.
* Workers that already possess artifact data should be able to serve that data to other workers.
* Kubernetes should provide orchestration primitives where available, but the core distribution engine must remain independent of Kubernetes.

The system treats every model, dataset, package, checkpoint, directory, or arbitrary file tree as an **immutable artifact**.

The core abstraction is:

```text
External Source
      |
      v
Artifact Manifest
      |
      v
Files
      |
      v
Content-addressed Chunks
      |
      v
Distributed Peer Cache
      |
      v
Materialized Local Artifact
```

The system has two planes:

### Control plane

Responsible for:

* Artifact metadata
* Desired state
* Peer discovery
* Distribution coordination
* Placement/topology information
* Progress/status
* Policy
* Kubernetes integration

### Data plane

Responsible for moving bytes:

* Source storage -> worker
* Worker -> worker
* Chunk verification
* Local persistence
* Resume
* Cache management

The control plane must **never proxy artifact bytes** in the normal data path.

---

# 2. Goals

## 2.1 Primary goals

1. Efficiently distribute large artifacts across many machines.
2. Reduce redundant reads from external object storage.
3. Allow workers with cached content to become sources for other workers.
4. Support arbitrary multi-file directory trees, not only ML models.
5. Use content-addressed chunks to enable deduplication and incremental distribution.
6. Support S3-compatible storage and provide an extensible source-provider abstraction.
7. Support resumable transfers.
8. Verify every chunk before accepting it.
9. Continue operating correctly after worker/controller restarts.
10. Make Kubernetes integration first-class but optional.
11. Provide a simple CLI and API.
12. Measure distribution efficiency and expose observability data.
13. Provide a clean extension point for high-performance ML/GPU transfers later.

## 2.2 ML-specific goals

1. Efficiently distribute model checkpoints consisting of many files.
2. Support model versions without retransmitting unchanged chunks.
3. Allow inference workers to consume a completed local artifact.
4. Support prefetching before an inference deployment.
5. Support compatibility metadata for artifacts that must only be reused on compatible hardware/software.
6. Leave room for future GPU-aware/NIXL/RDMA/GDS transports without coupling the core system to NVIDIA-specific technology.

---

# 3. Non-goals

The initial project is NOT:

* An inference server.
* A model-training framework.
* A Kubernetes replacement.
* A general distributed filesystem.
* A POSIX filesystem mounted over the network.
* A replacement for S3/GCS/Azure Blob.
* A scheduler for GPU workloads.
* A tensor-parallel runtime.
* A direct clone of NVIDIA ModelExpress.
* A global Internet-scale P2P network.

The first version should focus on:

> **Reliable, content-addressed, topology-aware P2P distribution of immutable artifacts across trusted compute nodes.**

---

# 4. Design Principles

## 4.1 Artifact-first, not model-first

The core system must not understand model-specific file formats.

A model is simply:

```text
Artifact
  ├── config.json
  ├── tokenizer.json
  ├── model-00001.safetensors
  ├── model-00002.safetensors
  └── ...
```

The same system must work for:

* LLMs
* embedding models
* LoRA adapters
* datasets
* checkpoints
* compiled kernels
* application bundles
* Docker-like layers
* arbitrary directories
* large binary files

## 4.2 Immutable artifacts

An artifact version is immutable.

If content changes, it produces a new artifact identity.

This makes:

* caching
* verification
* deduplication
* replication
* rollback

much easier.

## 4.3 Content addressing

Chunks are identified by cryptographic content hash.

Conceptually:

```text
chunk hash = SHA-256(chunk bytes)
```

The system should never trust a peer merely because the peer claims to possess a chunk.

The receiver verifies the bytes against the expected hash.

## 4.4 Control plane / data plane separation

Control plane:

```text
Who?
What?
Where?
Which version?
Which chunks?
Which peers?
```

Data plane:

```text
Actual bytes.
```

The tracker should not become a bandwidth bottleneck.

## 4.5 Reconciliation over imperative commands

The desired state should be declarative.

Instead of:

```text
"Download v2 now."
```

prefer:

```text
"Artifact X@v2 should exist on these nodes."
```

Workers reconcile toward that state.

This makes the system resilient to missed notifications, restarts, and transient failures.

## 4.6 Kubernetes is an integration, not a dependency

The core engine must run:

* on Kubernetes
* on bare metal
* under another scheduler
* in a local development environment

Kubernetes should provide:

* deployment
* node discovery
* CRDs
* controller framework
* service discovery
* GPU scheduling
* lifecycle management

The artifact engine must remain independent.

---

# 5. High-Level Architecture

```text
                         +----------------------+
                         |   Artifact Sources   |
                         |----------------------|
                         | S3 / GCS / Azure     |
                         | MinIO / HTTP / FS    |
                         +----------+-----------+
                                    |
                                    |
                              ingest / fetch
                                    |
                                    v
                    +-------------------------------+
                    |       Artifact Registry        |
                    |-------------------------------|
                    | Artifact metadata              |
                    | Manifest                       |
                    | Version                        |
                    | Source metadata                |
                    | Compatibility metadata        |
                    +---------------+---------------+
                                    |
                                    v
                    +-------------------------------+
                    |        Control Plane           |
                    |-------------------------------|
                    | Distribution Controller        |
                    | Peer/Chunk Tracker             |
                    | Placement / topology           |
                    | Policy                         |
                    +---------------+---------------+
                                    |
                     +--------------+--------------+
                     |              |              |
                     v              v              v
                +---------+    +---------+    +---------+
                | Node A  |<-->| Node B  |<-->| Node C  |
                |---------|    |---------|    |---------|
                |spiderd|    |spiderd|    |spiderd|
                | cache   |    | cache   |    | cache   |
                +----+----+    +----+----+    +----+----+
                     |              |              |
                     v              v              v
                  Local FS       Local FS       Local FS
                     |              |              |
                     v              v              v
                 Application    Inference      Application
```

---

# 6. Kubernetes Architecture

Kubernetes is optional for the core engine, but the reference deployment should support Kubernetes.

```text
                        Kubernetes API Server
                                 |
                    +------------+-------------+
                    |                          |
                    v                          v
             ModelDeployment CRD       Node / Pod metadata
                    |
                    v
        +---------------------------+
        | Distribution Controller   |
        |                           |
        | Reconciliation loop       |
        +-------------+-------------+
                      |
              desired artifact
                      |
       +--------------+--------------+
       |              |              |
       v              v              v
   GPU Node A     GPU Node B     GPU Node C
   +---------+    +---------+    +---------+
   |spiderd|    |spiderd|    |spiderd|
   +---------+    +---------+    +---------+
```

### Kubernetes resources to reuse

Use Kubernetes rather than reimplementing:

* API Server
* CRDs
* Deployments
* DaemonSets
* Services
* ConfigMaps
* Secrets
* RBAC
* Node labels/taints
* Resource requests/limits
* GPU device plugins
* Service discovery
* Pod lifecycle
* Restart handling
* Health probes

### Components to implement

* CRD schema
* Kubernetes controller/operator
* `spiderd` DaemonSet
* status reporting
* artifact-specific reconciliation logic

---

# 7. Core Components

## 7.1 Artifact Registry

Stores logical artifact metadata.

Example:

```yaml
artifact:
  name: gpt-x
  version: "2.0"
  artifact_id: sha256:...
  created_at: ...
  source:
    type: s3
    uri: s3://bucket/gpt-x/v2/
  manifest: sha256:...
```

The registry does NOT need to store artifact bytes.

It stores metadata and immutable manifest references.

Possible implementations:

### PoC

PostgreSQL or SQLite.

### Production

PostgreSQL or another strongly consistent metadata database.

---

# 8. Artifact Manifest

The manifest is the central data structure.

Example:

```json
{
  "schemaVersion": 1,
  "artifactId": "sha256:abc...",
  "name": "gpt-x",
  "version": "2.0",
  "files": [
    {
      "path": "config.json",
      "size": 8192,
      "chunks": [
        {
          "hash": "sha256:111...",
          "offset": 0,
          "size": 8192
        }
      ]
    },
    {
      "path": "model-00001.safetensors",
      "size": 107374182400,
      "chunks": [
        {
          "hash": "sha256:222...",
          "offset": 0,
          "size": 4194304
        }
      ]
    }
  ]
}
```

The manifest must preserve:

* relative file path
* file size
* file mode where relevant
* file type
* chunk ordering
* chunk hashes
* total artifact identity

Symlink handling must be explicitly defined and disabled by default for security.

---

# 9. Chunking

Chunking converts files into independently transferable units.

Initial recommended PoC:

```text
Fixed chunk size = 4 MiB
```

Example:

```text
10 GB file
   |
   +-- 4 MiB chunk
   +-- 4 MiB chunk
   +-- ...
```

The chunk size must be configurable.

Possible future strategies:

* fixed-size chunks
* content-defined chunking
* adaptive chunk size
* file-type-specific chunking

For the PoC, fixed-size chunks are strongly preferred because they simplify implementation and benchmarking.

---

# 10. Content-Addressed Local Cache

Each `spiderd` maintains a local cache.

Conceptually:

```text
/var/lib/spider/

  chunks/
    sha256/
      aa/
        aabbcc...
      bb/
        bbccdd...

  manifests/
    sha256-abc.json

  artifacts/
    gpt-x/
      2.0/
        config.json
        model-00001.safetensors
```

The chunk store is the authoritative local cache.

The materialized artifact directory is a view/output generated from verified chunks.

---

# 11. Artifact Source Abstraction

External storage must be pluggable.

Define an interface conceptually similar to:

```text
ArtifactSource

list(path)
stat(path)
read(path, offset, length)
open(path)
```

Initial adapters:

1. S3
2. S3-compatible storage / MinIO
3. Local filesystem

Future adapters:

* GCS
* Azure Blob
* HTTP
* HTTPS
* OCI registry
* NFS
* custom object stores

The core distribution engine must not contain S3-specific logic.

---

# 12. Peer / Chunk Tracker

The tracker answers:

> Which nodes currently advertise possession of which chunks?

Example:

```text
chunk A
  -> node-17
  -> node-42
  -> node-91

chunk B
  -> node-8
  -> node-31
```

The tracker should contain metadata only.

It must NOT proxy chunk bytes.

## Registration

When `spiderd` receives a verified chunk:

```text
spiderd -> tracker

chunk:
  sha256:abc...

node:
  node-17

capabilities:
  region: ap-south
  zone: ap-south-1a
```

The tracker records the availability.

## Heartbeats

Workers periodically send:

* node ID
* health
* advertised artifacts/chunks
* capacity
* network topology
* optional bandwidth information

Stale peers must expire automatically.

---

# 13. Peer Selection

The downloader should rank candidate peers.

Initial scoring:

```text
same host/network locality
    >
same rack/zone
    >
same datacenter
    >
same region
    >
remote region
    >
origin storage
```

Additional factors:

* observed throughput
* latency
* peer health
* concurrent transfer count
* source reliability
* available bandwidth
* cache pressure

The first implementation can use deterministic topology scoring.

Later versions can use dynamic performance measurements.

---

# 14. P2P Transfer Protocol

The protocol should support:

* request chunk
* chunk metadata
* streaming bytes
* checksum
* cancellation
* timeout
* retry
* resume
* concurrency control

Recommended initial transport:

```text
gRPC + streaming
```

The protocol should be abstracted so that alternative transports can be introduced later.

Possible future transports:

* QUIC
* HTTP/3
* RDMA
* NIXL
* specialized datacenter transports

---

# 15. Distribution Workflow

## 15.1 Publish

User runs:

```bash
spiderctl publish \
  --source s3://bucket/gpt-x/v2 \
  --name gpt-x \
  --version 2.0
```

The publisher:

1. Enumerates files.
2. Reads file metadata.
3. Chunks files.
4. Calculates hashes.
5. Creates manifest.
6. Registers artifact metadata.
7. Optionally uploads chunks to an origin cache.
8. Returns immutable artifact ID.

---

# 16. Synchronization Workflow

Worker wants:

```text
gpt-x@2.0
```

`spiderd`:

1. Gets desired artifact.
2. Fetches manifest.
3. Checks local chunks.
4. Calculates missing chunks.
5. Asks tracker for candidate peers.
6. Selects peers.
7. Downloads chunks in parallel.
8. Verifies each chunk.
9. Stores verified chunks atomically.
10. Reconstructs/materializes files.
11. Verifies final artifact.
12. Marks artifact READY.
13. Advertises new chunks to tracker.

---

# 17. Example P2P Fan-Out

Initial state:

```text
                    S3
                    |
                    v
                 Node A
```

Node A downloads the artifact.

Then:

```text
                    S3
                    |
                    v
                 Node A
                /      \
               v        v
            Node B    Node C
             /  \       |
            v    v      v
          Node D Node E Node F
```

Each node can become a source after verified chunks arrive.

The distribution system should avoid making the first node a permanent bottleneck.

---

# 18. Concurrent Chunk Scheduling

If a worker needs:

```text
A B C D E F G H
```

and peers are:

```text
Peer 1 -> A B C
Peer 2 -> D E
Peer 3 -> F G
S3     -> H
```

the worker should download concurrently.

The scheduler should support:

* per-peer concurrency
* global worker concurrency
* bandwidth limits
* backpressure
* prioritization
* retry
* cancellation

---

# 19. Resumability

If a worker crashes at:

```text
700 / 1000 chunks
```

after restart:

```text
700 verified chunks
300 missing chunks
```

It must continue without redownloading verified chunks.

Partially downloaded chunks must not be advertised as available.

Use temporary files:

```text
chunk.tmp
   |
   | verify
   v
chunk
```

Rename atomically after verification.

---

# 20. Integrity and Security

Every chunk must be verified.

Minimum:

```text
SHA-256
```

Future options:

* SHA-512
* BLAKE3

The manifest itself should have an immutable identity.

Production should support signed manifests.

Example:

```text
Artifact
   |
Manifest
   |
Signature
```

A worker must verify:

1. Manifest authenticity.
2. Expected chunk hash.
3. Chunk size.
4. Final file size.
5. Final artifact identity.

Never execute artifact contents during distribution.

---

# 21. Trust Model

The initial PoC assumes a trusted cluster.

Production must assume:

* compromised worker
* malicious peer
* corrupted storage
* replayed artifact
* unauthorized artifact access

Requirements:

* mTLS between services
* node identity
* authenticated tracker registration
* authorization for artifact access
* signed manifests
* encrypted transfers
* audit logging

A peer is never trusted merely because it is registered.

The hash is the final integrity check.

---

# 22. Cache Management

Each node has finite disk capacity.

Cache policies:

### MVP

LRU based on artifact/chunk last-access time.

### Future

* LRU
* LFU
* weighted popularity
* pinning
* TTL
* priority
* storage quotas
* artifact-level eviction

Example:

```yaml
cache:
  maxBytes: 2Ti
  policy: lru
  pinned:
    - gpt-x@2.0
```

Pinned artifacts must not be evicted.

---

# 23. Prefetch

The system should support:

```bash
spiderctl prefetch gpt-x@2.0 --selector gpu=h100
```

This means:

> Make the artifact available before the workload actually needs it.

This is important for:

* model rollouts
* autoscaling
* blue/green deployments
* scheduled workloads

---

# 24. Kubernetes CRD

Suggested initial CRD:

```yaml
apiVersion: artifact.fabric/v1alpha1
kind: ArtifactDeployment
metadata:
  name: gpt-x
spec:
  artifact:
    name: gpt-x
    version: "2.0"

  placement:
    nodeSelector:
      accelerator: nvidia-h100

  policy:
    prefetch: true
    minReadyPercent: 100

  cache:
    pin: true
```

The controller reconciles this desired state.

---

# 25. CRD Status

Example:

```yaml
status:
  desired:
    artifact: gpt-x@2.0

  nodes:
    total: 100
    ready: 73
    syncing: 20
    failed: 7

  progress:
    bytesTotal: 500000000000
    bytesAvailable: 450000000000

  conditions:
    - type: Ready
      status: "False"
      reason: DistributionInProgress
```

This provides operational visibility through Kubernetes.

---

# 26. Kubernetes Controller Responsibilities

The controller should:

1. Watch `ArtifactDeployment`.
2. Validate the requested artifact.
3. Resolve placement selectors.
4. Determine desired nodes.
5. Monitor `spiderd` status.
6. Update CRD status.
7. Trigger/reconcile retries.
8. Handle node additions/removals.
9. Coordinate prefetch.
10. Avoid moving artifact bytes through the Kubernetes API.

The controller should NOT:

* transfer chunks
* proxy files
* become the data path
* store huge artifact metadata in Kubernetes

---

# 27. `spiderd` Responsibilities

`spiderd` is the core node agent.

Modules:

```text
spiderd
|
+-- Reconciler
+-- Manifest Manager
+-- Local Chunk Store
+-- Materializer
+-- Peer Client
+-- Source Client
+-- Transfer Scheduler
+-- Integrity Verifier
+-- Cache Manager
+-- Tracker Client
+-- Health/Status Server
+-- Metrics
```

---

# 28. Generic API

CLI:

```bash
spiderctl publish
spiderctl inspect
spiderctl fetch
spiderctl sync
spiderctl status
spiderctl peers
spiderctl cache
spiderctl prefetch
spiderctl delete
```

Core API conceptually:

```text
publish(source) -> ArtifactID

resolve(name, version) -> Manifest

sync(artifactID, destination) -> SyncResult

status(artifactID) -> Status

prefetch(artifactID, placement) -> Job

evict(artifactID) -> Result
```

---

# 29. Local Agent API

`spiderd` should expose a local API for applications.

Example:

```text
GET /v1/artifacts/{name}/{version}

POST /v1/sync
GET /v1/status
GET /v1/cache
```

An inference server should be able to ask:

```text
"Is gpt-x@2.0 ready?"
```

and optionally:

```text
"Ensure gpt-x@2.0 is ready."
```

The inference server should NOT need to understand P2P.

---

# 30. Materialization

The system separates:

```text
chunk cache
```

from:

```text
materialized artifact
```

Example:

```text
Chunk Store
    |
    +-- hash A
    +-- hash B
    +-- hash C
    |
    v
Materializer
    |
    v
/models/gpt-x/2.0/
    |
    +-- config.json
    +-- tokenizer.json
    +-- model-00001.safetensors
```

The inference runtime reads the normal filesystem path.

---

# 31. Deduplication

Content addressing enables cross-artifact deduplication.

Example:

```text
v1:
A B C D E F

v2:
A B C D X Y
```

Only:

```text
X Y
```

need to be newly transferred.

This applies across:

* model versions
* different artifacts
* shared libraries
* kernel caches
* datasets

---

# 32. Artifact Compatibility

The generic core must allow optional compatibility metadata.

Example:

```json
{
  "compatibility": {
    "gpuVendor": "nvidia",
    "gpuArchitecture": "sm100",
    "cuda": "13.x",
    "runtime": "vllm",
    "runtimeVersion": "..."
  }
}
```

The core does not interpret all fields.

It provides metadata to the placement/source-selection layer.

This becomes important for ML-specific artifacts.

---

# 33. ML / GPU Extension

The core system should eventually support a pluggable fast transport:

```text
Artifact Fabric
       |
       +-- Standard TCP/gRPC
       |
       +-- QUIC
       |
       +-- RDMA
       |
       +-- NIXL
       |
       +-- GDS
```

The core must not require NVIDIA hardware.

This allows the project to learn from systems such as NVIDIA ModelExpress while remaining broader in scope. NVIDIA's current ModelExpress design similarly separates control-plane peer discovery from data-plane transfer and uses capability-based path selection, including P2P RDMA, object-store streaming, GDS, and fallback loading. The important differentiation here is that our core abstraction is a **generic content-addressed artifact fabric**, rather than an inference-runtime-specific GPU weight loader.

---

# 34. ModelExpress Differentiation

The system must NOT simply become a clone of ModelExpress.

ModelExpress focuses heavily on accelerating the model-weight lifecycle and getting compatible weights into GPU memory quickly, including GPU-to-GPU transfers and inference-runtime integrations.

DAF focuses on:

```text
Any artifact
     |
Any storage source
     |
Content-addressed chunks
     |
P2P distributed cache
     |
Any compute node
```

ML/GPU acceleration is an extension.

This gives the project two layers:

```text
+---------------------------------------+
| ML/GPU acceleration                   |
| NIXL / RDMA / GDS / runtime adapters |
+---------------------------------------+
| Generic Artifact Fabric               |
| manifests / chunks / P2P / cache     |
+---------------------------------------+
```

---

# 35. Topology Awareness

The system should represent topology:

```text
Region
  |
  +-- Zone
       |
       +-- Rack
            |
            +-- Host
                 |
                 +-- GPU
```

Peer selection should prefer local sources.

Example:

```text
same host
   >
same rack
   >
same zone
   >
same datacenter
   >
same region
   >
remote region
   >
origin
```

This is critical for avoiding expensive cross-datacenter traffic.

---

# 36. Multi-Datacenter Architecture

```text
                         Global Registry
                               |
                         Origin Storage
                               |
              +----------------+----------------+
              |                |                |
              v                v                v
           US-East           EU-West         AP-South
              |                |                |
          Regional          Regional        Regional
           Seeds             Seeds           Seeds
              |                |                |
          +---+---+        +---+---+       +---+---+
          |   |   |        |   |   |       |   |   |
        Rack Rack Rack    Rack Rack Rack  Rack Rack Rack
          |                |                |
        Nodes             Nodes            Nodes
```

The system should avoid cross-region distribution unless necessary.

---

# 37. Failure Handling

Failures to handle:

### Peer disappears

Retry another peer.

### Peer sends corrupt data

Reject chunk and mark peer unhealthy.

### Object store unavailable

Continue from available peer caches where possible.

### Tracker unavailable

Existing transfers should continue where possible.

New discovery may temporarily degrade.

### Controller restarts

Workers reconcile desired state again.

### Worker restarts

Resume from local verified chunks.

### Disk fills

Eviction policy activates.

### Network partition

Workers continue local work and reconcile later.

---

# 38. Consistency Model

The system should use:

> **Eventually consistent distribution state with strongly verified artifact content.**

The tracker can temporarily have stale information.

This is acceptable because:

* a peer may disappear
* retries are expected
* content hashes are authoritative
* final readiness is local

The system must not rely on the tracker being perfectly synchronized.

---

# 39. Observability

Metrics:

### Distribution

```text
artifact_sync_total
artifact_sync_duration_seconds
artifact_bytes_transferred
artifact_bytes_from_origin
artifact_bytes_from_peers
artifact_chunks_downloaded
artifact_chunks_deduplicated
```

### Peer

```text
peer_transfer_total
peer_transfer_failures
peer_transfer_bytes
peer_throughput
peer_latency
```

### Cache

```text
cache_hits
cache_misses
cache_bytes
cache_evictions
cache_utilization
```

### Controller

```text
desired_nodes
ready_nodes
syncing_nodes
failed_nodes
```

### Important KPI

The most important metric is:

```text
origin_bytes_saved
```

and:

```text
P2P_bytes / total_bytes
```

---

# 40. Logging

Structured logs.

Every transfer should have:

```text
artifact_id
file
chunk_hash
source
destination
attempt
duration
bytes
result
```

Avoid logging secrets or object-store credentials.

---

# 41. Tracing

Use OpenTelemetry.

Trace:

```text
sync request
   |
   +-- manifest lookup
   |
   +-- tracker lookup
   |
   +-- peer selection
   |
   +-- chunk transfer
   |
   +-- verification
   |
   +-- materialization
```

This will be useful for diagnosing slow distribution.

---

# 42. Security Requirements

Production:

* TLS/mTLS
* workload identity
* RBAC
* signed manifests
* encrypted storage credentials
* short-lived object-store credentials
* artifact authorization
* peer authentication
* audit trail
* resource quotas
* bandwidth quotas
* malicious-peer handling
* path traversal protection
* symlink restrictions
* decompression bomb protection if archives are later supported

---

# 43. API / Protocol Versioning

Every protocol should include:

```text
protocolVersion
schemaVersion
```

Backward compatibility should be explicitly designed.

The manifest must be versioned independently of the daemon binary.

---

# 44. PoC Scope

The PoC should NOT attempt:

* RDMA
* GPU-to-GPU
* multi-region deployment
* Kubernetes production hardening
* signed artifacts
* dynamic chunking
* DHT
* complex scheduling
* advanced eviction

The PoC should prove the core thesis.

## PoC environment

One physical machine or VM running:

```text
Docker / Kubernetes
```

with:

```text
MinIO
Tracker
Artifact Registry
3-10 spiderd workers
```

Use a large synthetic artifact.

Example:

```text
10 GB
1000 files/chunks
```

The test should simulate:

```text
Origin -> Worker A
Worker A -> Worker B/C/D
Worker B/C/D -> additional workers
```

---

# 45. PoC Experiments

## Experiment 1 — Baseline

10 workers independently download from MinIO.

Measure:

```text
total origin bytes
completion time
aggregate throughput
```

## Experiment 2 — P2P

Only initial workers use MinIO.

Remaining workers use peers.

Compare.

Expected hypothesis:

```text
Origin bandwidth ↓
Total completion time ↓
```

## Experiment 3 — Failure

Kill a peer during transfer.

Expected:

```text
transfer resumes from another peer
```

## Experiment 4 — Restart

Kill worker after 50% completion.

Expected:

```text
worker resumes without redownloading verified chunks
```

## Experiment 5 — Deduplication

Publish v1 and v2 where 80% of chunks are unchanged.

Expected:

```text
v2 transfer ~= changed chunks
```

## Experiment 6 — Topology

Simulate:

```text
same rack
same zone
remote zone
origin
```

Verify preferred peer selection.

---

# 46. PoC Success Criteria

The PoC is successful if:

1. A multi-file artifact can be published.
2. A manifest is generated.
3. Multiple workers can synchronize it.
4. Workers can transfer chunks from each other.
5. Origin bandwidth is significantly reduced.
6. Corrupt chunks are rejected.
7. Interrupted downloads resume.
8. Workers can restart without losing progress.
9. Artifact versions deduplicate unchanged chunks.
10. The system exposes useful metrics.
11. Kubernetes integration can express desired artifact state.
12. The core daemon works without Kubernetes.

Do not set arbitrary performance targets before benchmarking the environment. First establish a baseline.

---

# 47. Production Phase

After PoC validation:

## Phase 1 — Core reliability

* persistent metadata DB
* production tracker
* retries
* timeouts
* rate limiting
* backpressure
* cache eviction
* better concurrency
* protocol versioning

## Phase 2 — Kubernetes

* production CRDs
* operator
* RBAC
* HA controller
* status conditions
* node topology integration
* rolling artifact rollout

## Phase 3 — Security

* mTLS
* signed manifests
* identity
* authorization
* auditing

## Phase 4 — Scale

* tracker sharding
* regional trackers
* topology-aware routing
* peer limits
* hierarchical seed architecture

## Phase 5 — Performance

* zero/low-copy paths
* QUIC
* optimized chunk sizes
* parallel I/O
* connection pooling
* adaptive peer selection

## Phase 6 — ML acceleration

* model-aware metadata
* GPU compatibility
* NIXL integration
* RDMA
* GDS
* inference-runtime adapters

---

# 48. Production Multi-Region Control Plane

At large scale, use hierarchical control planes:

```text
                 Global Control Plane
                         |
             +-----------+-----------+
             |                       |
        US Control Plane        EU Control Plane
             |                       |
       Regional Tracker         Regional Tracker
             |                       |
        Local Workers            Local Workers
```

The global layer handles:

* artifact identity
* version
* policy
* region availability

Regional layers handle:

* peer discovery
* local chunk availability
* regional distribution

This avoids putting every worker in one global tracker.

---

# 49. Important Architectural Decision: Tracker vs DHT

### PoC

Use centralized tracker.

Advantages:

* easy
* observable
* deterministic
* easy to debug

### Future

Consider:

* gossip
* DHT
* hierarchical trackers

Do NOT implement a DHT during the first PoC.

The goal is to prove P2P distribution, not decentralized metadata.

---

# 50. Important Architectural Decision: Chunk Size

Start with:

```text 4 MiB
```

Make it configurable.

Benchmark:

```text 1 MiB
4 MiB
16 MiB
64 MiB
```

Measure:

* metadata overhead
* throughput
* recovery efficiency
* tracker load
* disk I/O
* peer scheduling overhead

Do not prematurely optimize.

---

# 51. Important Architectural Decision: Language

Recommended:

### Go

Use Go for:

* `spiderd`
* tracker
* controller
* CLI
* protocol implementation

Reasons:

* strong networking support
* concurrency
* easy deployment
* static binaries
* Kubernetes ecosystem
* good operational tooling

Potential future high-performance transport libraries can be implemented in Rust/C++ and exposed through a clean interface if required.

---

# 52. Suggested Repository

```text
artifact-fabric/
|
+-- cmd/
|   +-- spiderd/
|   +-- spiderctl/
|   +-- tracker/
|   +-- controller/
|
+-- api/
|   +-- proto/
|   +-- crd/
|
+-- pkg/
|   +-- artifact/
|   +-- manifest/
|   +-- chunk/
|   +-- cache/
|   +-- peer/
|   +-- transfer/
|   +-- source/
|   +-- scheduler/
|   +-- topology/
|   +-- security/
|
+-- adapters/
|   +-- s3/
|   +-- filesystem/
|   +-- kubernetes/
|
+-- deploy/
|   +-- kubernetes/
|   +-- helm/
|
+-- benchmarks/
|
+-- examples/
|
+-- docs/
|
+-- tests/
```

---

# 53. Recommended Implementation Order

The coding agent should NOT begin with Kubernetes.

Implement in this order:

```text
1. Artifact manifest
        ↓
2. Chunker
        ↓
3. Content-addressed local cache
        ↓
4. Filesystem source
        ↓
5. S3/MinIO source
        ↓
6. Single-node sync
        ↓
7. Peer protocol
        ↓
8. Tracker
        ↓
9. Multi-node P2P sync
        ↓
10. Resume/failure handling
        ↓
11. Metrics
        ↓
12. Kubernetes integration
        ↓
13. CRD/controller
        ↓
14. Topology-aware scheduling
        ↓
15. Production security
        ↓
16. Performance optimization
        ↓
17. GPU/RDMA extensions
```

This isolates the core idea from Kubernetes and prevents infrastructure complexity from hiding problems in the P2P engine.

---

# 54. Phase 0 — Design Validation

Before writing significant code, the coding agent should produce:

1. Component diagram.
2. Sequence diagrams.
3. API contracts.
4. Manifest schema.
5. Chunk protocol.
6. State machines.
7. Failure matrix.
8. Storage schema.
9. Benchmark plan.
10. Security threat model.

Do not start implementation until these are reviewed.

---

# 55. State Machine

An artifact on a worker:

```text
UNKNOWN
   |
   v
DISCOVERED
   |
   v
SYNCING
   |
   +------> FAILED
   |          |
   |          v
   +------ RETRY
   |
   v
VERIFYING
   |
   v
MATERIALIZING
   |
   v
READY
   |
   v
EVICTED
```

Chunks:

```text
MISSING
   |
   v
DOWNLOADING
   |
   v
VERIFYING
   |
   v
AVAILABLE
```

Never advertise `DOWNLOADING` chunks as available.

---

# 56. Important Sequence Diagram

```text
Controller        spiderd A       Tracker       spiderd B       S3
    |                  |               |               |             |
    | desired v2       |               |               |             |
    |----------------->|               |               |             |
    |                  | manifest      |               |             |
    |                  |-------------->|               |             |
    |                  |<--------------|               |             |
    |                  |               |               |             |
    |                  | missing chunks|               |             |
    |                  |-------------->|               |             |
    |                  |<--------------|               |             |
    |                  |               |               |             |
    |                  |<-------------------- chunks from B --------|
    |                  |               |               |             |
    |                  | verify        |               |             |
    |                  | store         |               |             |
    |                  |-------------->| advertise     |             |
    |                  |               |               |             |
    |                  | READY         |               |             |
    |<-----------------|               |               |             |
```

The actual implementation should allow a worker to fetch missing chunks directly from S3 when no suitable peer exists.

---

# 57. Benchmark Dashboard

The PoC should expose:

```text
Artifact: gpt-x@2.0

Total size:              10 GB
Chunks:                  2,560

Origin bytes:            2.1 GB
Peer bytes:              8.0 GB
Deduplicated bytes:      3.4 GB

Origin bandwidth saved:  79%

Average peer throughput: 1.8 GB/s

Workers:
  Ready:                 10/10
  Failed:                0

Distribution time:
  P2P:                   42 sec
  Origin-only:           181 sec
```

The actual numbers must come from measurements, not assumptions.

---

# 58. Operational SLO Candidates

Production targets should eventually be defined around:

### Availability

```text
spiderd availability
tracker availability
controller availability
```

### Distribution

```text
p99 sync completion time
p99 chunk transfer latency
origin bandwidth reduction
```

### Reliability

```text
failed transfers
corrupt chunks
resume success rate
```

### Cache

```text
cache hit ratio
eviction rate
```

Targets should be established after PoC measurements.

---

# 59. Open Questions for the Coding Agent

The implementation planner should explicitly investigate:

1. Fixed chunks vs content-defined chunks.
2. gRPC vs QUIC.
3. PostgreSQL vs embedded metadata for the tracker.
4. How many chunks can the tracker efficiently track?
5. How to efficiently represent chunk availability.
6. How to prevent tracker overload.
7. How to perform hierarchical peer discovery.
8. How to avoid P2P amplification storms.
9. How to rate-limit seed nodes.
10. How to choose optimal replication.
11. Whether artifacts should be eagerly materialized or lazily materialized.
12. How to support sparse files.
13. How to handle symlinks securely.
14. How to support files larger than local filesystem limits.
15. How to garbage-collect chunks safely.
16. How to implement atomic artifact activation.
17. How to support signed manifests.
18. How to integrate with Kubernetes node topology.
19. How to add GPU-aware transport without contaminating the generic core.
20. How to benchmark against centralized downloads and existing artifact systems.

---

# 60. Future Extensions

Potential future capabilities:

### Artifact replication

```text
desired replicas = N
```

### Intelligent prefetch

Predict upcoming model releases/workloads.

### Cache-aware scheduling

Place inference workloads where the model is already cached.

### Cross-cluster distribution

```text
Cluster A -> Cluster B
```

### Delta distribution

Only changed chunks move.

### GPU-aware distribution

```text
GPU -> GPU
```

### Runtime-aware artifacts

Associate artifacts with:

* vLLM
* TensorRT-LLM
* SGLang
* custom runtimes

### Compiled artifact propagation

Distribute:

* Triton caches
* CUDA caches
* compiled kernels
* autotuning results

### Trainer -> inference distribution

Support rapidly changing checkpoints and weight versions.

---

# 61. Final Architectural Positioning

The project should be positioned as:

> **A distributed, content-addressed artifact fabric that turns external object storage into a topology-aware P2P cache for compute clusters.**

The central abstraction is:

```text
              Immutable Artifact
                     |
                  Manifest
                     |
                   Files
                     |
                  Chunks
                     |
           Content-addressed cache
                     |
             +-------+-------+
             |       |       |
           Node A  Node B  Node C
             \       |       /
              \      |      /
                 P2P Mesh
```

Kubernetes is an integration layer:

```text
Kubernetes
   |
   +-- CRD
   +-- Controller
   +-- DaemonSet
   +-- Node metadata
   +-- Scheduling
   +-- Lifecycle
```

The core framework remains:

```text
spiderd
tracker
registry
transfer engine
cache
source adapters
```

The ML/GPU layer is an extension:

```text
NIXL
RDMA
GDS
GPU-aware transfers
runtime adapters
```

---

# 62. Definition of Done

The project should not be considered successful merely because a model can be downloaded.

The PoC is successful when it demonstrates:

```text
                 External Storage
                       |
                       v
                Initial Seeds
                       |
             +---------+---------+
             |         |         |
             v         v         v
           Peer      Peer      Peer
             |         |         |
             +---------+---------+
                       |
                 Local Artifact
                       |
                  Application
```

with measurable evidence that:

1. Origin traffic decreases as peer participation increases.
2. Distribution time improves over origin-only downloads.
3. Failed peers do not corrupt the resulting artifact.
4. Workers resume after restart.
5. Artifact versions reuse unchanged chunks.
6. The same mechanism works for arbitrary directory trees.
7. Kubernetes is not required by the core engine.
8. Kubernetes CRDs/controllers can declaratively manage desired artifact availability.
9. The architecture can later support high-performance GPU/RDMA paths.
10. The system remains understandable and operable at every stage.

---

# 63. Guiding Principle

The most important design rule for the implementation is:

> **Separate what the artifact is from where its bytes currently live.**

The artifact is defined by its immutable manifest and content hashes.

Its bytes may exist simultaneously:

```text
S3
  |
  +-- Regional seed
  |
  +-- Node A
  |
  +-- Node B
  |
  +-- Node C
  |
  +-- Node D
```

The system's job is to make the artifact available at the destination using the **fastest, cheapest, safest available path**, while preserving content integrity.

That principle should guide every implementation decision.
