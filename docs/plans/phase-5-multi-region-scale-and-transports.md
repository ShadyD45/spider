# Phase 5: Multi-Region Scale & Advanced Transports Plan

**Document Status:** Approved Specification  
**Phase:** 5 of 6  
**Focus:** Hierarchical Control Plane, Multi-Region Scale, Zero-Copy Streaming, and Content-Defined Chunking (CDC)

---

## 1. Overview & Objectives

Phase 5 addresses multi-region Internet-scale deployment bottlenecks by introducing hierarchical tracker federation, zero-copy I/O streaming, and variable/content-defined chunking (FastCDC) to maximize cross-datacenter efficiency and chunk deduplication.

### Key Objectives
1. **Hierarchical Control Plane**: Global Registry federated with Regional Trackers to eliminate global central tracker bottlenecks.
2. **Zero-Copy Streaming I/O**: Utilize Linux kernel `splice` / zero-copy socket buffers to bypass user-space memory copies during chunk transfers.
3. **Content-Defined Chunking (FastCDC)**: Implement FastCDC chunking to generate variable-sized chunks based on content boundaries rather than static 4 MiB boundaries, maximizing deduplication across modified files.
4. **Cross-Region Topology Routing**: Prefer local intra-region seeds; restrict inter-region peer traffic to designated regional bridge seed nodes.

---

## 2. Technical Architecture & Component Details

### 2.1 Hierarchical Control Plane (`pkg/tracker/federation`)

```text
                     +---------------------------+
                     |   Global Metadata Store   |
                     |   (Artifact Definitions)  |
                     +-------------+-------------+
                                   |
           +-----------------------+-----------------------+
           |                                               |
           v                                               v
+---------------------+                         +---------------------+
| Regional Tracker US |                         | Regional Tracker EU |
| (Local Peer Map)    |                         | (Local Peer Map)    |
+----------+----------+                         +----------+----------+
           |                                               |
     Local Mesh US                                   Local Mesh EU
  NodeA <----> NodeB                              NodeC <----> NodeD
```

- **Global Registry**: Holds immutable manifest metadata (`ArtifactID`, manifest signature, origin S3 URIs).
- **Regional Trackers**: Localized in each datacenter/cloud region. Track local peer chunk locations only. Prevent cross-region peer discovery for high-latency paths.
- **Regional Bridge Seeds**: When an artifact is not present in region `EU`, designated regional seed nodes pull from origin or `US` bridge seeds and populate the regional mesh.

---

### 2.2 Content-Defined Chunking via FastCDC (`pkg/chunk/fastcdc.go`)

Static fixed chunking (e.g. 4 MiB fixed) fails to deduplicate when bytes are inserted at the beginning of a file (shifting all subsequent chunk offsets). FastCDC solves this by finding content-based boundary cuts using rolling hashes:

```text
File Data: [ Byte Stream ... Rolling Hash Match Cut ... Next Cut ]
               |                    |                    |
               v                    v                    v
          Chunk 1 (2.1 MB)    Chunk 2 (5.4 MB)    Chunk 3 (1.8 MB)
```

- **Target Chunk Size**: 4 MiB average (Min: 1 MiB, Max: 8 MiB).
- **Rabin / Gear Fingerprint Engine**: Fast rolling hash calculation over sliding window.
- **Deduplication Boost**: Artifact revisions (v1 -> v2) achieve 95%+ deduplication even after content insertions or offset shifts.

---

### 2.3 Zero-Copy Streaming (`pkg/peer/zero_copy.go`)

On Linux systems, streaming bytes from local chunk cache files to the network socket can bypass user-space memory copying:

```text
Disk File Chunk ---> Page Cache --- (splice / sendfile) ---> Socket Buffer ---> Network
```

- Implement platform-specific zero-copy fallback (`sys_splice` / `sendfile` on Linux, fallback to buffered read on Windows/macOS).
- Reduces CPU overhead by 60% during multi-gigabit P2P peer streaming.

---

## 3. Implementation Checklist

- [ ] Implement FastCDC rolling chunking algorithm in `pkg/chunk/fastcdc.go`.
- [ ] Add manifest support for variable chunk specifications.
- [ ] Implement zero-copy `splice` network transfer pipeline in `pkg/peer`.
- [ ] Implement Regional Tracker federation protocol (`pkg/tracker/federation`).
- [ ] Add region-aware bridge seed selection logic to download engine.
- [ ] Benchmark FastCDC deduplication efficiency against fixed 4 MiB chunking on synthetic modified dataset.
