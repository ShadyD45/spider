# Spider benchmarks

Latest numbers from `scripts/run-benchmarks.ps1` / `scripts/run-benchmarks.sh`. Re-run those scripts to refresh this file.

**Host:** Windows amd64, 11th Gen Intel Core i5-11320H @ 3.20 GHz  
**Date:** 2026-08-15

---

## Grafana & Prometheus

Live stack (left running after `run-compose-benchmark`):

| Service | URL | Notes |
|---|---|---|
| Grafana | `http://<host>:3000/d/spider/spider-mesh` | `admin` / `admin` |
| Prometheus | `http://<host>:9090` | Scrapes worker + tracker metrics |
| Tracker | `http://<host>:9091/metrics` | Mapped from container `:9090` |

On **Podman Desktop for Windows**, `localhost` often does not forward container ports. Use the Podman VM IP instead, e.g. `http://172.19.x.x:3000/d/spider/spider-mesh` (run `wsl -d podman-machine-default -- ip -4 -o addr show eth0 scope global` to find the current address).

---

## Two benchmark modes

| Mode | Command | Stack | Grafana |
|---|---|---|---|
| **Compose fleet** (primary) | `./scripts/run-compose-benchmark.sh` | Real tracker, Redis, 3 workers, MinIO, Prometheus | **Yes** |
| **In-process loopback** (optional) | `spiderctl benchmark --size=500 --workers=6` | Ephemeral goroutines on localhost | No |

Compose benchmark uses **`tmp/origin/payload.bin`** (bind-mounted at `/bench/origin` on workers). Materialized output goes to `/data/bench/dest-*` and is **deleted after each run**; chunk caches live in per-worker volumes (`worker-N-data`). Grafana and Prometheus data persist in named volumes (`grafana-data`, `prometheus-data`, `tracker-data`).

```bash
./scripts/build-image.sh
./scripts/run-compose-benchmark.sh          # builds image, starts stack, runs 500 MB x 3 workers
# or: ./scripts/run-compose-benchmark.ps1 -SkipStack   # stack already up
```

---

## Microbenchmarks

```text
goos: windows
goarch: amd64
cpu: 11th Gen Intel(R) Core(TM) i5-11320H @ 3.20GHz

pkg: spider/pkg/chunk
BenchmarkChunker4MiB-8             	      51	  26592863 ns/op	 630.89 MB/s	20972907 B/op	      21 allocs/op
BenchmarkSHA256HashCalculation-8   	     358	   3668705 ns/op	1143.27 MB/s	     208 B/op	       3 allocs/op

pkg: spider/pkg/cache
BenchmarkCacheAtomicPut4MiB-8   	      20	  62205305 ns/op	  67.43 MB/s	   35850 B/op	      25 allocs/op
```

| Primitive | ns/op | Throughput | Allocs |
|---|---|---|---|
| SHA-256 (4 MiB) | 3.67 ms | 1143 MB/s | 3 |
| Fixed 4 MiB chunker (16 MiB stream) | 26.6 ms | 631 MB/s | 21 |
| Atomic cache Put + on-disk re-hash | 62.2 ms | 67 MB/s | 25 |

---

## Fleet distribution

### In-process (`spiderctl benchmark`) — 500 MB × 6 workers

Same-host loopback; no compose/Grafana. Useful for quick engine regression checks.

| Metric | Direct origin | Spider P2P | Improvement |
|---|---|---|---|
| Duration | 1m 1.5s | 1m 18.6s | 0.78× wall clock |
| Origin data | 3000 MB | 152 MB | **94.9% origin saved** |
| Peer data | 0 | 2848 MB | offloaded to mesh |

### Compose stack (`run-compose-benchmark`) — 500 MB × 3 workers

Real `artifactd` nodes, central tracker (Redis + SQLite), Prometheus scrape.

| Metric | Direct origin | Spider P2P | Improvement |
|---|---|---|---|
| Duration | 34.1s | 35.4s | 0.96× wall clock |
| Origin data | 1500 MB | 0 MB | **100% origin saved** |
| Peer data | 0 | 1000 MB | offloaded to mesh |
| Fleet throughput | 43.9 MB/s | 42.4 MB/s | — |

Prometheus delta (full run): `origin_downloaded=0`, `peer_transferred=1048576000`.

---

## Interpreting results (read this first)

### Primary metric: origin bytes, not wall clock

Spider’s main value on a real fleet is **reducing traffic to origin storage** (S3, MinIO, artifact registry egress). Always compare:

- `origin_bytes` / `origin_chunks` in sync logs (`reused=… peer_chunks=… origin_bytes=…`)
- Prometheus: `spider_origin_bytes_downloaded_total` vs `spider_peer_bytes_transferred_total`

Wall-clock duration can be **equal to or slower than** direct origin on a single host. That does **not** mean P2P failed if origin bytes dropped.

### Recorded results summary (2026-08-15)

| Benchmark | Workers | Origin (baseline) | Origin (P2P) | Peer (P2P) | Wall clock (baseline → P2P) |
|---|---|---|---|---|---|
| In-process loopback | 6 × 500 MB | 3000 MB | 152 MB (**94.9% saved**) | 2848 MB | 61.5s → 78.6s (**0.78×**) |
| Compose fleet | 3 × 500 MB | 1500 MB | 0 MB (**100% saved**) | 1000 MB | 34.1s → 35.4s (**0.96×**) |

Compose mesh: worker-1 seeds; workers 2–3 pull 500 MB each from worker-1; worker-1 reuses local cache. No worker reads `/bench/origin` during mesh sync.

### What “good” looks like on a real deployment

| Signal | Same-machine testbed | Real multi-host fleet |
|---|---|---|
| Origin bytes on leaf workers | Low / zero | **Low / zero** (primary win) |
| Wall clock vs direct origin | Often ~1× or slower | Can improve when origin is remote or rate-limited |
| Peer bytes | High (local bridge traffic) | High (rack/AZ-local mesh) |
| Grafana origin vs peer panels | Shows shift after compose run | Same metrics at scale |

---

## Limitations of the current testbed

These runs are **misleading for wall-clock speedup** if taken out of context:

1. **Single physical machine** — All workers, tracker, and origin bind mount share one CPU, disk, and (on Windows) one Podman/WSL VM. P2P traffic never crosses a real network bottleneck you are trying to avoid.

2. **Origin is a local bind mount** — Baseline reads `./tmp/origin/payload.bin` mounted at `/bench/origin`. That is faster than S3/MinIO over the network, so baseline is an optimistic upper bound.

3. **Single seed fan-out** — One worker (worker-1) serves two peers. Baseline uses three parallel origin readers; mesh centralizes serving on one node.

4. **Protocol overhead without geographic win** — gRPC chunk streaming, tracker lookups, SHA-256 verification, and scheduler inflight caps add work that only pays off when origin is **slow, expensive, or far away**.

5. **In-process vs compose** — Loopback benchmark uses ephemeral goroutines (6 workers); compose uses 3 real containers. Numbers are not directly comparable across modes.

---

## Pending benchmark work

- [ ] **Multi-machine fleet** — Separate seed host, multiple worker hosts, remote object-store origin (different AZ/region).
- [ ] **Rate-limited origin** — Throttle S3/MinIO to simulate production egress caps.
- [ ] **Larger artifacts & more workers** — e.g. 10+ nodes, multi-GB models.
- [ ] **Document refreshed results** in this file after each multi-host run.

---

## How to refresh

```powershell
# Full suite: micro + compose fleet (feeds Grafana)
.\scripts\run-benchmarks.ps1

# Compose only (stack left running)
.\scripts\run-compose-benchmark.ps1

# Optional fast in-process check (no Grafana)
go build -o bin/spiderctl.exe ./cmd/spiderctl
.\bin\spiderctl.exe benchmark --size=500 --workers=6 --chunk-size=4
```
