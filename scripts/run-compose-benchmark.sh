#!/usr/bin/env bash
# Fleet benchmark against the Podman/Docker compose stack (tracker, workers, Prometheus, Grafana).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SIZE_MB="${SIZE_MB:-500}"
CHUNK_MB="${CHUNK_MB:-4}"
PAYLOAD="${PAYLOAD:-tmp/origin/payload.bin}"
ORIGIN_DIR="$(dirname "$PAYLOAD")"
COMPOSE_FILE="${COMPOSE_FILE:-podman-compose.yml}"
SKIP_STACK="${SKIP_STACK:-0}"
WORKERS=(worker-1 worker-2 worker-3)
CHUNK_BYTES=$((CHUNK_MB * 1024 * 1024))
WANT_BYTES=$((SIZE_MB * 1024 * 1024))

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE=(podman-compose -f "$COMPOSE_FILE")
elif command -v docker >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$COMPOSE_FILE")
else
  echo "error: podman-compose or docker compose required" >&2
  exit 1
fi

ensure_payload() {
  mkdir -p "$ORIGIN_DIR" tmp/bench tmp/work/cache
  go run ./scripts/ensure-payload/main.go "$PAYLOAD" "$WANT_BYTES"
}

wait_stack() {
  echo "--- waiting for tracker /healthz ---"
  for _ in $(seq 1 90); do
    if curl -sf http://127.0.0.1:9091/healthz >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  echo "error: tracker not ready on :9091" >&2
  exit 1
}

prom_sum() {
  local query="$1"
  curl -sfG "http://127.0.0.1:9090/api/v1/query" --data-urlencode "query=${query}" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); r=d.get('data',{}).get('result',[]); print(r[0]['value'][1] if r else '0')" 2>/dev/null \
    || echo "0"
}

reset_workers() {
  echo "--- reset worker caches and bench dest ---"
  for w in "${WORKERS[@]}"; do
    "${COMPOSE[@]}" exec -T "$w" sh -c 'rm -rf /var/lib/spider/chunks/* /data/bench/dest-baseline /data/bench/dest-mesh 2>/dev/null; mkdir -p /data/bench'
  done
  echo "--- restart workers (refresh tracker chunk index) ---"
  "${COMPOSE[@]}" restart worker-1 worker-2 worker-3
  sleep 8
}

write_manifest() {
  echo "--- host publish -> tmp/bench/manifest.json ---"
  go build -o bin/spiderctl ./cmd/spiderctl
  ./bin/spiderctl publish \
    --source="$ORIGIN_DIR" \
    --name=bench-model \
    --version=1.0 \
    --chunk-size="$CHUNK_BYTES" \
    --output=tmp/bench/manifest.json \
    --cache-dir=tmp/work/cache \
    --tracker=127.0.0.1:50051 || true
}

parallel_sync() {
  local dest="$1"
  local origin="${2:-}"
  local logs=()
  local pids=()
  for w in "${WORKERS[@]}"; do
    local log="/tmp/spider-compose-bench-${w}-$(basename "$dest").log"
    logs+=("$log")
    (
      args=(artifactctl sync --manifest=/data/bench/manifest.json --dest="$dest" --daemon=127.0.0.1:50052)
      if [[ -n "$origin" ]]; then args+=(--origin="$origin"); fi
      "${COMPOSE[@]}" exec -T "$w" "${args[@]}" >"$log" 2>&1
    ) &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  echo "${logs[@]}"
}

sum_metrics() {
  local total_origin=0 total_peer=0
  for log in "$@"; do
    line="$(grep -E '^reused=' "$log" | tail -1 || true)"
    [[ -z "$line" ]] && continue
    ob="$(echo "$line" | sed -n 's/.*origin_bytes=\([0-9]*\).*/\1/p')"
    pb="$(echo "$line" | sed -n 's/.*peer_bytes=\([0-9]*\).*/\1/p')"
    total_origin=$((total_origin + ob))
    total_peer=$((total_peer + pb))
  done
  echo "$total_origin $total_peer"
}

cleanup_dest() {
  echo "--- cleanup worker materialized dirs ---"
  for w in "${WORKERS[@]}"; do
    "${COMPOSE[@]}" exec -T "$w" sh -c 'rm -rf /data/bench/dest-baseline /data/bench/dest-mesh'
  done
}

echo "================================================================="
echo "     SPIDER — COMPOSE STACK BENCHMARK (${SIZE_MB} MB x 3 workers) "
echo "================================================================="

ensure_payload

if [[ "$SKIP_STACK" != "1" ]]; then
  "${ROOT}/scripts/build-image.sh"
  "${COMPOSE[@]}" up -d
fi

wait_stack
write_manifest
reset_workers

ORIGIN_DL_BEFORE="$(prom_sum 'sum(spider_origin_bytes_downloaded_total)')"
PEER_BEFORE="$(prom_sum 'sum(spider_peer_bytes_transferred_total)')"

echo ""
echo "=== Scenario 1: Direct origin (each worker reads /bench/origin) ==="
START=$(date +%s)
mapfile -t BASE_LOGS < <(parallel_sync /data/bench/dest-baseline /bench/origin | tr ' ' '\n')
END=$(date +%s)
read -r BASE_ORIGIN BASE_PEER <<< "$(sum_metrics "${BASE_LOGS[@]}")"
BASE_SEC=$((END - START))
BASE_ARTIFACT=$((SIZE_MB * ${#WORKERS[@]}))

reset_workers

echo ""
echo "=== Scenario 2: P2P mesh (seed on worker-1, fan-out to fleet) ==="
"${COMPOSE[@]}" exec -T worker-1 artifactctl publish \
  --source=/bench/origin \
  --name=bench-model \
  --version=1.0 \
  --chunk-size="$CHUNK_BYTES" \
  --output=/data/bench/manifest.json \
  --tracker=central-tracker:50051 \
  --cache-dir=/var/lib/spider \
  --node-id=worker-1

START=$(date +%s)
mapfile -t MESH_LOGS < <(parallel_sync /data/bench/dest-mesh "" | tr ' ' '\n')
END=$(date +%s)
read -r MESH_ORIGIN MESH_PEER <<< "$(sum_metrics "${MESH_LOGS[@]}")"
MESH_SEC=$((END - START))

ORIGIN_DL_AFTER="$(prom_sum 'sum(spider_origin_bytes_downloaded_total)')"
PEER_AFTER="$(prom_sum 'sum(spider_peer_bytes_transferred_total)')"

cleanup_dest

python3 - <<PY
base_o=$BASE_ORIGIN
mesh_o=$MESH_ORIGIN
mesh_p=$MESH_PEER
base_s=$BASE_SEC
mesh_s=$MESH_SEC
artifact=$BASE_ARTIFACT
saved=(base_o-mesh_o)/base_o*100 if base_o else 0
speed=base_s/mesh_s if mesh_s else 0
base_tp=artifact/base_s if base_s else 0
mesh_tp=artifact/mesh_s if mesh_s else 0
print("")
print("METRIC                    DIRECT ORIGIN (BASELINE)   SPIDER P2P MESH   IMPROVEMENT")
print("------                    ------------------------   ---------------   -----------")
print(f"Duration                  {base_s}s                      {mesh_s}s               {speed:.2f}x speedup")
print(f"Fleet Throughput          {base_tp:.2f} MB/s                  {mesh_tp:.2f} MB/s           —")
print(f"Origin Data Transferred   {base_o/1024/1024:.2f} MB                  {mesh_o/1024/1024:.2f} MB           {saved:.1f}% bandwidth saved")
print(f"Peer Data Transferred     0.00 MB                  {mesh_p/1024/1024:.2f} MB           —")
PY

echo ""
echo "Prometheus totals (delta this run): origin_downloaded=$(( ORIGIN_DL_AFTER - ORIGIN_DL_BEFORE )) peer_transferred=$(( PEER_AFTER - PEER_BEFORE ))"
echo ""
echo "Grafana:    http://localhost:3000/d/spider/spider-mesh  (admin / admin)"
echo "Prometheus: http://localhost:9090"
echo "Stack left running — do not compose down until you have screenshots."
echo "================================================================="
