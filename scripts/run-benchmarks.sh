#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

FILE="${1:-tmp/origin/payload.bin}"

echo "================================================================="
echo "        SPIDER ARTIFACT MESH — BENCHMARK SUITE RUNNER           "
echo "================================================================="

echo ""
echo "--- 1. Go microbenchmarks ---"
go test -count=1 -bench="." -benchmem ./pkg/chunk ./pkg/cache

echo ""
echo "--- 2. Compose fleet benchmark (500 MB x 3 workers, feeds Grafana) ---"
PAYLOAD="$FILE" ./scripts/run-compose-benchmark.sh

echo ""
echo "--- Optional: in-process loopback (fast, no Grafana) ---"
echo "  ./bin/spiderctl benchmark --file=$FILE --size=500 --workers=6 --chunk-size=4"

echo ""
echo "================================================================="
echo "                 BENCHMARK SUITE COMPLETED                      "
echo "================================================================="
