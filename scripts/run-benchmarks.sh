#!/usr/bin/env bash
set -euo pipefail

echo "================================================================="
echo "        SPIDER ARTIFACT MESH — BENCHMARK SUITE RUNNER           "
echo "================================================================="

echo ""
echo "--- 1. Running Go Microbenchmarks ---"
go test -bench=. -benchmem ./pkg/...

echo ""
echo "--- 2. Running End-to-End Fleet Distribution Benchmark (50 MB x 4 Nodes) ---"
./artifactctl benchmark --size=50 --workers=4 --chunk-size=4

echo ""
echo "--- 3. Running Larger Scale Distribution Benchmark (100 MB x 6 Nodes) ---"
./artifactctl benchmark --size=100 --workers=6 --chunk-size=4

echo ""
echo "================================================================="
echo "                 BENCHMARK SUITE COMPLETED                      "
echo "================================================================="
