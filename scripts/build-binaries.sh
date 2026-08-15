#!/usr/bin/env bash
# Cross-compile Linux static binaries into dist/linux/ for the container image.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${ROOT}/dist/linux"
LDFLAGS="-s -w"

mkdir -p "${OUT}"

export CGO_ENABLED=0
export GOOS=linux
export GOARCH="${GOARCH:-amd64}"
export GOFLAGS="-trimpath"

cd "${ROOT}"

build_one() {
  local name="$1"
  local pkg="$2"
  echo "  ${name} <- ${pkg}"
  go build -ldflags="${LDFLAGS}" -o "${OUT}/${name}" "${pkg}"
}

echo "--- Go build (linux/${GOARCH}) -> dist/linux ---"
build_one tracker ./cmd/tracker
build_one spiderd ./cmd/spiderd
build_one spiderctl ./cmd/spiderctl

echo "Done: ${OUT}"
