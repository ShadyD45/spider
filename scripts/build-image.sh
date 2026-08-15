#!/usr/bin/env bash
# Build Linux binaries, then assemble the runtime container image (no compile in Containerfile).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${SPIDER_IMAGE:-localhost/spider:local}"
CONTAINER="${CONTAINER_CMD:-}"

cd "${ROOT}"

"${ROOT}/scripts/build-binaries.sh"

if [[ -z "${CONTAINER}" ]]; then
  if command -v podman >/dev/null 2>&1; then
    CONTAINER=podman
  elif command -v docker >/dev/null 2>&1; then
    CONTAINER=docker
  else
    echo "error: podman or docker required" >&2
    exit 1
  fi
fi

echo "--- ${CONTAINER} build -t ${IMAGE} ---"
"${CONTAINER}" build -t "${IMAGE}" -f Containerfile .

echo "Image ready: ${IMAGE}"
echo "Start stack: podman-compose -f podman-compose.yml up -d"
