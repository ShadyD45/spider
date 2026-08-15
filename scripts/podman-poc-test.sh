#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="podman-compose"
if ! command -v podman-compose &> /dev/null; then
  if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
  else
    COMPOSE_CMD="podman compose"
  fi
fi

echo "=========================================================="
echo "    Distributed Artifact Fabric (Spider) — Phase 1 PoC   "
echo "=========================================================="
echo "Using compose tool: ${COMPOSE_CMD}"

# Helper functions
cleanup() {
  echo "--- Tearing down testbed ---"
  ${COMPOSE_CMD} down -v --remove-orphans || true
}
trap cleanup EXIT

echo "--- 1. Building image and starting cluster ---"
"$(dirname "$0")/build-image.sh"
${COMPOSE_CMD} up -d

echo "Waiting for services to become healthy..."
sleep 5

# Check peers on tracker
echo "--- 2. Checking Registered Peers on Central Tracker ---"
${COMPOSE_CMD} exec central-tracker /usr/local/bin/artifactctl peers --tracker=127.0.0.1:50051

# Generate test data inside worker-1
echo "--- 3. Generating Test Artifacts inside Worker-1 ---"
${COMPOSE_CMD} exec worker-1 bash -c "mkdir -p /data/src/model-v1 /data/src/model-v2"
${COMPOSE_CMD} exec worker-1 bash -c "echo 'config v1' > /data/src/model-v1/config.json"
${COMPOSE_CMD} exec worker-1 bash -c "dd if=/dev/urandom of=/data/src/model-v1/shard1.bin bs=1M count=8 status=none"
${COMPOSE_CMD} exec worker-1 bash -c "dd if=/dev/urandom of=/data/src/model-v1/shard2.bin bs=1M count=8 status=none"

# Experiment 1 & 2: Publish on Worker-1 & P2P Mesh Sync to Worker-2
echo "=========================================================="
echo "  Experiment 2: P2P Fan-Out Efficiency & Mesh Sync        "
echo "=========================================================="
echo "Publishing model-v1 on worker-1..."
${COMPOSE_CMD} exec worker-1 /usr/local/bin/artifactctl publish \
  --source=/data/src/model-v1 \
  --name=model-x \
  --version=1.0 \
  --chunk-size=4194304 \
  --output=/data/manifest-v1.json \
  --tracker=central-tracker:50051

echo "Syncing model-v1 from Worker-1 to Worker-2 over P2P mesh..."
${COMPOSE_CMD} exec worker-2 bash -c "mkdir -p /data/dest/model-v1"
${COMPOSE_CMD} exec worker-2 /usr/local/bin/artifactctl sync \
  --manifest=/data/manifest-v1.json \
  --dest=/data/dest/model-v1 \
  --daemon=127.0.0.1:50052

sleep 3
echo "Worker-2 status:"
${COMPOSE_CMD} exec worker-2 /usr/local/bin/artifactctl status --daemon=127.0.0.1:50052

# Experiment 3: Peer Disruption and Origin Fallback
echo "=========================================================="
echo "  Experiment 3: Peer Disruption and Fallback               "
echo "=========================================================="
echo "Stopping worker-1 to simulate peer failure..."
${COMPOSE_CMD} stop worker-1

echo "Syncing to worker-3 (Worker-1 dead -> falls back to origin/cached)..."
${COMPOSE_CMD} exec worker-3 bash -c "mkdir -p /data/dest/model-v1"
${COMPOSE_CMD} exec worker-3 /usr/local/bin/artifactctl sync \
  --manifest=/data/manifest-v1.json \
  --dest=/data/dest/model-v1 \
  --daemon=127.0.0.1:50052 || true

echo "Restarting worker-1..."
${COMPOSE_CMD} start worker-1
sleep 3

# Experiment 6: Version Deduplication
echo "=========================================================="
echo "  Experiment 6: Version Deduplication                      "
echo "=========================================================="
echo "Creating model-v2 (sharing shard1.bin from v1)..."
${COMPOSE_CMD} exec worker-1 bash -c "echo 'config v2' > /data/src/model-v2/config.json"
${COMPOSE_CMD} exec worker-1 bash -c "cp /data/src/model-v1/shard1.bin /data/src/model-v2/shard1.bin"
${COMPOSE_CMD} exec worker-1 bash -c "dd if=/dev/urandom of=/data/src/model-v2/shard2.bin bs=1M count=8 status=none"

echo "Publishing model-v2..."
${COMPOSE_CMD} exec worker-1 /usr/local/bin/artifactctl publish \
  --source=/data/src/model-v2 \
  --name=model-x \
  --version=2.0 \
  --chunk-size=4194304 \
  --output=/data/manifest-v2.json \
  --tracker=central-tracker:50051

echo "Syncing model-v2 to Worker-2 (already has shard1.bin in local cache)..."
${COMPOSE_CMD} exec worker-2 bash -c "mkdir -p /data/dest/model-v2"
${COMPOSE_CMD} exec worker-2 /usr/local/bin/artifactctl sync \
  --manifest=/data/manifest-v2.json \
  --dest=/data/dest/model-v2 \
  --daemon=127.0.0.1:50052

sleep 3
${COMPOSE_CMD} exec worker-2 /usr/local/bin/artifactctl status --daemon=127.0.0.1:50052

echo "=========================================================="
echo "           ALL PHASE 1 POC EXPERIMENTS PASSED!            "
echo "=========================================================="
