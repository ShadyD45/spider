#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:-./testdata}"
mkdir -p "${TARGET_DIR}/model-v1" "${TARGET_DIR}/model-v2"

echo "=== Generating Synthetic Artifacts in ${TARGET_DIR} ==="

# 1. Generate Model v1
echo '{"architectures": ["SpiderTransformer"], "hidden_size": 4096, "num_layers": 32}' > "${TARGET_DIR}/model-v1/config.json"
echo '{"version": "1.0", "vocab_size": 32000, "type": "bpe"}' > "${TARGET_DIR}/model-v1/tokenizer.json"

echo "Generating model-v1 safetensors shard 1 (16 MiB)..."
dd if=/dev/urandom of="${TARGET_DIR}/model-v1/model-00001.safetensors" bs=1M count=16 status=none

echo "Generating model-v1 safetensors shard 2 (16 MiB)..."
dd if=/dev/urandom of="${TARGET_DIR}/model-v1/model-00002.safetensors" bs=1M count=16 status=none

# 2. Generate Model v2 (sharing shard 1, modifying shard 2 and config)
echo '{"architectures": ["SpiderTransformer"], "hidden_size": 4096, "num_layers": 32, "version": "2.0"}' > "${TARGET_DIR}/model-v2/config.json"
cp "${TARGET_DIR}/model-v1/tokenizer.json" "${TARGET_DIR}/model-v2/tokenizer.json"
cp "${TARGET_DIR}/model-v1/model-00001.safetensors" "${TARGET_DIR}/model-v2/model-00001.safetensors"

echo "Generating model-v2 modified safetensors shard 2 (16 MiB)..."
dd if=/dev/urandom of="${TARGET_DIR}/model-v2/model-00002.safetensors" bs=1M count=16 status=none

echo "=== Synthetic Artifacts Successfully Generated ==="
ls -lh "${TARGET_DIR}/model-v1"
ls -lh "${TARGET_DIR}/model-v2"
