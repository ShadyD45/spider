# Phase 6: High-Speed ML & GPU Acceleration Extensions Plan

**Document Status:** Approved Specification  
**Phase:** 6 of 6  
**Focus:** Pluggable Transport Layer, RDMA, GPUDirect Storage (GDS), NIXL Integration, and ML Inference Runtime Adapters

---

## 1. Overview & Objectives

Phase 6 implements optional high-performance transport extensions designed specifically for Machine Learning (LLM) serving clusters equipped with NVIDIA/AMD GPU hardware, RDMA networks (InfiniBand/RoCE), and high-throughput inference runtimes (vLLM, TensorRT-LLM, SGLang).

### Key Objectives
1. **Pluggable Transport Abstraction**: Maintain strict separation between generic artifact distribution logic and high-performance hardware transports (gRPC default, RDMA / NIXL optional).
2. **GPUDirect Storage (GDS) / Direct I/O**: Direct high-speed streaming from local storage/P2P NICs directly into GPU HBM memory, bypassing CPU host RAM bottlenecks.
3. **Hardware & Accelerator Compatibility Metadata**: Support artifact compatibility manifests matching GPU architecture (e.g. `sm90`, `sm100`), CUDA versions, and tensor layout runtimes.
4. **ML Inference Framework Adapters**: Integrations for vLLM, TensorRT-LLM, and HuggingFace Cache enabling transparent background weight preloading.

---

## 2. Technical Architecture & Extensions

### 2.1 Pluggable Transport Layer (`pkg/transport`)

```text
                  +-----------------------------------+
                  |      Artifact Mesh Core Engine     |
                  +-----------------+-----------------+
                                    |
                    +---------------+---------------+
                    | Transport Interface           |
                    +---------------+---------------+
                                    |
        +---------------------------+---------------------------+
        |                           |                           |
        v                           v                           v
+---------------+           +---------------+           +---------------+
| Standard TCP  |           |     QUIC      |           |  RDMA / NIXL  |
| / gRPC Stream |           |  Transport    |           | (InfiniBand/  |
|  (Default)    |           |               |           |  RoCE / GDS)  |
+---------------+           +---------------+           +---------------+
```

- **Transport Interface**:
  ```go
  type Transport interface {
      Name() string
      CanHandle(peer PeerCapabilities) bool
      TransferChunk(ctx context.Context, peer PeerInfo, chunkHash string) ([]byte, error)
  }
  ```

---

### 2.2 ML Compatibility Metadata (`api/v1/compatibility.go`)

Manifests can optionally declare hardware compatibility requirements to prevent incompatible weight deployment:

```json
{
  "compatibility": {
    "gpuVendor": "nvidia",
    "gpuArchitecture": "sm90",
    "cudaVersion": "12.x",
    "runtime": "vllm",
    "quantization": "fp8_e4m3"
  }
}
```

- **Placement Engine**: `artifactd` validates node hardware capabilities against artifact compatibility metadata before accepting sync request. Incompatible artifacts return explicit `ErrIncompatibleHardware` status.

---

### 2.3 ML Inference Framework Adapters (`adapters/runtimes/`)

#### vLLM Model Preloader Adapter
Provide a Python lightweight client library (`artifact_mesh_vllm`) or sidecar HTTP API enabling inference servers to poll readiness:

```python
from artifact_mesh import ArtifactClient

client = ArtifactClient(daemon_url="http://localhost:50052")

# Ensure LLM weights are synchronized locally before initializing vLLM engine
model_path = client.ensure_artifact_ready(
    name="meta-llama/Llama-3-70B",
    version="1.0"
)

# Pass materialized local directory to vLLM
llm = LLM(model=model_path)
```

---

## 3. Implementation Checklist

- [ ] Implement `pkg/transport` interface and standard gRPC default adapter.
- [ ] Add optional CGO / Rust FFI binding stub for RDMA / NIXL zero-copy transport library.
- [ ] Add compatibility metadata parsing and validation to `api/v1/manifest.go`.
- [ ] Implement Python SDK (`adapters/python/`) for vLLM & HuggingFace integration.
- [ ] Write documentation and benchmark guidelines for GPU cluster evaluation.
