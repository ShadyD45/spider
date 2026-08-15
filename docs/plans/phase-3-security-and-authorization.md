# Phase 3: Security & Enterprise Authorization Plan

**Document Status:** Approved Specification  
**Phase:** 3 of 6  
**Focus:** Mutual TLS (mTLS), Signed Manifests, Identity Attestation, and Security Guardrails

---

## 1. Overview & Objectives

Phase 3 introduces enterprise-grade security hardening across all communication planes and storage subsystems. It converts the open PoC network into a Zero-Trust fabric capable of running in hostile or multi-tenant cluster environments.

### Key Objectives
1. **Mutual TLS (mTLS)**: Enforce mandatory mTLS for all gRPC connections (Daemon-to-Tracker, Daemon-to-Daemon).
2. **Node Identity & Certificate Management**: Implement node identity attestation (SPIFFE/SPIRE or internal CA certificate provisioning).
3. **Cryptographically Signed Manifests**: Enforce manifest signing using Ed25519 keypairs to prevent artifact tampering or malicious manifest injection.
4. **Access Control & RBAC**: Token-based authorization for publishing, fetching, and advertising artifacts.
5. **Directory Traversal & Symlink Protection**: Phase 2 ships a path-join escape guard in `pkg/materializer`. Phase 3 adds symlink policy (`allowSymlinks: false` by default) and hardens the guard under mTLS/signed-manifest threat models.

---

## 2. Technical Architecture & Security Protocols

### 2.1 Mutual TLS Architecture (`pkg/security/tls`)

All internal gRPC transport is encrypted and mutually authenticated:

```text
  +-------------------+                          +-------------------+
  |   artifactd A     |    mTLS (gRPC Stream)    |   artifactd B     |
  |  (Client Cert A)  | <----------------------> |  (Server Cert B)  |
  +-------------------+                          +-------------------+
            \                                              /
             \                                            /
              v                                          v
      +------------------------------------------------------+
      |         Internal Certificate Authority (CA)          |
      |             (Root CA Cert Validation)                |
      +------------------------------------------------------+
```

- **Cipher Suites**: TLS 1.3 only (`TLS_AES_256_GCM_SHA384`, `TLS_CHACHA20_POLY1305_SHA256`).
- **SAN Validation**: Certificates must contain Subject Alternative Name formatted as `spiffe://artifact-mesh/node/<node-id>`.

---

### 2.2 Ed25519 Signed Manifests (`pkg/security/signature`)

To prevent arbitrary code execution or tampered artifact injection:

```json
{
  "schemaVersion": 1,
  "artifactId": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "name": "gpt-x",
  "version": "2.0",
  "signature": {
    "keyId": "publisher-key-2026",
    "algorithm": "Ed25519",
    "value": "base64:3b6a9f..."
  },
  "files": [...]
}
```

- **Publish Verification**: `artifactctl publish` signs canonical manifest payload with publisher's private key.
- **Sync Verification**: `artifactd` validates the signature against trusted public keys before accepting manifest and queuing chunk downloads. Unsigned or invalid manifests are rejected immediately.

---

### 2.3 Storage Safety & Path Traversal Guardrails (`pkg/materializer/security.go`)

To prevent malicious manifests from overwriting host files (e.g. `../../../../etc/passwd`):

1. **Path Sanitization**: Relative paths in manifests are resolved strictly against target materialization directory using `filepath.Clean`.
2. **Escape Guard**:
   ```go
   cleanPath := filepath.Join(baseDir, manifestFile.Path)
   if !strings.HasPrefix(cleanPath, filepath.Clean(baseDir)+string(filepath.Separator)) {
       return fmt.Errorf("security violation: path escape detected: %s", manifestFile.Path)
   }
   ```
3. **Symlink Policy**:
   - Symlinks disabled by default (`allowSymlinks: false`).
   - If enabled via explicit policy, symlink targets must resolve within the artifact root directory. Symlinks pointing to target paths outside the artifact root are rejected.

---

### 2.4 Threat Model & Mitigation Matrix

| Threat | Impact | Mitigation Strategy |
|---|---|---|
| **Rogue Peer Injection** | Attacker joins mesh and advertises bad bytes | mTLS node certificate authentication + mandatory SHA-256 chunk hash verification |
| **Tampered Manifest** | Malicious file list or source URI inserted | Ed25519 signature validation on manifest payload before processing |
| **Path Traversal Attack** | Manifest contains `../../etc/shadow` path | Strict path cleaning and prefix boundary validation |
| **Man-In-The-Middle** | Interception of chunk streaming bytes | Enforced TLS 1.3 transport encryption |
| **DoS via Memory Bomb** | Attacker requests infinite chunk streams | Max gRPC frame limits (64 KiB buffer, 4 MiB max message size) |

---

## 3. Implementation Checklist

- [ ] Implement `pkg/security/tls` helper for loading CA, client certificates, and server certificates.
- [ ] Add CLI flags to `artifactd`, `tracker`, and `artifactctl` for `--tls-ca`, `--tls-cert`, and `--tls-key`.
- [ ] Implement Ed25519 manifest signature generation in `cmd/artifactctl publish`.
- [ ] Implement signature validation module in `pkg/manifest`.
- [x] Path-join escape guard in `pkg/materializer` (Phase 2). Symlink policy remains Phase 3.
- [ ] Add security unit tests verifying rejection of path-traversal manifests and untrusted TLS clients.
- [ ] Update `podman-compose.yml` to generate local TLS certificates (using `cfssl` or `openssl`) and enforce mTLS across containers.
