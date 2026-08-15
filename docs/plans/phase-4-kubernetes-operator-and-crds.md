# Phase 4: Kubernetes Operator & CRD Plan

**Document Status:** Approved Specification  
**Phase:** 4 of 6  
**Focus:** Declarative Kubernetes Integration, Custom Resource Definitions (CRDs), Operator Controller, and DaemonSet Management

---

## 1. Overview & Objectives

Phase 4 embeds the Artifact Mesh into Kubernetes environments as a first-class declarative integration without coupling the underlying core Go distribution engine to Kubernetes APIs.

### Key Objectives
1. **Custom Resource Definition (`ArtifactDeployment`)**: Define CRD schema `artifact.fabric/v1alpha1` representing desired artifact placement across cluster nodes.
2. **Kubernetes Controller (`cmd/controller`)**: Build a `controller-runtime` operator that watches `ArtifactDeployment` CRDs, evaluates node selectors/topology, and instructs `spiderd` daemons via gRPC/REST.
3. **`spiderd` DaemonSet Manifests**: Helm charts and Kustomize manifests deploying `spiderd` on cluster nodes with hostPath persistent storage (`/var/lib/spider`).
4. **Node Topology Mapping**: Automatically map Kubernetes node labels (`topology.kubernetes.io/zone`, `topology.kubernetes.io/region`, `kubernetes.io/hostname`) into `spiderd` topology registration.
5. **CRD Status Subresource**: Expose real-time distribution progress (desired vs ready nodes, bytes transferred, distribution latency) on the CRD object.

---

## 2. CRD Specification & Schema Architecture

### 2.1 CRD Definition (`deploy/kubernetes/crd.yaml`)
```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: artifactdeployments.artifact.fabric
spec:
  group: artifact.fabric
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              required:
                - artifact
              properties:
                artifact:
                  type: object
                  required:
                    - name
                    - version
                  properties:
                    name:
                      type: string
                    version:
                      type: string
                placement:
                  type: object
                  properties:
                    nodeSelector:
                      type: object
                      additionalProperties:
                        type: string
                policy:
                  type: object
                  properties:
                    prefetch:
                      type: boolean
                    minReadyPercent:
                      type: integer
                      minimum: 0
                      maximum: 100
                cache:
                  type: object
                  properties:
                    pin:
                      type: boolean
            status:
              type: object
              properties:
                nodesTotal:
                  type: integer
                nodesReady:
                  type: integer
                nodesSyncing:
                  type: integer
                bytesTotal:
                  type: integer
                bytesAvailable:
                  type: integer
                conditions:
                  type: array
                  items:
                    type: object
                    properties:
                      type:
                        type: string
                      status:
                        type: string
                      reason:
                        type: string
                      message:
                        type: string
  scope: Namespaced
  names:
    plural: artifactdeployments
    singular: artifactdeployment
    kind: ArtifactDeployment
    shortNames:
      - artdep
```

---

### 2.2 Controller Architecture (`cmd/controller`)

```text
  Kubernetes API Server
            |
            v  (Watch ArtifactDeployment & Nodes)
  +---------------------------------------------------+
  |          Artifact Operator Controller             |
  |                                                   |
  |  1. Reconcile CRD Spec                            |
  |  2. Resolve Node Placement (nodeSelector)         |
  |  3. Dispatch desired state to target spiderd      |
  |  4. Poll status & update CRD Status subresource   |
  +---------------------------------------------------+
            |
            v  (gRPC Control Calls)
  +---------------------------------------------------+
  |               spiderd DaemonSet                   |
  |  [Node 1]          [Node 2]          [Node 3]     |
  +---------------------------------------------------+
```

- **Reconciliation Rules**:
  - Controller identifies nodes matching `.spec.placement.nodeSelector`.
  - For each target node, controller issues a `SyncArtifact` command to the node's local `spiderd` daemon.
  - Controller tracks sync progress and updates `.status.nodesReady` until `minReadyPercent` threshold is met.

---

### 2.3 `spiderd` DaemonSet Deployment (`deploy/kubernetes/daemonset.yaml`)

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: spiderd
  namespace: artifact-system
spec:
  selector:
    matchLabels:
      app: spiderd
  template:
    metadata:
      labels:
        app: spiderd
    spec:
      containers:
        - name: spiderd
          image: spider/spiderd:latest
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: NODE_ZONE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.labels['topology.kubernetes.io/zone']
            - name: TRACKER_SERVICE
              value: "tracker.artifact-system.svc.cluster.local:50051"
          volumeMounts:
            - mountPath: /var/lib/spider
              name: storage-volume
      volumes:
        - name: storage-volume
          hostPath:
            path: /var/lib/spider
            type: DirectoryOrCreate
```

---

## 3. Implementation Checklist

- [ ] Define Go struct types for `ArtifactDeployment` CRD (`api/v1alpha1/types.go`).
- [ ] Generate Kubernetes deepcopy methods (`controller-gen object`).
- [ ] Implement `cmd/controller` using `sigs.k8s.io/controller-runtime`.
- [ ] Write reconciliation loop targeting matching node daemons.
- [ ] Map Kubernetes node topology labels (`topology.kubernetes.io/zone`, `rack`) into daemon registration.
- [ ] Write CRD status updater updating `nodesReady`, `bytesAvailable`, and `Ready` conditions.
- [ ] Provide Helm chart (`deploy/helm/artifact-mesh/`) packaging Tracker, Controller, and `spiderd` DaemonSet.
- [ ] Add local KinD / Minikube integration test script.
