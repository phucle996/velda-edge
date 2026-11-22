# Health & Readiness Probe Plugin (`health_probe`)

```text
+-----------------------------------------------------------------------------------+
|                        Health & Readiness Probe Architecture                      |
|                                                                                   |
|  [Cluster Orchestrator] (Kubernetes Kubelet / AWS ALB / GCP LB / Consul)          |
|         │                                                                         |
|         │ HTTP/1.1 Probes (GET /livez, GET /readyz)                               |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Management Port 9091 (Strictly Disjoint from Traffic Listeners 80/443)     │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Direct Socket Handoff (Zero-mpsc)       |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ velda-shadow-{i} (SCHED_IDLE, Assigned Core i Companion Worker)            │  |
|  │  - Sequential Inline Accept Loop (Zero tokio::spawn, Zero Heap Allocations)  │  |
|  │  - Bounded Execution Timeout (Anti-Slowloris timeout_ms guard)              │  |
|  │  - Stack Buffer (512B) + Register SWAR Path Matching (eval_swar_64/32x2)    │  |
|  │  - Branchless ALU Index Selection: index = 2 - probe_match - probe_ok       │  |
|  │  - Contiguous L1D Cache Wire Responses (195B, #[repr(C, align(64))])        │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Atomic State Query (Acquire / Release)  |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ HealthState (AtomicBool Flags: is_live, is_ready, is_draining)              │  |
|  │  - SIGTERM received -> is_draining=true -> /readyz immediately returns 503  │  |
|  │  - Orchestrator prunes pod endpoints before active connections drain        │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
+-----------------------------------------------------------------------------------+
```

The **`health_probe`** plugin is Velda Edge's dedicated container health and readiness daemon. It runs an out-of-band, zero-allocation HTTP management server on a dedicated port (e.g., `0.0.0.0:9091`) to serve orchestrator liveness and readiness checks (`/livez`, `/readyz`) completely isolated from the Data Plane request serving path.

---

## 1. What This Plugin Does

1. **Orchestrator Lifecycle Integration**:
   - Exposes `/livez` to verify event-loop responsiveness and process vitality (failing probes trigger pod restarts).
   - Exposes `/readyz` to signal readiness for client traffic (failing or draining probes cause orchestrators to prune the instance from routing pools).
2. **Dedicated Management Port Isolation**:
   - Operates on an isolated port (`listen`), strictly disjoint from customer traffic ports (e.g., ports 80, 443, 8080). Even under extreme traffic spikes or L7 DDoS saturation on traffic ports, orchestrator probes receive immediate responses without false-positive container evictions.
3. **Sequential Inline Execution (Zero `tokio::spawn`)**:
   - Eliminates per-probe task allocation and green-thread queue overhead. A single idle-scheduled worker processes incoming probe connections sequentially using a 512-byte stack buffer and static wire responses, eliminating heap allocations and task scheduling contention on the serving path.
4. **Adaptive Register SWAR & Branchless ALU Evaluation**:
   - Inspects the request path using SIMD Within A Register (SWAR) across 64-bit (`u64`) or dual 32-bit (`u32x2`) registers.
   - Computes target response indices through branchless arithmetic (`index = 2 - probe_match - probe_ok`), avoiding branch misprediction penalties.
5. **Contiguous L1D Cache-Resident Wire Responses**:
   - Packs all pre-compiled HTTP/1.1 response frames (`200 OK`, `503 Service Unavailable`, `404 Not Found`) into a single 64-byte aligned 195-byte contiguous block (`WIRE_RESPONSES`), eliminating dynamic string formatting and memory allocation during socket streaming.
6. **Graceful Draining Coordination**:
   - Upon receiving a shutdown signal (SIGTERM/SIGINT), atomically sets `is_draining = true`. The `/readyz` endpoint immediately responds with `503 Service Unavailable`, prompting the cluster orchestrator to prune the instance from upstream endpoints *before* active TCP/H2/QUIC streams begin terminating.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/background/health_probe/`
- **Execution Stage**: `BackgroundPlugin` (compile-time fixed)
- **Runtime Placement**: Operates entirely on the Shadow Logistics Worker (`velda-shadow-{i}`) running under `SCHED_IDLE` on its assigned core $i$ (recommended: Core 0).
- **Data Plane Touchpoints**:
  - Frontline Traffic Workers (`velda-worker-{0..N-1}`) **never** interact with the probe listener, never route health checks, and never send messages over `mpsc` channels to this plugin.
  - Sockets are pre-reserved through `velda-core::socket::GlobalSocketManager` using `SocketRole::Shadow` to guarantee anti-collision invariants against ingress listeners.

---

## 3. Requirements & Prerequisites

1. **Disjoint Port Allocation**: The configured `listen` address (e.g., `0.0.0.0:9091`) must not overlap with any public or internal traffic listeners declared in `velda-ingress`.
2. **Explicit Configuration Contract**: Every configuration parameter must be intentionally declared in `plugins.json`. No implicit or silent fallback values exist.
3. **Container Network Exposure**: When deploying inside containerized environments (Kubernetes, Docker, ECS), the probe port (e.g., 9091) must be exposed in the container definition (`containerPort: 9091`) and mapped to the container network namespace.

---

## 4. Quick Start Configuration

Declare the plugin in your `plugins.json`:

```json
{
  "id": "health_probe",
  "type": "health_probe",
  "enabled": true,
  "config": {
    "listen": "0.0.0.0:9091",
    "liveness_path": "/livez",
    "readiness_path": "/readyz",
    "check_interval_ms": 1000,
    "timeout_ms": 500
  }
}
```

### Kubernetes Pod Definition Example

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: velda-edge
spec:
  containers:
  - name: velda-edge
    image: velda-edge:latest
    ports:
    - name: http
      containerPort: 80
    - name: https
      containerPort: 443
    - name: health
      containerPort: 9091
    livenessProbe:
      httpGet:
        path: /livez
        port: 9091
      initialDelaySeconds: 2
      periodSeconds: 5
      timeoutSeconds: 1
      failureThreshold: 3
    readinessProbe:
      httpGet:
        path: /readyz
        port: 9091
      initialDelaySeconds: 1
      periodSeconds: 2
      timeoutSeconds: 1
      failureThreshold: 2
```

---

## 5. Documentation Directory

For deeper technical and operational information, refer to the accompanying specifications:

- **[RUNBOOK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/health_probe/RUNBOOK.md)**: Field-by-field parameter explanations, tuned operational presets (Kubernetes Production, Bare-Metal WAN Edge, Graceful Draining), emergency incident triage matrix (P0–P3), CLI diagnostic toolkit, and troubleshooting playbooks.
- **[ARCHITECTURE.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/health_probe/ARCHITECTURE.md)**: Deep micro-architectural specification, memory geometry (195B L1D contiguous layout), SWAR assembly extraction (`eval_swar_64`), branchless ALU mechanics, and physical hardware boundaries without dogmatic commitments.
- **[BENCHMARK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/health_probe/BENCHMARK.md)**: Standardized zero-bloat 5-tier benchmark specification across 6 operational scenarios (`KB 01` to `KB 06`), comparing against Envoy Proxy, Nginx, and Kong Gateway.
