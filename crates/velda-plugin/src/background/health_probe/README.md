# Health & Readiness Probe Plugin (`health_probe`)

Auxiliary background service exposing ultra-lightweight cloud-native health and readiness endpoints (`/healthz`, `/readyz`) on an isolated auxiliary port for Kubernetes Kubelet, Docker Swarm, and external load balancers.

---

## 1. Overview & Service Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `health_probe` |
| **Canonical Phase** | `background` |
| **Execution Runtime** | Isolated auxiliary thread with `SCHED_IDLE` / `nice(19)` cooperative scheduling |
| **Default Socket** | `0.0.0.0:9091` (Auxiliary Port) |
| **Hot-Path Overhead** | **Zero**. Runs completely out-of-band; zero overhead on traffic workers |
| **Response Payload** | **Zero-Byte Body (`Content-Length: 0`)**. Static byte constants in `.rodata`; zero heap allocation |
| **Memory Access** | Lock-free atomic states (`AtomicBool`); 1–2 nanoseconds read overhead |

---

## 2. Auxiliary Port Isolation Invariant

> [!IMPORTANT]
> **Auxiliary Port Isolation Invariant**: The health probe socket (`listen_addr`) runs as an auxiliary background service and **MUST NEVER** collide with any Data Plane traffic listener ports (e.g., ports `80`, `443`, `8080`, `8443`, `50051`).
> 
> The configuration compiler validates that the set of auxiliary ports and traffic listener ports are strictly disjoint:
> $$\text{ListenerPorts} \cap \text{AuxiliaryPorts} = \emptyset$$

Running health checks on an auxiliary port prevents cascading outages: even under extreme traffic load or DDoS saturation on traffic ports, Kubelet health probes are answered immediately in microseconds without timeout or false-positive container restarts.

---

## 3. Cloud-Native Health Semantics (Zero Payload)

Kubernetes Kubelet (`httpGet probe`) and Cloud Load Balancers exclusively check the **HTTP Status Code** ($\ge 200$ and $< 400$ indicates Success; $\ge 400$ indicates Failure). Response bodies are completely discarded by orchestrators.

`health_probe` leverages this by returning **empty bodies** (`Content-Length: 0`) using immutable static byte constants (`&'static [u8]`) embedded in `.rodata`. Zero allocations, zero heap format strings, and minimal packet size (59 bytes).

### 3.1. Liveness Probe (`/healthz`)
- **Purpose**: Verifies the Velda Edge process is running, event loops are healthy, and the application is not deadlocked.
- **Fail Action**: Kubernetes restarts the container.
- **Responses**:
  - `HTTP/1.1 200 OK` (0 bytes payload): Healthy.
  - `HTTP/1.1 503 Service Unavailable` (0 bytes payload): Process deadlocked or failing.

### 3.2. Readiness Probe (`/readyz`) & Graceful Draining
- **Purpose**: Verifies the Data Plane is ready to receive traffic (all ingress listeners bound, configuration snapshot compiled and loaded into RAM).
- **Graceful Draining**: When a termination signal (`SIGTERM`) is received, readiness **immediately flips to `503 Service Unavailable`**. This signals Kubernetes to remove the Pod IP from the service endpoints and ingress load balancers *before* the Data Plane drains active connections.
- **Fail Action**: Kubernetes routes user traffic away from the Pod (does **NOT** restart container).
- **Responses**:
  - `HTTP/1.1 200 OK` (0 bytes payload): Ready to receive user traffic.
  - `HTTP/1.1 503 Service Unavailable` (0 bytes payload): Initializing or draining existing connections.

---

## 4. Configuration Schema

As with all Velda Edge plugins, **never specify `phase` or `priority`** in user configuration. Execution phase is statically inferred as `background`.

```json
{
  "id": "health_probe",
  "config": {
    "listen_addr": "0.0.0.0:9091",
    "liveness_path": "/healthz",
    "readiness_path": "/readyz"
  }
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `listen_addr` | string | `"0.0.0.0:9091"` | Dedicated auxiliary IP:Port socket to listen on. |
| `liveness_path` | string | `"/healthz"` | HTTP URL path for Kubernetes Liveness probes. |
| `readiness_path` | string | `"/readyz"` | HTTP URL path for Kubernetes Readiness probes. |

---

## 5. Kubernetes Pod Specification Example

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
        path: /healthz
        port: 9091
      initialDelaySeconds: 2
      periodSeconds: 5
    readinessProbe:
      httpGet:
        path: /readyz
        port: 9091
      initialDelaySeconds: 1
      periodSeconds: 3
```
