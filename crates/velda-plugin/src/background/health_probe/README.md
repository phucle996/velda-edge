# Health & Readiness Probe Plugin (`health_probe`)

Auxiliary background service exposing ultra-lightweight cloud-native health and readiness endpoints (`/livez`, `/readyz`) on an isolated auxiliary port for Kubernetes Kubelet, Docker Swarm, and external load balancers.

---

## 1. Overview & Service Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `health_probe` |
| **Canonical Phase** | `background` |
| **Execution Runtime** | Isolated auxiliary thread with `SCHED_IDLE` / `nice(19)` cooperative scheduling |
| **Auxiliary Socket** | `0.0.0.0:9091` (Auxiliary Port) |
| **Hot-Path Overhead** | **Zero**. Runs completely out-of-band; zero overhead on traffic workers |
| **Response Payload** | **Zero-Byte Body (`Content-Length: 0`)**. Static byte constants in `.rodata`; zero heap allocation |
| **Memory Access** | Lock-free atomic states (`AtomicBool`); 1–2 nanoseconds read overhead |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `listen` | String | **Yes** | Valid `IP:PORT` socket address (must not collide with ingress listeners) | `"0.0.0.0:9091"` | Dedicated auxiliary IP:Port socket to bind and serve health checks. |
| `liveness_path` | String | **Yes** | Must start with `/` | `"/livez"` | HTTP URL path for Kubernetes Liveness probes. Returns `200 OK` when healthy, `503` when failed. |
| `readiness_path` | String | **Yes** | Must start with `/` | `"/readyz"` | HTTP URL path for Kubernetes Readiness probes. Returns `200 OK` when ready, `503` when draining. |
| `check_interval_ms` | Integer (u64) | **Yes** | `100 ..= 60000` ms | `1000` | Background health state evaluation interval in milliseconds. |
| `timeout_ms` | Integer (u64) | **Yes** | `100 ..= 30000` ms | `500` | Health check probe evaluation timeout in milliseconds. |

---

## 3. Auxiliary Port Isolation Invariant

> [!CAUTION]
> **Auxiliary Port Isolation Invariant**: The health probe socket (`listen`) runs as an auxiliary background service and **MUST NEVER** collide with any Data Plane traffic listener ports (e.g., ports `80`, `443`, `8080`, `8443`, `50051`).
> 
> The configuration compiler validates that the set of auxiliary ports and traffic listener ports are strictly disjoint:
> $$\text{ListenerPorts} \cap \text{AuxiliaryPorts} = \emptyset$$

Running health checks on an auxiliary port prevents cascading outages: even under extreme traffic load or DDoS saturation on traffic ports, orchestrator health probes are answered immediately in microseconds without timeout or false-positive container restarts.

---

## 4. Configuration Manifest Example (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "health_probe",
      "config": {
        "listen": "0.0.0.0:9091",
        "liveness_path": "/livez",
        "readiness_path": "/readyz",
        "check_interval_ms": 1000,
        "timeout_ms": 500
      }
    }
  ]
}
```

> [!NOTE]
> Background plugins are not attached to individual HTTP/gRPC routes. They run as standalone auxiliary daemons managed by the runtime lifecycle engine.

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
        path: /livez
        port: 9091
      initialDelaySeconds: 2
      periodSeconds: 5
    readinessProbe:
      httpGet:
        path: /readyz
        port: 9091
      initialDelaySeconds: 1
      periodSeconds: 2
```
