# Prometheus Metrics Exporter Plugin (`prometheus_exporter`)

```text
+-----------------------------------------------------------------------------------+
|                        Prometheus Exporter Architecture                           |
|                                                                                   |
|  [External Scraper] (Prometheus Server / VictoriaMetrics / Grafana Agent)         |
|         │                                                                         |
|         │ HTTP/HTTPS GET /metrics (Port 9090 Management Port)                     |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Management Socket (Strictly Disjoint from Ingress Traffic Listeners)        │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Direct Socket Handoff (Zero-mpsc)       |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ velda-shadow-{i} (SCHED_IDLE, Assigned Core i Companion Worker)            │  |
|  │  - Concurrency Limiter (Semaphore bounds concurrent scrapers)               │  |
|  │  - Hard Timeout Guard (timeout_ms clamps slow scrapers)                     │  |
|  │  - Optional Transport Security (TLS / mTLS + Bearer / Basic Auth + CORS)    │  |
|  │  - Reusable Scratch Buffer Pool (BufferPool eliminates heap churn)          │  |
|  │  - Pre-baked Label Skeletons (template.rs pre-allocates 858 metric lines)   │  |
|  │  - Division-Free Integer Formatting (lut.rs reciprocal multiplication Itoa) │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Lock-Free Point-in-Time Atomic Snapshot |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ velda_observability::MetricsStore                                           │  |
|  │  - 36 Pipeline Slots x Counters / Gauges / Latency Histograms              │  |
|  │  - Frontline Traffic Workers record metrics via lock-free fetch_add         │  |
|  └─────────────────────────────────────────────────────────────────────────────┘  |
+-----------------------------------------------------------------------------------+
```

The **`prometheus_exporter`** plugin is Velda Edge's dedicated telemetry export daemon (`phase: "background"`). It serves OpenMetrics and Prometheus text-format scrape endpoints (`GET /metrics`) on an isolated management port (e.g. `0.0.0.0:9090`), decoupled from the Data Plane request serving path.

---

## 1. What This Plugin Does

1. **Standardized OpenMetrics / Prometheus Telemetry**:
   - Exposes comprehensive L4/L7 gateway telemetry: request counters, active stream gauges, byte throughput, error distribution, connection counts, and histogram latency buckets across all 36 pipeline slot combinations.
2. **Dedicated Management Port Isolation**:
   - Binds strictly to a dedicated management port (`listen`, default `0.0.0.0:9090`), strictly disjoint from customer traffic ports (e.g. 80, 443). Traffic surges or L7 DDoS attacks on ingress listeners cannot saturate or block telemetry scrapers.
3. **Zero Heap Allocation Scrape Processing (Buffer Pooling)**:
   - Reuses pre-allocated scratch buffers (`BufferPool`) across scrape cycles. Buffers are checked out via RAII (`PooledBuffer`), populated with metrics, written to the socket, and cleared back to the pool, eliminating heap churn during steady-state scraping.
4. **Pre-baked Label Skeletons & Division-Free Itoa**:
   - Pre-bakes exact metric name, label combinations, and `# HELP` / `# TYPE` strings into static byte slices (`template.rs`).
   - Converts integer metrics to ASCII using division-free reciprocal multiplication and a 200-byte 2-digit lookup table (`lut.rs`), eliminating costly hardware integer division (`DIV`) instructions.
5. **Lock-Free Point-in-Time Atomics**:
   - Frontline Traffic Workers update metrics via `fetch_add(Ordering::Relaxed)`. Telemetry extraction executes point-in-time atomic loads without acquiring locks or suspending worker threads.
6. **Configurable Enterprise Security**:
   - Supports optional TLS / mutual TLS (mTLS with client CA validation), Bearer Token authentication, HTTP Basic authentication, and CORS headers for direct browser scraping (e.g. Grafana dashboards).

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/background/prometheus_exporter/`
- **Execution Phase**: `phase: "background"`
- **Runtime Placement**: Operates entirely on the Shadow Logistics Worker (`velda-shadow-{i}`) running under `SCHED_IDLE` on its assigned core $i$.
- **Data Plane Touchpoints**:
  - Frontline Traffic Workers (`velda-worker-{0..N-1}`) **never** format metrics, never serialize strings, and never execute scrape socket I/O.
  - Sockets are reserved through `velda-core::socket::GlobalSocketManager` using `SocketRole::Shadow` to guarantee anti-collision invariants against ingress traffic listeners.

---

## 3. Requirements & Prerequisites

1. **Disjoint Port Allocation**: The configured `listen` address (e.g. `0.0.0.0:9090`) must not collide with any ingress traffic listeners.
2. **Explicit Declarative Contract**: Every configuration block (`scrape`, `tls`, `cors`, `auth`) must be explicitly declared in `plugins.json`. Disabled features must be explicitly declared as `"none"`, `false`, `[]`, or `null`.
3. **Scraper Network Access**: Ensure firewalls or Kubernetes NetworkPolicies permit incoming TCP traffic to port 9090 strictly from authorized Prometheus scrapers.

---

## 4. Quick Start Configuration

Declare the plugin in your `plugins.json`:

```json
{
  "id": "prometheus_exporter",
  "type": "prometheus_exporter",
  "enabled": true,
  "config": {
    "listen": "0.0.0.0:9090",
    "path": "/metrics",
    "protocol": "http",
    "scrape": {
      "timeout_ms": 5000,
      "max_concurrency": 16,
      "max_buffer_size_kb": 256
    },
    "tls": {
      "mode": "none",
      "cert_file": null,
      "key_file": null,
      "client_ca_file": null
    },
    "cors": {
      "enabled": false,
      "allow_origins": [],
      "allow_methods": [],
      "allow_headers": []
    },
    "auth": {
      "mode": "none",
      "token": null,
      "username": null,
      "password": null
    }
  }
}
```

### Prometheus Scraper Configuration (`prometheus.yml`)

```yaml
scrape_configs:
  - job_name: 'velda_edge'
    scrape_interval: 10s
    scrape_timeout: 5s
    static_configs:
      - targets: ['127.0.0.1:9090']
    metrics_path: /metrics
```

---

## 5. Documentation Directory

For deeper technical and operational information, refer to the accompanying specifications:

- **[RUNBOOK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/prometheus_exporter/RUNBOOK.md)**: Field-by-field parameter explanations, tuned operational presets (Kubernetes Cluster, High-Security mTLS, High-Throughput Edge), emergency incident triage matrix (P0–P3), CLI diagnostic toolkit, and troubleshooting playbooks.
- **[ARCHITECTURE.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/prometheus_exporter/ARCHITECTURE.md)**: Deep micro-architectural specification, memory geometry (200-byte LUT, pre-baked skeletons), reciprocal division-free assembly proof (`fast_write_u64`), lock-free atomic extraction, and physical hardware boundaries without dogmatic commitments.
- **[BENCHMARK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/background/prometheus_exporter/BENCHMARK.md)**: Standardized zero-bloat 5-tier benchmark specification across 6 operational scenarios (`KB 01` to `KB 06`), comparing against Envoy Proxy, Nginx, and Kong Gateway.
