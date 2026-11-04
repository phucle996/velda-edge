# Standard Stream Access Log Plugin (`std_http_access_log`) — Operator & Configuration Runbook

This runbook provides complete operational guidance, configuration reference, production recipes, and log shipping architectures for the **`std_http_access_log`** post-response plugin in Velda Edge.

---

## 1. Configuration Reference (`plugins.json`)

The `std_http_access_log` plugin is declared under the `plugins` array in `plugins.json`:

```json
{
  "plugins": [
    {
      "id": "std_http_access_logger",
      "plugin": "std_http_access_log",
      "config": {
        "target": "stdout",
        "format": "json",
        "include_user_agent": true,
        "include_request_id": true,
        "include_route_id": true,
        "include_upstream": true,
        "include_latency": true,
        "buffer_capacity": 4096,
        "sampling": {
          "200": 0.05,
          "201": 0.10,
          "204": 0.05,
          "400": 0.50,
          "401": 1.00,
          "403": 1.00,
          "404": 0.50,
          "429": 1.00,
          "500": 1.00,
          "502": 1.00,
          "503": 1.00,
          "504": 1.00
        }
      }
    }
  ]
}
```

### Parameter Specification

| Field | Type | Required | Constraints | Recommended | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `target` | `string` | **Yes** | `"stdout"` or `"stderr"` | `"stdout"` | Target file descriptor (`fd 1` or `fd 2`). Direct file paths (e.g. `/var/log/...`) are **strictly rejected**. |
| `format` | `string` | **Yes** | `"json"`, `"text"`, `"clf"` | `"json"` | Serialization format written to the standard output stream. |
| `include_user_agent` | `boolean` | **Yes** | `true` or `false` | `true` | Captures client `user-agent` header (up to 192 bytes inline without heap allocation). |
| `include_request_id` | `boolean` | **Yes** | `true` or `false` | `true` | Captures canonical request ID (`x-request-id`, up to 16 bytes inline). |
| `include_route_id` | `boolean` | **Yes** | `true` or `false` | `true` | Captures matched route identifier prefix (up to 8 bytes inline). |
| `include_upstream` | `boolean` | **Yes** | `true` or `false` | `true` | Captures selected physical backend address (`10.0.1.5:8080`, up to 16 bytes inline). |
| `include_latency` | `boolean` | **Yes** | `true` or `false` | `true` | Captures end-to-end request latency in microseconds (`latency_us`). |
| `buffer_capacity` | `integer` | **Yes** | `128..=65536` | `4096` | Number of 256-byte slots per worker CPU core lane. Automatically rounded to next power of 2. |
| `sampling` | `map` | **Yes** | Exact 3-digit numeric status keys (`100..=599`), values `0.0..=1.0` | Exact map | Status-aware sampling map. Wildcard strings (`"2xx"`) and implicit defaults are strictly forbidden. |

---

## 2. Status-Aware Sampling Contract

### 2.1 The Exact 3-Digit Invariant
Velda Edge strictly enforces deterministic configuration:
- Status keys **MUST** be exact 3-digit numeric HTTP status codes (`"200"`, `"404"`, `"502"`).
- Wildcard syntax (`"2xx"`, `"4xx"`, `"5xx"`, `"*"`) is **forbidden** and will cause configuration synchronization to fail validation with an actionable error.
- There is **no implicit `default_rate`**: any status code not declared in the `sampling` map receives an absolute sampling rate of `0.0` (0%).

### 2.2 Micro-Architectural Rationale
- The entire sampling table (`[u16; 600]`) occupies only **1,200 bytes** ($< 1.2\ \text{KB}$), fitting permanently into the L1 Data Cache of the CPU core.
- If a response code has rate `0.0`, the frontline worker executes a single branch `threshold == 0` and escapes in **$< 1.5\ \text{ns}$** without generating random numbers, taking locks, or touching ring buffers.

### 2.3 Recommended Sampling Profiles

#### High-Traffic Public API (> 50,000 req/s)
Log successes selectively while retaining 100% of errors and rate limits:
```json
"sampling": {
  "200": 0.01,
  "201": 0.05,
  "204": 0.01,
  "301": 0.10,
  "302": 0.10,
  "400": 0.20,
  "401": 1.00,
  "403": 1.00,
  "404": 0.10,
  "429": 1.00,
  "500": 1.00,
  "502": 1.00,
  "503": 1.00,
  "504": 1.00
}
```

#### Payment & Critical Security Gateway
100% full audit trail across all returned status codes:
```json
"sampling": {
  "200": 1.00,
  "201": 1.00,
  "400": 1.00,
  "401": 1.00,
  "403": 1.00,
  "422": 1.00,
  "429": 1.00,
  "500": 1.00,
  "502": 1.00,
  "503": 1.00
}
```

---

## 3. Output Formats

### 3.1 JSON Format (`"format": "json"`)
Standard structured JSON, emitted as a single newline-delimited record:
```json
{"timestamp_ms":1712345678901,"status":200,"method":"GET","version":"HTTP/2.0","latency_us":4520,"bytes":65535,"request_id":"uuid-abc-123","route_id":"rt-auth","upstream":"10.0.0.1:80","user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0"}
```

### 3.2 Common Log Format / Combined (`"format": "clf"`)
Standard Nginx / Apache Combined log format:
```text
10.0.0.1 - - [1712345678901] "GET / HTTP/2.0" 200 65535 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0"
```

### 3.3 Text Key-Value Format (`"format": "text"`)
Human-readable structured key-value format for CLI debugging:
```text
[1712345678901] status=200 method=GET proto=HTTP/2.0 latency_us=4520 bytes=65535 req_id=uuid-abc-123 route=rt-auth upstream=10.0.0.1:80 ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0"
```

---

## 4. Route Attachment (`routes.json`)

To enable access logging for a route, add the plugin instance ID to the route's `plugins` list:

```json
{
  "routes": [
    {
      "id": "route_api_v1",
      "match": {
        "path": {
          "prefix": "/v1/"
        }
      },
      "plugins": [
        "waf_guard",
        "std_http_access_logger"
      ],
      "upstream": "upstream_backend"
    }
  ]
}
```

---

## 5. Production Operations & Log Shipping

### 5.1 Why Direct File Logging is Forbidden
Velda Edge strictly forbids writing directly to log files (`"target": "file"`):
- File writes induce kernel page cache flush stalls, synchronous mutex contention, and blocking disk I/O on the proxy process.
- File rotation (`logrotate`, `SIGHUP`) creates file-descriptor race conditions and truncated buffers.
- In containerized environments (Kubernetes / Docker), writing to standard streams (`stdout`/`stderr`) is the Twelve-Factor App standard.

### 5.2 Kubernetes / Container Log Shipping Architecture
All access logs emitted to `stdout` are captured by the container runtime (CRI-O / containerd) and streamed to node-level log agents:

```
[ Frontline Worker ] ──(Non-Temporal VMOVNTDQ)──► [ RAM Ring Buffer ]
                                                         │
                                               [ Shadow Worker ]
                                                         │ (Single libc::write)
                                                         ▼
                                                    [ stdout (fd 1) ]
                                                         │
                                              [ Container Runtime ]
                                                         │
                                        ┌────────────────┴────────────────┐
                                        ▼                                 ▼
                                 [ Vector Agent ]                 [ FluentBit / Promtail ]
                                        │                                 │
                                        ▼                                 ▼
                               [ ClickHouse / S3 ]              [ Grafana Loki / ELK ]
```

### 5.3 Vector Agent DaemonSet Pipeline
Sample `vector.yaml` configuration to parse Velda JSON access logs from container stdout:

```yaml
sources:
  velda_stdout:
    type: kubernetes_logs
    include_units: ["velda-edge"]

transforms:
  parse_access_logs:
    type: remap
    inputs: ["velda_stdout"]
    source: |
      . = parse_json!(.message)
      .service = "velda-edge"

sinks:
  clickhouse_sink:
    type: clickhouse
    inputs: ["parse_access_logs"]
    endpoint: "http://clickhouse.logging.svc:8123"
    database: "edge_logs"
    table: "access_logs"
```

---

## 6. Sizing & Memory Footprint

The memory footprint of `std_http_access_log` is deterministic and bounded at startup:

$$\text{Total Memory} = \text{Worker Core Count} \times \text{Buffer Capacity} \times 256\ \text{bytes}$$

| Worker Cores | `buffer_capacity` | Total RAM Allocated | Peak Drain Throughput |
| :--- | :--- | :--- | :--- |
| 4 cores | 2,048 slots | $2\ \text{MB}$ | 200,000 logs/sec |
| 8 cores | 4,096 slots | $8\ \text{MB}$ | 500,000 logs/sec |
| 16 cores | 4,096 slots | $16\ \text{MB}$ | 1,000,000 logs/sec |
| 32 cores | 8,192 slots | $64\ \text{MB}$ | 2,500,000 logs/sec |

> [!TIP]
> Memory is allocated as page-aligned 4KB chunks (`libc::MADV_SEQUENTIAL`) directly in physical RAM and bypassed from L1/L2/L3 cache, so increasing `buffer_capacity` has **zero impact** on CPU cache performance.

---

## 7. Metrics & Diagnostics

The plugin exposes atomic telemetry counters for operational health:

| Metric | Type | Description | Alert Condition |
| :--- | :--- | :--- | :--- |
| `std_http_access_log.logged_count` | Counter | Total log records successfully pushed to the ring buffer. | Stagnant under active downstream traffic |
| `std_http_access_log.dropped_count` | Counter | Total log records dropped due to saturated ring buffer (fail-open). | Rate > 0.01% of total traffic |

### Troubleshooting Guide: Dropped Logs Detected (`dropped_count > 0`)
1. **Cause**: Downstream stdout consumer or container runtime is blocking on terminal I/O or node disk pressure.
2. **Mitigation**:
   - Double `buffer_capacity` in `plugins.json` (e.g. from `4096` to `8192`) to absorb higher burst volumes.
   - Reduce sampling rate for high-volume status 200 traffic (`"200": 0.01`).
   - Check container node I/O wait (`iostat -xz 1`) to ensure log shipping daemon (Vector/FluentBit) is keeping up.
