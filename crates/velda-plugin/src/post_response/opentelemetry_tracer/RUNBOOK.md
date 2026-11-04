# OpenTelemetry Tracer Plugin (`opentelemetry_tracer`) — Operator & Configuration Runbook

This runbook provides complete operational guidance, configuration reference, production tuning recipes, memory sizing formulas, and alerting rules for the **`opentelemetry_tracer`** post-response plugin in Velda Edge.

---

## 1. Configuration Reference (`plugins.json`)

The `opentelemetry_tracer` plugin is declared under the `plugins` array in route configurations:

```json
{
  "plugins": [
    {
      "id": "otel_tracer_prod",
      "plugin": "opentelemetry_tracer",
      "config": {
        "protocol": "grpc",
        "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
        "service_name": "velda-edge-prod",
        "batch_size": 256,
        "flush_interval_ms": 100,
        "buffer_capacity": 4096,
        "connect_timeout_ms": 5000,
        "request_timeout_ms": 10000,
        "headers": {
          "x-api-key": "prod-tracing-token",
          "x-environment": "production"
        },
        "sampling": {
          "default": 1.0,
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
| :--- | :--- | :---: | :--- | :--- | :--- |
| `protocol` | `string` | **Yes** | `"grpc"` or `"http"` | `"grpc"` | Exporter transport protocol. `"grpc"` uses HTTP/2 binary Protobuf (port 4317); `"http"` uses HTTP/1.1 JSON (port 4318). |
| `endpoint` | `string` | **Yes** | Valid URI (`grpc://`, `grpcs://`, `http://`, `https://`) | `grpc://...:4317` | Target OpenTelemetry Collector ingestion endpoint. |
| `service_name` | `string` | **Yes** | Alphanumeric string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge"` | Service name populated in OpenTelemetry Resource attributes (`service.name`). |
| `batch_size` | `integer` | **Yes** | `1..=4096` | `256` | Maximum number of spans drained per batch before dispatching to the collector. |
| `flush_interval_ms` | `integer` | **Yes** | `10..=60000` | `100` | Maximum wait duration (in milliseconds) before flushing an incomplete batch. |
| `buffer_capacity` | `integer` | **Yes** | `128..=262144` | `4096` | Number of 256-byte slots allocated per worker CPU core lane in physical RAM. |
| `connect_timeout_ms` | `integer` | **Yes** | `100..=60000` | `5000` | Connection establishment timeout in milliseconds. |
| `request_timeout_ms` | `integer` | **Yes** | `100..=60000` | `10000` | Export request completion timeout in milliseconds. |
| `headers` | `object` | **Yes** | Map of strings | `{}` | Custom HTTP/gRPC metadata headers attached to export requests. |
| `sampling` | `object` | Optional | Map of 3-digit status codes to floats (`0.0..=1.0`) | See recipes | Status-aware direct index sampling rates. |

---

## 2. Memory Sizing & Capacity Planning Formulas

### RAM Slot Buffer Footprint Formula

The physical DRAM allocated across all worker cores for trace spans is calculated as:

$$\text{RAM}_{\text{tracer}} = N_{\text{workers}} \times \text{buffer\_capacity} \times 256\ \text{bytes}$$

#### Example Configurations:

| Worker Cores ($N_{\text{workers}}$) | Slot Capacity per Lane | Memory Footprint (RAM) | Absorption at 75,000 req/s (10% sampling) |
| :---: | :---: | :---: | :---: |
| 4 | 2,048 | **2.0 MB** | ~1.1 seconds collector latency spike |
| 8 | 4,096 | **8.0 MB** | ~4.3 seconds collector latency spike |
| 16 | 4,096 | **16.0 MB** | ~8.7 seconds collector latency spike |
| 32 | 8,192 | **64.0 MB** | ~17.4 seconds collector latency spike |

*Note*: Because all slots are pre-allocated and page-aligned (4KB) at cold start with `MADV_SEQUENTIAL`, zero page faults occur during normal runtime.

---

## 3. Production Deployment Recipes

### Recipe 1: Production High-Load Edge Gateway (OTLP/gRPC + Pareto Sampling)
*Objective: Maximum throughput, minimal network egress, 100% error visibility.*

```json
{
  "protocol": "grpc",
  "endpoint": "grpc://otel-collector.internal.net:4317",
  "service_name": "velda-api-edge",
  "batch_size": 256,
  "flush_interval_ms": 100,
  "buffer_capacity": 8192,
  "connect_timeout_ms": 3000,
  "request_timeout_ms": 5000,
  "headers": {
    "x-tenant-id": "production"
  },
  "sampling": {
    "default": 1.0,
    "200": 0.01,
    "204": 0.01,
    "400": 0.25,
    "500": 1.00,
    "502": 1.00,
    "503": 1.00,
    "504": 1.00
  }
}
```

### Recipe 2: Debug & Staging Environment (OTLP/HTTP JSON + Full Trace Sampling)
*Objective: 100% trace capture for end-to-end debugging on Jaeger UI.*

```json
{
  "protocol": "http",
  "endpoint": "http://127.0.0.1:4318/v1/traces",
  "service_name": "velda-staging-edge",
  "batch_size": 32,
  "flush_interval_ms": 50,
  "buffer_capacity": 2048,
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 10000,
  "headers": {},
  "sampling": {
    "default": 1.0
  }
}
```

---

## 4. Observability & Prometheus Metrics

The plugin exports real-time atomic operational metrics to the edge monitoring subsystem:

| Metric Name | Type | Description | Alerting Rule |
| :--- | :---: | :--- | :--- |
| `velda_plugin_otel_tracer_logged_total` | Counter | Cumulative number of spans successfully queued to RAM. | Rate must correlate with sampled request traffic. |
| `velda_plugin_otel_tracer_dropped_total` | Counter | Cumulative number of spans dropped due to ring buffer saturation. | `rate(...) > 0` triggers Warning alert. |
| `velda_plugin_otel_tracer_buffer_utilization` | Gauge | Instantaneous fill ratio of SPSC ring buffer slots ($0.0 - 1.0$). | `utilization > 0.85` triggers High Queue Pressure alert. |

### Prometheus Alerting Rules Example (`prometheus.rules.yml`)

```yaml
groups:
  - name: velda-otel-tracer-alerts
    rules:
      - alert: VeldaOtelTracerHighDropRate
        expr: rate(velda_plugin_otel_tracer_dropped_total[1m]) > 5
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Velda Edge OTel Tracer is dropping spans"
          description: "OTel Tracer on {{ $labels.instance }} is dropping {{ $value }} spans/s due to downstream collector backpressure."

      - alert: VeldaOtelTracerCollectorUnreachable
        expr: rate(velda_plugin_otel_tracer_logged_total[5m]) > 0 and rate(otelcol_receiver_accepted_spans[5m]) == 0
        for: 3m
        labels:
          severity: critical
        annotations:
          summary: "OTel Collector is not receiving spans from Velda Edge"
          description: "Spans are queued in Edge RAM but zero spans are acknowledged by the collector."
```

---

## 5. Operational Troubleshooting & Runbook Triage

### Triage 1: High `dropped_count` (Spans Dropping on Hot Path)
* **Symptom**: `velda_plugin_otel_tracer_dropped_total` is increasing steadily.
* **Root Cause**: The SPSC ring buffer is saturated because the Shadow Worker cannot flush batches to the collector fast enough.
* **Resolution Steps**:
  1. Check network connectivity between Edge nodes and the OTel Collector endpoint.
  2. Inspect OTel Collector CPU and memory metrics (Collector may be throttling or undergoing garbage collection).
  3. Increase `batch_size` from `64` to `256` or `512` to increase network transmission efficiency.
  4. Temporarily decrease high-volume status code sampling rates (e.g. reduce `"200"` from `0.05` to `0.01`).

### Triage 2: Spans Not Appearing in Jaeger / Grafana Tempo
* **Symptom**: Edge reports `logged_count` incrementing, but no spans appear on the visualization UI.
* **Resolution Steps**:
  1. Verify the collector endpoint port:
     - If `protocol = "grpc"`, target port must be `4317` (gRPC), NOT `4318`.
     - If `protocol = "http"`, target URI must end in `/v1/traces` on port `4318`.
  2. Inspect Edge access logs for collector connection refused (`ECONNREFUSED`) or TLS handshake failures.
  3. Verify `service_name` filter in Jaeger search bar.

### Triage 3: Broken Trace Trees (Orphan Gateway Spans)
* **Symptom**: Gateway spans appear as root spans instead of child spans of client requests.
* **Resolution Steps**:
  1. Verify that `w3c_trace_context` is declared **before** `opentelemetry_tracer` in the route's `plugins` array.
  2. Confirm downstream clients send valid standard W3C `traceparent` headers (`00-{trace_id}-{span_id}-{flags}`).
  3. Verify that `x-parent-span-id` is present in `HeaderWorkspace` during post-response processing.
