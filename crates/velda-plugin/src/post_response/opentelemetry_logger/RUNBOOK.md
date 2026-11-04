# OpenTelemetry Logger Plugin (`opentelemetry_logger`) — Operator & Configuration Runbook

This runbook provides complete operational guidance, configuration reference, production tuning recipes, sizing formulas, and alerting rules for the **`opentelemetry_logger`** post-response plugin in Velda Edge.

---

## 1. Configuration Reference (`plugins.json`)

The `opentelemetry_logger` plugin is declared under the `plugins` array in route configurations:

```json
{
  "plugins": [
    {
      "id": "otel_logger_prod",
      "plugin": "opentelemetry_logger",
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
          "x-api-key": "prod-logging-token",
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
| `batch_size` | `integer` | **Yes** | `1..=4096` | `256` | Maximum number of records drained per batch before dispatching to the collector. |
| `flush_interval_ms` | `integer` | **Yes** | `10..=60000` | `100` | Maximum wait duration (in milliseconds) before flushing an incomplete batch. |
| `buffer_capacity` | `integer` | **Yes** | `128..=262144` | `4096` | Number of 256-byte slots allocated per worker CPU core lane. |
| `connect_timeout_ms` | `integer` | **Yes** | `100..=60000` | `5000` | TCP and TLS connection establishment timeout in milliseconds. |
| `request_timeout_ms` | `integer` | **Yes** | `100..=60000` | `10000` | Round-trip timeout for OTLP export requests in milliseconds. |
| `headers` | `map` | **Yes** | Key-value pairs (`{}` if empty) | Auth tokens | Custom HTTP/gRPC headers sent with every export request (e.g. Ingestion tokens). |
| `sampling` | `map` | **Yes** | Key: exact status code or `"default"`, Value: `0.0..=1.0` | Production map | Status-aware sampling table. Wildcard patterns (`"2xx"`) are strictly rejected. |

---

## 2. Production Deployment Topologies

### Topology A: Local DaemonSet / Sidecar Collector (Recommended)
Deploying the OpenTelemetry Collector as a Kubernetes DaemonSet or Pod sidecar on the same physical host:
- **Low Latency**: Network round-trip to localhost (`grpc://127.0.0.1:4317`) is $< 0.1\ \text{ms}$.
- **Zero TLS Overhead**: Traffic is contained within the Linux network namespace, eliminating TLS handshake CPU costs on the collector link.
- **Resilience**: The local collector buffers records on local disk or in memory if upstream cloud sinks (Datadog, Loki) experience transient outages.

### Topology B: Remote Clustered Collector Pool
When exporting directly across the internal network to a dedicated Collector pool:
- Use `protocol: "grpc"` to minimize wire bandwidth.
- Ensure `endpoint` resolves via internal Layer 4 load balancer (e.g. AWS NLB, Azure Load Balancer).
- Set `connect_timeout_ms: 3000` and `request_timeout_ms: 5000` to prevent socket accumulation during collector rolling updates.

---

## 3. Sizing & Memory Capacity Calculator

The RAM footprint of `opentelemetry_logger` is deterministic and allocated upfront during gateway bootstrap:

$$\text{RAM Footprint (Bytes)} = N_{\text{workers}} \times \text{buffer\_capacity} \times 256\ \text{bytes}$$

### Capacity Planning Reference

| CPU Worker Cores | `buffer_capacity` | RAM per Core | Total Gateway RAM Reserved | Max In-Flight Logs |
| :---: | :---: | :---: | :---: | :---: |
| 2 Cores | 1,024 slots | 256 KB | **512 KB** | 2,048 logs |
| 4 Cores | 4,096 slots | 1 MB | **4 MB** | 16,384 logs |
| 8 Cores | 4,096 slots | 1 MB | **8 MB** | 32,768 logs |
| 16 Cores | 8,192 slots | 2 MB | **32 MB** | 131,072 logs |

> [!NOTE]
> Even on a 16-core edge gateway under intense traffic, reserving 8,192 slots per core consumes only **32 MB of RAM**, guaranteeing total immunity against Out-Of-Memory (OOM) events.

---

## 4. Prometheus Alerting Rules

Deploy the following alerting rules to Prometheus / VictoriaMetrics:

```yaml
groups:
  - name: velda_opentelemetry_logger_alerts
    rules:
      - alert: VeldaOtelLoggerRingBufferSaturated
        expr: rate(opentelemetry_logger_dropped_total[2m]) > 0
        for: 1m
        labels:
          severity: warning
          subsystem: telemetry
        annotations:
          summary: "Velda Edge OTLP logger is dropping log records"
          description: "Gateway instance {{ $labels.instance }} has dropped {{ $value }} logs/s over the last 2 minutes. The OTel Collector may be overloaded or down."

      - alert: VeldaOtelCollectorDown
        expr: rate(opentelemetry_logger_flush_errors_total[2m]) > 5
        for: 2m
        labels:
          severity: critical
          subsystem: telemetry
        annotations:
          summary: "Velda Edge cannot connect to OpenTelemetry Collector"
          description: "OTLP flush requests from instance {{ $labels.instance }} to {{ $labels.endpoint }} are continuously failing."
```

---

## 5. Troubleshooting Playbook

### Incident 1: `dropped_count` Is Incrementing
- **Symptoms**: `opentelemetry_logger_dropped_total` is increasing; some log records are missing in Grafana Loki / Jaeger.
- **Root Cause**: The ring buffer is full because the collector is processing batches slower than traffic arrival, or network bandwidth is saturated.
- **Diagnostic Steps**:
  1. Check OTel Collector health: `curl -s http://<collector>:13133/` (Collector health check port).
  2. Inspect collector CPU and memory usage.
  3. Verify whether `protocol` is set to `"http"`. If yes, change to `"grpc"` to cut payload size by 76%.
  4. Increase `buffer_capacity` from `4096` to `8192` or `16384`.
  5. Refine `sampling` table to lower sample rates for high-volume 200 OK routes.

### Incident 2: Collector Connection Refused / Timeouts
- **Symptoms**: Error logs in gateway logs: `Failed to flush OTLP logs batch to collector`.
- **Diagnostic Steps**:
  1. Verify DNS resolution: `nslookup <collector_hostname>`.
  2. Verify port reachability: `nc -zv <collector_ip> 4317` (gRPC) or `4318` (HTTP).
  3. Check if TLS is enabled: If endpoint uses `grpcs://` or `https://`, ensure trusted CA certificates are installed on the gateway host.
