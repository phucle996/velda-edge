# OpenTelemetry Metrics Exporter — SRE On-Call Runbook & Incident Triage Guide

This runbook guides Site Reliability Engineers (SREs) and on-call operators in diagnosing, troubleshooting, and resolving production incidents involving the **OpenTelemetry Metrics Exporter Plugin** (`otlp_metrics_exporter`).

---

## 1. Parameter Reference & Operational Impact

| Field | Production Range | Operational Impact | Recommended Default |
| :--- | :--- | :--- | :--- |
| `protocol` | `"grpc"` \| `"http"` | `"grpc"` streams binary Protobuf over HTTP/2, saving CPU and wire bandwidth. `"http"` emits human-readable JSON over HTTP/1.1. | `"grpc"` |
| `endpoint` | Valid URI | Target OTLP Collector address. If unreachable, exporter enters non-blocking exponential backoff without stalling request traffic. | `"grpc://127.0.0.1:4317"` |
| `interval_secs` | `1 ..= 3600` | Telemetry push interval in seconds. Lower values increase freshness but generate higher network load. | `10` |
| `connect_timeout_ms` | `100 ..= 60000` | Connection establishment timeout. Prevents hung TCP/TLS handshakes from blocking the shadow task loop. | `5000` |
| `request_timeout_ms` | `100 ..= 60000` | Export request timeout before aborting and logging a failure. | `10000` |
| `service_name` | ASCII string | Logical service name injected into OpenTelemetry Resource attributes (`service.name`). | `"velda-edge-prod"` |
| `headers` | Key-value pairs | Custom headers sent with every request (e.g. Bearer auth, tenant IDs). | `{}` |

---

## 2. Tuned Production Presets

### Preset A: High-Throughput Kubernetes Production Daemon (gRPC / HTTP/2)
```json
{
  "protocol": "grpc",
  "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
  "interval_secs": 10,
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 10000,
  "service_name": "velda-edge-prod",
  "headers": {}
}
```
*Rationale: In-place Protobuf stamping and persistent HTTP/2 stream multiplexing deliver maximum telemetry throughput with zero heap allocations.*

### Preset B: Staging / Local Debugging REST Telemetry (HTTP/1.1 JSON)
```json
{
  "protocol": "http",
  "endpoint": "http://127.0.0.1:4318/v1/metrics",
  "interval_secs": 5,
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 10000,
  "service_name": "velda-edge-dev",
  "headers": {}
}
```
*Rationale: Pre-baked JSON template and 2-digit LUT allow seamless ingestion into JSON-based monitoring proxies without requiring gRPC tooling.*

### Preset C: Secure Cross-Datacenter WAN Telemetry (gRPC + TLS + Token Auth)
```json
{
  "protocol": "grpc",
  "endpoint": "grpcs://otel.eu-west-1.aws.internal:4317",
  "interval_secs": 15,
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 10000,
  "service_name": "velda-edge-wan",
  "headers": {
    "authorization": "Bearer prod-secret-token-v2"
  }
}
```
*Rationale: 15-second cadence minimizes WAN traffic and TLS renegotiation while keeping central dashboards fully updated.*

---

## 3. SRE Incident Triage Matrix (P0 – P3)

| Severity | Incident Pattern | Symptoms & Indicators | Immediate Mitigation Action |
| :---: | :--- | :--- | :--- |
| **P0** | **Process Panic on Startup / Host Starvation** | Gateway fails boot or memory usage spikes uncontrollably. | Check `endpoint` validity and network connectivity. Activate emergency bypass by removing the plugin from `plugins.json` and pushing hot reload. |
| **P1** | **Telemetry Blackhole (Metrics Missing)** | `failed_count` continuously increments; collector shows 0 incoming data points. | 1. Check if the OpenTelemetry Collector is alive on port 4317/4318.<br>2. Gateway operates in Fail-Open mode: live traffic is **never** impacted.<br>3. Inspect network firewall rules. |
| **P2** | **Collector Rejection / Authentication Failure** | Collector returns HTTP 401/403 or non-zero gRPC status (`UNAUTHENTICATED`, `INVALID_ARGUMENT`). | 1. Update `headers` in `plugins.json` with a valid Bearer token.<br>2. Verify OpenTelemetry Collector version (must be $\ge$ v0.80.0).<br>3. Reload configuration. |
| **P3** | **Dashboard Telemetry Gaps / Staleness** | Metrics graphs on Grafana/Datadog show periodic gaps or delayed points. | 1. Reduce `interval_secs` from 30s/60s to 10s.<br>2. Check Collector queue depth and disk I/O throttling. |

---

## 4. Incident Triage Decision Flowchart

```mermaid
flowchart TD
    ALERT["Alert: OTLP Metrics Export Failing (failed_count increasing)"] --> CHK_COLLECTOR{"Is Collector Endpoint\n(Port 4317 / 4318) Reachable?"}

    CHK_COLLECTOR -->|"No / Connection Refused"| ACT_COLLECTOR["1. Verify OpenTelemetry Collector process\n2. Note: Gateway runs Fail-Open (zero traffic impact)\n3. Restart Collector service"]
    CHK_COLLECTOR -->|"Yes / Port Open"| CHK_STATUS{"What Error or Status\nDoes Collector Return?"}

    CHK_STATUS -->|"HTTP 401/403 or gRPC Unauthenticated"| FIX_AUTH["Update authorization Bearer token in headers\nvia Control Plane reload"]
    CHK_STATUS -->|"HTTP 400 or gRPC InvalidArgument"| FIX_SCHEMA["Verify Collector version (>= v0.80.0)\nCheck service_name string constraints"]
    CHK_STATUS -->|"Timeout / Reset"| CHK_NET{"Is Network Experiencing\nLoss or High Jitter?"}

    CHK_NET -->|"Network Congested"| ACT_INTERVAL["Increase interval_secs to reduce export frequency"]
    CHK_NET -->|"Network Normal"| CHK_THREAD["Inspect CPU of velda-shadow-{i} thread\n(Nominal baseline < 0.8% CPU)"]
```

---

## 5. Diagnostic CLI Toolkit

### Command 1: Inspect Export Logs
Check whether telemetry batches are successfully reaching the collector:
```bash
journalctl -u velda-edge -f -o cat | grep -E "Flushed OTLP metrics|Failed to send OTLP"
```

### Command 2: Inspect Socket Connection State to Collector
Verify that the exporter has established a persistent TCP/TLS connection:
```bash
ss -tinh 'dport = :4317 or dport = :4318'
```

### Command 3: Test Raw Reachability to Collector
Verify network port access:
```bash
nc -zv 127.0.0.1 4317
```

### Command 4: Check HTTP JSON Collector Endpoint via `curl`
If using HTTP mode:
```bash
curl -v -X POST "http://127.0.0.1:4318/v1/metrics" \
  -H "Content-Type: application/json" \
  -d '{"resourceMetrics":[]}'
```

### Command 5: Verify Thread CPU Separation Between Workers and Shadow
Ensure shadow logistics execution does not steal CPU from traffic cores:
```bash
pidstat -u -t -p $(pgrep velda-edge) 1 5
```

---

## 6. Emergency Playbooks

### Playbook A: Throttling Export Frequency During Network Congestion
If the internal network between the Gateway and the Collector is saturated:
1. Update `interval_secs` from `10` to `30` or `60` in `plugins.json`.
2. Push reload notification:
   ```bash
   curl -X POST http://127.0.0.1:8080/api/v1/plugins/reload
   ```

### Playbook B: Rotating Authentication Tokens (Zero-Downtime Reload)
When an expired Bearer token triggers authentication errors:
1. Generate an active token from your secrets manager.
2. Update the `headers` field in `plugins.json`:
   ```json
   "headers": {
     "authorization": "Bearer <NEW_TOKEN>"
   }
   ```
3. Push reload notification. The shadow worker seamlessly applies the new token on the next tick.

### Playbook C: Dynamically Disabling Exporter (Zero-Downtime Bypass)
To stop the exporter without interrupting client traffic:
1. Remove the `otlp_metrics_exporter` block from `plugins.json`.
2. Push reload notification. The shadow worker cancels the background export task cleanly.
