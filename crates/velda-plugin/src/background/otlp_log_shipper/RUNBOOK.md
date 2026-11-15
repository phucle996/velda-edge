# OpenTelemetry Log Shipper — SRE On-Call Runbook & Incident Triage Guide

This runbook guides Site Reliability Engineers (SREs) and on-call operators in diagnosing, troubleshooting, and resolving production incidents involving the **OpenTelemetry Log Shipper Plugin** (`otlp_log_shipper`).

---

## 1. Parameter Reference & Operational Impact

| Field | Production Range | Operational Impact | Recommended Default |
| :--- | :--- | :--- | :--- |
| `protocol` | `"http"` \| `"grpc"` | `"grpc"` uses binary Protobuf over HTTP/2, saving CPU and network bandwidth. `"http"` emits standard JSON over HTTP/1.1. | `"grpc"` |
| `endpoint` | Valid URI | Ingestion endpoint URL of the target OpenTelemetry Collector. If unreachable, shipper backs off without blocking request traffic. | `"grpc://127.0.0.1:4317"` |
| `service_name` | ASCII string | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). | `"velda-edge-prod"` |
| `flush_interval_ms` | `100 ..= 60000` | Periodic timer interval for waking up shadow worker to drain SPSC rings and ship buffered records. Lower values reduce log latency. | `100` |
| `max_batch_size` | `1 ..= 10000` | Maximum number of log records grouped into a single exported payload. Sizing balances batching efficiency against RAM allocation. | `1000` |
| `connect_timeout_ms`| `100 ..= 60000` | Connection establishment timeout. Prevents hung TCP/TLS handshakes from blocking the shadow task loop. | `1000` |
| `request_timeout_ms`| `100 ..= 60000` | Export request timeout before aborting and logging a failure. | `1000` |
| `headers` | Key-value pairs | Custom HTTP or gRPC headers sent with every export request (e.g. Bearer auth tokens, tenant IDs). | `{}` |

---

## 2. Tuned Production Presets

### Preset A: High-Throughput Kubernetes Production Daemon (gRPC / HTTP/2)
```json
{
  "protocol": "grpc",
  "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
  "service_name": "velda-edge-prod",
  "flush_interval_ms": 100,
  "max_batch_size": 1000,
  "connect_timeout_ms": 1000,
  "request_timeout_ms": 1000,
  "headers": {}
}
```
*Rationale: Direct Protobuf wire skeletons and persistent HTTP/2 stream multiplexing maximize log delivery throughput with zero Prost heap churn.*

### Preset B: Standard REST / JSON Profile (HTTP/1.1 Keep-Alive)
```json
{
  "protocol": "http",
  "endpoint": "http://otel-collector.monitoring.svc.cluster.local:4318/v1/logs",
  "service_name": "velda-edge-prod",
  "flush_interval_ms": 100,
  "max_batch_size": 1000,
  "connect_timeout_ms": 1000,
  "request_timeout_ms": 1000,
  "headers": {}
}
```
*Rationale: Pre-baked static JSON envelope and 2-digit LUT allow direct ingestion into HTTP-based log aggregation systems.*

### Preset C: Secure Cross-Datacenter WAN Logging (gRPC + TLS)
```json
{
  "protocol": "grpc",
  "endpoint": "grpcs://otel.eu-west-1.aws.internal:4317",
  "service_name": "velda-edge-wan",
  "flush_interval_ms": 200,
  "max_batch_size": 2000,
  "connect_timeout_ms": 2000,
  "request_timeout_ms": 2000,
  "headers": {
    "authorization": "Bearer prod-secret-token-v2"
  }
}
```
*Rationale: 200ms cadence and 2000-record batching minimize WAN packet overhead and TLS renegotiation.*

---

## 3. SRE Incident Triage Matrix (P0 – P3)

| Severity | Incident Pattern | Symptoms & Indicators | Immediate Mitigation Action |
| :---: | :--- | :--- | :--- |
| **P0** | **Collector Outage / Total Export Failure** | Collector completely unreachable; 100% export failures; `failed_pushes` climbs rapidly. | 1. Note: Gateway runs Fail-Open (zero client traffic impact).<br>2. Check Collector service health.<br>3. Route to backup collector via hot reload. |
| **P1** | **Collector Throttling / Queue Saturation** | Collector returns HTTP 429 / 503; export latency spikes above `request_timeout_ms`. | 1. Increase `flush_interval_ms` to 250ms.<br>2. Horizontally scale downstream OpenTelemetry Collector cluster. |
| **P2** | **Authentication / TLS Handshake Failure** | Collector returns HTTP 401/403 or gRPC `UNAUTHENTICATED`; TLS handshake fails. | 1. Update `headers` in `plugins.json` with a valid Bearer token.<br>2. Validate collector certificate SANs.<br>3. Reload configuration. |
| **P3** | **Idle Connection Termination by Firewall** | Firewall closes idle TCP connection; reconnects on next tick. | Transparently recovered on next export tick; no operator action required. |

---

## 4. Incident Triage Decision Flowchart

```mermaid
flowchart TD
    ALERT["Alert: OTLP Log Export Failure (failed_pushes increasing)"] --> CHK_COLLECTOR{"Is Collector Endpoint\n(Port 4317 / 4318) Reachable?"}

    CHK_COLLECTOR -->|"No / Connection Refused"| ACT_COLLECTOR["1. Note: Gateway runs Fail-Open (zero traffic impact)\n2. Verify Collector process / container health\n3. Route to backup collector via plugins.json reload"]
    CHK_COLLECTOR -->|"Yes / Port Open"| CHK_STATUS{"What Error or Status\nDoes Collector Return?"}

    CHK_STATUS -->|"HTTP 429 / 503 or Stream Reset"| FIX_QUEUE["P1: Collector queue saturated:\n1. Scale Collector cluster\n2. Increase flush_interval_ms"]
    CHK_STATUS -->|"HTTP 401/403 or gRPC Unauthenticated"| FIX_AUTH["P2: Update authorization Bearer token in headers\nvia Control Plane reload"]
    CHK_STATUS -->|"Timeout / Reset"| CHK_NET{"Is Network Experiencing\nLoss or High Jitter?"}

    CHK_NET -->|"Network Congested"| ACT_BATCH["Increase max_batch_size to amortize network overhead"]
    CHK_NET -->|"Network Normal"| CHK_THREAD["Inspect CPU usage of velda-shadow-{i}\n(Nominal baseline < 15% CPU at 50K RPS)"]
```

---

## 5. Diagnostic CLI Toolkit

### Command 1: Test Collector Endpoint Connectivity
Test raw connectivity to the OpenTelemetry Collector:
```bash
# Test HTTP collector endpoint
curl -v -X POST http://127.0.0.1:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[]}'

# Test gRPC collector endpoint
grpcurl -plaintext 127.0.0.1:4317 list
```

### Command 2: Inspect Active TCP Socket Connection State
Verify that the exporter has established a persistent connection:
```bash
ss -t -a 'dport = :4318 or dport = :4317'
```

### Command 3: Query Edge Export Metrics
Check real-time export health:
```bash
curl -s http://127.0.0.1:9091/metrics | grep -E "otlp_log_shipper"
```

### Command 4: Monitor Per-Thread CPU Utilization
Ensure shadow logistics execution does not steal CPU from traffic cores:
```bash
pidstat -p $(pgrep velda-edge) -t 1 5
```

---

## 6. Emergency Playbooks

### Playbook A: Collector Blackhole Mitigation (Dynamic Failover to Backup Collector)
If the primary OpenTelemetry Collector fails completely:
1. Update `endpoint` in `plugins.json` to point to the backup collector:
   ```json
   {
     "id": "otlp_log_shipper",
     "config": {
       "protocol": "grpc",
       "endpoint": "grpc://otel-collector-backup.monitoring.svc.cluster.local:4317",
       "service_name": "velda-edge-production",
       "flush_interval_ms": 100,
       "max_batch_size": 1000,
       "connect_timeout_ms": 1000,
       "request_timeout_ms": 1000,
       "headers": {}
     }
   }
   ```
2. Apply immediately without gateway restart:
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

### Playbook C: Dynamically Disabling Log Shipper (Zero-Downtime Bypass)
To stop the log shipper without interrupting client traffic:
1. Remove the `otlp_log_shipper` block from `plugins.json`.
2. Push reload notification. The shadow worker cleanly cancels the background export task.
