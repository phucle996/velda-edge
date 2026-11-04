# Standard Stream gRPC Access Log Plugin (`std_grpc_access_log`) — Operator & Configuration Runbook

---

## 1. Declarative Configuration Reference

`std_grpc_access_log` is configured in `plugins.json` and attached to gRPC routes in `routes.json`:

```json
{
  "id": "std_grpc_logger",
  "plugin": "std_grpc_access_log",
  "phase": "post_response",
  "config": {
    "target": "stdout",
    "format": "json",
    "include_request_id": true,
    "include_route_id": true,
    "include_upstream": true,
    "include_peer_addr": true,
    "include_latency": true,
    "buffer_capacity": 4096,
    "sampling": {
      "0": 0.05,
      "OK": 0.05,
      "DEADLINE_EXCEEDED": 1.0,
      "RESOURCE_EXHAUSTED": 1.0,
      "UNAVAILABLE": 1.0,
      "INTERNAL": 1.0
    }
  }
}
```

### Production Validation Rules (Zero Silent Fallbacks)
- `target`: Must be strictly `"stdout"` or `"stderr"`. Direct file paths (`"/var/log/grpc.log"`) are rejected fail-fast at configuration load time.
- `format`: Must be `"json"` or `"text"`.
- `buffer_capacity`: Must be bounded between `128` and `65,536` entries per core lane.
- `sampling`: Exact canonical status codes (`0..=16`) or names (`"OK"`, `"UNAVAILABLE"`, etc.). Wildcards (`"*"`, `"2xx"`) are rejected immediately.

---

## 2. Production Shipping Architectures

Standard streams emitted by `std_grpc_access_log` are designed for zero-copy container logging via Vector, Fluent Bit, or standard journald collectors.

```
┌──────────────────────────────────────────────┐
│ Velda Edge Process (PID 1)                   │
│ Frontline Workers  ──> SPSC Ring Buffer      │
│ Shadow Drainer     ──> libc::write(stdout)   │
└───────────────────────┬──────────────────────┘
                        │ /dev/stdout (Pipe fd 1)
                        ▼
┌──────────────────────────────────────────────┐
│ Container Log Collector (Vector / Fluent Bit)│
│ - Zero Docker/K8s JSON file bloat            │
│ - Ships directly to ClickHouse / OpenSearch  │
└──────────────────────────────────────────────┘
```

---

## 3. Prometheus Observability & Alerting Rules

### Key Metrics Exported

| Metric | Type | Description | Alert Condition |
| :--- | :--- | :--- | :--- |
| `std_grpc_access_log.logged_count` | Counter | Total gRPC records pushed to ring buffer | Stagnant under active RPC traffic |
| `std_grpc_access_log.dropped_count` | Counter | Records dropped due to saturated ring buffer | Rate > 0.01% of total traffic |

### Prometheus Alert Definition

```yaml
groups:
  - name: velda_grpc_access_log_alerts
    rules:
      - alert: GrpcAccessLogDropsDetected
        expr: rate(std_grpc_access_log_dropped_count[1m]) > 0
        for: 30s
        labels:
          severity: warning
          subsystem: logging
        annotations:
          summary: "Velda Edge is dropping gRPC access log records"
          description: "Ring buffer saturation detected. The Shadow Logistics Worker is backpressured or stdout buffer is full."
```

---

## 4. Troubleshooting Playbook

### Symptom: `dropped_count` is increasing
1. Check standard output receiver backpressure:
   ```bash
   lsof -p $(pgrep velda-edge) -a -d 1
   ```
2. If piping to `fluent-bit` or `vector`, inspect log shipper CPU usage.
3. Increase `buffer_capacity` in `plugins.json` from `4096` to `16384` to absorb larger downstream micro-bursts.
