# Access Log Plugin (`access_log`)

Lock-free, zero-blocking request access logging plugin evaluated during the `post_response` phase. Extracts response status, payload size, and lifecycle metadata headers, enqueueing structured records into a bounded channel for background draining without performing synchronous disk I/O or blocking traffic worker threads.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `access_log` |
| **Canonical Phase** | `post_response` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | Single-digit to tens of nanoseconds ($< 50\ \text{ns}$); **0 disk I/O, 0 lock contention** |

---

## 2. Architectural Design & Zero-IO Invariant

In high-concurrency edge systems, access logging is often a major source of p99 latency degradation when implemented with synchronous writes or stdout printing:

```
// ANTI-PATTERN on traffic worker thread:
println!("{} {} {}", status, req_id, latency); // Synchronous mutex lock on stdout!
file.write_all(log_line)?;                    // Blocks worker thread on kernel disk buffer flush!
```

### The Velda Solution: Non-Blocking Producer $\to$ Background Consumer

1. **Hot Path (`on_response`)**:
   - The traffic worker thread only extracts cheap metadata from `L7Response` status and headers.
   - It performs an atomic `sender.try_send(record)` into a bounded lock-free channel.
   - **Guaranteed non-blocking**: If the background flusher or disk falls behind, the record is immediately dropped, incrementing `dropped_count`. The downstream client request is **never paused or delayed**.
2. **Cold Path (Background Draining)**:
   - A dedicated background task consumes from the channel, batching multiple records into buffered writes or streaming them to external telemetry collectors.

### Access Log vs Business Logging vs Tracing
- **Access Log**: Standardized edge HTTP transport record (`status`, `bytes`, `latency`, `request_id`, `route_id`, `upstream`).
- **Application Logs / Tracing**: Deep payload or business logic tracing is managed separately via W3C TraceContext headers (`traceparent`, `tracestate`) without burdening the edge access log.

---

## 3. Configuration Schema

As with all Velda Edge plugins, **never specify `phase` or `priority`** in user configuration. The execution phase is statically inferred as `post_response`, and execution order follows the route's array declaration order (FIFO).

```json
{
  "id": "access_log",
  "config": {
    "include_request_id": true,
    "include_route_id": true,
    "include_upstream": true,
    "include_latency": true,
    "format": "json",
    "buffer_capacity": 65536
  }
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `include_request_id` | boolean | `true` | Extracts `x-request-id` header if present. |
| `include_route_id` | boolean | `true` | Extracts `x-route-id` or `x-velda-route` header if present. |
| `include_upstream` | boolean | `true` | Extracts `x-upstream-addr` or `x-upstream-id` header if present. |
| `include_latency` | boolean | `true` | Extracts `x-response-time` or `x-upstream-response-time` header if present. |
| `format` | string | `"json"` | Serialization format (`"json"` or `"text"`). |
| `buffer_capacity` | integer | `65536` | Bounded capacity of the lock-free non-blocking queue. |

---

## 4. Route Attachment Example

```json
{
  "id": "api-orders-route",
  "path": "/api/v1/orders",
  "upstream_id": "orders-backend",
  "plugins": [
    { "id": "resource_limit" },
    { "id": "jwt_auth" },
    { "id": "security_headers" },
    { "id": "access_log" }
  ]
}
```
