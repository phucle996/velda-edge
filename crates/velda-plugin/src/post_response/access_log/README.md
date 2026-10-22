# Access Log Plugin (`access_log`)

Lock-free, zero-blocking request access logging plugin evaluated during the `post_response` phase. Extracts response status, payload size, and lifecycle metadata headers, enqueueing structured records into a bounded channel for background draining without performing synchronous disk I/O or blocking traffic worker threads.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **Zero-IO Hot Path**: The request serving worker thread only extracts cheap metadata from `L7Response` and enqueues into a lock-free bounded ring buffer. If the ring buffer fills, records are dropped non-blocking, ensuring downstream clients never experience p99 latency spikes.
>
> **Never Specify `phase` in User Configuration**: The execution phase is statically inferred by Velda as `post_response`. Execution order strictly follows the route's `plugins` array order (FIFO).

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `access_log` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Hot Path Latency** | < 50ns (Zero disk I/O, zero lock contention) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `format` | `string` | **Yes** | `"json"`, `"text"`, or `"clf"` | `"json"` | Serialization output format for access log records. |
| `include_request_id` | `boolean` | **Yes** | `true` or `false` | `true` | Extracts and records authoritative `x-request-id` header from request. |
| `include_route_id` | `boolean` | **Yes** | `true` or `false` | `true` | Extracts and records canonical `x-route-id` header from response. |
| `include_upstream` | `boolean` | **Yes** | `true` or `false` | `true` | Extracts and records target upstream cluster address (`x-upstream-addr`). |
| `include_latency` | `boolean` | **Yes** | `true` or `false` | `true` | Extracts and records upstream processing latency breakdown (`x-response-time`). |
| `buffer_capacity` | `integer` | **Yes** | `128..=65536` | `4096` | Bounded capacity of the lock-free log dispatch queue. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "edge_access_logger",
      "plugin": "access_log",
      "config": {
        "format": "json",
        "include_request_id": true,
        "include_route_id": true,
        "include_upstream": true,
        "include_latency": true,
        "buffer_capacity": 4096
      }
    }
  ]
}
```

---

## 4. Route Attachment (`routes.json`)

Attach the plugin to an HTTP route via its unique identifier in the `plugins` array. Typically placed as the final post-response plugin:

```json
{
  "routes": [
    {
      "id": "route_api",
      "match": {
        "path": {
          "prefix": "/api/"
        }
      },
      "plugins": [
        "edge_security_headers",
        "edge_access_logger"
      ],
      "upstream": "cluster_main"
    }
  ]
}
```

---

## 5. Architectural Design & Zero-IO Invariant

In high-concurrency edge systems, access logging is often a major source of p99 latency degradation when implemented with synchronous writes or stdout printing:

```
ANTI-PATTERN on traffic worker thread:
println!("{} {} {}", status, req_id, latency); // Synchronous mutex lock on stdout!
file.write_all(log_line)?;                    // Blocks worker thread on kernel disk flush!
```

### The Velda Solution: Non-Blocking Producer $\to$ Background Consumer

1. **Hot Path (`on_response`)**:
   - The traffic worker thread only extracts metadata from `L7Response` status and headers.
   - It performs an atomic `sender.try_send(record)` into a bounded lock-free channel.
   - **Guaranteed non-blocking**: If the background flusher falls behind, the record is immediately dropped, incrementing `dropped_count`. The downstream client request is **never delayed**.
2. **Cold Path (Background Draining)**:
   - A dedicated background task consumes from the channel, batching multiple records into buffered writes or streaming them to external telemetry collectors.
