# Request Mirroring Plugin (`request_mirror`)

Production-grade traffic shadowing plugin evaluated during the `pre_upstream` phase. Duplicates live client requests asynchronously to an experimental or staging upstream cluster without blocking, delaying, or impacting the primary response.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `request_mirror` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Client Latency Impact** | **0 nanoseconds**. Asynchronous fire-and-forget queue dispatch |
| **Memory Overhead** | **0 memcpy**. Request bodies are shared via `bytes::Bytes` atomic reference-counted pointers |

---

## 2. Core Architecture & Invariants

```
                             [ Downstream Client ]
                                       │
                                       ▼
                                [ Velda Edge ]
                                /            \
       (Luồng chính - Primary) /              \ (Luồng nhân bản - Shadow)
                              ▼                ▼
                  [ Upstream Production ]    [ Upstream Shadow / Staging ]
                              │                │
                     (Trả về 200 OK)    (Response bị vứt bỏ hoàn toàn)
                              │
                              ▼
                     [ Downstream Client ]
```

### 2.1. System Invariant: Mandatory `X-Velda-Mirror: true`
To ensure microservices across the entire infrastructure can reliably detect shadow traffic:
- The header `X-Velda-Mirror: true` is an **immutable, hardcoded system constant**.
- It is **automatically injected** into every mirrored request and is **not configurable**.
- Microservice backends can universally inspect this header to enable dry-run mode, mock database writes, or disable real SMS/email notifications.

### 2.2. Zero-Copy Payload Sharing via Smart Pointers
When mirroring requests with large bodies (e.g., 100 KB JSON payloads or binary gRPC frames):
- The payload is represented by `bytes::Bytes`.
- Cloning `Bytes` merely increments an atomic reference count on the existing buffer in RAM. **No heap memory copying (0 memcpy) occurs**.
- Both the primary upstream worker and the background shadow task read from the same underlying memory buffer.

### 2.3. Failure Isolation & Non-Blocking Dispatch
- Mirrored requests are enqueued into a bounded channel using non-blocking `try_send`.
- If the mirror upstream or shadow queue becomes saturated, the mirror request is dropped immediately in $< 1\ \text{ns}$, incrementing `dropped_count`.
- Mirror connection timeouts, socket resets, or 5xx responses are completely discarded and **never impact the downstream client**.

### 2.4. Pre-warmed Upstream Infrastructure
The mirror target (`mirror_upstream`) is configured as a first-class `Upstream` in Velda Edge:
- It uses pre-warmed connection pools (TCP/TLS keep-alive, HTTP/2 multiplexing) without incurring per-request handshake overhead.
- It is independently protected by circuit breakers and health trackers, preventing runaway shadow traffic from exhausting gateway resources.

---

## 3. Configuration Schema

As with all Velda Edge plugins, **never specify `phase` or `priority`** in user configuration. Execution phase is statically inferred as `pre_upstream`, and execution order follows the route's array order.

```json
{
  "id": "request_mirror",
  "config": {
    "mirror_upstream": "orders-staging",
    "sample_percent": 10,
    "replace_host": "staging-orders.internal.net",
    "ignore_methods": ["DELETE"]
  }
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `mirror_upstream` | string | **Required** | Logical identifier of the target mirror upstream. |
| `sample_percent` | integer | `100` | Percentage of matching traffic to mirror (`0` to `100`). |
| `replace_host` | string | `null` | Optional replacement value for the `Host` header sent to the mirror upstream. |
| `ignore_methods` | string[] | `[]` | HTTP methods that should never be mirrored (e.g. `["DELETE"]`). |
| `queue_capacity` | integer | `65536` | Capacity of the non-blocking queue before dropping shadow requests. |

---

## 4. Route Attachment Examples

### Example 1: Staging Cluster Shadowing (10% Sample)
```json
{
  "id": "orders-api-route",
  "path": "/api/v1/orders",
  "upstream_id": "orders-production",
  "plugins": [
    {
      "id": "request_mirror",
      "config": {
        "mirror_upstream": "orders-staging",
        "sample_percent": 10,
        "replace_host": "staging-orders.internal.net",
        "ignore_methods": ["DELETE"]
      }
    },
    { "id": "access_log" }
  ]
}
```

### Example 2: Full Traffic Shadowing for System Rewrite Verification
```json
{
  "id": "catalog-api-route",
  "path": "/api/v1/catalog",
  "upstream_id": "catalog-legacy-java",
  "plugins": [
    {
      "id": "request_mirror",
      "config": {
        "mirror_upstream": "catalog-rust-service",
        "sample_percent": 100
      }
    }
  ]
}
```
