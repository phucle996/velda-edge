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

### System Invariant: Mandatory `X-Velda-Mirror: true`
The header `X-Velda-Mirror: true` is an **immutable system constant** automatically injected into every mirrored request. Microservices can inspect this header to run in dry-run mode, mock database writes, or disable real notifications.

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `mirror_upstream` | String | **Yes** | Non-empty string identifier of target mirror upstream pool | `"orders-staging"` | Logical identifier of the target mirror upstream cluster. |
| `sample_percent` | Number (f64) | **Yes** | `0.0 ..= 100.0` (%) | `10.0` (for staging shadow) / `100.0` (full mirror) | Percentage of matching traffic to mirror asynchronously. |
| `replace_host` | String / null | **Yes** | Hostname string, or `null` to retain original host | `"staging-orders.internal.net"` | Optional replacement Host header sent to the mirror upstream. |
| `ignore_methods` | Array of strings | **Yes** | Valid RFC 9110 HTTP method tokens (`[]` if none) | `["POST", "PUT", "PATCH", "DELETE"]` (for read-only mirror) | HTTP methods excluded from traffic shadowing. |
| `queue_capacity` | Integer (usize) | **Yes** | `16 ..= 1000000` | `65536` | Capacity of the non-blocking channel buffer before dropping shadow requests. |

---

## 3. Configuration Examples

### Production Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "request_mirror",
      "config": {
        "mirror_upstream": "orders-staging",
        "sample_percent": 10.0,
        "replace_host": "staging-orders.internal.net",
        "ignore_methods": [
          "POST",
          "PUT",
          "PATCH",
          "DELETE"
        ],
        "queue_capacity": 65536
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "orders-api-route",
  "match": {
    "path": "/api/v1/orders"
  },
  "upstream_id": "orders-production",
  "plugins": [
    "jwt_auth",
    "request_mirror",
    "access_log"
  ]
}
```
