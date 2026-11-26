# Request Mirroring Plugin (`request_mirror`)

Production-grade traffic shadowing plugin evaluated during the `pre_upstream` phase. Writes the resolved dense `mirror_upstream` index into the upper 16 bits of the `u32` upstream target register (`[mirror: u16 | primary: u16]`) using zero-allocation `request_id` entropy sampling and an O(1) method bitmask.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `request_mirror` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Client Latency Impact** | Non-blocking shadow offload (`velda-shadow-{i}`); primary never waits on mirror I/O |
| **Payload Forwarding** | Verbatim request clone (zero extra headers added; zero-copy `Bytes` refcount sharing across buffered & chunked streams) |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `mirror_upstream` | String | **Yes** | Non-empty upstream ID present in loaded `upstreams` | `"orders-staging"` | Logical identifier of the target mirror upstream pool. |
| `sample_percent` | Number (f64) | **Yes** | `0.0 ..= 100.0` (%) | `10.0` (staging shadow) / `100.0` (full mirror) | Percentage of matching traffic sampled for mirroring. |
| `ignore_methods` | Array of strings | **Yes** | Valid RFC 9110 HTTP method tokens (`[]` if none) | `["POST", "PUT", "PATCH", "DELETE"]` | HTTP methods excluded from mirroring. |

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
        "ignore_methods": [
          "POST",
          "PUT",
          "PATCH",
          "DELETE"
        ]
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
    "security_headers"
  ]
}
```
