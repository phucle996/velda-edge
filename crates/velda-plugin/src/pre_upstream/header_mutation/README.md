# Header Mutation Plugin (`header_mutation`)

High-performance request header transformation and protocol sanitization plugin evaluated during the `pre_upstream` phase. Modifies outgoing request headers before upstream dispatch, supporting RFC hop-by-hop stripping, header removal, renaming, dynamic variable interpolation, and appending.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `header_mutation` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Nanosecond-level; zero allocation for static values and pre-compiled constants, zero disk/network I/O |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied, using empty arrays `[]` or empty maps `{}` for unused actions.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `strip_hop_by_hop` | Boolean | **Yes** | `true` or `false` | `true` | Removes RFC 9110 hop-by-hop headers (`Connection`, `Keep-Alive`, `Proxy-Authenticate`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`) and any tokens listed in `Connection`. |
| `remove_headers` | Array of strings | **Yes** | Valid RFC 9110 header names (`[]` if none) | `["x-internal-secret", "x-debug-mode"]` | Headers to strip from request before upstream dispatch. |
| `set_headers` | Map<String, String> | **Yes** | Key-value pairs of header names and values/variables (`{}` if none) | `{"x-gateway": "velda-edge", "x-request-id": "$request_id"}` | Headers to set/overwrite on outgoing request. |
| `append_headers` | Map<String, String> | **Yes** | Key-value pairs of header names and append values (`{}` if none) | `{"x-forwarded-for": "$client_ip"}` | Headers to append values to on outgoing request. |

### Supported Dynamic Variables
- `$client_ip`: Downstream peer IP address.
- `$request_id`: Unique monotonic request identifier.
- `$route_id`: Matched route identifier.
- `$upstream_id`: Target upstream identifier.
- `$scheme`: Request scheme (`http` or `https`).
- `$host`: Host authority header or URI authority.
- `$method`: HTTP request method (`GET`, `POST`, etc.).
- `$path`: Request URI path.

---

## 3. Configuration Examples

### Production Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "header_mutation",
      "config": {
        "strip_hop_by_hop": true,
        "remove_headers": [
          "x-internal-secret",
          "x-debug-mode"
        ],
        "set_headers": {
          "x-gateway": "velda-edge",
          "x-request-id": "$request_id",
          "x-client-ip": "$client_ip",
          "x-route-id": "$route_id"
        },
        "append_headers": {
          "x-forwarded-for": "$client_ip"
        }
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "checkout-route",
  "match": {
    "path": "/api/v2/checkout"
  },
  "upstream_id": "checkout-service",
  "plugins": [
    "header_mutation",
    "jwt_auth"
  ]
}
```
