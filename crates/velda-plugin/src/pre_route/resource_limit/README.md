# Resource Limit Plugin (`resource_limit`)

Ultra-low-latency ingress perimeter defense plugin that enforces strict physical bounds on URI lengths, header counts, cumulative header framing bytes, and individual header sizes at the edge before routing trie matching, body streaming, or upstream connection leasing.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `resource_limit` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Can generate `Action::Respond` 414 / 431) |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) — gRPC limits are handled natively by `velda-grpc` |
| **Hot-Path Overhead** | Single-digit nanoseconds (~2–4ns); branch-minimized single-pass; **0 heap allocations** |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `max_uri_length` | Integer (usize) | **Yes** | `64 ..= 65536` bytes | `8192` (8 KB) | Maximum allowed total URI length in bytes (Authority + Path + `?` + Query string). Violations return `414 URI Too Long`. |
| `max_headers_count` | Integer (usize) | **Yes** | `8 ..= 1024` | `64` | Maximum allowed number of distinct header entries in the request. Violations return `431 Request Header Fields Too Large`. |
| `max_header_bytes` | Integer (usize) | **Yes** | `512 ..= 1048576` bytes (1 MB) | `32768` (32 KB) | Cumulative byte size of all header names, values, and wire framing (`name + ": " + val + "\r\n"`). Violations return `431 Request Header Fields Too Large`. |
| `max_single_header_bytes` | Integer (usize) | **Yes** | `64 ..= max_header_bytes` | `8192` (8 KB) | Maximum byte size of any individual header name, value, or multi-value aggregate. Violations return `431 Request Header Fields Too Large`. |

All error responses are pre-computed at configuration compile time with immutable `application/json; charset=utf-8` bodies and **`Cache-Control: no-store`**, resulting in **zero allocations on error paths** and neutralizing CPDoS attacks.

---

## 3. Security Mitigations Matrix

| Vulnerability / Attack Vector | Exploit Syntax / Scenario | Behavior in Naive Proxy | Mitigation in `resource_limit` |
| :--- | :--- | :--- | :--- |
| **CPDoS (Cache-Poisoned Denial of Service)** | Attacker floods oversized headers to a public cacheable URI, forcing a 431 response | 431 cached by CDN because `Cache-Control` header is missing, denying service to all legitimate users | Both 414 and 431 responses strictly mandate pre-baked `Cache-Control: no-store` |
| **Header Name Bombing** | Attacker sends a 16 KB header name with a 1-byte value (`X-AAAA...[16KB]...: 1`) | Only `val.len()` is checked against `max_single_header_bytes`, allowing massive names to exhaust upstream backend memory | `max_single_header_seen` tracks both `name_len` and `val_len`, rejecting header names that exceed threshold |
| **Multi-Value Header Fragmentation Bypass** | Attacker splits a 16 KB payload into 8 separate 2 KB lines with the same header name (e.g. `Cookie`) | Each individual slice passes `< 4096`, and downstream backend concatenates them into 16 KB, causing buffer overflow | Aggregates consecutive entries sharing the same `HeaderName` in a single pass without heap allocation |
| **Absolute-Form URI Authority Bypass** | Client sends `GET http://<huge-30KB-authority>/path HTTP/1.1` | Only `path().len() + query().len()` is evaluated (2 bytes), letting 30 KB authority pass | Evaluates full URI length (`authority().len() + path_and_query().len()`) |
| **Slowloris & Header Bombing DoS** | Client streams hundreds of small headers to exhaust memory tables | Allocates dynamic linked lists or hash maps indefinitely | Rejects immediately if `req.headers.len() > max_headers_count` before scanning bytes |

---

## 4. Configuration Examples

### Manifest Declaration (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "resource_limit",
      "config": {
        "max_uri_length": 8192,
        "max_headers_count": 64,
        "max_header_bytes": 32768,
        "max_single_header_bytes": 8192
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "api-orders-route",
  "match": {
    "path": "/api/v1/orders"
  },
  "upstream_id": "orders-backend",
  "plugins": [
    "ip_filter",
    "resource_limit",
    "cors_preflight",
    "waf_lite",
    "jwt_auth"
  ]
}
```

*Recommended FIFO order*: `ip_filter` $\to$ `resource_limit` $\to$ `cors_preflight` $\to$ `waf_lite`.  
Putting `resource_limit` immediately after IP filtering ensures that oversized requests are terminated before `waf_lite` expends CPU cycles scanning large strings.
