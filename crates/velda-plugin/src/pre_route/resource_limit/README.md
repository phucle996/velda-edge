# Resource Limit Plugin (`resource_limit`)

Ultra-low-latency ingress boundary defense plugin that enforces strict physical bounds on URI lengths, header counts, and cumulative header bytes at the Edge before routing trie matching, body streaming, or upstream connection leasing.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `resource_limit` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | Single-digit nanoseconds ($< 15\ \text{ns}$); **0 heap allocations** |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `max_uri_length` | Integer (usize) | **Yes** | `64 ..= 65536` bytes | `8192` (8 KB) | Maximum allowed total URI length in bytes (Path + `?` + Query string). Violations return `414 URI Too Long`. |
| `max_headers_count` | Integer (usize) | **Yes** | `8 ..= 1024` | `64` | Maximum allowed number of distinct header entries in the request. Violations return `431 Request Header Fields Too Large`. |
| `max_header_bytes` | Integer (usize) | **Yes** | `512 ..= 1048576` bytes (1 MB) | `32768` (32 KB) | Cumulative byte size of all header names, values, and wire framing. Violations return `431 Request Header Fields Too Large`. |
| `max_single_header_bytes` | Integer (usize) | **Yes** | `64 ..= 524288` bytes ($\le \text{max\_header\_bytes}$) | `8192` (8 KB) | Maximum byte length of any individual header value. Violations return `431 Request Header Fields Too Large`. |

All error responses are pre-computed at configuration compile time with immutable `application/json` bodies, resulting in **zero allocations on error paths**.

---

## 3. Architectural Rationale: Why Limit Resources at `pre_route`?

While Radix Trie prefix lookup stops early and does not traverse the entire URI string, an unbounded URI or header payload threatens the rest of the edge and backend ecosystem:

1. **Query Strings Bypass Trie Lookup**: The router inspects only the URI path. An attacker can send a 100 KB query string that would otherwise force subsequent WAF regex inspection, logging, and upstream serialization buffers to bloat.
2. **Upstream Backend Buffer Explosion**: Backend servers (Tomcat 8 KB, Node.js 16 KB, Nginx 8 KB) abort connections when headers or request lines exceed their thresholds, destroying connection pool reuse.
3. **Preserving L1 `HeaderWorkspace` Invariants**: Velda Edge pre-warms a fixed `HeaderWorkspace` inside CPU L1 cache to achieve zero allocations. Bounding header sizes prevents CPU cache eviction and fallback `malloc` invocations.

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
