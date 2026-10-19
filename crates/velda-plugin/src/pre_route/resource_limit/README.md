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

## 2. Architectural Rationale: Why Limit Resources at `pre_route`?

A common question in high-performance proxy architecture is:

> *"If a Radix Trie matches routes by prefix, matching terminates as soon as the prefix matches or a mismatch occurs at an early branch. Why do we still need an edge URI length limit?"*

While it is true that Radix Trie prefix lookup stops early and does not traverse the entire URI string, an unbounded URI or header payload threatens the rest of the edge and backend ecosystem:

### 1. 95% of Large URIs Reside in the Query String
The Radix Trie matcher inspects only the URI `path` (`/api/v1/orders`). It completely ignores query strings (`?filter=...`).
- When a client sends a 64 KB or 1 MB URL, the query string bypasses route matching unaffected.
- Subsequent inspection plugins (such as `waf_lite`) must scan every byte of that oversized query string for SQLi, XSS, and RCE threats, ballooning CPU cycle usage from nanoseconds to milliseconds.
- Access logging and tracing exporters must allocate and serialize these gigantic URLs, causing severe allocator churn and buffer bloat.
- Upstream HTTP encoders must re-serialize and push those oversized strings over the socket.

### 2. Upstream Backend Buffer Explosion & Connection Pool Aborts
Even if the Edge gateway handles a 64 KB request line, the backend microservices behind it almost certainly will not:
- **Spring Boot / Tomcat**: Default `server.max-http-header-size = 8 KB` (inclusive of the request line).
- **Node.js**: Default `--max-http-header-size = 16 KB`.
- **Nginx**: Default `large_client_header_buffers = 8 KB`.

If Velda Edge forwards an oversized URI or header block to the upstream backend, the backend will immediately terminate the connection (`TCP RST` or `Connection: close` with `400 Bad Request / 414 URI Too Long`). This destroys persistent connection reuse in Velda's Upstream Connection Pool, forcing the edge to clean up dead sockets and establish new TCP/TLS handshakes under high load.

### 3. Preserving L1 `HeaderWorkspace` Cache Invariants
Velda Edge pre-warms a fixed `HeaderWorkspace` inside CPU L1/L2 cache (committed to MMU at bootstrap) to achieve zero-allocation header processing.
- If an attacker floods the gateway with 200 headers or 64 KB header values (Header Bombing / Slowloris), the workspace overflows, forcing fallback heap allocations (`malloc`) and evicting hot cache lines across worker cores.
- `resource_limit` guarantees that headers stay strictly within L1 cache bounds.

### 4. Wildcard Route Protection
For wildcard routes (e.g. `/static/*filepath` or dynamic proxies), the router must capture and slice the remainder of the URI. Unbounded URIs force large memory slices and potential buffer copying.

---

## 3. Enforced Limits & RFC Error Responses

| Configuration Key | Type | Default | Violated Status Code | Description |
| :--- | :---: | :---: | :---: | :--- |
| `max_uri_length` | integer | `4096` bytes (4 KB) | **`414 URI Too Long`** (RFC 9110 §15.5.15) | Total length of Path + `?` + Query string. |
| `max_headers_count` | integer | `64` | **`431 Request Header Fields Too Large`** (RFC 6585 §5) | Maximum number of header entries in the request. |
| `max_header_bytes` | integer | `16384` bytes (16 KB) | **`431 Request Header Fields Too Large`** | Cumulative byte size of all header names, values, and wire framing. |
| `max_single_header_bytes` | integer | `8192` bytes (8 KB) | **`431 Request Header Fields Too Large`** | Maximum byte length of any individual header value. |

All error responses are pre-computed at configuration compile time with immutable `application/json` bodies, resulting in **zero allocations on error paths**.

---

## 4. Configuration Schema

As with all Velda Edge plugins, **never specify `phase` or `priority`** in user configuration. The execution phase is statically inferred as `pre_route`, and execution order follows the route's array declaration order (FIFO).

```json
{
  "id": "resource_limit",
  "config": {
    "max_uri_length": 4096,
    "max_headers_count": 64,
    "max_header_bytes": 16384,
    "max_single_header_bytes": 8192
  }
}
```

### Route Attachment Example

```json
{
  "id": "api-orders-route",
  "path": "/api/v1/orders",
  "upstream_id": "orders-backend",
  "plugins": [
    { "id": "ip_filter" },
    { "id": "resource_limit" },
    { "id": "cors_preflight" },
    { "id": "waf_lite" },
    { "id": "jwt_auth" }
  ]
}
```

*Recommended FIFO order*: `ip_filter` $\to$ `resource_limit` $\to$ `cors_preflight` $\to$ `waf_lite`.  
Putting `resource_limit` immediately after IP filtering ensures that oversized requests are terminated before `waf_lite` expends CPU cycles scanning large strings.
