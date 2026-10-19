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

## 2. Architecture & Execution Semantics

### Mechanism, Not Policy
Velda Edge provides raw, predictable mechanism without enforcing opinionated policy:
- **Strict FIFO Ordering**: The execution order within `pre_upstream` matches the route's `plugins` array order exactly.
- **Single Invocation**: Each declared plugin instance is invoked once per request.
- **Full Operator Control**: If you place `header_mutation` before authentication plugins to sanitize or rename headers, it runs first. If placed after authentication to inject claims, it runs after.

### Deterministic Internal Mutation Order
When a single `header_mutation` instance executes, operations are applied in a deterministic 5-step sequence:

```
Incoming Request
       │
       ▼
1. Strip Hop-by-Hop Headers (RFC 9110 / RFC 7230 connection-specific tokens)
       │
       ▼
2. Remove Blacklisted Headers (Stripping sensitive internal headers)
       │
       ▼
3. Rename Headers (Mapping legacy header names to standard upstream targets)
       │
       ▼
4. Set Headers (Overwriting headers with static values or dynamic variables)
       │
       ▼
5. Append Headers (Appending values, e.g. multi-hop X-Forwarded-For)
       │
       ▼
Forward to Upstream
```

### Supported Dynamic Variables

Header values can reference runtime request variables:

| Variable | Description | Source / Fallback |
| :--- | :--- | :--- |
| `$client_ip` | Downstream peer IP address | First IP from `x-forwarded-for`, `x-real-ip`, or `"127.0.0.1"` |
| `$request_id` | Unique request identifier | Reuses `x-request-id` if present; otherwise generates monotonic hex ID |
| `$route_id` | Matching route identifier | `x-route-id`, `x-velda-route`, or `"unknown-route"` |
| `$upstream_id`| Target upstream identifier | `x-upstream-id` or `"default-upstream"` |
| `$scheme` | Request protocol scheme | `req.uri.scheme()`, `x-forwarded-proto`, or `"http"` |
| `$host` | Host authority | `Host` header or URI authority |
| `$method` | Request HTTP method | `req.method` (`"GET"`, `"POST"`, etc.) |
| `$path` | Request URI path | `req.uri.path()` |

---

## 3. Configuration Schema

As with all Velda Edge plugins, **never specify `phase` or `priority`** in user configuration. Execution phase is statically inferred as `pre_upstream`, and execution order follows the route's array order.

```json
{
  "id": "header_mutation",
  "config": {
    "strip_hop_by_hop": true,
    "remove": [
      "x-internal-secret",
      "x-debug-mode"
    ],
    "rename": {
      "x-legacy-token": "authorization"
    },
    "set": {
      "x-gateway": "velda-edge",
      "x-request-id": "$request_id",
      "x-client-ip": "$client_ip",
      "x-route-id": "$route_id"
    },
    "append": {
      "x-forwarded-for": "$client_ip"
    }
  }
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `strip_hop_by_hop` | boolean | `true` | Removes RFC 9110 hop-by-hop headers (`Connection`, `Keep-Alive`, `Proxy-Authenticate`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`) and any tokens listed in `Connection`. |
| `remove` | string[] | `[]` | Headers to strip from the request before upstream dispatch. |
| `rename` | object | `{}` | Key-value map of headers to rename (`{"old_name": "new_name"}`). |
| `set` | object | `{}` | Key-value map of headers to set/overwrite with static strings or dynamic variables. |
| `append` | object | `{}` | Key-value map of headers to append to existing values. |

---

## 4. Route Attachment Example

```json
{
  "id": "checkout-route",
  "path": "/api/v2/checkout",
  "upstream_id": "checkout-service",
  "plugins": [
    {
      "id": "header_mutation",
      "config": {
        "strip_hop_by_hop": true,
        "remove": ["x-internal-role"],
        "set": {
          "x-gateway": "velda-edge",
          "x-request-id": "$request_id",
          "x-client-ip": "$client_ip"
        }
      }
    },
    {
      "id": "jwt_auth",
      "config": {
        "secret": "supersecret"
      }
    },
    { "id": "access_log" }
  ]
}
```
