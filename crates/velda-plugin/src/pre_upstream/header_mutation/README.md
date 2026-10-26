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
| **Supported Protocols** | HTTP (`http/1.1`, `http/2`, `http/3`) & gRPC |
| **Hot-Path Overhead** | Nanosecond-level; **100% Lock-Free**; **0 heap allocations** for static headers |

### What it Protects & Solves
- **Hop-by-Hop Stripping & Anti-Abuse (CVE-2022-26143)**: Automatically strips RFC 9110 / RFC 7230 hop-by-hop headers (`Connection`, `Keep-Alive`, `Upgrade`, etc.) while protecting sensitive protocol headers (`Host`, `Authorization`, `X-Real-IP`, `X-Request-Id`) from strip abuse.
- **Header Injection & Dynamic Interpolation**: Authoritatively injects runtime metadata (`$client_ip`, `$request_id`, `$route_id`, `$upstream_id`, `$scheme`) into headers before forwarding upstream.
- **Anti-Shadowing Header Renaming**: Renames headers in-place on the stack with anti-shadowing protection (clears target header before inserting).
- **Identity & Traceability**: Eliminates client-spoofed internal headers by wiping blacklisted tokens (`remove_headers`).

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied, using empty arrays `[]` or empty maps `{}` for unused actions.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `strip_hop_by_hop` | Boolean | **Yes** | `true` or `false` | `true` | Removes RFC 9110 hop-by-hop headers (`Connection`, `Keep-Alive`, `Proxy-Authenticate`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`) and any tokens listed in `Connection`. Enforces anti-abuse protections preventing clients from stripping `Host`, `Authorization`, `X-Real-IP`, etc. (CVE-2022-26143). |
| `remove_headers` | Array of strings | **Yes** | Valid RFC 9110 header names (`[]` if none) | `["x-internal-secret", "x-debug-mode"]` | Headers to strip from request before upstream dispatch. Essential routing/framing headers (`Host`, `Content-Length`, `Transfer-Encoding`, `X-Request-Id`, and pseudo-headers `:`) cannot be removed. |
| `rename_headers` | Map<String, String> | No | Source to target header name mapping (`{}` if none) | `{"x-old-auth": "authorization"}` | Renames headers in-place on the stack with anti-shadowing protection. |
| `set_headers` | Map<String, String> | **Yes** | Key-value pairs of header names and values/variables (`{}` if none) | `{"x-gateway": "velda-edge", "x-request-id": "$request_id"}` | Headers to set/overwrite on outgoing request. Framing and hop-by-hop headers cannot be overwritten. |
| `append_headers` | Map<String, String> | **Yes** | Key-value pairs of header names and append values (`{}` if none) | `{"x-forwarded-for": "$client_ip"}` | Headers to append values to on outgoing request. |

### Supported Dynamic Variables
- `$client_ip`: Downstream peer IP address (authoritatively resolved from edge `x-real-ip` first, falling back to nearest right-most `x-forwarded-for`).
- `$request_id`: Unique monotonic request identifier (zero-allocation Slot 0 W3C header).
- `$scheme`: Request scheme (`http` or `https`).
- `$host`: Host authority header or URI authority.
- `$method`: HTTP request method (`GET`, `POST`, etc.).
- `$path`: Request URI path.
- `$$`: Escapes literal `$` character.

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
        "rename_headers": {
          "x-client-device": "x-device-type"
        },
        "set_headers": {
          "x-gateway": "velda-edge",
          "x-request-id": "$request_id",
          "x-client-ip": "$client_ip",
          "x-forwarded-proto": "$scheme"
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

---

## 4. Security & Anti-Evasion Invariants

1. **CVE-2022-26143 Hop-by-Hop Strip Defense**:
   - The plugin prevents malicious downstream clients from stripping essential protocol and security headers (`Host`, `Authorization`, `X-Request-Id`, `X-Real-IP`, `X-Forwarded-For`) by including them inside the `Connection` header.
2. **Anti-Shadowing Header Renaming**:
   - Destination header names are wiped clean before inserting the renamed value, eliminating header shadowing and smuggling vulnerabilities.
3. **CRLF & Header Injection Rejection**:
   - All static configuration names and values are strictly verified at compile time by `velda-sync` against RFC 9110 character bounds, preventing response splitting and CRLF injection.

---

## 5. Documentation Suite Links

For comprehensive architectural design, empirical Azure benchmarks, and security test matrices, refer to the dedicated documents:

- **[Architectural Specification](ARCHITECTURE.md)**: Internal data structures, single-pass hop-by-hop checking, and pre-partitioned static/dynamic arrays.
- **[Performance Benchmark Report](BENCHMARK.md)**: Live Azure VM cluster measurements, latency percentiles, and 1.48% overhead verification.
- **[Test Plan & Security Verification](TEST.md)**: Automated unit test suite, CVE-2022-26143 validation, and live cURL reproduction logs.

