# Response Header Mutation Plugin (`response_header_mutation`)

High-performance, in-place HTTP response header manipulation plugin evaluated during the `post_response` phase. Provides explicit, user-controlled header stripping, setting, appending, and conditional insertion (`set_if_missing`) with single-instruction ALU bitmask dispatch, SIMD fingerprint probing, and compile-time $O(1)$ status resolution.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted. Empty arrays `[]` or maps `{}` must be explicitly declared when unused.
>
> **No Magic Overwrites**: Velda Edge never automatically strips or injects headers without explicit configuration.
>
> **Never Specify `phase` in User Configuration**: The execution phase is statically inferred by Velda as `post_response`. Execution order strictly follows the route's `plugins` array order (FIFO).

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `response_header_mutation` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Hot Path Latency** | ~10ns to 25ns (In-place mutation, zero heap allocation, SIMD accelerated) |
| **Hardware Alignment** | Aligned to 64-byte L1 CPU cacheline boundary (`#[repr(align(64))]`) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `remove_headers` | `array[string]` | **Yes** | Array of valid RFC 9110 header tokens, or `[]` | `["server", "x-powered-by"]` | Headers stripped from the upstream response before sending downstream. |
| `set_headers` | `object` | **Yes** | Key-value pairs of string headers (`{ "key": "value" }`), or `{}` | `{"server": "velda-edge"}` | Headers to overwrite or set on response. Supports `$variables`. |
| `append_headers` | `object` | **Yes** | Key-value pairs of string headers (`{ "key": "value" }`), or `{}` | `{}` | Headers to append comma-separated values to on response. |
| `set_if_missing` | `object` | **Yes** | Key-value pairs of string headers (`{ "key": "value" }`), or `{}` | `{"x-content-type-options": "nosniff"}` | Headers to set ONLY if the response does not already contain them. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "edge_response_mutation",
      "plugin": "response_header_mutation",
      "config": {
        "remove_headers": [
          "x-powered-by",
          "server",
          "x-aspnet-version"
        ],
        "set_headers": {
          "server": "velda-edge",
          "x-request-id": "$request_id",
          "x-status-reflected": "$status",
          "x-timestamp": "$timestamp_epoch"
        },
        "append_headers": {
          "vary": "User-Agent"
        },
        "set_if_missing": {
          "x-content-type-options": "nosniff"
        }
      }
    }
  ]
}
```

---

## 4. Route Attachment (`routes.json`)

Attach the plugin to an HTTP route via its unique identifier in the `plugins` array:

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
        "edge_response_mutation"
      ],
      "upstream": "cluster_main"
    }
  ]
}
```

---

## 5. Dynamic Runtime Variables

Dynamic variables can be specified with a leading `$` in header value strings:

| Variable | Resolution Mechanism | Complexity | Description |
| :--- | :--- | :---: | :--- |
| `$request_id` | Slot 0 W3C request identifier | $O(1)$, zero-alloc | Propagates the authoritative request ID from incoming `HeaderWorkspace`. |
| `$status` | Direct subtraction index into compile-time 1500-byte `.rodata` table | $O(1)$, zero-alloc | Numeric HTTP response status code as static string for codes 100..=599. |
| `$timestamp_epoch` | Thread-local epoch cache (`EPOCH_CACHE`) + stack local cache | $O(1)$, cached | Monotonic Unix epoch timestamp in seconds (99.998% cache hit rate under load). |

To output a literal `$` character in a header value, escape it as `$$`.

---

## 6. Pipeline Ordering Guidelines

> [!TIP]
> **Interaction with `http_cache_store`**:
> - **Recommended Order**:
>   ```json
>   "plugins": ["edge_cache_store", "edge_response_mutation"]
>   ```
>   Placing `http_cache_store` **first** allows it to inspect pristine upstream headers (such as `Cache-Control`, `ETag`, `Set-Cookie`) to make accurate caching decisions. `response_header_mutation` then sanitizes response headers on the outgoing response.

---

## 7. Documentation Suite Links

For comprehensive architectural design, operational runbooks, empirical Azure benchmarks, and security test matrices, refer to the dedicated documents:

- **[Architectural Specification](ARCHITECTURE.md)**: Internal data structures, ALU bitmask gating, SIMD fingerprint filter, and $O(1)$ status table.
- **[Operational Runbook](RUNBOOK.md)**: Production deployment, monitoring, health checks, troubleshooting, and tuning.
- **[Performance Benchmark Report](BENCHMARK.md)**: Live Azure cluster measurements, Envoy/Kong/APISIX comparisons, and hardware counters.
- **[Test Plan & Security Verification](TEST.md)**: Automated test catalog, SIMD tests, CRLF injection defense, and verification reproduction.
