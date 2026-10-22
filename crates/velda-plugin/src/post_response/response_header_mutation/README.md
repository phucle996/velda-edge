# Response Header Mutation Plugin (`response_header_mutation`)

High-performance, in-place HTTP response header manipulation plugin evaluated during the `post_response` phase. Provides explicit, user-controlled header stripping, setting, appending, and conditional insertion (`set_if_missing`).

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
| **Hot Path Latency** | ~15ns to 30ns (In-place mutation, zero heap allocation for static headers) |

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
          "x-status-reflected": "$status"
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

## 5. Dynamic Variables

Dynamic variables can be specified with a leading `$` in header value strings:

| Variable | Description |
| :--- | :--- |
| `$request_id` | Propagates the unique request ID (from `x-request-id` header or system generator) |
| `$route_id` | Propagates the matched route identifier (`x-route-id`) |
| `$upstream_addr` | Propagates the upstream backend endpoint (`x-upstream-addr`) |
| `$status` | Numeric HTTP response status code as string (e.g. `"200"`, `"503"`) |
| `$timestamp_epoch` | Monotonic Unix epoch timestamp in seconds |

---

## 6. Pipeline Ordering Guidelines

> [!TIP]
> **Interaction with `http_cache_store`**:
> - **Recommended Order**:
>   ```json
>   "plugins": ["edge_cache_store", "edge_response_mutation"]
>   ```
>   Placing `http_cache_store` **first** allows it to inspect pristine upstream headers (such as `Cache-Control`, `ETag`, `Set-Cookie`) to make accurate caching decisions. `response_header_mutation` then sanitizes response headers on the outgoing response.
