# Response Header Mutation Plugin (`response_header_mutation`)

High-performance, in-place HTTP response header manipulation plugin evaluated during the `post_response` phase. Provides explicit, user-controlled header stripping, setting, appending, and conditional insertion (`set_if_missing`).

> [!IMPORTANT]
> **No Magic Overwrites**: Velda Edge never automatically strips or injects headers without explicit configuration. All mutation lists (`remove`, `set`, `add`, `set_if_missing`) default to empty arrays.
>
> **Never Specify `phase` in User Configuration**: The execution phase is statically inferred by Velda as `post_response`. Execution order strictly follows the route's `plugins` array order (FIFO).

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `response_header_mutation` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Hot Path Latency** | ~15ns to 30ns (In-place mutation, zero heap allocation for static headers) |

---

## 2. Configuration Schema

```json
{
  "id": "response_header_mutation",
  "config": {
    "remove": [
      "x-powered-by",
      "server",
      "x-aspnet-version",
      "x-runtime"
    ],
    "set": [
      { "name": "server", "value": "my-gateway" },
      { "name": "x-request-id", "value": "$request_id" },
      { "name": "x-status-reflected", "value": "$status" }
    ],
    "add": [
      { "name": "vary", "value": "User-Agent" }
    ],
    "set_if_missing": [
      { "name": "x-content-type-options", "value": "nosniff" }
    ]
  }
}
```

Both JSON key-value objects (`"set": { "server": "my-gateway" }`) and array format (`"set": [{"name": "server", "value": "my-gateway"}]`) are supported.

---

## 3. Dynamic Variables

Dynamic variables can be specified with a leading `$` in header value strings:

| Variable | Description |
| :--- | :--- |
| `$request_id` | Propagates the unique request ID (from `x-request-id` header or fallback) |
| `$route_id` | Propagates the matched route identifier (`x-route-id`) |
| `$upstream_addr` | Propagates the upstream backend endpoint (`x-upstream-addr`) |
| `$status` | Numeric HTTP response status code as string (e.g. `"200"`, `"503"`) |
| `$timestamp_epoch` | Monotonic Unix epoch timestamp in seconds |

---

## 4. Pipeline Execution Order & Caveats

> [!WARNING]
> **Plugin Chain Execution Order (FIFO)**:
> Plugins in the `post_response` phase execute in the exact order declared in the route's `plugins` array.
>
> **Interaction with `http_cache_store`**:
> - **Recommended Order**:
>   ```json
>   "plugins": ["http_cache_store", "response_header_mutation"]
>   ```
>   Placing `http_cache_store` **first** allows it to evaluate genuine upstream headers (such as `Cache-Control`, `ETag`, `Set-Cookie`) to make accurate caching decisions. `response_header_mutation` will then sanitize headers on the outgoing response.
> - **Caution with Cache-Control**: Never use `response_header_mutation` to insert `Cache-Control: no-cache, no-store` before `http_cache_store`, as doing so will cause the cache engine to bypass caching.
