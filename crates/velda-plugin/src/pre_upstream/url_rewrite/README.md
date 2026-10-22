# URL Rewrite Plugin (`url_rewrite`)

Production-grade, zero-allocation URI path and query string transformation plugin evaluated during the `pre_upstream` execution phase. Adapts public edge routing contracts into internal microservice endpoint structures with multi-level optimization and RFC 3986 path traversal protection.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `url_rewrite` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::new(AccessLevel::Write, AccessLevel::None)` |
| **Payload Passthrough** | **100% Zero-Copy streaming**. Request and response bodies are never buffered |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Single Responsibility** | Operates strictly on URI (`path` and `query`). `Host` / `:authority` header mutation belongs to [`header_mutation`](../header_mutation/) |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `path_rewrite_mode` | String | **Yes** | `"prefix"`, `"regex"`, `"exact"`, or `"none"` | `"prefix"` (prefix rewriting) / `"none"` (query only) | Path rewriting strategy to evaluate. |
| `path_match` | String | **Yes** | Prefix or regex pattern (empty string `""` if mode is `"none"`) | `"/api/v1"` | Pattern matched against incoming URI path. |
| `path_replacement` | String | **Yes** | Replacement path string (must start with `/` if mode is active, or `""` if `"none"`) | `""` (for strip) / `"/internal/v2"` (replace) | Replacement target path. Supports regex capture tokens `$1`, `$2` in `"regex"` mode. |
| `query_rewrite_mode` | String | **Yes** | `"keep"`, `"strip"`, `"replace"`, or `"append"` | `"keep"` | Query string transformation policy. |
| `query_replacement` | String / null | **Yes** | Non-empty string when mode is `"replace"` or `"append"`, otherwise `null` | `null` | Static query string parameter payload to substitute or append. |
| `sanitize_path` | Boolean | **Yes** | `true` or `false` | `true` | Enables RFC 3986 normalization (collapses redundant `//`, strips `/./`, blocks `/../` traversal). |

---

## 3. Query String Handling Policies

- `"keep"`: Preserves downstream client query string intact. `query_replacement` must be `null`.
- `"strip"`: Completely removes query string before upstream forwarding. `query_replacement` must be `null`.
- `"replace"`: Replaces entire query string with `query_replacement`.
- `"append"`: Appends `query_replacement` to existing query parameters (e.g. `gateway=velda&cluster=prod`).

---

## 4. Configuration Examples

### Prefix Rewrite Example (`plugins.json`)

Strip `/api/v1` prefix so upstream microservice receives `/orders/42`:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "url_rewrite",
      "config": {
        "path_rewrite_mode": "prefix",
        "path_match": "/api/v1",
        "path_replacement": "",
        "query_rewrite_mode": "keep",
        "query_replacement": null,
        "sanitize_path": true
      }
    }
  ]
}
```

### Regex Rewrite with Query Append (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "url_rewrite",
      "config": {
        "path_rewrite_mode": "regex",
        "path_match": "^/users/([0-9]+)/orders/([0-9]+)$",
        "path_replacement": "/orders/$2/user/$1",
        "query_rewrite_mode": "append",
        "query_replacement": "source=edge_gateway",
        "sanitize_path": true
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
    "url_rewrite",
    "jwt_auth"
  ]
}
```
