# HTTP Cache Lookup Plugin (`http_cache_lookup`)

High-performance in-memory cache lookup plugin evaluated during the `pre_upstream` lifecycle phase. Computes normalized cache keys, inspects the designated [`HttpCacheZone`], and serves cached responses with zero-copy payload bytes immediately to the client on `CACHE HIT`.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `http_cache_lookup` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | HTTP only (`HTTP/1.1`, `HTTP/2`, `HTTP/3`) |
| **Hot Path Latency** | ~25ns lookup; Zero disk I/O, zero network calls |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `zone` | String | **Yes** | Non-empty alphanumeric string (must match `http_cache_store`) | `"api_cache"` | Shared in-memory cache zone identifier. |
| `cacheable_methods` | Array of strings | **Yes** | Non-empty list of HTTP methods (`"GET"`, `"HEAD"`) | `["GET", "HEAD"]` | HTTP methods eligible for cache lookup. |
| `ignore_query_order` | Boolean | **Yes** | `true` or `false` | `true` | Sorts query parameters alphabetically so `/items?a=1&b=2` and `/items?b=2&a=1` hit the same entry. |
| `honor_client_no_cache` | Boolean | **Yes** | `true` or `false` | `false` | If true, honors client `Cache-Control: no-cache` / `Pragma: no-cache`. Set `false` to prevent cache-busting DoS. |
| `bypass_authenticated_requests` | Boolean | **Yes** | `true` or `false` | `true` | Enforces RFC 9111 §3.5: requests with `Authorization` bypass cache unless partitioned via `key_headers`. |
| `key_headers` | Array of strings | **Yes** | List of valid RFC 9110 header names (`[]` if none) | `["x-user-id", "authorization"]` | Request header names incorporated into cache key for per-user / multi-tenant caching. |
| `max_memory_mb` | Integer (usize) | **Yes** | `1 ..= 1048576` (1MB to 1TB) | `256` | Maximum memory limit in megabytes allocated for the in-memory cache zone. |
| `default_ttl_secs` | Integer (u64) | **Yes** | `1 ..= 31536000` (1s to 1 year) | `300` (5 minutes) | Fallback TTL in seconds when upstream response omits `Cache-Control: max-age`. |

---

## 3. RFC 9111 §3.5 Authentication Safety Shield

To prevent accidental cross-user data leakage when private APIs are mistakenly configured with cache plugins:
1. **Automatic Bypass**: Requests with an `Authorization` header are bypassed by default (`Action::Continue`), unless `key_headers` partitions by the token, or the entry was stored with `Cache-Control: public` / `s-maxage`.
2. **Per-User Caching**: When `key_headers` includes `authorization` or a user discriminator (such as `x-user-id` or session cookie), each user receives a mathematically isolated cache key:
   $$\text{Key} = \text{Method} + \text{Host} + \text{Path} + \text{Query} + \mathbf{|\text{authorization}=...|\text{x-user-id}=...}$$

---

## 4. Configuration Examples

### Production Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "http_cache_lookup",
      "config": {
        "zone": "api_cache",
        "cacheable_methods": [
          "GET",
          "HEAD"
        ],
        "ignore_query_order": true,
        "honor_client_no_cache": false,
        "bypass_authenticated_requests": true,
        "key_headers": [
          "x-user-id",
          "authorization"
        ],
        "max_memory_mb": 256,
        "default_ttl_secs": 300
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "catalog-route",
  "match": {
    "path": "/api/v1/catalog"
  },
  "upstream_id": "catalog-backend",
  "plugins": [
    "http_cache_lookup",
    "jwt_auth",
    "http_cache_store"
  ]
}
```
