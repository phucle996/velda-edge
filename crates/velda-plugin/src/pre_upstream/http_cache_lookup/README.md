# HTTP Cache Lookup Plugin (`http_cache_lookup`)

High-performance in-memory cache lookup plugin evaluated during the `pre_upstream` lifecycle phase. Computes normalized cache keys, inspects the designated [`HttpCacheZone`], and serves cached responses with zero-copy payload bytes immediately to the client on `CACHE HIT`.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `http_cache_lookup` |
| **Lifecycle Phase** | `pre_upstream` |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Hot Path Latency** | ~25ns lookup (Zero disk I/O, zero network calls) |

---

## 2. Configuration Schema

```json
{
  "zone": "api_cache",
  "cacheable_methods": ["GET", "HEAD"],
  "ignore_query_order": true,
  "honor_client_no_cache": false,
  "max_memory_mb": 256,
  "default_ttl_secs": 300
}
```

- **`zone`**: Identifier of the shared in-memory cache zone (shared with `http_cache_store`).
- **`cacheable_methods`**: HTTP methods eligible for caching (default: `["GET", "HEAD"]`).
- **`ignore_query_order`**: Alphabetically sorts query parameters so `/items?a=1&b=2` and `/items?b=2&a=1` hit the same cache entry.
- **`honor_client_no_cache`**: If `true`, honors client `Cache-Control: no-cache` requests by bypassing cache. Default `false` to protect backends from client-induced cache busting.
