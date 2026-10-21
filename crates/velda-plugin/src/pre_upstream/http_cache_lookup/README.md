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
  "bypass_authenticated_requests": true,
  "key_headers": ["x-user-id", "authorization"],
  "max_memory_mb": 256,
  "default_ttl_secs": 300
}
```

- **`zone`**: Identifier of the shared in-memory cache zone (shared with `http_cache_store`).
- **`cacheable_methods`**: HTTP methods eligible for caching (default: `["GET", "HEAD"]`).
- **`ignore_query_order`**: Alphabetically sorts query parameters so `/items?a=1&b=2` and `/items?b=2&a=1` hit the same cache entry.
- **`honor_client_no_cache`**: If `true`, honors client `Cache-Control: no-cache` requests by bypassing cache. Default `false` to protect backends from client-induced cache busting.
- **`bypass_authenticated_requests`**: If `true` (default), enforces **RFC 9111 §3.5 Safety Shield**: requests with an `Authorization` header bypass cache lookup unless the cached entry is explicitly declared `public` by upstream, or partitioned via `key_headers`. Prevents private user data leaks.
- **`key_headers`**: Array of request header names (e.g. `["x-user-id", "authorization"]`) to incorporate into the cache key. Enables safe, isolated **Per-User / Multi-Tenant Caching**. Header names are normalized to lowercase and sorted deterministically.

---

## 3. RFC 9111 §3.5 Authentication Safety Shield

To prevent accidental cross-user data leakage when private APIs are mistakenly configured with cache plugins:
1. **Automatic Bypass**: Requests with an `Authorization` header are bypassed by default (`Action::Continue`), unless `key_headers` partitions by the token, or the entry was stored with `Cache-Control: public` / `s-maxage`.
2. **Per-User Caching**: When `key_headers` includes `authorization` or a user discriminator (such as `x-user-id` or session cookie), each user receives a mathematically isolated cache key:
   $$\text{Key} = \text{Method} + \text{Host} + \text{Path} + \text{Query} + \mathbf{|\text{authorization}=...|\text{x-user-id}=...}$$
   Alice and Bob can both cache their personal profile or dashboard safely without cross-contamination.

---

## 4. Smart Variant Matching in Cache Lookup

On `CACHE HIT`, `http_cache_lookup` negotiates content coding directly against in-memory variants via `CacheEntry::negotiate_smart_variant`:

1. **Content Negotiation**: Inspects the incoming request's `Accept-Encoding` header.
2. **Zero-IO / Hit Over Miss**:
   - If the client accepts multiple codecs and an acceptable pre-compressed variant exists in RAM (`zstd`, `br`, or `gzip`), it is selected and served immediately with zero CPU compression overhead.
   - Attaches `Content-Encoding: <algo>`, `Content-Length: <variant.len()>`, `Age`, `X-Cache: HIT`, and `Vary: Accept-Encoding`.
3. **Lazy Miss Fallback**:
   - In Lazy Demand-Driven mode, if a `CacheEntry` exists but lacks a representation acceptable to the client (e.g., entry has only `br`, but client is a legacy tool requiring `identity`), `http_cache_lookup` cleanly falls through to `Action::Continue` (Cache MISS).
   - Upstream is invoked to retrieve the missing representation, which `http_cache_store` merges into the entry for subsequent clients.
