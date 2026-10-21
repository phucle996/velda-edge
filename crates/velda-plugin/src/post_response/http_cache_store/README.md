# HTTP Cache Store Plugin (`http_cache_store`)

Evaluated on outgoing responses returned from upstream backends during the `post_response` lifecycle phase. Inspects response cacheability per RFC 9111, stores valid payloads into the designated in-memory [`HttpCacheZone`], and injects diagnostic `X-Cache` and `Age` response headers.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `http_cache_store` |
| **Lifecycle Phase** | `post_response` |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Reverse Capability** | `PluginCapability::new(AccessLevel::Write, AccessLevel::Read)` |
| **Hot Path Latency** | ~35ns store (Zero disk I/O, zero network calls) |

---

## 2. Configuration Schema

```json
{
  "zone": "api_cache",
  "cacheable_status": [200, 203, 204, 206, 300, 301, 404],
  "max_payload_bytes": 2097152,
  "default_ttl_secs": 300,
  "respect_cache_control": true,
  "bypass_authenticated_requests": true,
  "key_headers": ["x-user-id", "authorization"],
  "async_precompression": true,
  "max_memory_mb": 256
}
```

- **`zone`**: Identifier of the shared in-memory cache zone (must match `http_cache_lookup`).
- **`cacheable_status`**: Status codes permitted to be stored in cache.
- **`max_payload_bytes`**: Maximum body size in bytes to cache (default: `2097152` = 2MB). Responses exceeding this are bypassed to avoid RAM exhaustion.
- **`respect_cache_control`**: Honors upstream `Cache-Control: no-store` and `private`, and extracts `s-maxage` / `max-age` values for TTL.
- **`bypass_authenticated_requests`**: If `true` (default), enforces **RFC 9111 §3.5 Safety Shield**: responses to requests with an `Authorization` header are bypassed (`X-Cache: BYPASS`) unless upstream explicitly marks the response `public` or `s-maxage`, or partitioned via `key_headers`.
- **`key_headers`**: Array of request header names (e.g. `["x-user-id", "authorization"]`) matching `http_cache_lookup` for Per-User / Multi-Tenant Caching.
- **`async_precompression`**: If `true` (default), activates **Asynchronous Background Pre-compression**. On Cache Miss, the uncompressed response is served immediately to the client with **0ms wait time**, while multi-variant pre-compression (Gzip L1 SIMD, Brotli L2, Zstd L1) is offloaded to a background task and merged into cache via `zone.merge_variants`.

---

## 3. Dual-Mode Caching Architecture (Eager vs Lazy)

`http_cache_store` seamlessly supports **two distinct operational strategies** purely through pipeline ordering declarative configuration:

### Strategy A: Eager Multi-Variant with Asynchronous Background Pre-compression (Default)
**Pipeline Order**: `[..., http_cache_store, response_compression]`
* **Mechanism**:
  1. `http_cache_store` executes first and receives raw, uncompressed payload bytes from upstream.
  2. Stores pristine canonical `raw_body` into RAM cache immediately (0ms wait time for client).
  3. When `async_precompression` is enabled (default), offloads Pareto-tuned multi-variant compression (`gzip: 1`, `brotli: 2`, `zstd: 1`) to a background worker (`tokio::task::spawn_blocking`).
  4. The background worker merges computed variants into `HttpCacheZone` via `merge_variants`.
  5. Subsequent downstream requests with any `Accept-Encoding` achieve instant **100k+ RPS Zero-CPU Cache HITs**.
* **Best For**: High-throughput Hot APIs, public websites, and traffic where maximum throughput and instantaneous multi-client readiness are paramount.

### Strategy B: Lazy Demand-Driven (Memory-Optimized Mode)
**Pipeline Order**: `[..., response_compression, http_cache_store]`
* **Mechanism**:
  1. `response_compression` executes first, compressing the raw body into the specific codec requested by the current client (e.g., Brotli 145B) and injecting `Content-Encoding: br`.
  2. `http_cache_store` executes second, observes `Content-Encoding`, and switches to **Single-Variant Ingestion**.
  3. Merges only this requested variant into the zone (`variants.brotli = Some(145B)`).
  4. Subsequent requests from other client types seamlessly merge additional variants into the existing `CacheEntry` via `store_variant` without recompressing or clearing prior variants.
* **Best For**: Long-tail catalog APIs (millions of rarely-accessed URLs), memory-constrained edge hardware, or traffic where 95%+ of clients share the same modern codec.

---

## 4. Invariants & Safeguards

1. **Pure Payload Model**: Cache zones never persist request/response headers maps (`HeaderMap`). Only payload bytes (`raw_body`, `variants`), status code, and `Content-Type` are stored.
2. **Zero Double-Compression**: If a response carries `Content-Encoding`, `http_cache_store` never attempts to recompress the payload.
3. **Atomic Single-Key Invalidation**: All variants for a URL live in a unified `CacheEntry` keyed by URI hash. Purging a route purges all variants atomically in $O(1)$ time.
