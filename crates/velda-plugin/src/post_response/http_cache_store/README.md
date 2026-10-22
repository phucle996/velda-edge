# HTTP Cache Store Plugin (`http_cache_store`)

Evaluated on outgoing responses returned from upstream backends during the `post_response` lifecycle phase. Inspects response cacheability per RFC 9111, stores valid payloads into the designated in-memory [`HttpCacheZone`], and injects diagnostic `X-Cache` and `Age` response headers.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **Strict Protocol Invariant**: Restricted exclusively to HTTP routes (`http/1.1`, `http/2`, `http/3`).
>
> **Zone Pairing Invariant**: The `zone` parameter must match the corresponding `zone` declared in `http_cache_lookup` for the cache entry to be retrieved.

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

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `zone` | `string` | **Yes** | Non-empty string identifier | `"default"` | Identifier of the shared in-memory cache zone (must match `http_cache_lookup`). |
| `cacheable_status` | `array[integer]` | **Yes** | Non-empty array of HTTP status codes (`200..=599`) | `[200, 203, 204, 206, 300, 301, 404]` | HTTP status codes eligible for caching. |
| `max_payload_bytes` | `integer` | **Yes** | `1..=67108864` (up to 64MB) | `2097152` (2MB) | Maximum response body size in bytes eligible for caching. |
| `default_ttl_secs` | `integer` | **Yes** | `1..=31536000` (1 second to 1 year) | `300` | Default TTL in seconds when upstream response omits `Cache-Control: max-age` / `s-maxage`. |
| `respect_cache_control` | `boolean` | **Yes** | `true` or `false` | `true` | Honors upstream `Cache-Control: no-store` / `private` and extracts upstream max-age. |
| `bypass_authenticated_requests` | `boolean` | **Yes** | `true` or `false` | `true` | Enforces RFC 9111 §3.5 safety shield: bypasses caching requests carrying `Authorization` unless marked `public`. |
| `key_headers` | `array[string]` | **Yes** | Array of valid RFC 9110 header tokens, or `[]` | `[]` | Custom request header names incorporated into cache key for segmented/multi-tenant caching. |
| `async_precompression` | `boolean` | **Yes** | `true` or `false` | `true` | Activates asynchronous background pre-compression (Gzip, Brotli, Zstd) for 0ms client wait time on Cache Miss. |
| `max_memory_mb` | `integer` | **Yes** | `1..=65536` (up to 64GB) | `256` | Maximum memory allocated to this zone in megabytes if instantiated by this plugin. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "edge_cache_store",
      "plugin": "http_cache_store",
      "config": {
        "zone": "default",
        "cacheable_status": [200, 203, 204, 206, 300, 301, 404],
        "max_payload_bytes": 2097152,
        "default_ttl_secs": 300,
        "respect_cache_control": true,
        "bypass_authenticated_requests": true,
        "key_headers": [],
        "async_precompression": true,
        "max_memory_mb": 256
      }
    }
  ]
}
```

---

## 4. Route Attachment (`routes.json`)

Attach the plugin to an HTTP route via its unique identifier in the `plugins` array. For optimal caching and compression, place `http_cache_store` before `response_compression`:

```json
{
  "routes": [
    {
      "id": "route_api",
      "match": {
        "path": {
          "prefix": "/api/v1/"
        }
      },
      "plugins": [
        "edge_cache_lookup",
        "edge_cache_store",
        "edge_compressor"
      ],
      "upstream": "cluster_main"
    }
  ]
}
```

---

## 5. Dual-Mode Caching Architecture (Eager vs Lazy)

`http_cache_store` seamlessly supports **two distinct operational strategies** purely through pipeline ordering declarative configuration:

### Strategy A: Eager Multi-Variant with Asynchronous Background Pre-compression (Default)
**Pipeline Order**: `[..., http_cache_store, response_compression]`
* **Mechanism**:
  1. `http_cache_store` executes first and receives raw, uncompressed payload bytes from upstream.
  2. Stores pristine canonical `raw_body` into RAM cache immediately (0ms wait time for client).
  3. When `async_precompression` is enabled (`true`), offloads Pareto-tuned multi-variant compression (`gzip: 1`, `brotli: 2`, `zstd: 1`) to a background task (`tokio::task::spawn_blocking`).
  4. The background worker merges computed variants into `HttpCacheZone` via `merge_variants`.
  5. Subsequent downstream requests with any `Accept-Encoding` achieve instant **Zero-CPU Cache HITs**.
* **Best For**: High-throughput Hot APIs, public websites, and traffic where maximum throughput and instantaneous multi-client readiness are paramount.

### Strategy B: Lazy Demand-Driven (Memory-Optimized Mode)
**Pipeline Order**: `[..., response_compression, http_cache_store]`
* **Mechanism**:
  1. `response_compression` executes first, compressing the raw body into the specific codec requested by the current client (e.g., Brotli) and injecting `Content-Encoding: br`.
  2. `http_cache_store` executes second, observes `Content-Encoding`, and switches to **Single-Variant Ingestion**.
  3. Merges only this requested variant into the zone (`variants.brotli = Some(...)`).
  4. Subsequent requests from other client types seamlessly merge additional variants into the existing `CacheEntry` via `store_variant` without recompressing or clearing prior variants.
* **Best For**: Long-tail catalog APIs, memory-constrained edge hardware, or traffic where 95%+ of clients share the same modern codec.

---

## 6. Invariants & Safeguards

1. **Pure Payload Model**: Cache zones never persist request/response headers maps (`HeaderMap`). Only payload bytes (`raw_body`, `variants`), status code, and `Content-Type` are stored.
2. **Zero Double-Compression**: If a response carries `Content-Encoding`, `http_cache_store` never attempts to recompress the payload.
3. **Atomic Single-Key Invalidation**: All variants for a URL live in a unified `CacheEntry` keyed by URI hash. Purging a route purges all variants atomically in $O(1)$ time.
