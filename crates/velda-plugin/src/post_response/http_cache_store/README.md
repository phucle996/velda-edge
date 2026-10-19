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
  "max_memory_mb": 256
}
```

- **`zone`**: Identifier of the shared in-memory cache zone (must match `http_cache_lookup`).
- **`cacheable_status`**: Status codes permitted to be stored in cache.
- **`max_payload_bytes`**: Maximum body size in bytes to cache (default: `2097152` = 2MB). Responses exceeding this are bypassed to avoid RAM exhaustion.
- **`respect_cache_control`**: Honors upstream `Cache-Control: no-store` and `private`, and extracts `s-maxage` / `max-age` values for TTL.
