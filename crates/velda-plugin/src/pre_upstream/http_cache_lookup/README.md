# HTTP Cache Lookup Plugin (`http_cache_lookup`)

In-memory HTTP response cache lookup plugin executed inline during the `pre_upstream` phase on Frontline Traffic Workers. Computes a zero-allocation 64-bit cache key from `(req.state.route_id, req.state.method, compiled_key_headers)`, negotiates payload variants against `Accept-Encoding` and configured priority-ordered `preferred_encodings`, and short-circuits on `HIT` via `Action::Respond(L7Response)`.

```text
+-------------------------------------------------------------------------------+
|                       HTTP CACHE LOOKUP PIPELINE                              |
+-------------------------------------------------------------------------------+
|  L7Request (req.state: RequestState, req.headers: HeaderWorkspace)            |
|    |                                                                          |
|    +---> [1] method_mask check (req.state.method)                             |
|    |       └─ unsupported method ──────────────────────────► Action::Continue |
|    +---> [2] storage.route_key_hash(route_id, method, hdrs, compiled_keys)    |
|    +---> [3] storage.lookup_variant(hash, accept_encoding, preferred, auth)   |
|            ├─ AuthBypass / Miss ───────────────────────────► Action::Continue |
|            └─ Hit ─► HeaderWorkspace + Body::Bytes ─► Action::Respond(resp)   |
+-------------------------------------------------------------------------------+
```

---

## 1. What This Plugin Does

- **Route + Method + Header-Segmented Keys**: Keys cache slots strictly by `(req.state.route_id, req.state.method, compiled_key_headers)` rather than host/path/query strings. Route and listener matching have already selected `route_id`; `key_headers` segments entries per user or tenant (`x-user-id`, `x-tenant-id`, etc.), distinguishing missing headers from empty values.
- **Priority-Ordered Variant Negotiation**:
  - When the request carries no `Accept-Encoding` header, lookup serves plain-text (`raw_body`) if cached, or misses so upstream populates plain-text.
  - When `Accept-Encoding` is present, lookup filters available compressed variants strictly through configured priority-ordered `preferred_encodings` (`zstd`, `br`, `gzip`), falling back to `raw_body` if `identity` is acceptable.
- **Direct `Action::Respond` Delivery**: On `HIT`, builds a clean `HeaderWorkspace` (`Content-Type`, `Content-Length`, `Age`, `X-Cache: HIT`, plus `Content-Encoding` / `Vary: Accept-Encoding` when applicable) paired with zero-copy `Body::Bytes` backed by the cache slab.
- **RFC 9111 §3.5 Auth Safety Shield**: Bypasses requests carrying `Authorization` when `bypass_authenticated_requests == true` and `authorization` is absent from `key_headers`, unless the cached entry carries `is_public == true`.
- **Hardware Memory Ceiling**: Validates `max_memory_mb` per plugin and across distinct `zone`s against `max_cache_memory_mb()` (`50%` of system/cgroup RAM from `velda_core::hardware`).

---

## 2. Where It Sits in Velda Edge

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `http_cache_lookup` |
| **Phase** | `pre_upstream` |
| **Worker Affinity** | Frontline Traffic Workers (`velda-worker-{i}`) |
| **PBWW (`pbww()`)** | `0` (lock-free shard `RwLock::read()` + atomic access bit) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Supported Protocols** | `SupportedProtocols::ALL_HTTP` (`HTTP/1.1`, `HTTP/2`, `HTTP/3`) |
| **Zone Pairing** | Shares [`HttpCacheStorage`](../../http_cache/storage.rs) by `zone`; must declare identical `key_headers` as `http_cache_store` on the same `zone`. |

---

## 3. Configuration Parameters (`plugins.json`)

Every field is explicitly required and unknown fields are rejected at sync (`#[serde(deny_unknown_fields)]`):

| Field | Type | Required | Constraints / Allowed Values | Description |
| :--- | :---: | :---: | :--- | :--- |
| `zone` | `string` | **Yes** | Non-empty string (must match `http_cache_store`) | Shared in-memory cache storage zone identifier. |
| `cacheable_methods` | `string[]` | **Yes** | Non-empty list of valid HTTP methods (e.g. `["GET", "HEAD"]`) | Methods evaluated for cache lookup against `req.state.method`. |
| `bypass_authenticated_requests` | `bool` | **Yes** | `true` or `false` | Bypasses lookup when `Authorization` is present unless partitioned by `key_headers` or marked public. |
| `key_headers` | `string[]` | **Yes** | Valid RFC 9110 header names (`[]` if none); must match `http_cache_store` for the same `zone` | Headers included in deterministic cache key hashing. |
| `preferred_encodings` | `string[]` | **Yes** | Non-empty ordered list from `"zstd"`, `"br"`, `"gzip"` (no duplicates) | Priority-ordered encoding allowlist evaluated against `Accept-Encoding`. |
| `max_memory_mb` | `usize` | **Yes** | `1 ..= max_cache_memory_mb()` (`50%` of host/cgroup RAM from `velda-core`) | Memory budget allocated for this zone if initialized here. |
| `default_ttl_secs` | `u64` | **Yes** | `1 ..= 31_536_000` | Default TTL fallback in seconds if this plugin initializes the zone storage. |

---

## 4. Quick Start Configuration

```json
{
  "id": "http_cache_lookup",
  "config": {
    "zone": "api_cache",
    "cacheable_methods": ["GET", "HEAD"],
    "bypass_authenticated_requests": true,
    "key_headers": ["x-tenant-id"],
    "preferred_encodings": ["zstd", "br", "gzip"],
    "max_memory_mb": 256,
    "default_ttl_secs": 300
  }
}
```

Attach paired plugins on an HTTP route (`routes.json`):

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
