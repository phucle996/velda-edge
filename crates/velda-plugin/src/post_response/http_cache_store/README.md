# HTTP Cache Store Plugin (`http_cache_store`)

Evaluated on outgoing responses returned from upstream backends during the `post_response` lifecycle phase. Inspects method, status, and upstream `Cache-Control` eligibility per RFC 9111, derives the canonical `(route_id, method, key_headers)` key directly from `RequestState` and request headers, streams valid payloads into the shared [`HttpCacheStorage`], and emits diagnostic `X-Cache` and `Age` response headers.

```text
+-------------------------------------------------------------------------------+
|                        HTTP CACHE STORE PIPELINE                              |
+-------------------------------------------------------------------------------+
|  on_response(req_state: &RequestState, req_headers: &HeaderWorkspace, resp)   |
|    |                                                                          |
|    +---> [1] X-Cache == "HIT" or non-cacheable method ──────► return          |
|    +---> [2] status / Cache-Control (no-cache|no-store|private) ─► X-Cache: BYPASS |
|    +---> [3] RFC 9111 Auth Shield / payload > max_payload_bytes ─► BYPASS     |
|    +---> [4] hash = storage.route_key_hash(route_id, method, req_headers)     |
|    +---> [5] storage.store_bytes()  [stream_payload_to_dram -> 2 MiB slab]    |
|            ├─► X-Cache: MISS, Age: 0                                          |
|            └─► optional variant pre-compression (raw & len >= 256)            |
+-------------------------------------------------------------------------------+
```

> [!IMPORTANT]
> **Protocol & Hardware Invariants**:
> - Restricted exclusively to HTTP routes (`HTTP/1.1`, `HTTP/2`, `HTTP/3`).
> - `zone` and `key_headers` must match the corresponding `http_cache_lookup` configuration (`validate_plugin_dependencies` enforces `key_headers` parity across each shared `zone`).
> - Unknown config fields are rejected (`#[serde(deny_unknown_fields)]`), and aggregate cache budgets across all zones cannot exceed `50%` of system/cgroup memory reported by `velda-core`.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `http_cache_store` |
| **Lifecycle Phase** | `post_response` |
| **Worker Affinity** | Frontline Traffic Workers (`velda-worker-{i}`) |
| **PBWW (`pbww()`)** | `0` (optional async variant pre-compression offloaded to `tokio::task::spawn_blocking`) |
| **Supported Protocols** | `SupportedProtocols::ALL_HTTP` (`HTTP/1.1`, `HTTP/2`, `HTTP/3`) |
| **Reverse Capability** | `PluginCapability::new(AccessLevel::Write, AccessLevel::Read)` |

---

## 2. Configuration Parameters (`plugins.json`)

| Field | Type | Required | Constraints / Allowed Values | Default / Notes | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `zone` | `string` | **Yes** | Non-empty string identifier | Must match `http_cache_lookup` | Shared in-memory cache storage zone name. |
| `cacheable_methods` | `string[]` | No | Non-empty list of valid HTTP methods | `["GET", "HEAD"]` | HTTP methods eligible for cache storage (checked against `req_state.method`). |
| `cacheable_status` | `u16[]` | **Yes** | Non-empty array in `200..=599` | Explicit (e.g. `[200, 203, 204, 206, 300, 301, 404]`) | HTTP status codes eligible for caching. |
| `max_payload_bytes` | `usize` | **Yes** | `1 ..= 2_097_152` (`SLAB_SIZE`, 2 MiB) | Explicit (e.g. `2097152`) | Maximum response payload size stored in a single 2 MiB slab. |
| `default_ttl_secs` | `u64` | **Yes** | `1 ..= 31_536_000` | Explicit (e.g. `300`) | Fallback TTL in seconds when upstream omits `Cache-Control: max-age` / `s-maxage`. |
| `respect_cache_control` | `bool` | **Yes** | `true` or `false` | Explicit (`true` recommended) | Honors upstream `Cache-Control: no-cache`, `no-store`, `private`, `max-age`, `s-maxage`, and `public`. |
| `bypass_authenticated_requests` | `bool` | **Yes** | `true` or `false` | Explicit (`true` recommended) | Enforces RFC 9111 §3.5: bypasses requests carrying `Authorization` unless `public` or partitioned via `key_headers`. |
| `key_headers` | `string[]` | **Yes** | Valid RFC 9110 header names (`[]` if none); must match `http_cache_lookup` in same `zone` | Explicit | Request header names hashed together with `(route_id, method)`. |
| `async_precompression` | `bool` | **Yes** | `true` or `false` | Explicit | Offloads background variant compression (`gzip`, `brotli`, `zstd`) via `spawn_blocking` when uncompressed body `>= 256` bytes and MIME is compressible. |
| `max_memory_mb` | `usize` | **Yes** | `1 ..= max_cache_memory_mb()` (`50%` of RAM/cgroup limit from `velda-core`) | Explicit | Memory ceiling for this zone if created here (runtime clamps to `SLAB_SIZE * 2 ..= max_cache_memory_bytes()`). |

---

## 3. Configuration Example (`plugins.json`) & Route Attachment (`routes.json`)

```json
{
  "id": "http_cache_store",
  "config": {
    "zone": "api_cache",
    "cacheable_methods": ["GET", "HEAD"],
    "cacheable_status": [200, 203, 204, 206, 300, 301, 404],
    "max_payload_bytes": 2097152,
    "default_ttl_secs": 300,
    "respect_cache_control": true,
    "bypass_authenticated_requests": true,
    "key_headers": ["x-tenant-id"],
    "async_precompression": true,
    "max_memory_mb": 256
  }
}
```

For optimal caching + compression ordering, place `http_cache_store` before `response_compression` on routes:

```json
{
  "id": "route_api",
  "match": {
    "path": { "prefix": "/api/v1/" }
  },
  "plugins": [
    "http_cache_lookup",
    "http_cache_store",
    "response_compression"
  ],
  "upstream": "cluster_main"
}
```

---

## 4. Dual-Mode Caching & Micro-Architectural Foundations

1. **Eager Multi-Variant (`[http_cache_store, response_compression]`)**: `http_cache_store` captures pristine uncompressed bytes as `raw_body`, emits `X-Cache: MISS` and `Age: 0`, and optionally generates `gzip`/`brotli`/`zstd` variants asynchronously (if `async_precompression == true`) or synchronously (`false`).
2. **Lazy Single-Variant (`[response_compression, http_cache_store]`)**: When upstream or an earlier plugin already encoded the body (`Content-Encoding`), `http_cache_store` stores only that compressed variant without double-compressing.
3. **Compile-Time DRAM Confinement (`dram.rs`)**: `stream_payload_to_dram` dispatches at compile time:
   - `x86_64 + avx2`: `_mm256_stream_si256` (`32B` non-temporal stores when `len >= 64`) or `_mm_stream_si128` (`32..64`).
   - `x86_64` baseline (`sse2`): `_mm_stream_si128` (`16B` non-temporal stores when `len >= 32`).
   - `not(x86_64)` (`aarch64` / portable): `std::ptr::copy_nonoverlapping`.
4. **Shared `RequestState` Keying**: `http_cache_lookup` and `http_cache_store` both invoke `storage.route_key_hash(req_state, req_headers, compiled_key_headers)` — requiring zero transient request headers (`x-velda-cache-*` does not exist) and zero cross-phase header allocations.
5. **Pure Payload Storage**: Stores only payload slabs (`raw_body`, `variants`), `status`, and `Content-Type` inside 64 lock-striped CLOCK-evicted shards (`#[repr(align(64))]`), preventing accidental leakage of response headers (`Set-Cookie`, etc.).

---

## 5. Documentation Directory

- **[Operational Runbook](RUNBOOK.md)**: Parameter table, metrics reference, and production incident playbooks.
- **[Performance Benchmark Report](BENCHMARK.md)**: Benchmark evaluation matrix and target tiers.
- **[Test Verification Report](TEST.md)**: Observed `cargo test` results covering storage, key partitioning, validation, and variant negotiation.
