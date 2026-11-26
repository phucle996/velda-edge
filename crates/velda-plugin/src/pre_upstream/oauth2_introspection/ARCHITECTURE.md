# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`) — Deep Architectural Specification

```text
+---------------------------------------------------------------------------------------------------+
|                   OAUTH2 INTROSPECTION: HYBRID SYNC/ASYNC & 10%-RAM L3 ENGINE                     |
|                                                                                                   |
|  [Downstream Request (`L7Request`, HeaderWorkspace warm in L2)]                                   |
|         │                                                                                         |
|         ▼                                                                                         |
|  [1. Synchronous Warm Entry: `try_on_upstream_request`] (0 .await, 0 Future alloc)                |
|    • `prefetch_control_l1()` pulls 64B `IntrospectionCache` control line (`_MM_HINT_T0`)          |
|    • Extract token via pre-hashed `HeaderKey` AVX2/SSE2/NEON/SWAR lookups + split HTTP/2 Cookies  |
|    • Hash `SHA-256(boot_salt || token) -> fp: u128`                                               |
|    • `_MM_HINT_T2` (`prefetcht2`) -> L3 `BucketGroup` (`tags: TagLine([u8; 64])`)                 |
|    ├─► `Hit`: apply `insert_prehashed` claims + `hide_credentials` -> `Some(Action::Continue)`    |
|    ├─► `Stale` (`exp - now <= clamp(ttl/10, 3, 10)`): non-blocking `try_dispatch_shadow` refresh  |
|    ├─► `Forbidden` / `Unauthorized`: reject `403` / `401` (`0` IdP calls)                         |
|    └─► `Miss` (or `cache_ttl_seconds == 0`): return `None`                                        |
|         │                                                                                         |
|         ▼                                                                                         |
|  [2. Cold Yield Entry: `on_upstream_request_async` (.await yields `velda-worker-{id}`)]           |
|    • `InFlightCoordinator` (64 shards, capped at 4,096 concurrent distinct tokens)                |
|    • Coalesces duplicate concurrent misses onto 1 leader `tokio::sync::broadcast` channel         |
|    • Leader calls `IdpKeepAliveClient::introspect(token).await`:                                  |
|        ├─ Endpoint via `velda-discovery` (`SocketAddr` | `HostsFileSource` -> `DnsResolver`)      |
|        └─ Async `tokio::net::TcpStream` (+ `velda_tls::client::connect` for `https://`)           |
|    • Commits outcome to L3 SIMD bucket (`exp`/`nbf` float/int parsed, `coarse_sec()`)             |
+---------------------------------------------------------------------------------------------------+
```

---

## 1. Execution Model Isolation: Blocking Elimination

Earlier versions of `oauth2_introspection` executed synchronous `std::net::TcpStream` I/O inside `on_upstream_request`, blocking the single-threaded core-pinned Tokio runtime (`velda-worker-{id}`) whenever an unseen token arrived.

The redesigned `PreUpstreamPlugin` splits execution cleanly:
1. **`try_on_upstream_request(&self, req, target_upstream) -> Option<Action>`**:
   - Invoked first by `velda_plugin::run_pre_upstream_plugins_async` across HTTP/1.1, HTTP/2, HTTP/3, gRPC/TCP, and gRPC/UDP pipelines.
   - All warm cache paths (`Hit`, `Stale`, `Forbidden`, `Unauthorized`) and missing-token rejections execute synchronously without allocating a `Pin<Box<dyn Future>>` or yielding to the async executor.
2. **`on_upstream_request_async(...)` (`.await` Cold Path Only)**:
   - Invoked strictly when `try_on_upstream_request` returns `None` (an uncached token or `cache_ttl_seconds == 0`).
   - Yields the individual connection coroutine while resolving DNS, executing TLS handshakes, or awaiting RFC 7662 HTTP responses, allowing tens of thousands of other concurrent streams on the same `velda-worker-{id}` thread to proceed uninterrupted.

---

## 2. Three-Tier Cache Hierarchy & Dynamic 10% RAM Growth (`cache.rs`)

### 2.1 Removal of Unbounded `FxHashMap` & Global `thread_local!`
The previous `CORE_L1_CACHE` global `thread_local!` shared `fp -> CachedClaims` across config reloads without tracking scope changes, creating an authorization bypass if `required_scopes` tightened on reload; and shard `FxHashMap::retain()` scans caused $O(N^2)$ stalls without bounding heap growth.

The new design eliminates both:
- Each `Oauth2IntrospectionPlugin` owns an isolated `Arc<IntrospectionCache>` keyed by an independent OS CSPRNG 256-bit `boot_salt`, isolating cache entries cleanly across reloads and multi-tenant plugin instances.
- Token keys are strictly `SHA-256(boot_salt || token) -> u128` (`u128` integer key in GPRs), blocking algorithmic hash-flooding and shard-targeting attacks.

### 2.2 64-Byte L1D Control Line + 10% Host RAM L3 Sizing
- **`IntrospectionCache`** is compile-time verified at `size_of == 64`, `align == 64`:
  - `boot_salt: [u8; 32]` (`0x00..0x20`)
  - `shards: Option<Box<[CacheShard]>>` (`0x20..0x30`, `None` allocates `0` bytes when both TTLs are `0`)
  - `cache_ttl_secs`, `negative_ttl_secs`, `stale_refresh_secs`, `enabled`, `shard_mask`, `max_groups_log2`, `shard_shift` (`0x30..0x40`)
- **`compute_cache_geometry(total_memory_bytes, cores)`**:
  - Reads `velda_core::hardware::global_hardware_topology().memory_bytes()`.
  - Computes hard budget ceiling: `max_bytes = (total_memory_bytes / 100) * 10` (`10%` of host/cgroup RAM).
  - Allocates lean initial capacity (`4` `BucketGroup`s per shard $\approx 835\text{ KiB}$ on 64 shards) and dynamically doubles saturated shards in-place (`grow_shard_doubling`, splitting entries via bit-shift `(fp >> shard_shift) & new_mask`) while below `max_groups_per_shard`. Once the **10% RAM ceiling** is hit, `BucketGroup::evict_clock()` enforces bounded O(1) eviction.

### 2.3 Unified 64-Way `BucketGroup` Layout (`_MM_HINT_T2`)
- `tags: TagLine([u8; 64])` occupies offset `0x00..0x40` (Cache Line 0), encoding empty (`0x00`), positive (`0x01..=0x7F`), and reject (`0x81..=0xFF`, `401` or `403`) states.
- `flags: [u8; 64]` stores exact state (`ValidPass`, `ForbiddenScope`, `Unauthorized`) plus bit `0x80` (`FLAG_REVALIDATING`) for lockless single-flight shadow deduplication.
- Before acquiring a shard `RwLock::read()`, `lookup()` loads `groups_base` and `group_mask` atomically and emits `prefetcht2` (`_MM_HINT_T2` / `prfm pldl3keep`), pulling only the target 64-byte `TagLine` from L3 Shared LLC while L2 remains dedicated to `HeaderWorkspace`.
- Multi-target SIMD scanners (`AVX2` `vpcmpeqb`, `SSE2`, `aarch64` NEON, and zero-borrow 64-bit `SWAR`) evaluate all 64 tags in one pass; `__rust_alloc` is `0` on cached hits.

---

## 3. Adaptive Stale-While-Revalidate (`10%`, `3..=10s`) & `coarse_sec()`

- `compute_stale_refresh_secs(cache_ttl_seconds)`:
  - `ttl <= 3s`: `0` (no background refresh window).
  - `ttl > 3s`: `(ttl / 10).clamp(3, 10)` seconds.
- Idle pool timestamps (`PooledStream::last_used_sec`), stale entry checks, and background revalidation deadlines rely exclusively on `velda_core::clock::coarse_sec()`, avoiding `Instant::now()` / `clock_gettime` syscall overhead.
- When a shadow refresh observes `active: false` after user logout, `store_negative` atomically flips the existing L3 slot's `tags` MSB (`0x80`) and `flags` (`FLAG_UNAUTHORIZED`), immediately blocking subsequent requests.

---

## 4. `velda-discovery` DNS, TLS & Chunked Keep-Alive Client (`client.rs`)

- **URI Fidelity**: Parses `introspection_url` via `http::Uri`; distinguishes `http://` (default port `80`, plain async `TcpStream`) from `https://` (default port `443`, `velda_tls::client::connect` via `OutboundTlsConfig`), supporting IPv4, bracketed `[IPv6]`, and DNS hostnames with arbitrary ports.
- **Discovery Integration**: IP literals dial directly; hostnames query `velda_discovery::DnsResolverProvider` (RAM `DnsCache` $\to$ `/etc/hosts` `HostsFileSource` $\to$ `/etc/resolv.conf` UDP DNS with LKG resilience).
- **Wire Framing**: Parses both `Content-Length` and incremental `Transfer-Encoding: chunked` (with chunk extensions) bounded at `64 KiB`, with single transparent retry when a pooled idle Keep-Alive connection closes remotely.
