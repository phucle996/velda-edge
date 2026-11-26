# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`) — Deep Architectural Specification

```text
+---------------------------------------------------------------------------------------------------+
|             OAUTH2 INTROSPECTION: SHARE-NOTHING TWO-TIER CACHE + ISOLATED ETP ADAPTERS            |
|                                                                                                   |
|  [Downstream Request (`L7Request`, HeaderWorkspace warm in L2)]                                   |
|         │                                                                                         |
|         ▼                                                                                         |
|  [1. Synchronous Warm Entry: `try_on_upstream_request`] (0 .await, 0 Future alloc)                |
|    • `prefetch_control_l1()` pulls 64B `IntrospectionCache` control line (`_MM_HINT_T0`/`pldl1keep`)
|    • Extract token via pre-hashed `HeaderKey` / split HTTP/2 Cookies / Query; reject `> 16 KiB`   |
|    • Hash `SHA-256(boot_salt || 0x00 || token) -> fp: u128`                                       |
|    • Select core-local lane `&lanes[current_worker_lane() & lane_mask]`                           |
|    • Issue `_MM_HINT_T2` (`pldl3keep`) -> Hot L3 `BucketGroup` (`tags: TagLine([u8; 64])`)        |
|    ├─► `Hit`: apply `insert_prehashed` claims + `hide_credentials` -> `Some(Action::Continue)`    |
|    ├─► `Stale` (`exp - now <= clamp(ttl/10, 3, 10)`): non-blocking `try_dispatch_shadow` refresh  |
|    ├─► `Forbidden` / `Unauthorized`: reject `403` / `401` (`0` external calls)                    |
|    └─► `Miss` (or `cache_ttl_seconds == 0`): return `None`                                        |
|         │                                                                                         |
|         ▼                                                                                         |
|  [2. Cold Yield Entry: `on_upstream_request_async` (.await yields `velda-worker-{id}`)]           |
|    • `InFlightCoordinator` (`inflight.rs`: 64 shards x 64 slots = 4,096 distinct tokens max)      |
|    • Leader queries `providers::<etp_kind>::introspect(pool, params, token).await` (`velda-etp`)  |
|    • Commits `(exp_opt, ColdOutcome)` to Leader lane + broadcasts to Followers (each caches locally)
+---------------------------------------------------------------------------------------------------+
```

---

## 1. Execution Model Isolation: Hot Sync vs Coalesced Cold ETP I/O

1. **`try_on_upstream_request(&self, req, target_upstream) -> Option<Action>`**:
   - Invoked first by async pipelines across HTTP/1.1, HTTP/2, HTTP/3, gRPC/TCP, and gRPC/UDP.
   - All warm cache decisions (`Hit`, `Stale`, `Forbidden`, `Unauthorized`), oversized (`> 16 KiB`) checks, and missing-token rejections complete synchronously without allocating a `Pin<Box<dyn Future>>`.
2. **`on_upstream_request_async(...)` (`inflight.rs`)**:
   - Invoked strictly on `CacheLookup::Miss` or positive cache bypass (`cache_ttl_seconds == 0`).
   - `InFlightCoordinator` (`INFLIGHT_SHARDS = 64`, `MAX_INFLIGHT_PER_SHARD = 64`) coalesces simultaneous misses on identical `fp: u128` via `tokio::sync::broadcast`, while allowing saturated bursts above 4,096 concurrent distinct tokens to fall through cleanly without blocking.
   - Both the Leader and woken Followers write the evaluated decision into their respective calling `current_worker_lane()` cache shard so subsequent traffic on every participating core immediately hits warm L3.

---

## 2. Share-Nothing Per-Core Two-Tier Cache (`cache.rs`)

### 2.1 Memory Geometry & Caps
- **`IntrospectionCache`** is `#[repr(C, align(64))]` (`size_of == 64`, `align == 64`):
  - `boot_salt: [u8; 32]` (`0x00..0x20`) keyed per plugin instance via `SystemRandom` CSPRNG (cleanly isolates entries across reloads & routes).
  - `lanes: Option<Box<[CoreCacheLane]>>` (`0x20..0x30`, `None` when both positive & negative TTLs are `0`).
  - TTLs, masks, `stale_refresh_secs`, `enabled` (`0x30..0x40`).
- **Two-Tier Hardware Budgeting (`hardware_sized`)**:
  - **Tier 1 (`Hot L3`)**: Probed from `/sys/devices/system/cpu/cpu0/cache/index3/{size,shared_cpu_list}` on Linux (`1 MiB` fallback elsewhere), capped so total aggregate Hot L3 across all worker lanes never exceeds **`MAX_HOT_L3_TOTAL_BYTES = 4 MiB`**. First-touch allocated per lane so NUMA pages bind locally to the owning core.
  - **Tier 2 (`Cold DRAM` Spillover)**: Lazily allocated (`None` until first valid Hot L3 eviction), capped at **`min(2% RAM, MAX_COLD_DRAM_TOTAL_BYTES = 256 MiB)`** divided across active core lanes.

### 2.2 Victim Demotion, Promotion & 401 Flood Immunity
- When `Hot L3` replaces a live slot for a different fingerprint (`victim.fp != fp`):
  - If `FLAG_VALID_PASS` or `FLAG_FORBIDDEN_SCOPE` and `victim.exp > now_epoch`, `demote_victim_if_valid` lazily allocates Cold DRAM (if enabled) and inserts the victim into `cold[(fp >> 20) & cold_mask]`.
  - `FLAG_UNAUTHORIZED` (`401`) slots **never demote** into Cold DRAM, preventing unauthenticated token-spraying attacks from evicting legitimate active sessions.
- On a Hot L3 miss followed by an unexpired Cold DRAM match, `lookup()` atomically removes the entry from Cold DRAM and **promotes** it back to Hot L3 (demoting any displaced live Hot victim back to Cold).
- Every `CoreCacheLane` uses an `AtomicU8` core-local guard (`LaneGuard`), incurring zero cross-core lock bouncing on thread-per-core `velda-worker-{i}` while safely supporting shadow `velda-shadow-{i}` writes targeting `origin_lane` and arbitrary async test schedulers.

---

## 3. Adaptive Stale-While-Revalidate (`coarse_sec()`)

- `compute_stale_refresh_secs(cache_ttl_seconds)`: `(ttl / 10).clamp(3, 10)` for `ttl > 3`, `0` otherwise.
- When a warm entry enters the final stale window and `FLAG_REVALIDATING` is unset (or expired past `stale_window`), `mark_revalidating` sets `FLAG_REVALIDATING` + `reval_at = now_epoch`, captures `origin_lane = cache.current_lane_idx()`, and fires `revalidate_on_shadow` via `crate::hook::try_dispatch_shadow`.
- If the background refresh fails (`Unavailable`) or the shadow queue is full, `finish_revalidation_on_lane(origin_lane, fp)` clears `FLAG_REVALIDATING`; if the IdP reports `active: false`, `store_negative_on_lane` flips the slot to `FLAG_UNAUTHORIZED`.

---

## 4. Pure Dispatch (`providers/mod.rs`) & Isolated 3-File Provider Subfolders (`providers/*`)

`providers/mod.rs` acts strictly as a zero-logic dispatcher (`execute(&IntrospectionCall)` / `revalidate(&IntrospectionCall)`). Each supported ETP provider subfolder contains three focused files:
- **`mod.rs`**: Pure export root (`pub mod client; pub mod response; pub use response::{execute, revalidate};`).
- **`client.rs`**: Self-contained HTTP/1.1 Keep-Alive roundtrip over its `velda-etp` `StreamPool` (`acquire()`, `1`-shot `reconnect()` on broken/stale sockets, and `recreate_in_background()` on `Connection: close` or EOF framing).
- **`response.rs`**: Provider-specific JSON parsing, `exp`/`nbf`/`scope`/claim evaluation, and direct commit into `IntrospectionCache` (`store_positive_on_lane`, `store_negative_on_lane`, `finish_revalidation_on_lane`).

Provider specializations:
- **RFC 7662 Form Introspection (`keycloak`, `hydra`, `authelia`, `okta`, `oidc`)**: `POST` form-encoded `token` (+ optional `token_type_hint`), supporting `Basic`, `Post`, and `None` client auth plus ETP `bearer_token` & `custom_headers`.
- **Bearer UserInfo & Custom Path (`auth0`, `entra_id`, `cognito`)**: `GET` default UserInfo paths with `Authorization: Bearer <token>` (`200` $\to$ `active: true` + claims, `401/403` $\to$ `active: false`), switching to RFC 7662 `POST` when custom `path` + client credentials are provided.
- **Vault (`vault`)**: Uses `VaultEtp::resolve_token()` (`AppRole`/static token + 403 invalidation retry) for `POST /v1/auth/token/lookup` (mapping `entity_id`/`display_name`, `policies`, and `ttl`), falls back cleanly to `GET /v1/auth/token/lookup-self` when ETP has no token, and supports `/v1/identity/oidc/introspect`.
