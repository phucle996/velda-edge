# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`)

```text
+---------------------------------------------------------------------------------------------------+
|                   OAUTH2 INTROSPECTION — HYBRID WARM SYNC / COLD ASYNC ARCHITECTURE               |
|                                                                                                   |
|  [L2 Warm: Downstream Request (`HeaderWorkspace` / URI)]                                          |
|         │                                                                                         |
|         ▼ 1. Pre-hashed `HeaderKey` / split `Cookie` / URI Query extraction                       |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [L1D Control Line: 64B `IntrospectionCache` (`_MM_HINT_T0`)]                                │  |
|  │  - `SHA-256(boot_salt || token)` -> `fp: u128` (16B integer in GPRs, 0 heap alloc)          │  |
|  │  - `_MM_HINT_T2` (`prefetcht2`) -> L3 64-Way SIMD `BucketGroup` (`tags: [u8; 64]`)          │  |
|  └──────────────────────┬───────────────────────────────────────────┬─────────────────────────┘  |
|                          │                                           │                            |
|                  (99.9%+ Warm Hit / Stale)                   (0.1% Cold Miss / TTL=0)             |
|                          ▼                                           ▼                            |
|  ┌──────────────────────────────────────┐    ┌─────────────────────────────────────────────────┐  |
|  │ Fast Sync Path (0 .await, 0 alloc)   │    │ Non-Blocking `.await` Single-Flight             │  |
|  │  • `Hit`: inject present claims      │    │  • Coalesces concurrent identical misses        │  |
|  │  • `Stale` (10% clamped `3..=10s`):  │    │  • `velda-discovery` (`/etc/hosts` + DNS)       │  |
|  │    offload async refresh to          │    │  • Async Keep-Alive HTTP or `velda_tls` HTTPS   │  |
|  │    `velda-shadow-{id}` (`coarse_sec`)│    │  • Commit L3 (capped <= 10% host RAM)           │  |
|  │  • `403` / `401` unified Tag Bit 7   │    │                                                 │  |
|  └──────────────────────┬───────────────┘    └───────────────────────┬─────────────────────────┘  |
|                         └──────────────────────┬─────────────────────┘                            |
|                                                ▼ `Action::Continue`                               |
|                                  [Upstream Service Request Pipe]                                  |
+---------------------------------------------------------------------------------------------------+
```

The **`oauth2_introspection`** plugin enforces RFC 7662 OAuth 2.0 Token Introspection for opaque reference tokens (or revocable access tokens) against external Authorization Servers (Ory Hydra, Keycloak, Auth0, Okta, Entra ID).

---

## 1. What This Plugin Does

1. **Non-Blocking Warm/Cold Hybrid Execution**:
   - **Warm Hits**: Evaluated synchronously via `try_on_upstream_request` with zero `.await` state machines and zero heap allocations on `velda-worker-{id}`.
   - **Cold Misses**: Handed off to `on_upstream_request_async`, yielding the single stream coroutine cleanly via `.await` with sharded **Single-Flight coalescing** so concurrent misses never block unrelated traffic on the worker thread or stampede the IdP.
2. **Dynamic 10%-RAM-Capped L3 SIMD Cache (`u128`)**:
   - Compresses arbitrary-length tokens via keyed `SHA-256(boot_salt || token)` into 128-bit integers (`u128`).
   - Starts compact (~835 KiB upfront) and dynamically grows shards within a strict **10% host/cgroup RAM ceiling** (`velda-core`), switching to O(1) SIMD clock-hand eviction once the ceiling is reached.
3. **Adaptive Background Stale-While-Revalidate (`coarse_sec()`)**:
   - Within the final `10%` of `cache_ttl_seconds` (clamped to `3s..=10s` for TTL `> 3s`), continues serving warm claims and dispatches an async IdP refresh to `velda-shadow-{id}`. If the user has logged out (`active: false`), the shadow task immediately flips the L3 slot to Negative (`401`).
   - Supports `cache_ttl_seconds: 0` for sensitive routes requiring immediate zero-window revocation.
4. **`velda-discovery` Resolution + Real HTTP/HTTPS TLS + Chunked Keep-Alive**:
   - Resolves IdP hostnames through `velda-discovery` (`/etc/hosts`, RAM `DnsCache`, async `/etc/resolv.conf` UDP DNS with LKG fallback), connects directly to IPv4 and `[IPv6]` literals, and enforces real TLS 1.2/1.3 (`velda_tls` + `OutboundTlsConfig`) for `https://` URLs.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_upstream/oauth2_introspection/`
- **Execution Stage**: `PreUpstreamPlugin` (`pbww = 0`; sporadic stale refreshes use `try_dispatch_shadow` to `velda-shadow-{id}`)
- **Supported Protocols**: `SupportedProtocols::ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, and gRPC)

---

## 3. Requirements & Prerequisites

1. **Strict Schema (`deny_unknown_fields`)**: Legacy `max_cache_entries` has been removed cleanly; total cache memory is governed automatically by the 10% RAM topology ceiling from `velda-core`.
2. **Transport Security (`https://`)**: Production deployments passing `client_id` / `client_secret` over untrusted networks should configure `introspection_url` with `https://` and optional custom CA / mTLS certificates under `tls`.

---

## 4. Quick Start Configuration

```json
{
  "id": "oauth2_introspection",
  "config": {
    "introspection_url": "https://idp.internal:8443/oauth/introspect",
    "client_id": "velda-edge-gateway",
    "client_secret": "replace-with-gateway-secret",
    "auth_method": "client_secret_basic",
    "token_headers": ["x-access-token", "authorization"],
    "token_cookies": ["access_token"],
    "token_query_params": ["token"],
    "token_type_hint": "access_token",
    "cache_ttl_seconds": 30,
    "negative_cache_ttl_seconds": 10,
    "keepalive_timeout_secs": 60,
    "pool_max_idle_connections": 10,
    "request_timeout_ms": 500,
    "required_scopes": ["read", "orders"],
    "claims_to_headers": [
      { "claim": "sub", "header": "x-user-id" },
      { "claim": "client_id", "header": "x-client-id" },
      { "claim": "scope", "header": "x-authenticated-scope" }
    ],
    "hide_credentials": true,
    "fallback_behavior": "reject"
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](RUNBOOK.md)**: Complete parameter table, revocation-window tuning guide (`0s` vs `15s..30s`), TLS/mTLS recipes, and incident playbooks.
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Hardware cache layout (`L1D`/`L2`/`L3`), 10% RAM shard doubling, Single-Flight cold coordinator, `velda-discovery` & async HTTP/HTTPS Keep-Alive design.
- **[BENCHMARK.md](BENCHMARK.md)**: 5-tier benchmark matrix and verified `x86-64-v3` release assembly audit (`0` `__rust_alloc` calls on warm cache hits).
- **[TEST.md](TEST.md)**: Verified `cargo test` execution report across all 19 unit & live async mock-IdP tests.
