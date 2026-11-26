# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`)

```text
+---------------------------------------------------------------------------------------------------+
|             OAUTH2 INTROSPECTION — PER-CORE TWO-TIER WARM SYNC / ETP ASYNC ARCHITECTURE           |
|                                                                                                   |
|  [L2 Warm: Downstream Request (`HeaderWorkspace` / Split `Cookie` / URI Query)]                   |
|         │                                                                                         |
|         ▼ 1. Pre-hashed `HeaderKey` extraction + `> 16 KiB` oversized token rejection             |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [L1D Control Line: 64B `IntrospectionCache` (`_MM_HINT_T0` / `pldl1keep`)]                   │  |
|  │  - `SHA-256(boot_salt || token)` -> `fp: u128` (GPR integer fingerprint)                    │  |
|  │  - Select Share-Nothing Core Lane `&lanes[current_worker_lane() & lane_mask]`               │  |
|  │  - Hot L3 Cap (<= 4 MiB total across cores) <-> Lazy Cold DRAM Spillover (<= 2% RAM, 256M)  │  |
|  └──────────────────────┬───────────────────────────────────────────┬─────────────────────────┘  |
|                         │                                           │                            |
|                  (Warm Hit / Stale)                           (Cold Miss / TTL=0)                |
|                         ▼                                           ▼                            |
|  ┌──────────────────────────────────────┐    ┌─────────────────────────────────────────────────┐  |
|  │ Fast Sync Path (0 .await, 0 alloc)   │    │ `InFlightCoordinator` (inflight.rs, 64x64)      │  |
|  │  • `Hit`: inject present claims      │    │  • Coalesces concurrent misses per `fp: u128`     │  |
|  │  • `Stale` (10% clamped `3..=10s`):  │    │  • Dispatches to `providers/<etp_kind>` via     │  |
|  │    offload async refresh to          │    │    attached `velda-etp` `StreamPool` (`etp_id`)   │  |
|  │    `velda-shadow-{id}` (`origin_lane`)│    │  • Commits leader & follower core lanes        │  |
|  │  • `403` / `401` unified Tag Bit 7   │    │                                                 │  |
|  └──────────────────────┬───────────────┘    └───────────────────────┬─────────────────────────┘  |
|                         └──────────────────────┬─────────────────────┘                            |
|                                                ▼ `Action::Continue`                               |
|                                  [Upstream Service Request Pipe]                                  |
+---------------------------------------------------------------------------------------------------+
```

The **`oauth2_introspection`** plugin enforces OAuth 2.0 / OIDC token introspection and Vault token verification for opaque reference tokens against external Authorization Servers configured as **`velda-etp`** pools (`keycloak`, `hydra`, `authelia`, `okta`, `oidc`, `auth0`, `entra_id`, `cognito`, and `vault`).

---

## 1. What This Plugin Does

1. **Multi-Provider `velda-etp` Pool Integration (`providers/*`)**:
   - Delegates all TCP/TLS/mTLS handshakes, baseline pre-warming, and `EtpPageArena` burst spillover cleanly to the referenced `etp_id` pool.
   - Dispatches into 9 isolated provider adapters with provider-native defaults and wire semantics:
     - **RFC 7662 POST**: `keycloak`, `hydra`, `authelia`, `okta`, `oidc`.
     - **Bearer UserInfo / Optional Custom Introspect**: `auth0`, `entra_id`, `cognito`.
     - **Vault Token Lookup / OIDC**: `vault` (supports `POST /v1/auth/token/lookup`, unauthenticated `/v1/auth/token/lookup-self`, AppRole 403 re-login retry, and `/v1/identity/oidc/introspect`).
2. **Non-Blocking Warm/Cold Hybrid Execution + `inflight.rs`**:
   - **Warm Hits**: Evaluated synchronously via `try_on_upstream_request` with zero `.await` and zero `Future` heap allocations.
   - **Cold Misses**: Coalesced via `InFlightCoordinator` (`inflight.rs`, 64 shards x 64 slots) and executed over borrowed ETP `StreamLease` keep-alive sockets (`1`-shot `reconnect()` on stale sockets).
3. **Share-Nothing Per-Core Two-Tier Cache (`Hot L3` + `Cold DRAM`)**:
   - Keys arbitrary tokens with keyed `SHA-256(boot_salt || token) -> u128` after rejecting `> 16 KiB` tokens.
   - Sizes Hot L3 per core from physical L3 LLC topology (capped `<= 4 MiB` aggregate across cores) and lazily allocates Cold DRAM spillover (capped `<= 2%` RAM / `256 MiB` aggregate).
   - Valid (`Allow` / `Forbidden`) victims evicted from Hot L3 demote cleanly to Cold DRAM and re-promote on hit; `401 Unauthorized` entries never enter Cold DRAM.
4. **Adaptive Stale-While-Revalidate (`coarse_sec()`)**:
   - Within the final `10%` of `cache_ttl_seconds` (clamped `3s..=10s` for TTL `> 3s`), serves warm claims and offloads an async ETP refresh to `velda-shadow-{id}` targeting the exact `origin_lane`.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_upstream/oauth2_introspection/`
- **Execution Stage**: `PreUpstreamPlugin` (`pbww = 0`; sporadic stale refreshes offload via `try_dispatch_shadow`)
- **Supported Protocols**: `SupportedProtocols::ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC)

---

## 3. Requirements & Prerequisites

1. **Declarative ETP Entry (`etp.json`)**: Define the IdP or Vault backend in `etps[]` (`pool_size`, TLS/CA/SNI, and endpoint/credentials) and bind it via `etp_id`.
2. **Strict Schema (`deny_unknown_fields`)**: Legacy direct URL/pool knobs (`introspection_url`, `keepalive_timeout_secs`, `pool_max_idle_connections`, `tls`) are not accepted in plugin config—pool lifecycle lives exclusively in `velda-etp`.

---

## 4. Quick Start Configuration

```json
{
  "id": "oauth2_introspection",
  "config": {
    "etp_id": "keycloak-prod",
    "path": "/realms/master/protocol/openid-connect/token/introspect",
    "client_id": "velda-edge-gateway",
    "client_secret": "replace-with-gateway-secret",
    "auth_method": "client_secret_basic",
    "token_headers": ["x-access-token", "authorization"],
    "token_cookies": ["access_token"],
    "token_query_params": ["token"],
    "token_type_hint": "access_token",
    "cache_ttl_seconds": 30,
    "negative_cache_ttl_seconds": 10,
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

- **[RUNBOOK.md](RUNBOOK.md)**: Parameter table, supported 9 ETP provider defaults, revocation tuning, and incident playbooks.
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Per-Core Two-Tier cache geometry (`Hot L3 <= 4 MiB` + `Cold DRAM <= 2% RAM`), `InFlightCoordinator` (`inflight.rs`), and isolated `providers/*` adapters.
- **[BENCHMARK.md](BENCHMARK.md)**: 5-tier benchmark scenarios and symbol profile.
- **[TEST.md](TEST.md)**: Verified `cargo test` execution report across `oauth2_introspection` and `velda-sync` ETP reference tests.
