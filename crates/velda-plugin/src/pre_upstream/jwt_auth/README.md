# JWT Authentication Plugin (`jwt_auth`)

```text
+---------------------------------------------------------------------------------------------------+
|                     UNIFIED JWT AUTH — INTERNAL & EXTERNAL ETP MODES                              |
|                                                                                                   |
|  Config (`mode` encoded as 1 bit in `DirectoryLine::flags`):                                      |
|    ├─► MODE_INTERNAL (0): Static key (`secret_key` / `public_key_pem`), `pbww() == 0`, 0 net I/O  |
|    └─► MODE_EXTERNAL (1): Borrows ETP Lease in Shadow (`pbww() 1..=100`, `coarse_sec()` loop)     |
|                           └─► providers/<etp_kind> ──► private `JwksStore` (`ArcSwap<KeyTable>`)  |
|                                                                                                   |
|  [L2 Warm: Downstream Request (`HeaderWorkspace` / URI)]                                          |
|         │                                                                                         |
|         ▼ 1. Pre-hashed `HeaderKey` / Cookie / Query extraction + 2-dot structural guard          |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [L1D Cache Line 0: 64B `JwtCache` Control (`_MM_HINT_T0`)]                                  │  |
|  │  - `SHA-256(boot_salt || jwt)` -> `fp: u128` (16B integer, 0 heap alloc)                    │  |
|  │  - Bit-shift `(fp & shard_mask)` & `((fp >> shift) & group_mask)` -> `_MM_HINT_T2` L3 hint  │  |
|  └──────────────────────────────────────────────┬──────────────────────────────────────────────┘  |
|                                                 ▼                                                 |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [L3 Shared LLC: Contiguous 64-Way SIMD `BucketGroup` (`#[repr(C, align(64))]`)]             │  |
|  │  - `tags: [u8; 64]` (AVX2/SSE2/NEON/SWAR) -> `key_tags[idx]` vs `JwksKeyTable` slot tag     │  |
|  │  - Positive Hit -> `insert_prehashed` from `Arc<[(HeaderKey, HeaderValue)]>` (`0` alloc)    │  |
|  └──────────────────────────────────────────────┬──────────────────────────────────────────────┘  |
|                                                 │ Cache Miss                                      |
|                                                 ▼                                                 |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [Cold Cryptographic Verification (`store.rs` + `crypto.rs`)]                                │  |
|  │  - L1D `DirectoryLine`: `tags[16]` + `algs_packed` (`16 x u4`) + `valid_mask` (`u16`)       │  |
|  │  - L3 Integer Meta: salted `kid_ints: [u64; 16]` + `key_spans` -> contiguous `key_bytes`    │  |
|  │  - Constant-time `ring` verify (`HS*`, `RS*`, `ES*`, `EdDSA`) + Claims Check & Cache Commit │  |
|  └──────────────────────────────────────────────┬──────────────────────────────────────────────┘  |
|                                                 │ `Action::Continue`                              |
|                                                 ▼                                                 |
|  [Upstream Forwarding Pipe]                                                                       |
+---------------------------------------------------------------------------------------------------+
```

The **`jwt_auth`** plugin is Velda Edge's unified L7 JSON Web Token (RFC 7519) authentication and key-rotation engine. It verifies symmetric and asymmetric tokens on the hot path using hardware-aligned L1D/L3 key structures and optionally rotates keys asynchronously on Shadow Workers over isolated `velda-etp` connection pools.

---

## 1. What This Plugin Does

1. **Dual-Mode Operation (`internal` Static vs `external` ETP)**:
   - **`internal` (`MODE_INTERNAL = 0`, `pbww() == 0`)**: Pins a static symmetric (`secret_key`) or asymmetric (`public_key_pem`) key in RAM at config load with zero background tasks or network I/O.
   - **`external` (`MODE_EXTERNAL = 1`, `pbww() 1..=100`)**: Borrows pre-connected TCP/TLS streams in the Shadow Logistics loop (`velda_core::clock::coarse_sec()`) from an attached `etp_id` across 11 self-contained providers: `vault`, `aws_secrets_manager`, `s3`, `keycloak`, `hydra`, `authelia`, `okta`, `auth0`, `entra_id`, `cognito`, and `oidc`.
2. **L1D Bit-Mapped `JwksStore` + L3 Key Byte Arena**:
   - Compresses key resolution into a 64-byte L1D `DirectoryLine` holding `tags: [u8; 16]`, packed 4-bit algorithms (`algs_packed`: 16 x `u4`), `valid_mask: u16` (1 active primary slot + up to 15 standby/overlap slots), and `flags`.
   - Stores boot-salted 64-bit integer `kid_ints`, per-slot `key_tags`, and `KeySpan` descriptors in L3-aligned lines pointing into a contiguous `key_bytes: Box<[u8]>` Key Byte Arena.
3. **Granular Per-Key Cache Coherence**:
   - L3 `JwtCache` tags each verified token entry with its signing slot's `key_tag`. Rotating in new standby keys preserves cached tokens signed by non-retired keys, while modifying or retiring a `kid` invalidates only affected tokens immediately.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_upstream/jwt_auth/`
- **Execution Stage**:
  - Hot path: `PreUpstreamPlugin` (`on_upstream_request`) on Frontline Traffic Workers (`velda-worker-{i}`).
  - Background path (when `mode == "external"`): `run_background` scheduled via `pbww` (`1..=100`, default `20`) on Shadow Logistics Workers (`velda-shadow-{i}`).
- **Supported Protocols**: `SupportedProtocols::ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC).

---

## 3. Requirements & Prerequisites

1. **Strict Schema (`deny_unknown_fields`)**: Invalid or unrecognized fields fail `velda-sync` staging immediately.
2. **Mode Key Invariants**:
   - `internal`: Requires `algorithm` and either `secret_key` (`HS*`) or `public_key_pem` (`RS*`/`ES*`/`EdDSA`, `>= 2048-bit` RSA); forbids `etp_id` and `path`; forces `pbww = 0`.
   - `external`: Requires non-empty `etp_id` referencing a supported `velda-etp` provider in `etp.json`. Both `algorithm` and static keys may be `null` (each ETP slot supplies its own verified `u4` algorithm); when both a static fallback key and `etp_id` exist, ETP keys take Slot 0 primary priority while the static key can optionally remain as a standby slot (`preserve_static_key: true`).
3. **Wire Status Contract**:
   - Absent token: `401 Unauthorized` + `WWW-Authenticate: Bearer realm="velda-edge"` (gRPC: `UNAUTHENTICATED`).
   - Invalid token / signature / expired / `nbf` / `iss` / `aud` mismatch / `NoActiveKeys`: `401 Unauthorized` + `WWW-Authenticate: Bearer error="invalid_token"`.
   - `assert_claims` mismatch: `403 Forbidden` (gRPC: `PERMISSION_DENIED`).

---

## 4. Quick Start Configuration

### Static (`internal` mode)
```json
{
  "id": "jwt_auth",
  "config": {
    "mode": "internal",
    "algorithm": "EdDSA",
    "secret_key": null,
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEA...\n-----END PUBLIC KEY-----",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": "https://auth.example.com",
    "audience": "api.velda.io",
    "claims_to_headers": { "sub": "x-user-id" },
    "assert_claims": {},
    "cache_ttl_secs": 300
  }
}
```

### Live ETP Key Rotation (`external` mode)
```json
{
  "id": "jwt_auth",
  "config": {
    "mode": "external",
    "etp_id": "keycloak-prod",
    "path": "/realms/velda/protocol/openid-connect/certs",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": "https://idp.example.com/realms/velda",
    "audience": "api.velda.io",
    "claims_to_headers": { "sub": "x-user-id" },
    "assert_claims": {},
    "cache_ttl_secs": 300
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](RUNBOOK.md)**: Complete parameter reference, `etp.json` pairing recipes (Vault, AWS, S3, Keycloak/Hydra/Authelia/OIDC/Cloud IdPs), wire observability, and triage playbooks.
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: L1D `DirectoryLine` (64B) bit-map + `u4` algorithm packing, L3 salted integer `kid_ints` + Key Byte Arena, per-`key_tag` cache coherence, and ETP provider isolation.
- **[BENCHMARK.md](BENCHMARK.md)**: 5-tier benchmark specification and release assembly audit notes.
- **[TEST.md](TEST.md)**: Live verified `cargo test` execution report across `jwt_auth`, `velda-etp`, and `velda-sync`.
