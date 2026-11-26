# JWT Authentication Plugin (`jwt_auth`)

```text
+---------------------------------------------------------------------------------------------------+
|                             JWT Authentication — 3-Tier Hardware Workflow                         |
|                                                                                                   |
|  [L2 Warm: Downstream Request (`HeaderWorkspace` / URI)]                                          |
|         │                                                                                         |
|         ▼ 1. Pre-hashed `HeaderKey` / Cookie / Query extraction + 2-dot structural guard          |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [L1D Cache Line 0: 64B `JwtCache` Control (`_MM_HINT_T0`)]                                  │  |
|  │  - `SHA-256(boot_salt || jwt)` -> `fp: u128` (16B integer in GPRs, 0 heap alloc)            │  |
|  │  - Bit-shift `(fp & shard_mask)` & `((fp >> shift) & group_mask)` -> `_MM_HINT_T2` L3 hint  │  |
|  └──────────────────────────────────────────────┬──────────────────────────────────────────────┘  |
|                                                 │                                                 |
|                                                 ▼                                                 |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [L3 Shared LLC: Contiguous 64-Way SIMD `BucketGroup` (`#[repr(C, align(64))]`)]             │  |
|  │  - Cache Line 0 (`tags: [u8; 64]`): AVX2 / SSE2 / NEON / SWAR scan + `tzcnt` -> `slot_idx`  │  |
|  │  - Unified Tag Bit 7 (`0x80`): Positive (`0x01..0x7F`) + Reject `401`/`403` (`0x81..0xFF`)  │  |
|  │  - Positive Hit -> `insert_prehashed` from `Arc<[(HeaderKey, HeaderValue)]>` (`0` alloc)    │  |
|  └──────────────────────────────────────────────┬──────────────────────────────────────────────┘  |
|                                                 │ Cache Miss                                      |
|                                                 ▼                                                 |
|  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │ [Cold Cryptographic Verification (`crypto.rs` + `table.rs`)]                                │  |
|  │  - `JwksKeyTable` Cache Line 0 `[u64; 8]` lookup (up to 128B `kid`, epoch-tracked rotation) │  |
|  │  - Constant-time `ring` verify (`HS256/384/512`, `RS256/384/512`, `ES256/384`, `EdDSA`)     │  |
|  │  - Enforce `exp`, `nbf`, `iss`, `aud`, `assert_claims` -> Commit L3 Bucket & Inject Claims  │  |
|  └──────────────────────────────────────────────┬──────────────────────────────────────────────┘  |
|                                                 │ `Action::Continue`                              |
|                                                 ▼                                                 |
|  [Upstream Forwarding Pipe]                                                                       |
+---------------------------------------------------------------------------------------------------+
```

The **`jwt_auth`** plugin is Velda Edge's L7 JSON Web Token (RFC 7519) authentication and claim-projection engine. It verifies symmetric and asymmetric signatures, caches decisions in a contiguous L3 SIMD-indexed table keyed by 128-bit cryptographic fingerprints, and projects present token claims into upstream request headers.

---

## 1. What This Plugin Does

1. **Multi-Algorithm Constant-Time Signature Verification**:
   - Supports `HS256`, `HS384`, `HS512`, `RS256`, `RS384`, `RS512`, `ES256`, `ES384`, and `EdDSA` (`Ed25519`, RFC 8037).
   - Operates with static key material (`secret_key` or `public_key_pem`) or live background key rotation via `jwks_rotator`.
2. **Three-Tier (`L1D` / `L2` / `L3`) Contiguous SIMD Decision Cache**:
   - Compresses arbitrary-length JWT strings into 128-bit secret-salted SHA-256 integers (`u128`).
   - Unifies positive (`200` pass), invalid (`401`), and claim-denied (`403`) decisions in a single 64-way set-associative L3 bucket scanned via `AVX2` / `SSE2` / `NEON` / `SWAR`.
   - Automatically invalidates cached tokens upon `jwks_rotator` key rotation via atomic `SharedJwksStore` epoch tracking.
3. **Declarative Policy & Claim Projection**:
   - Enforces `exp`, `nbf` (integer and float RFC 7519 `NumericDate`), `iss`, `aud`, and `assert_claims`.
   - Projects present claims (`claims_to_headers`, supporting nested dot paths and arrays) into `HeaderWorkspace` via pre-hashed `HeaderKey` descriptors without deleting unrelated client headers when optional claims are absent.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_upstream/jwt_auth/`
- **Execution Stage**: `PreUpstreamPlugin` (`pbww = 0`, executes inline on Frontline Traffic Workers `velda-worker-{i}`)
- **Supported Protocols**: `SupportedProtocols::ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, and gRPC)
- **Companion Plugin**: Optionally pairs with `jwks_rotator` (`BackgroundPlugin`, `pbww = 5` on Shadow Logistics Workers) sharing an atomic `SharedJwksStore`.

---

## 3. Requirements & Prerequisites

1. **Strict Schema (`deny_unknown_fields`)**: Every required field in `JwtAuthConfig` must be declared under `"id": "jwt_auth"`. Unknown fields fail `velda-sync` validation immediately.
2. **Key Provisioning Invariant**:
   - Symmetric (`HS256`/`HS384`/`HS512`): requires non-empty `secret_key`; `public_key_pem` must be `null`.
   - Asymmetric (`RS256`/`RS384`/`RS512`/`ES256`/`ES384`/`EdDSA`): `secret_key` must be `null`; requires either a valid ASN.1 DER/PEM `public_key_pem` (`>= 2048-bit` for RSA) **or** `public_key_pem: null` paired with an active `jwks_rotator` targeting `"jwt_auth"`.
3. **Wire Status Contract**:
   - Missing token: `401 Unauthorized` + `WWW-Authenticate: Bearer realm="velda-edge"` (gRPC: `UNAUTHENTICATED`).
   - Malformed / bad signature / expired / `nbf` / `iss` / `aud` mismatch: `401 Unauthorized` + `WWW-Authenticate: Bearer error="invalid_token"`.
   - Valid token failing `assert_claims`: `403 Forbidden` (gRPC: `PERMISSION_DENIED`).

---

## 4. Quick Start Configuration

```json
{
  "id": "jwt_auth",
  "config": {
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
    "claims_to_headers": {
      "sub": "x-user-id",
      "roles": "x-user-roles"
    },
    "assert_claims": {},
    "cache_ttl_secs": 300
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](RUNBOOK.md)**: Complete parameter reference, `claims_to_headers` & `header_mutation` composition guide, production presets, and incident triage playbooks.
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Byte-exact `64B` L1D control layout, L3 contiguous 64-way SIMD bucket mechanics, zero-borrow SWAR math proof, and epoch-coherent key rotation.
- **[BENCHMARK.md](BENCHMARK.md)**: 5-tier benchmark matrix and verified `x86-64-v3` release assembly audit (`0` `__rust_alloc` calls in `on_upstream_request`).
- **[TEST.md](TEST.md)**: Verified `cargo test` execution report across all 22 `jwt_auth` and 9 `jwks_rotator`/dependency unit tests.
