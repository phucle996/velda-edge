# JWT Authentication Plugin (`jwt_auth`) — Deep Architectural Specification

```text
+---------------------------------------------------------------------------------------------------+
|                     JWT AUTHENTICATION: 3-TIER HARDWARE RESIDENCY & EXECUTION                     |
|                                                                                                   |
|  [Tier L2: `HeaderWorkspace` / URI]                                                               |
|    • Token decoded at ingress on current `velda-worker-{id}` core                                 |
|    • Pre-hashed `HeaderKey` AVX2/SSE2/NEON/SWAR lookup (`get_all_prehashed`)                      |
|    • Structural 2-dot check (`is_plausible_jwt_shape`, <= 16 KiB) rejects junk before hashing     |
|         │                                                                                         |
|         ▼                                                                                         |
|  [Tier L1D: 64-Byte `JwtCache` Control Block (`offset 0x00` of `JwtAuthPlugin`, `_MM_HINT_T0`)]   |
|    • `0x00..0x20` `boot_salt: [u8; 32]`     ──► `SHA-256(boot_salt || jwt)` -> `fp: u128`         |
|    • `0x20..0x30` `lanes: Option<Box<..>>`  ──► `None` (0B heap) when `cache_ttl_secs == 0`       |
|    • `0x30..0x38` `default_ttl_secs: u32` | `negative_ttl_secs: u32`                              |
|    • `0x38..0x40` `lane_mask` | `hot_group_mask` | `cold_group_mask` | `enabled` | `has_cold`     |
|         │                                                                                         |
|         ├──► Index core-local lane via `current_worker_lane() & lane_mask` (0 Mutex / 0 futex)    |
|         ├──► `prefetcht2` / `prfm pldl3keep` on `&lane.hot_base[hot_idx].tags`                   |
|         ▼                                                                                         |
|  [Tier L3 Hot Cap (NUMA-Local, <= 4 MiB Total Across All Cores)] ──► On eviction (valid only):    |
|    • `BucketGroup`: 64B `tags` | `keys` | `expiries` | `key_tags` | `decisions`  ──► Demote    |
|                                                                                      │            |
|  [Tier DRAM Cold Spillover (Lazy, <= 2% RAM, max 256 MiB Total)] ◄───────────────────┘            |
|    • Hit in Cold DRAM swaps/promotes back to Hot L3; 401 sprays never demote into Cold DRAM.      |
+---------------------------------------------------------------------------------------------------+
```

---

## 1. Private `JwksStore` Hardware Layout (L1D Directory + L3 Integer Meta + Key Byte Arena)

Unlike pointer-heavy `HashMap<String, Key>` tables, [`JwksStore`](store.rs) wraps an `ArcSwap<JwksKeyTable>` owned privately by `JwtAuthPlugin` (`16` slots: `1` active primary slot + up to `15` standby/overlap slots, no redundant `active_count` counter):

### Tier 1 — L1D Cache Line 0 (`DirectoryLine`, Exact `64` Bytes)

```text
+---------------------------------------------------------------------------+
| 0x00..0x10 (16B) | tags: [u8; 16]        | 8-bit kid tag (0x00 = Empty)   |
| 0x10..0x18 (8B)  | algs_packed: u64      | 16 x u4 algorithms (0..8, 15)  |
| 0x18..0x1A (2B)  | valid_mask: u16       | Occupancy bitmask (`0` = empty)|
| 0x1A..0x1B (1B)  | primary_slot: u8      | Primary active slot index      |
| 0x1B..0x1C (1B)  | flags: u8             | Bit0 Wildcard | Bit1 External  |
| 0x1C..0x1D (1B)  | tag_shift: u8         | Default 56                     |
| 0x1D..0x20 (3B)  | _pad: [u8; 3]         | Explicit alignment padding     |
| 0x20..0x28 (8B)  | table_version_tag: u64| Version tag for unbound rejects|
| 0x28..0x40 (24B) | _reserved: [u8; 24]   | Tail pad to exact 64B line     |
+---------------------------------------------------------------------------+
```

Compile-time assertions guarantee `size_of::<DirectoryLine>() == 64`, `align_of::<DirectoryLine>() == 64`, and `offset_of!(JwksKeyTable, dir) == 0`:
- **Per-Slot `u4` Algorithm Packing**: Every supported JWT algorithm fits in 4 bits (`HS256=0 ..= EdDSA=8`, `ALG_INVALID=0x0F`). Decoding `(algs_packed >> (slot_idx * 4)) & 0x0F` eliminates string comparison on the hot path and enforces exact `header_alg_u4 == slot_alg_u4` checks before reading cryptographic bytes.
- **Zero `active_count` Overhead**: `valid_mask == 0` encodes empty store (`NoActiveKeys`); `valid_mask & !(1 << primary_slot)` isolates all standby keys cleanly in 1 instruction.
- **1-Bit Runtime Mode Separation**: `DIR_FLAG_EXTERNAL_MODE` (`1 << 1`) in `DirectoryLine::flags` mirrors `mode_bit` (`0 = internal`, `1 = external`), matching `JwtAuthPlugin::pbww()`.

### Tier 2 & Tier 3 — L3 Metadata Lines & Key Byte Arena

Moving past L1D `DirectoryLine` triggers `_MM_HINT_T2` (`prfm pldl3keep`) onto `kid_ints`:
1. **`kid_ints: [u64; 16]`** (`2` aligned L3 cache lines = `128B`): Stores integerized `kid` hashes computed once at key ingestion via `Trunc64(SHA-256(boot_salt || b"kid:" || raw_kid))` clamped `>= 2`. Because `boot_salt` already lives inside L1D `JwtCache` at boot across all nodes/restarts, arbitrary-length string `kid`s never pollute cache lines.
2. **`key_tags: [u64; 16]`** (`128B`): `FNV-1a(kid_u64 || alg_u4 || raw_bytes)` per slot, clamped `>= 2`.
3. **`key_spans: [KeySpan; 16]`** (`128B`): `{ offset: u32, len: u16, expected_sig_len: u16 }` indexing into the contiguous **`key_bytes: Box<[u8]>`** Key Byte Arena holding raw DER RSA PKCS#1, EC uncompressed points, Ed25519 32B keys, or raw HMAC secrets (paired with optional pre-expanded `ring::hmac::Key` handles).

---

## 2. Per-`kid` `key_tag` Cache Coherence Across Rotation

`BucketGroup` replaces coarse global epochs with **`key_tags: [u64; 64]`**:
- Every valid token entry records the `key_tag` of the `JwksKeyTable` slot whose key verified its signature.
- Every unbound reject (`UnknownKid`, malformed/expired, `ClaimAssertionFailed`) records `table_version_tag`.
- On cache hit, [`JwksKeyTable::is_cache_tag_valid`](store.rs) checks the primary slot tag in a single branch (`key_tags.0[p_idx] == cached_tag`), falling back to `standby_mask` bits if needed.
- **Benefit during rotation**: When an external provider publishes a new primary key (`Slot 0`) while keeping old keys as standby slots (`Slots 1..15`), existing tokens signed by non-retired `kid`s **remain valid cache hits**. The moment a key is retired from all slots or its bytes/algorithm change, its old `key_tag` disappears and cached tokens signed by that key miss immediately.

---

## 3. Unified Single-Pass 64-Way SIMD Tag Scanning & Zero-Borrow SWAR

Each `BucketGroup` (`#[repr(C, align(64))]`) places `tags: TagLine([u8; 64])` at offset `0x0000..0x0040`:
- `0x00`: Empty slot.
- `0x01..=0x7F` (`Bit 7 = 0`): Positive verified entry (`tag7`).
- `0x81..=0xFF` (`Bit 7 = 1`): Rejected entry (`0x80 | tag7`).

Multi-target compile-time dispatch in [`simd.rs`](simd.rs) scans all 64 tags in one pass with zero runtime CPUID sniffing:
- `x86_64` + `avx2` (`x86-64-v3`): 2x 256-bit `_mm256_cmpeq_epi8` + `_mm256_movemask_epi8` $\to$ `u64` mask (`tzcnt` / `blsr`).
- `x86_64` + `sse2` (`x86-64-v1`): 4x 128-bit `_mm_cmpeq_epi8` + `_mm_movemask_epi8`.
- `aarch64` (`NEON`): 4x `vld1q_u8` + `vceqq_u8` + bit-weighted `vaddv_u8`.
- **Portable 64-Bit Zero-Borrow SWAR Fallback**: Computes $z = \neg\big(((w \mathbin{\&} \text{0x7F7F...}) \oplus (\text{0x0101...} \times \text{target7}) \mathbin{|} \text{0x8080...}) - \text{0x0101...}\big) \mathbin{\&} \text{0x8080...}$ (MSB `0x80` prevents borrow across byte lanes), packing all 8 MSBs via $(z \gg 7) \times \text{0x0102\_0408\_1020\_4080} \gg 56$.

---

## 4. Isolated Self-Contained ETP Provider Adapters

- **Strict Connection/Logic Separation**: `velda-etp` strictly owns TCP/TLS handshake, DNS LKG resolution, pool lifecycle, and transport-level auth (`VaultEtp::resolve_token()`). It knows zero JWT paths or JWK formats.
- **Isolated Provider Modules (`providers/<kind>/mod.rs`)**: `providers/mod.rs` contains only `pub mod` exports and the top-level match dispatcher `fetch_etp_key_table()`. Every provider folder (`vault`, `aws_secrets_manager`, `s3`, `keycloak`, `hydra`, `authelia`, `okta`, `auth0`, `entra_id`, `cognito`, `oidc`) owns its complete request framing, response decoding (including chunked HTTP/1.1), JWK/JSON extraction, and `(kid, alg, VerifierKey)` assembly end-to-end.
- **Shadow Worker Execution**: External sync runs inside `JwtAuthPlugin::run_background` using `velda_core::clock::coarse_sec()`, prioritizing ETP keys from Slot 0 onward while appending `static_fallback` as a standby slot only when `preserve_static_key == true`.
