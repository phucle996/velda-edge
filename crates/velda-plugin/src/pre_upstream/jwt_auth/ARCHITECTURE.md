# JWT Authentication Plugin (`jwt_auth`) — Deep Architectural Specification

```text
+---------------------------------------------------------------------------------------------------+
|                     JWT AUTHENTICATION: 3-TIER HARDWARE RESIDENCY & EXECUTION                     |
|                                                                                                   |
|  [Tier L2: `HeaderWorkspace` / URI]                                                               |
|    • Token bytes decoded moments earlier on current `velda-worker-{id}` core                      |
|    • Pre-hashed `HeaderKey` AVX2/SSE2/NEON/SWAR lookup (`get_all_prehashed`)                      |
|    • Structural 2-dot check (`is_plausible_jwt_shape`, <= 16 KiB) rejects junk in ~3 ns           |
|         │                                                                                         |
|         ▼                                                                                         |
|  [Tier L1D: 64-Byte `JwtCache` Control Block (`offset 0x00` of `JwtAuthPlugin`, `_MM_HINT_T0`)]   |
|    • `0x00..0x20` `boot_salt: [u8; 32]`     ──► `SHA-256(boot_salt || jwt)` -> `fp: u128`         |
|    • `0x20..0x30` `shards: Option<Box<..>>` ──► `None` (0B heap) when `cache_ttl_secs == 0`       |
|    • `0x30..0x38` `default_ttl_secs: u32` | `negative_ttl_secs: u32`                              |
|    • `0x38..0x40` `shard_mask` | `group_mask` | `group_shift` (`shrx` bit-shift index)            |
|         │                                                                                         |
|         ├──► `prefetcht2` / `prfm pldl3keep` on `&shard.groups_base[group_idx].tags`              |
|         │    (OoOE overlaps ~14 ns L3 transfer with `epoch` Acquire load + shard Mutex lock)      |
|         ▼                                                                                         |
|  [Tier L3 Shared LLC: Contiguous 64-Way `BucketGroup` (`#[repr(C, align(64))]`)]                  |
|    • `0x0000..0x0040` `tags: TagLine([u8; 64])`  ──► 1 Cache Line SIMD scan (`vpcmpeqb` + `tzcnt`)|
|    • `0x0040..0x0440` `keys: [u128; 64]`         ──► Exact 128-bit equality at matched `idx`      |
|    • `0x0440..0x0640` `expiries: [u64; 64]`      ──► `expiries[idx] > req.ingress_time_sec()`     |
|    • `0x0640..0x0840` `epochs: [u64; 64]`        ──► `epochs[idx] == key_store.epoch()`           |
|    • `0x0840..`       `decisions: [CachedDecision; 64]` ──► `Arc<[(HeaderKey, HeaderValue)]>`     |
+---------------------------------------------------------------------------------------------------+
```

---

## 1. Cache Hierarchy Residency Design (`L1D` $\leftrightarrow$ `L2` $\leftrightarrow$ `L3`)

Velda Edge partitions CPU cache tiers explicitly across subsystems (`L1D` for `velda-router` and hot control state, `L2` for per-request `HeaderWorkspace`, `L3` Shared LLC for multi-megabyte shared arenas). `jwt_auth` aligns directly with this physical hierarchy:

1. **L2 Residency of Inbound Token Slice**:
   - Downstream decoders populate `HeaderWorkspace` on the serving core and keep header blocks warm in L2 (`_MM_HINT_T1`).
   - Because upstream pipe encoders read `HeaderWorkspace` again immediately after `pre_upstream` to serialize backend frames, `jwt_auth` reads token slices directly through normal temporal loads without issuing redundant non-temporal prefetches on `token_str`, and writes projected claim headers via `insert_prehashed` through the core Store Buffer back into L1D/L2.
2. **L1D Control Line (`JwtCache`, Compile-Time Verified `64` Bytes)**:
   - `JwtAuthPlugin` is `#[repr(C, align(64))]` with `cache: JwtCache` placed at byte offset `0x00`.
   - Entry into `on_upstream_request` issues `_MM_HINT_T0` (`prefetcht0` / `prfm pldl1keep`) on `&self.cache`. All metadata required to convert the JWT into `u128` and bit-shift into the L3 arena (`boot_salt`, `shards` pointer, `shard_mask`, `group_mask`, `group_shift`, and TTLs) resides in that single 64-byte L1D line:
     ```rust
     const _: () = assert!(std::mem::size_of::<JwtCache>() == 64);
     const _: () = assert!(std::mem::align_of::<JwtCache>() == 64);
     ```
3. **L3 Shared LLC Bucket Arena (`_MM_HINT_T2` / `pldl3keep`)**:
   - Default active geometry allocates `64` shards $\times$ `16` groups $\times$ `64` slots = **`65,536` concurrent token slots** (~3.6 MiB contiguous memory), fitting comfortably inside unified L3 LLC (16–64 MiB) without evicting `velda-router` from L1D or `HeaderWorkspace` from L2.
   - Because L3 is shared across all cores, a token verified on Core 0 is immediately hit in L3 (~14 ns) when subsequent HTTP/2 or `SO_REUSEPORT` requests from that client land on Core $N$.
   - Writes into `BucketGroup` use standard write-back stores rather than non-temporal streaming stores (`movntdq`), because `movntdq` flushes and invalidates cache lines out of L3 to DRAM (~80 ns), which would defeat cross-core L3 cache hits.

---

## 2. Cryptographic `u128` Compression & Forgery Resistance

Because a Positive Cache hit **bypasses asymmetric RSA/ECDSA/Ed25519 signature verification**, converting a raw JWT into `u128` must never use an unkeyed algebraic hash (`FxHash` / FNV), which is linearly invertible.

`JwtCache::token_to_u128` computes:
$$\text{fp} = \text{Trunc}_{128}\!\left(\text{SHA-256}(\text{boot\_salt}_{256} \,\|\, \text{jwt\_bytes})\right)$$
where `boot_salt` is 32 bytes sampled from OS CSPRNG (`SystemRandom`) at `JwtCache::new`:
- **Offline Preimage Immunity**: `boot_salt` exists only in process RAM; external callers cannot pre-compute colliding tokens or craft inputs targeting a single `shard_idx` / `group_idx`.
- **Online Collision Bound**: For $M = 65{,}536$ active cached tokens, the per-request forgery collision probability is:
  $$P(\text{collision}) \le \frac{65{,}536}{2^{128}} = 2^{-112} \approx 1.9 \times 10^{-34}$$
- **Fixed Footprint**: Regardless of whether an OIDC JWT is 400 bytes or 8 KiB, its cached key representation is strictly **16 bytes** (`u128`).

---

## 3. Unified Single-Pass 64-Way SIMD Tag Scanning & Zero-Borrow SWAR

Each `BucketGroup` (`#[repr(C, align(64))]`) places `tags: TagLine([u8; 64])` at offset `0x0000..0x0040` (Cache Line 0). Each byte encodes both occupancy, decision polarity, and 7-bit fingerprint:
- `0x00`: Empty slot.
- `0x01..=0x7F` (`Bit 7 = 0`, bits `0..6 = tag7`): Positive verified entry.
- `0x81..=0xFF` (`Bit 7 = 1`, bits `0..6 = tag7`): Rejected entry (`401 Unauthorized` or `403 Forbidden`).

A single lookup masks `(tag & 0x7F) == tag7` (`tag7 in 1..=127`), resolving **both** positive and negative caches in one pass:
- **Cache Miss**: Inspects exclusively Cache Line 0 (`64` bytes). When `mask == 0`, returns `CacheLookup::Miss` without touching `keys`, `expiries`, `epochs`, or heap pointers.
- **Multi-Target Compile-Time Dispatch** (zero runtime CPUID sniffing):
  - `x86_64` + `avx2` (`x86-64-v3`): 2x `_mm256_load_si256` + `_mm256_and_si256` + `_mm256_cmpeq_epi8` + `_mm256_movemask_epi8` $\to$ `u64` mask, traversed via `tzcnt` / `blsr`.
  - `x86_64` + `sse2` (`x86-64-v1`): 4x 128-bit `_mm_cmpeq_epi8` + `_mm_movemask_epi8`.
  - `aarch64` (`NEON`): 4x `vld1q_u8` + `vandq_u8` + `vceqq_u8` + bit-weighted `vaddv_u8` lane reduction.
  - **Universal Portable 64-Bit SWAR Fallback**:
    Let $x = (w \mathbin{\&} \text{0x7F7F...}) \oplus (\text{0x0101...} \times \text{target7})$. Every byte in $x$ has MSB $0$ ($x_k \in [0, 127]$). Setting MSB before subtracting $1$:
    $$z = \neg\big((x \mathbin{|} \text{0x8080...}) - \text{0x0101...}\big) \mathbin{\&} \text{0x8080...}$$
    Because $(x_k \mathbin{|} \text{0x80}) \ge \text{0x80} \ge 1$, subtraction **never borrows across byte boundaries**. Bit 7 of byte $k$ in $z$ is $1$ iff $x_k = 0$. Multiplying $(z \gg 7)$ by `0x0102_0408_1020_4080` and shifting `>> 56` packs all 8 byte MSBs into bits `0..7` with zero carry overflow (maximum byte-6 partial sum is `0xFE < 0x100`).

---

## 4. Contiguous `JwksKeyTable` & Epoch-Coherent Key Rotation

1. **Single-Cacheline `kid_index` + 128-Byte `raw_kid` Slots (`table.rs`)**:
   - `JwksKeyTable` (`#[repr(C, align(64))]`) packs all 8 FNV-1a `u64` key hashes into Cache Line 0 (`0x00..0x40`).
   - Each `JwksKeySlot` stores up to **128 bytes** of inline `raw_kid` (`MAX_KID_BYTES = 128`), supporting 36-byte UUIDv4 (Cognito/Keycloak), 43-byte Base64URL SHA-256 thumbprints (Entra ID/Okta), and 86-byte SHA-512 identifiers with zero heap allocation and zero silent truncation.
2. **Lock-Free Rotation Epoch Invalidation**:
   - `SharedJwksStore` pairs `ArcSwap<JwksKeyTable>` with `epoch: AtomicU64` (initialized to `1`).
   - When `jwks_rotator` publishes a rotated `JwksKeyTable`, `SharedJwksStore::update` increments `epoch` (`Release`), while frontline workers read `epoch()` (`Acquire`).
   - Any `JwtCache` slot with `epochs[idx] != current_epoch` immediately misses and re-verifies against the new key table—revoking compromised keys atomically without locking all 64 shards.
   - During cold boot before `jwks_rotator` completes its first fetch (`active_count == 0` $\to$ `JwtVerifyError::NoActiveKeys`), `jwt_auth` skips negative-cache insertion so valid client tokens are not blacklisted while keys are loading.

---

## 5. Allocation & Stack Memory Bounds

- **Cached Hot Path (`Positive`, `Unauthorized`, `Forbidden`)**: **Strictly `0` heap allocations** (`__rust_alloc == 0` verified in `x86-64-v3` release assembly). Positive hits clone an `Option<Arc<[(HeaderKey, HeaderValue)]>>` refcount (`None` when `claims_to_headers` is empty) and write inline `HeaderValue` clones via `HeaderWorkspace::insert_prehashed`.
- **Cold Verification Path (Cache Miss)**:
  - JOSE header decodes into a `512`-byte stack buffer (`JoseHeader<'_>` borrows `&str` slices with zero allocation).
  - Signatures decode into a `1024`-byte stack buffer (covering up to RSA-8192).
  - Payloads $\le 2\text{ KiB}$ Base64URL-decode onto stack (`Cow::Borrowed`), falling back to `Vec<u8>` (`Cow::Owned`) only for oversized payloads ($> 2\text{ KiB}$), then parse JSON claims once before committing the `Arc<[(HeaderKey, HeaderValue)]>` into L3 for subsequent requests.
