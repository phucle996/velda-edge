# HMAC Authentication Plugin (`hmac_auth`) — Architectural Specification

Internal specification of `CredentialTable` zero-hash AGU indexing, streaming RFC 2104 verification via `ring::hmac::Context`, `HeaderWorkspace` pre-hashed lookups, and post-verification `NonceCache` concurrency.

---

## 1. End-to-End Request Pipeline

```text
  L7Request (mut req.headers: HeaderWorkspace)
    │
    ├─► 1. access_key_header (HeaderKey::get_prehashed)
    │        └─► CredentialTable::lookup ─► AGU bit-shift / _pext_u64 ─► exact slice eq
    ├─► 2. timestamp_header (HeaderKey::get_prehashed)
    │        └─► |req.ingress_time_sec() - ts| <= clock_skew_seconds
    ├─► 3. nonce_header (if enforce_nonce == true)
    │        └─► nonce_fingerprint_128(trimmed_bytes)   [lock-free; no Mutex yet]
    ├─► 4. signature_header ─► decode_client_signature on [u8; 64] stack
    │        └─► hex (case-insensitive) or base64 length + decode guard
    ├─► 5. payload SHA-256 ─► Body::Bytes digest + optional x-content-sha256 verify
    ├─► 6. sign_canonical_request (hmac::Context::with_key(&cred.key))
    │        ├─► stream METHOD \n PATH \n SORTED_QUERY \n
    │        ├─► stream pre-hashed signed_headers (+ :authority fallback for H2/H3)
    │        └─► stream joined_line \n payload_hash ─► constant_time_eq(tag, decoded)
    ├─► 7. NonceCache::check_and_record_fp (single Mutex lock, post-HMAC verify)
    └─► 8. x-consumer-id: insert_prehashed (Some) or remove spoofed header (None)
```

---

## 2. Zero-Hash Static Credential Lookup ([credential.rs](credential.rs))

`HmacAuthPlugin` credentials are static for the lifetime of a plugin snapshot and swapped wholesale on reload. Instead of hashing `access_key_id` per request in a `HashMap`, `CredentialTable::build` analyzes the static key set at initialization and emits the narrowest collision-free bit-index plan over `Slot = Option<Box<CredentialEntry>>` (`8` bytes per slot via Rust null-pointer niche optimization):

```text
+-----------------------------------------------------------------------------+
|                    CredentialTable Dispatch Hierarchy                       |
+-----------------------------------------------------------------------------+
| K == 0  ─► DispatchPlan::Empty                                              |
| K == 1  ─► DispatchPlan::Single(Box<CredentialEntry>)  [0 index arithmetic] |
| K >= 2  ─► Search order:                                                    |
|   1. ByteWindow / TailByteWindow                                            |
|      Single byte at pos (from start or end), contiguous bit slice:          |
|      idx = ((key[pos] >> shift) as usize) & mask  ─► slots[idx] (idx << 3)  |
|   2. BitGather (<= 8 differing byte offsets packed into u64, <= 10 bits)    |
|      Greedy information-gain splitting builds minimal bitmask:              |
|      - x86_64 + bmi2: _pext_u64(word, bitmask)                              |
|      - other targets: portable mask-and-shift bit gather                    |
|   3. RadixBuckets (fallback when > 10 bits would exceed 1024 pointer slots) |
|      (key[pos] ^ (last << 3) ^ (len << 1)) & mask ─► short bucket scan      |
+-----------------------------------------------------------------------------+
```

- **Non-Contiguous Entry Storage**: Pointer slots (`Box<[Option<Box<CredentialEntry>>]>`) form a compact `8`-byte AGU-scaled table (`[base + idx * 8]`), while each `CredentialEntry` (`access_key_id`, `hmac::Key`, `consumer_header`) is individually heap-allocated at reload time without forcing unused table slots to reserve `hmac::Key` bytes inline.
- **Exact Identity Check**: Every slot hit always compares `entry.access_key_id.as_ref() == access_key_id` to prevent a forged caller key sharing the same extracted bits from matching.

---

## 3. Pre-Computed HMAC Key Schedule & Streaming Canonical Message ([canonical.rs](canonical.rs))

By RFC 2104:

$$\text{HMAC}(K, m) = H\Big((K' \oplus \text{opad}) \,\|\, H\big((K' \oplus \text{ipad}) \,\|\, m\big)\Big)$$

1. **Key Schedule Pre-Computation**: `ring::hmac::Key::new` pre-compresses both $(K' \oplus \text{ipad})$ and $(K' \oplus \text{opad})$ into internal SHA states at plugin initialization. Calling `hmac::Context::with_key(&cred.key)` copies the pre-compressed SHA state directly.
2. **Dynamic Canonical Prefix Invariant**: Because canonical input starts with `METHOD \n PATH \n QUERY \n ...` and `PATH`/`QUERY` vary per request, Merkle–Damgård state cannot be pre-compressed ahead of time across request boundaries without restricting path dynamism.
3. **Zero Intermediate Buffer**: `sign_canonical_request` streams slices directly into `hmac::Context::update` — eliminating intermediate `512`-byte buffer copies and dynamic buffer growth.
4. **Canonical Query Stack Allocation**: Queries with `0` or `1` parameter stream directly; queries with `2..=16` parameters are sorted on a stack `[(&str, &str); 16]` array, falling back to `Vec` only when `> 16` query parameters exist.
5. **Multi-Value Header Determinism**: `feed_prehashed_header` iterates `HeaderWorkspace::get_all_prehashed(key)`, joining multiple header lines with `,` per RFC 9110 §5.3 so multi-value smuggling cannot slip unsigned slots past verification.
6. **Raw Digest Stack Verification**: `decode_client_signature` validates length (`64`/`128` hex, `44`/`88` base64) and decodes directly into `[u8; 64]` on the stack prior to SHA-256 / HMAC execution, accepting both uppercase and lowercase hex and verifying raw bytes with constant-time XOR accumulation (`constant_time_eq`).

---

## 4. Sharded Nonce Replay Cache ([nonce.rs](nonce.rs))

- **Conditional Allocation**: Allocated as `Option<NonceCache>` (`None` when `enforce_nonce == false`, reserving zero shards or bucket memory when nonces are disabled).
- **Post-Verify Single-Lock Flow**: `nonce_fingerprint_128` computes a 128-bit fingerprint (`u128`) without locks before HMAC; `NonceCache::check_and_record_fp` acquires the shard `Mutex` **once** strictly **after** `constant_time_eq` passes. Unauthenticated traffic with forged signatures never touches shard mutexes or poisons nonces.
- **Bounded Shards**: `(cores.max(1) * 16).next_power_of_two().clamp(16, 512)` shards aligned to `#[repr(align(64))]`, storing `u64` expiration epochs with `MAX_ENTRIES_PER_SHARD = 8192` hard ceiling after expired-entry sweep.

---

## 5. Security & Memory Layout Invariants

- **Cache-Line Packed Hot Fields**: `HmacAuthPlugin` uses `#[repr(C, align(64))]`, packing hot booleans (`is_base64_encoding`, `is_sha512`, `enforce_nonce`, `validate_request_body`), `clock_skew_seconds`, `CredentialTable`, `NonceCache`, and pre-hashed `HeaderKey`s ahead of cold `id` and `config`.
- **`x-consumer-id` Scrubbing**: If a credential specifies `consumer_id: None`, `on_upstream_request` explicitly removes any pre-existing `x-consumer-id` header supplied by the downstream client.
- **Side-Channel Hygiene**: Length/syntax checks reject malformed signatures early; digest comparison executes in constant time (`constant_time_eq`); rejections short-circuit via `#[cold] #[inline(never)] reject_unauthorized` without leaking internal failure step.
