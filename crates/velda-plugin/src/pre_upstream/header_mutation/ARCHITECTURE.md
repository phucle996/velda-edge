# Header Mutation Plugin (`header_mutation`) — Architectural Specification

Technical specification of memory layout, compile-time pre-hashing, SIMD tag-filtered `HeaderWorkspace` operations, and complexity bounds in `header_mutation`.

---

## 1. Execution Model & Invariants

`header_mutation` runs on Frontline Traffic Workers (`velda-worker-{i}`) in `pre_upstream` against `req.headers: HeaderWorkspace`.

```text
+------------------------------------------------------------------------------+
|  HeaderMutationPlugin (#[repr(C, align(64))])                                |
|  [+0] op_mask: u8  (bit 0: OP_REMOVE | bit 1: OP_SET | bit 2: OP_APPEND)    |
+------------------------------------------------------------------------------+
        |
        v  op_mask & OP_REMOVE
+------------------------------------------------------------------------------+
|  For each key: &HeaderKey in remove_keys                                     |
|    1. req.headers.contains_prehashed(key)  <-- AVX2/SSE2/NEON/SWAR tag7 scan |
|    2. If hit: req.headers.remove(&key.name) <-- removes ALL matching slots   |
+------------------------------------------------------------------------------+
        |
        v  op_mask & OP_SET
+------------------------------------------------------------------------------+
|  For each (key, val) in sets                                                 |
|    req.headers.insert_prehashed(key, val.clone())                            |
|    - Updates first matching slot in place (clears bypass bit to 0)           |
|    - Removes any trailing duplicate slots                                    |
|    - Or pushes new slot with tag = key.tag7                                  |
+------------------------------------------------------------------------------+
        |
        v  op_mask & OP_APPEND
+------------------------------------------------------------------------------+
|  For each (name, val) in appends                                             |
|    req.headers.append(name.clone(), val.clone())                             |
+------------------------------------------------------------------------------+
        |
        v
  Action::Continue
```

### Core Architectural Invariants

1. **Zero Implicit Header Touches**: Unconfigured headers are never read, validated, or mutated. There is no hop-by-hop scanner, no `is_protected_*` / `is_forbidden_*` filter, and no fallback across headers.
2. **Bypass-Tag Agnostic Deletion & Overwrite**: `HeaderWorkspace::remove_by_name` and `HeaderWorkspace::insert_internal_tag7` match slots via `(tag & TAG_BYTE_HASH_MASK) == tag7` (`TAG_BYTE_HASH_MASK = 0x7F`), ignoring bit 7 (`TAG_BYTE_BYPASS_BIT = 0x80`). Configured rules can delete or overwrite bypass-tagged entries such as `x-request-id` or `host`.
3. **Deterministic Compile-Time Ordering**: During `HeaderMutationPlugin::from_config`, `remove_headers`, `set_headers`, and `append_headers` are parsed into strongly typed `HeaderName` / `HeaderValue` values and sorted lexicographically (`sort_unstable_by`), eliminating `HashMap` iteration-order variance across restarts.
4. **Compile-Time / Init-Time FNV-1a Pre-Hashing**: `remove_keys` and `sets` compute `HeaderKey { hash64, tag7, name }` once at plugin construction (`HeaderKey::from_name`), avoiding repeated FNV-1a hashing for `contains_prehashed` and `insert_prehashed` on the serving hot path.

---

## 2. Memory Layout (`HeaderMutationPlugin`)

Defined in [plugin.rs](plugin.rs) with `#[repr(C, align(64))]`:

```text
Offset 0 (Cache Line 0 - Hot Dispatch Metadata & Vec Headers)
+------------------+----------------------------------------------------------+
| op_mask: u8      | Bitmask of enabled ops (0 => immediate return Continue) |
| (padding: 7B)    | Aligns first Vec descriptor to 8-byte boundary           |
| remove_keys      | Vec<HeaderKey>            (24B ptr/cap/len)              |
| sets             | Vec<(HeaderKey, HeaderValue)> (24B ptr/cap/len)          |
| appends          | Vec<(HeaderName, HeaderValue)> (24B ptr/cap/len)         |
+------------------+----------------------------------------------------------+
Cold Introspection Region
+------------------+----------------------------------------------------------+
| id: String       | Plugin identifier                                        |
| config           | HeaderMutationConfig (remove, set, append retained cfg)  |
+------------------+----------------------------------------------------------+
```

---

## 3. SIMD Tag Filtering & Multi-Target Dispatch

When `contains_prehashed(key)` and `insert_prehashed(key, val)` probe `HeaderWorkspace::tags`, `velda-core` dispatches at compile time (`crates/velda-core/src/buffer/header.rs`) across the active target matrix:

- **`x86_64` + `avx2`**: Probes 32 `tags` bytes per cycle (`_mm256_and_si256` with `0x7F` + `_mm256_cmpeq_epi8` + `_mm256_movemask_epi8`).
- **`x86_64` + `sse2` (baseline)**: Probes 16 `tags` bytes per cycle (`_mm_and_si128` + `_mm_cmpeq_epi8` + `_mm_movemask_epi8`).
- **`aarch64` (`neon`)**: Probes 16 `tags` bytes per cycle (`vandq_u8` + `vceqq_u8` + 128-bit lane reduction).
- **Portable 64-bit SWAR fallback (`not(avx2|sse2|aarch64)`)**: Probes 8 `tags` bytes per cycle via general-purpose 64-bit ALU mask arithmetic.

False positive `tag7` hits (rate $\le 1/127$ for a uniform 7-bit tag modulo `{1..127}`) are verified immediately against exact `HeaderName` before returning or mutating a slot.

---

## 4. Complexity Bounds

Let $L$ be the current header count in `HeaderWorkspace` ($L \le 256$ hard ceiling), $R$ be `remove_keys.len()`, $S$ be `sets.len()`, and $A$ be `appends.len()`:

- **No-op (`op_mask == 0`)**: $O(1)$ — single byte load + branch.
- **Remove (`OP_REMOVE`)**: $O(R \cdot \lceil L / W \rceil)$ tag scan (where SIMD width $W \in \{32, 16, 8\}$), plus $O(L)$ contiguous slot shift only when a header is actually present and deleted.
- **Set (`OP_SET`)**: $O(S \cdot \lceil L / W \rceil)$ tag scan for overwrite lookup, or $O(1)$ append to end of `slots` when the header does not exist (until capacity doubling at `32 -> 64 -> 128 -> 256`).
- **Append (`OP_APPEND`)**: $O(A)$ amortized slot append (`HeaderWorkspace::append`).
- **Heap Allocations in `on_upstream_request`**: Zero function-local heap allocations in `HeaderMutationPlugin` itself; `HeaderName`/`HeaderValue` clones are reference-counted `Bytes` increments (`bytes::Bytes::clone`) or inline `HeaderName` copies. Dynamic heap growth only occurs inside `HeaderWorkspace` if `append`/`insert_prehashed` crosses a geometric block capacity boundary (`32 -> 64 -> 128 -> 256`).
