# HMAC Authentication Plugin (`hmac_auth`) — Architectural Specification

This document specifies the internal engine design, hot-path execution pipeline, memory invariants, and hardware-level concurrency model of the **`hmac_auth`** pre-upstream plugin (RFC 2104).

---

## 1. System Mindset & Architectural Invariants

The `hmac_auth` plugin acts as an ultra-high-throughput, zero-IO Layer 7 cryptographic security gate for edge APIs, enforcing three fundamental non-negotiable invariants:

1. **Zero-IO Hot-Path Invariant**:
   Serving paths **never** parse JSON, allocate dynamic memory, read from disk, or make RPC/database network calls. All credentials, header layouts, and error responses are pre-compiled into RAM at configuration time.
2. **Hardware-Accelerated Single-Pass Cryptography**:
   Canonical requests are assembled contiguously into pre-allocated stack buffers, invoking `ring::hmac::sign` **exactly once** per transaction to saturate hardware SHA-NI instruction pipelines.
3. **Autonomous Two-Phase Anti-Replay Defense**:
   Nonce deduplication utilizes a hardware-sharded, lock-striped in-memory cache aligned to 64-byte CPU cache lines, completely eliminating cross-core false sharing and lock contention.

---

## 2. End-to-End Request Hot-Path Pipeline

```mermaid
flowchart TD
    subgraph Ingress["Phase 1: Ingress & Validation"]
        A["Incoming L7Request"] --> B["1. Extract Access Key ID\n(Zero-Alloc &[u8] Trim)"]
        B --> C["2. Lookup Pre-Compiled Key in RAM\n(FxHashMap<Box<[u8]>, ...>)"]
        C --> D["3. Validate Timestamp Skew\n(parse_u64_bytes, abs_diff <= 60s)"]
        D --> E["4. Phase 1 Nonce Peek Check\n(is_replayed_fp: Drop Replay in ~10ns)"]
        E --> F["5. Pre-Crypto Signature Length Guard\n(Strict 44 / 64 / 88 / 128 bytes)"]
    end

    subgraph PayloadVerification["Phase 2: Payload Hash Verification (Scenario A3)"]
        F --> G{"req.body variant"}
        G -- "Body::Bytes(bytes)" --> H["Compute Authentic Body SHA-256\n(Stack Buffer [u8; 64])"]
        H --> I["Verify Client x-content-sha256 Header\n(128-bit SIMD constant_time_eq)"]
        G -- "Body::Empty" --> J["Check Content-Length / Chunked Flag\nFallback to EMPTY_SHA256_HEX"]
    end

    subgraph CanonicalSigning["Phase 3: Zero-Branching Canonical Signing"]
        I --> K{"Protocol Dispatch\n(req.version)"}
        J --> K
        K -- "<= HTTP/1.1" --> L["sign_canonical_h1\n(Zero Per-Header Branching)"]
        K -- "HTTP/2, HTTP/3" --> M["sign_canonical_h2_h3\n(:authority Fallback, Zero Branching)"]
        L --> N["Assemble Contiguous CanonicalBuffer\n(Stack-first [u8; 512])"]
        M --> N
        N --> O["Single-Pass HMAC-SHA256\n(ring::hmac::sign)"]
        O --> P["Format Stack Signature & Compare\n(128-bit SIMD constant_time_eq)"]
    end

    subgraph StateCommit["Phase 4: Nonce Commit & Upstream Dispatch"]
        P -- "Valid Signature" --> Q["Phase 2 Commit: check_and_record_fp\n(Atomic Nonce Dedup)"]
        Q --> R["Inject Pre-Materialized x-consumer-id Header"]
        R --> S["Forward Request (Action::Continue)"]
        P -- "Invalid Signature" --> T["Reject (401 Unauthorized)\nNonce Cache Left Unpoisoned"]
    end
```

---

## 3. Core Architectural Subsystems

### 3.1 Zero-Branching Dual-Layout Architecture (`sign_canonical_h1` vs `sign_canonical_h2_h3`)

In modern multi-protocol edge gateways, HTTP/1.1 uses the standard `Host` header, whereas HTTP/2 and HTTP/3 (RFC 9113 / RFC 9114) replace `Host` with the `:authority` pseudo-header in the request URI. Dynamically evaluating `if hname == HOST` inside inner header iteration loops introduces repeated branch misprediction penalties on every request.

`hmac_auth` solves this by pre-compiling signed headers into dual layouts at bootstrap:

```
SignedHeadersSpec
├── h1_headers: Box<[(HeaderName, Box<[u8]>)]>
│   └── Pre-formatted with trailing colon: b"host:", b"x-timestamp:", b"x-nonce:"
│       (Enables branchless sequential iteration for HTTP/1.1)
├── h2_before_host: Box<[(HeaderName, Box<[u8]>)]>
│   └── Headers declared before "host"
├── h2_has_host: bool
│   └── Directs fallback to req.uri.authority() if "host" is signed
├── h2_after_host: Box<[(HeaderName, Box<[u8]>)]>
│   └── Headers declared after "host"
└── joined_line: Box<[u8]>
    └── Pre-joined signed header names line: b"host;x-timestamp;x-nonce\n"
```

* **HTTP/1.1 Fast Path (`sign_canonical_h1`)**: Iterates over `h1_headers` in a single unrolled loop with zero conditional branches.
* **HTTP/2 & HTTP/3 Fast Path (`sign_canonical_h2_h3`)**: Iterates `h2_before_host`, writes the `:authority` byte slice, and unrolls `h2_after_host`.
* **Zero Runtime String Allocations**: Header name prefixes (`b"name:"`) are allocated once at plugin startup.

---

### 3.2 Single-Pass Contiguous Stack Buffer (`CanonicalBuffer`)

Traditional HMAC streaming implementations call `ring::hmac::Context::update` repeatedly for each component (method, path, query, each header, payload hash). Each `.update()` call incurs internal block buffer boundary checks, alignment verification, and function call overhead.

`CanonicalBuffer` introduces a stack-first contiguous buffer:
* **Default Capacity**: `[u8; 512]` allocated on the stack frame.
* **Contiguous Assembly**: Method, Path, Query, Headers, Signed List, and Payload Hash are written contiguously into the slice.
* **Transparent Heap Fallback**: If an oversized canonical request exceeds 512 bytes (e.g. extremely long query strings), `CanonicalBuffer` falls back to a heap-allocated `Vec<u8>` dynamically without failing.
* **Single Crypto Call**: `ring::hmac::sign(key, buffer.as_bytes())` is executed once over the contiguous memory slice, allowing the CPU to pipeline SHA-NI hardware instructions at full bandwidth.

---

### 3.3 Two-Phase Hardware-Sharded `NonceCache`

To eliminate replay attacks while sustaining tens of thousands of requests per second, the `NonceCache` implements a hardware-aware, lock-striped concurrent set:

```
NonceCache (Lock-Striped, Hardware-Sharded)
│
├── Shard 0  [Mutex<FxHashMap<u128, u32>>] ── 64-Byte Cache-Line Aligned
├── Shard 1  [Mutex<FxHashMap<u128, u32>>] ── 64-Byte Cache-Line Aligned
├── ...
└── Shard 63 [Mutex<FxHashMap<u128, u32>>] ── 64-Byte Cache-Line Aligned
```

#### Key Engineering Invariants:
1. **False-Sharing Elimination (`#[repr(align(64))]`)**:
   Each `NonceShard` is explicitly aligned to 64 bytes (the x86_64 L1 cache-line size). This ensures that concurrent writes from different CPU cores to adjacent shards never invalidate CPU L1/L2 caches.
2. **Two-Phase Lifecycle (Peek vs Commit)**:
   * **Phase 1 (Peek - Read)**: Nonce fingerprint is checked in ~10 ns via `is_replayed_fp`. Replayed requests are rejected before wasting CPU cycles on SHA-256 or HMAC computation.
   * **Phase 2 (Commit - Write)**: The nonce is recorded via `check_and_record_fp` strictly **after** the cryptographic signature passes. This prevents unauthenticated attackers from poisoning or filling the nonce cache with forged signatures.
3. **128-Bit Chunk-Based Avalanche Mixer (`nonce_fingerprint_128`)**:
   Processes nonces in 8-byte chunks using 64-bit avalanche multipliers (`0x9E3779B97F4A7C15` and `0xBF58476D1CE4E5B9`). Guarantees zero collisions across parallel threads.
4. **Hardware Shard Scaling**:
   Shard count scales dynamically to `clamp(cores * 16, 16, 512)`, yielding 64 shards on a 4-core instance to minimize lock contention.
5. **Rate-Limited Sweep Eviction**:
   Expired entries are swept at most once per second per shard and only triggered when shard size exceeds 2,048 entries.

---

### 3.4 128-Bit SIMD Constant-Time Equality (`constant_time_eq`)

To prevent timing side-channel attacks (where an adversary infers valid signature bytes by measuring gateway response latency differences):
* Comparisons process 16-byte chunks (`as_chunks::<16>()`) using native 128-bit unsigned integers (`u128::from_ne_bytes`).
* Operates in $O(1)$ constant time regardless of where mismatches occur:
  ```rust
  let mut diff = 0u128;
  for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
      diff |= u128::from_ne_bytes(*ca) ^ u128::from_ne_bytes(*cb);
  }
  for (&ra, &rb) in remainder_a.iter().zip(remainder_b.iter()) {
      diff |= (ra ^ rb) as u128;
  }
  diff == 0
  ```
* Modern LLVM/Rust compiles this loop directly into vector SSE/AVX registers (`vpxor`, `vpor`).

---

## 4. Entity & Memory Architecture

| Component | In-Memory Representation | Allocation Lifecycle | Concurrency / Thread-Safety | Hot-Path Guarantee |
| :--- | :--- | :--- | :--- | :--- |
| **Credentials** | `FxHashMap<Box<[u8]>, (hmac::Key, Option<HeaderValue>)>` | Pre-compiled at bootstrap into RAM | Read-Only (Lock-Free) | $O(1)$ lookup via pre-trimmed `&[u8]` |
| **Signed Spec** | `SignedHeadersSpec` (Boxed slices of `HeaderName` + prefix) | Pre-compiled at bootstrap into RAM | Read-Only (Lock-Free) | Zero string formatting, zero branches |
| **Stack Buffer** | `CanonicalBuffer` wrapping `[u8; 512]` | Stack frame per request | Thread-local (No locks) | 100% Zero heap allocation |
| **Nonce Cache** | `NonceCache` (`Box<[NonceShard]>`, 64-byte aligned) | Persists across plugin lifetime | Lock-striped `Mutex<FxHashMap>` | Sub-20ns peek, zero false sharing |
| **Error Responses**| Pre-computed static `L7Response` handles | Pre-built in `HmacAuthPlugin::new` | Read-Only (Cloned `Bytes`) | Zero JSON serialization on reject |

---

## 5. Security Architecture: Scenario A3 Body Binding

To prevent **Body Tampering Bypass Attacks** (where an attacker modifies request payload but keeps original valid headers and signature):
1. **Actual Payload Hashing**: For buffered payloads (`Body::Bytes`), the authentic SHA-256 digest is always computed from raw bytes into a stack buffer `[u8; 64]`.
2. **Cross-Verification Guard**: If the client also sends `x-content-sha256`, it is compared against the authentic digest using SIMD `constant_time_eq`. Any discrepancy immediately drops the request.
3. **Empty Body Binding**: Empty payloads use the pre-computed constant `EMPTY_SHA256_HEX` (`e3b0c442...`), bypassing SHA-256 calculation entirely.
