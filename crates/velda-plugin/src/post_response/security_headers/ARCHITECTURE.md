# Security Response Headers Plugin (`security_headers`) — Architectural Specification

This document specifies the internal engine design, execution pipeline, memory invariants, and hardware optimizations of the **`security_headers`** post-response plugin (W3C / RFC 6797 / RFC 7034 / RFC 7231 / OWASP Secure Headers).

---

## 1. System Mindset & Architectural Invariants

The `security_headers` post-response plugin operates at the Layer 7 egress phase of request serving, immediately following upstream response reception and directly preceding downstream wire encoding. It enforces five primary invariants:

1. **Dual-Representation Hardware Register Separation**:
   - **Values $\le 16$ bytes** (`nosniff`, `DENY`, `SAMEORIGIN`, `same-origin`, `require-corp`, `none`, `0`): Written directly into [`velda_core::HeaderWorkspace`]'s L1 Data Cache integer registers (`set_inline_ascii`). Stored as a raw 128-bit integer (`u128`) in `numeric_entries`, executing with a single 16-byte CPU `MOV` instruction without touching the heap or `HeaderMap` hash table.
   - **Values $> 16$ bytes** (Full HSTS directives, long CSP policies, Permissions-Policy): Stored as pre-computed zero-copy reference pointers (`HeaderValue`) in RAM and inserted into `HeaderWorkspace`'s underlying map.
2. **64-Byte CPU Cacheline Boundary Alignment (`#[repr(align(64))]`)**:
   The plugin struct is explicitly aligned to a 64-byte L1 Data Cache line. This guarantees that traffic worker threads running across independent CPU cores will never induce cacheline bouncing or false-sharing invalidations during concurrent response processing.
3. **Pre-Baked Contiguous Flat Array (`[CompiledHeader; 12]`)**:
   Up to 12 defensive response headers are pre-baked during configuration compilation into a fixed-size contiguous inline array. Unlike heap-allocated dynamic containers (`Vec<T>`), accessing these headers requires zero pointer indirections and zero heap dereferences.
4. **Single-Instruction ALU Bitmask (`op_mask: u16`)**:
   All active configuration directives (HSTS, nosniff, frame-options, COOP, COEP, CORP, etc.) are pre-compiled into a 16-bit integer bitmask, enabling the CPU to query feature presence using a single ALU instruction (`TEST AX, imm16`, $\le 1$ cycle).
5. **Prefetcher & Branch Predictor Harmony**:
   Iterating over a contiguous slice `[0..headers_len]` guarantees a perfectly predictable linear access stride for the CPU Stream Prefetcher and 0% branch misprediction rate on modern out-of-order execution pipelines.

---

## 2. End-to-End Response Hot-Path Pipeline

```mermaid
flowchart TD
    subgraph UpstreamResponse["Phase 1: Upstream Response Arrival"]
        A["Upstream L7Response Received"] --> B{"Plugin Chain Execution\n(post_response FIFO)"}
    end

    subgraph HotPathEvaluation["Phase 2: L1-Resident Linear Traversal"]
        B --> C["Read self.headers_len (u8)\n(0 to 12 elements)"]
        C --> D{"headers_len == 0?"}
        D -- "Yes" --> E["Zero-Op Pass-Through\n(1 cycle: CMP AL, 0)"]
        D -- "No" --> F["Linear Loop over [0..headers_len]\n(Stream Prefetcher Stride in L1 Cache)"]
    end

    subgraph DualRepresentationInjection["Phase 3: Dual-Representation Injection"]
        F --> G{"Header Length <= 16 bytes?\n(CompiledHeader variant)"}
        G -- "Yes (<= 16B)" --> H["Write to 128-bit Register Slot\n(set_inline_ascii in HeaderWorkspace)"]
        G -- "No (> 16B)" --> I["Insert HeaderValue Pointer\n(Zero-Copy Arc Pointer in RAM)"]
        H --> J{"More Elements?"}
        I --> J
        J -- "Yes" --> F
        J -- "No" --> K["Batch Render at Phase Exit\n(HeaderWorkspace::render())"]
        K --> L["Hand off to Protocol Codec\n(H1 Text / H2 Binary / H3 QPACK)"]
    end
```

---

## 3. Core Architectural Subsystems

### 3.1 Dual-Representation `CompiledHeader`

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledHeader {
    /// Values <= 16 bytes written directly into HeaderWorkspace's L1-resident 128-bit integer register.
    InlineRegister {
        name: HeaderName,
        len: u8,
        raw: [u8; 16],
    },
    /// Values > 16 bytes stored as zero-copy HeaderValue pointers in RAM.
    StandardPointer {
        name: HeaderName,
        value: HeaderValue,
    },
}
```

- When `len <= 16`, `raw` holds the exact bytes in a 16-byte buffer resident in L1 Cache.
- Writing to `HeaderWorkspace` invokes `set_inline_ascii(name, bytes)`:
  - Inside `HeaderWorkspace`, this assigns into `numeric_entries: [Option<NumericHeaderEntry>; 16]`.
  - The entry is represented as `raw: u128` (a single hardware integer register).
  - Materialization to ASCII wire text is deferred to the single-pass batch `HeaderWorkspace::render()` at the end of the post-response phase.

### 3.2 16-Bit ALU Configuration Bitmask (`op_mask: u16`)

```rust
pub const FLAG_HSTS: u16               = 1 << 0;
pub const FLAG_NOSNIFF: u16            = 1 << 1;
pub const FLAG_FRAME_OPTIONS: u16      = 1 << 2;
pub const FLAG_REFERRER_POLICY: u16    = 1 << 3;
pub const FLAG_CSP: u16                = 1 << 4;
pub const FLAG_PERMISSIONS_POLICY: u16 = 1 << 5;
pub const FLAG_COOP: u16               = 1 << 6;
pub const FLAG_COEP: u16               = 1 << 7;
pub const FLAG_CORP: u16               = 1 << 8;
pub const FLAG_CROSS_DOMAIN: u16       = 1 << 9;
pub const FLAG_XSS_PROTECTION: u16     = 1 << 10;
```

At configuration compilation, `compile_headers()` populates `op_mask`. Inspection executes via single-cycle register bitwise testing.

### 3.3 Hardware Prefetcher & Branch Prediction Analysis

1. **Hardware Stream Prefetcher (L1 / L2)**:
   - Modern CPUs (AMD Zen 3 / Zen 4, Intel Golden Cove / Raptor Cove) detect sequential strides across contiguous memory lines.
   - Because `headers: [CompiledHeader; 12]` is stored inline within `SecurityHeadersPlugin`, traversing `0..headers_len` represents a strictly ascending memory read.
   - The hardware stream prefetcher loads subsequent elements ahead of execution, maintaining zero cache wait states.
2. **Branch Predictor (TAGE Branch Predictor)**:
   - The loop iteration `for header in &self.headers[..self.headers_len as usize]` is bounded by a fixed integer.
   - The branch direction is always taken for `N-1` iterations and falls through on the `N`-th, yielding essentially 100% branch prediction accuracy.
   - Within the loop, the variant check `match header` is highly predictable because configuration is immutable throughout the life of the worker thread.

---

## 4. Memory Layout & Cacheline Analysis

```text
+---------------------------------------------------------------+ <- Offset 0 (Aligned 64B)
| id: String (ptr: 8B, cap: 8B, len: 8B = 24B)                  |
| config: SecurityHeadersConfig (~128B)                         |
| op_mask: u16 (2B)                                             |
| headers_len: u8 (1B)                                          |
| padding (5B)                                                  |
+---------------------------------------------------------------+ <- Offset 160 (Cachelines 3..10)
| headers: [CompiledHeader; 12]                                 |
|   Each element: 40 bytes (Tag: 8B + HeaderName: 8B + Payload) |
|   Total headers payload: 12 * 40B = 480B                      |
+---------------------------------------------------------------+ <- Offset 640
| struct alignment padding to 64-byte multiple (0B)             |
+---------------------------------------------------------------+ <- Total Size: 640B (10 exact cachelines)
```

No heap allocations occur during request serving (`on_response`). All header values are pre-computed handles stored contiguously.

---

## 5. Security & Browser Defense Mechanisms

| Header Directive | Size Category | Storage Tier | OWASP Threat Mitigation | Standard |
| :--- | :---: | :---: | :--- | :--- |
| `Strict-Transport-Security` | $> 16$B | RAM Pointer | Enforces HTTPS; defeats SSL-stripping & MITM downgrade attacks. | RFC 6797 |
| `X-Content-Type-Options: nosniff` | $7$B | **Inline u128** | Disables browser MIME-sniffing exploits (executable disguised as image). | RFC 7231 |
| `X-Frame-Options: DENY` | $4$B | **Inline u128** | Blocks iframe embedding; neutralizes Clickjacking. | RFC 7034 |
| `Referrer-Policy: strict-origin...` | $> 16$B / $\le 16$B | RAM Pointer / Register | Controls referrer metadata leakage across external origins. | W3C Referrer |
| `Content-Security-Policy` | $> 16$B | RAM Pointer | Restricts script, style, and media source domains; defeats XSS. | W3C CSP Level 3 |
| `Permissions-Policy` | $> 16$B | RAM Pointer | Disables browser hardware APIs (microphone, camera, geolocation). | W3C Permissions |
| `Cross-Origin-Opener-Policy: same-origin` | $11$B | **Inline u128** | Isolates browsing context groups; mitigates Spectre side-channels. | W3C COOP |
| `Cross-Origin-Embedder-Policy: require-corp` | $12$B | **Inline u128** | Requires embedded resources to explicitly grant cross-origin permission. | W3C COEP |
| `Cross-Origin-Resource-Policy: same-origin` | $11$B | **Inline u128** | Prevents cross-origin apps from reading sensitive resources. | W3C CORP |
| `X-Permitted-Cross-Domain-Policies: none` | $4$B | **Inline u128** | Restricts Flash / PDF cross-domain policy documents (`none`). | OWASP Standard |
| `X-XSS-Protection: 0` | $1$B | **Inline u128** | Disables legacy buggy XSS auditors which introduced security vulnerabilities. | OWASP Best Practice |
