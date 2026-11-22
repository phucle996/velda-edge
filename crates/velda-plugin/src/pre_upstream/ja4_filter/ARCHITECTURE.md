# JA4 TLS Fingerprint Filter (`ja4_filter`) — Deep Architecture

## 1. End-to-End Zero-Allocation Data Flow

```text
+-----------------------------------------------------------------------------------+
| 1. [velda-edge] DownstreamMeta::peek_tcp_ja4(&connection.stream)                  |
|    - Non-destructive kernel peek (TcpStream::peek up to 2048B on stack)           |
|    - Does NOT consume bytes from rustls handshake stream                          |
|                                                                                   |
| 2. [velda-tls] compute_ja4_u64(&buf[..n], false)                                  |
|    - Parses TLS Record (0x16) -> Handshake (0x01 ClientHello)                     |
|    - Filters RFC 8701 GREASE values (0x0a0a, 0x1a1a, ..., 0xfafa)                 |
|    - Sorts ciphers & extensions in fixed stack arrays ([u16; 128])                |
|    - Streams into Sha256 -> formats 36B [u8; 36] on stack                         |
|    - Hashes [u8; 36] -> u64 via FNV-1a 64-bit (non-zero; 0 = sentinel)            |
|                                                                                   |
| 3. [velda-core] RequestState (64 Bytes = 1 L1d Cache Line)                        |
|    - Compile-time invariant: assert!(size_of::<RequestState>() <= 64)             |
|    - ja4: u64 sits directly beside ingress_time_nanos inside the same cache line  |
|                                                                                   |
| 4. [velda-plugin] Ja4FilterPlugin::on_upstream_request()                          |
|    - ja4 == 0 -> immediate Continue                                               |
|    - len <= 16 -> unrolled linear scan                                            |
|    - len > 16  -> branchless binary search (O(log N))                             |
+-----------------------------------------------------------------------------------+
```

---

## 2. Memory Layout & Cache-Line Geometry

In `crates/velda-core/src/context.rs`, `RequestState` was previously `48` bytes (with `16` bytes of alignment padding remaining before crossing into a second `64`-byte L1d cache line). Storing the compressed `ja4: u64` (`8` bytes) keeps `std::mem::size_of::<RequestState>() == 64` — avoiding the cache-line split that a `36`-byte heap `String` would cause.

`Ja4FilterPlugin` compiles `deny` entries at load time into a contiguous, sorted, deduplicated `Box<[u64]>`:
- `16` entries = `128` bytes (fits in 2 L1d cache lines).
- `128` entries = `1,024` bytes (fits completely in L1d).

---

## 3. Verified Release Assembly (`x86_64`)

Audited from `cargo rustc -p velda-plugin --lib --release -- --emit=asm`:
- Symbol: `Ja4FilterPlugin::on_upstream_request` (`71` instructions).
- **Stack Frame**: **`0` bytes** (zero `sub rsp`, zero callee-saved register spills).
- **Function Calls**: **`0`** (`call` count = `0`).
- **Heap Allocations**: **`0`**.
- **Linear Scan (`<= 16` rules)**: LLVM unrolls 8-way (`cmp qword ptr [rdi + 8*r10 + 0..56], rdx`).
- **Binary Search (`>= 17` rules)**: Branchless midpoint update (`cmova r8, r9`).

---

## 4. Crate Boundary & Security Invariants

- **Strict Crate Isolation**: `velda-tls` has zero knowledge of `velda-plugin`, routes, or L7 policies; it only extracts `ja4: u64`. `velda-plugin` has zero knowledge of TLS sockets or `rustls`; it only reads `req.state.ja4`.
- **Fail-Safe Malformed Input Handling**: Non-TLS, truncated, or fragmented `ClientHello` buffers in `compute_ja4_u64` safely return `0` (`Action::Continue`) without panicking or disrupting `rustls` handshake validation.
- **External Wire Indistinguishability**: Rejected connections return generic `403 Forbidden` (HTTP) or `PERMISSION_DENIED` (gRPC) without exposing JA4 fingerprint headers.
