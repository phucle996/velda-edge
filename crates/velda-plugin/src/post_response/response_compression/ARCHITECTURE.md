# Response Compression Plugin — Architectural Design Specification

This document details the architectural invariants, hardware-level optimization patterns, and zero-allocation execution model of the `response_compression` plugin in **Velda Edge**.

---

## 1. System Topology & Lifecycle Integration

In accordance with the **Hourglass Architecture** (`docs/HOURGLASS_MODEL.md`), the `response_compression` plugin executes strictly during the **`post_response`** phase in the bottom cone of the request pipeline. It operates purely on outgoing response payloads after upstream completion or cache lookup.

```mermaid
flowchart TD
    subgraph Downstream Cone
        Client([HTTP/1.1, HTTP/2, HTTP/3 Client]) --> Ingress[Listener & Protocol Engine]
        Ingress --> BorrowWorkspace[Borrow HeaderWorkspace <br/> <i>req_headers: Accept-Encoding</i>]
        BorrowWorkspace --> Router[In-Memory Waist Router]
    end

    subgraph Upstream Cone
        Router --> CacheLookup{Cache Hit?}
        CacheLookup -->|Hit: Canonical Raw| PostResponsePipeline
        CacheLookup -->|Miss| UpstreamForward[Forward to Upstream]
        UpstreamForward --> UpstreamRaw[Receive Raw Body from Backend]
        UpstreamRaw --> CacheStore[http_cache_store: Store Canonical Raw]
        CacheStore --> PostResponsePipeline
    end

    subgraph PostResponse Phase [response_compression Plugin]
        PostResponsePipeline --> CheckGuard{Guards Pass?<br/><i>Status != 204/304<br/>No Content-Encoding<br/>Size >= min_length<br/>MIME Allowed</i>}
        CheckGuard -->|No| Passthrough[Return Raw Payload]
        CheckGuard -->|Yes| StackNeg[RFC 9110 Content Negotiation<br/><i>Zero-Alloc Stack Buffer</i>]
        StackNeg --> CodecDispatch{Selected Codec}
        CodecDispatch -->|Zstd| ZstdEngine[Thread-Local Zstd Bulk Compressor]
        CodecDispatch -->|Brotli| BrotliEngine[Brotli In-Memory Encoder]
        CodecDispatch -->|Gzip/Deflate| ZlibEngine[SIMD zlib-rs Encoder]
        ZstdEngine --> StreamRAM[Non-Temporal Store <br/> <i>_mm256_stream_si256 direct to RAM</i>]
        BrotliEngine --> StreamRAM
        ZlibEngine --> StreamRAM
        StreamRAM --> InflationGuard{Compressed < Raw?}
        InflationGuard -->|No| RevertRaw[Preserve Original Raw Body]
        InflationGuard -->|Yes| Finalize[Update Content-Length<br/>Inject Content-Encoding<br/>Inject Vary: Accept-Encoding]
        RevertRaw --> DownstreamExit
        Finalize --> DownstreamExit
        Passthrough --> DownstreamExit
    end

    DownstreamExit --> DownstreamEncode[Wire Framer & NIC Write]
    DownstreamEncode --> Client
```

---

## 2. Hardware-Conscious Design Invariants

### 2.1 Non-Temporal Streaming Store Directly to RAM (`streaming_copy.rs`)

Traditional HTTP proxies compress response buffers in normal CPU cacheable memory. When compressing thousands of concurrent responses (e.g. 32 KB JSON payloads), writing compressed output through L1/L2/L3 caches evicts critical metadata:
- Route lookup tables (`velda-router`)
- Active upstream connection pool entries (`velda-connection-pool`)
- TLS session keys and QUIC cryptographic state

This eviction phenomenon is known as **CPU Cache Thrashing**.

#### The Non-Temporal Solution:
Velda Edge utilizes hardware-level **Non-Temporal Store instructions** (`_mm256_stream_si256` on AVX2, `_mm_stream_si128` on SSE2) coupled with a memory fence (`_mm_sfence`):

```
┌─────────────────────────────────────────────────────────────┐
│ CPU Core (Registers)                                        │
└──────────────┬──────────────────────────────────────────────┘
               │ 32-Byte Chunk (_mm256_stream_si256)
               ▼
┌─────────────────────────────────────────────────────────────┐
│ Hardware Write-Combining (WC) Buffers                       │
└──────────────┬──────────────────────────────────────────────┘
               │ Drains in 64-Byte Cache-Line Bursts
               │ (Bypasses L1d, L2, and L3 Shared Caches)
               ▼
┌─────────────────────────────────────────────────────────────┐
│ System Physical RAM                                         │
└─────────────────────────────────────────────────────────────┘
```

1. **Cache Bypass**: Output writes do not allocate cache lines in L1 or L2.
2. **Write-Combining (WC)**: Chunks are gathered into internal processor WC buffers and flushed across the memory bus in full 64-byte bursts.
3. **Serialization Drain (`_mm_sfence`)**: Drains and commits all WC buffers prior to passing the `bytes::Bytes` slice to the network driver.

---

### 2.2 Compile-Time CPU Dispatch (`streaming_copy.rs`)

Rather than evaluating CPU feature flags or branching via `LazyLock<fn(...)>` on each request hot path, Velda Edge dispatches directly at **compile time** using `#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]` with `#[inline(always)]`:

```rust
#[inline(always)]
pub fn streaming_copy(src: &[u8], dst: &mut [u8]) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        unsafe { avx2_streaming_copy_impl(src, dst) };
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        unsafe { sse2_streaming_copy_impl(src, dst) };
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        scalar_streaming_copy(src, dst);
    }
}
```

- **Zero Indirect Call**: Eliminates `LazyLock` and function-pointer indirection; LLVM dead-code-eliminates non-matching branches at compile time.
- **Portability**: Automatically compiles down to SSE2 on baseline x86-64 or portable scalar copy on non-x86 architectures (e.g. ARM64 Neoverse / Apple Silicon).

---

### 2.3 Zero-Allocation Stack Negotiation (`negotiation.rs`)

Content negotiation under RFC 9110 § 12.5.3 requires parsing comma-separated tokens with quality factors (e.g. `gzip, deflate;q=0.8, br;q=1.0, zstd;q=0.9, *;q=0.1`).

#### Eliminating Heap Overhead:
- Standard implementations allocate temporary `String`, `Vec<String>`, and `HashMap` instances per request.
- Velda Edge uses a **stack-allocated array** bounded to 8 items (`[Option<ClientPreference>; 8]`):
  ```rust
  pub const MAX_STACK_PREFERENCES: usize = 8;
  ```
- **Bitmask Representation**: Algorithms are represented as single-bit masks (`ENCODING_GZIP`, `ENCODING_DEFLATE`, `ENCODING_BROTLI`, `ENCODING_ZSTD`), allowing bitwise comparisons and fast-path matching for single-algorithm headers (`gzip`, `zstd`, `br`) in < 5 ns.

---

### 2.4 Thread-Local Compressor Context & Scratch Buffer Reuse (`engine.rs`)

Zstandard compression contexts (`ZSTD_CCtx`) require ~128 KB of internal memory structures. Creating a new compressor context per request would induce massive heap churn (`malloc`/`free`) and catastrophic latency degradation.

Velda Edge enforces thread-local reuse:
```rust
thread_local! {
    static SCRATCH_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 * 1024));
    static ZSTD_COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
}
```

- **Zero Allocation on Serving**: `SCRATCH_BUF` is pre-warmed to 64 KB and reused across consecutive requests on the same OS thread.
- **Bulk Compressor Reuse**: `ZSTD_COMPRESSOR` maintains a prepared `zstd::bulk::Compressor` instance throughout the lifetime of the worker thread.

---

## 3. Strict Operational Invariants

| Invariant | Enforcement Mechanism | Rationale |
| :--- | :--- | :--- |
| **Exact Request Workspace Borrowing** | `req_headers: &velda_core::HeaderWorkspace` | Guarantees zero cross-request contamination in pipelined and multiplexed connections. |
| **Zero Double-Compression** | `resp.headers.contains_key(CONTENT_ENCODING)` | Upstream or cached compressed payloads are never compressed a second time. |
| **Inflation Safeguard** | `compressed.len() >= original_bytes.len()` | If compressed output expands (due to header/entropy limits), the raw uncompressed body is retained. |
| **Status Code Exclusion** | `204 No Content`, `304 Not Modified`, `1xx Informational` | RFC 9110 compliance: responses without permissible bodies are never modified. |
| **MIME Whitelist Matching** | Slice prefix matching before `;` (`eq_ignore_ascii_case`) | Zero-copy validation against configured content types (e.g. `application/json`). |
| **Cache Poisoning Prevention** | Injects `Vary: Accept-Encoding` | Prevents downstream caches from serving compressed variants to clients that cannot decode them. |
