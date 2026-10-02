# velda-http2

Dedicated Layer 7 HTTP/2 Protocol Engine for Velda Edge, implementing RFC 9113.

This crate is a **pure wire codec and streaming engine** — it translates between raw asynchronous byte streams and native HTTP/2 multiplexed streams (`Http2Request`/`Http2Response` and canonical `L7Request`/`L7Response`). It does not make routing decisions, manage clusters, or perform discovery. Those responsibilities belong to other bounded crates in the monorepo.

---

## What This Crate Does

When a client communicates with Velda Edge over HTTP/2, frames arrive asynchronously over a single TCP connection. This crate's responsibility is to:

1. **Terminate & Coordinate HTTP/2 Connections** (`Http2ServerConnection`): Runs H2 connection handshakes, exchanges SETTINGS frames, negotiates flow-control windows, and maintains the multiplexed stream accept loop.
2. **Decode Stream Requests & Bodies** (`decode_request`, `Http2StreamReceiver`): Parses pseudo-headers (`:method`, `:path`, `:scheme`, `:authority`), converts headers into zero-copy `HeaderMap`, enforces ingress `max_body_size`, and actively updates peer flow-control windows via `release_capacity`.
3. **Dispatch Stream Responses** (`Http2Responder`, `Http2StreamSender`): Encodes response headers and streams DATA frames concurrently without serialization head-of-line blocking, with built-in client disconnect detection.
4. **Wire Pipe Forwarding Isolated by Streaming Strategy** (`pipe/`):
   - `buffered`: Full in-memory request-response forwarding with streaming violation guards.
   - `server_stream`: Progressive response streaming (Server-Sent Events / SSE, LLM token generation, large downloads).
   - `client_stream`: Progressive request body upload with single response.
   - `duplex`: Full concurrent bidirectional streaming.
5. **Connect Upstream** (`Http2UpstreamConnector`): Establishes outbound multiplexed HTTP/2 client connections to physical upstream backends.

---

## Where It Sits In The Pipeline

```
Client TCP / TLS bytes
  │
  ▼
velda-transport          accept loop, path classification, TLS ALPN ("h2")
  │
  ▼
velda-edge pipeline      spawns per-connection task (pipeline/l7/http2.rs)
  │
  │   ┌──────────────────────────────────────────────────────────────────┐
  │   │  velda-http2 (THIS CRATE)                                        │
  │   │                                                                  │
  │   │  Phase 1: server_conn.accept_streaming_request()                 │
  │   │    → accepts new stream ID without blocking accept loop          │
  │   │    → extracts Http2RequestHead, receiver, responder              │
  │   │    → hook point: Auth / RateLimit / WAF Headers                  │
  │   │                                                                  │
  │   │  Phase 2: Pipe Strategy Dispatch (from upstream route policy)    │
  │   │    ├─ pipe_buffered: in-memory request & response buffering      │
  │   │    ├─ pipe_server_stream: progressive SSE / LLM chunk streaming  │
  │   │    ├─ pipe_client_stream: progressive request upload streaming   │
  │   │    └─ pipe_duplex: full-duplex bidirectional stream pump         │
  │   │                                                                  │
  │   │  Phase 3: responder.send_response(&resp)                         │
  │   │    → strips hop-by-hop headers in-place (sanitize_h2_headers)    │
  │   │    → dispatches HEADERS and zero-copy DATA frames                │
  │   │    → ensures explicit end_of_stream to avoid spurious RST_STREAM │
  │   └──────────────────────────────────────────────────────────────────┘
  │
  ▼
velda-router             route matching (Path, Host, Method, Headers)
velda-upstream           physical backend lifecycle & health tracking
velda-connection-pool    multiplexed H2 connection reuse
```

---

## Ingress Two-Way Architecture

The codebase strictly enforces the monorepo's **Ingress Two-Way Perspective**:

| Subsystem | Direction | Module | Key Types & Functions |
| :--- | :--- | :--- | :--- |
| **Downstream Ingress Gateway** | Client $\leftrightarrow$ Gateway | `server/` | `Http2ServerConnection`, `Http2Request`, `Http2RequestHead`, `Http2StreamReceiver`, `Http2Responder`, `Http2StreamSender` |
| **Upstream Ingress Gateway** | Gateway $\leftrightarrow$ Backend | `client/` | `Http2UpstreamConnector`, `Http2Response`, `Http2ResponseHead` |
| **Wire Pipes** | End-to-End Forwarding | `pipe/` | `Http2PipeStrategy`, `pipe_buffered`, `pipe_server_stream`, `pipe_client_stream`, `pipe_duplex` |
| **Header Sanitization** | RFC 9113 Normalization | `headers` | `sanitize_h2_headers` (in-place), `filter_h2_headers` (immutable copy) |
| **Capacity & Limits** | Host-Scaled Config | `config` | `Http2Config` (explicit 7 [`MemoryTier`] constants) |

---

## Architectural & Protocol Invariants

### 1. Zero Heap Churn Header Sanitization
RFC 9113 §8.2.2 strictly forbids hop-by-hop headers in HTTP/2 requests and responses (`connection`, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`, and invalid `te` values).
- `sanitize_h2_headers(&mut HeaderMap)` mutates existing header maps in-place without cloning or allocating a new `HeaderMap`.
- Achieves **5.87M ops/s** with near-zero allocations on the serving hot path.

### 2. Stream Error vs Connection Error Isolation (RFC 9113 §5.4)
- A payload exceeding `max_body_size` or an unparseable header on an individual stream triggers a localized stream rejection (`PayloadTooLarge` / `RST_STREAM`), **never** tearing down or interrupting innocent concurrent streams on the shared TCP connection.
- When stream errors occur, flow-control capacity is returned via `release_capacity` before stream cancellation, preventing window credit leakage.

### 3. Graceful Client Disconnect Handling
- In progressive response streaming (Server-Sent Events / LLM token generation), downstream client aborts are caught immediately:
  ```rust
  if sender.send_chunk(data).await.is_err() {
      tracing::debug!("Downstream client disconnected during H2 response streaming");
      return Ok(());
  }
  ```
  The pipeline stops upstream pumping and reclaims resources cleanly without error log spam or gateway panics.

### 4. Adversarial Resilience (CVE Mitigations)
- **Rapid Reset Attack (CVE-2023-44487)**: Enforces `max_consecutive_resets` threshold; processed **5,000 rapid resets in 6.77 ms** (~738K ops/s) with zero crash and zero memory bloat.
- **CONTINUATION Flood (CVE-2024-27983)**: Enforces `max_continuation_frames` bound per header block.
- **Payload Bombing**: Ingress limits enforced progressively chunk-by-chunk without loading oversized bodies into RAM.

---

## Performance Summary

| Benchmark Category | Measured Result | Evaluation |
| :--- | :--- | :--- |
| **Header Sanitization** | **170.37 ns** | 5.87 M ops/s |
| **Full Roundtrip (Empty Body)** | **9.75 µs** | 102.58 K ops/s |
| **Full Roundtrip (1KB Echo)** | **11.08 µs** | 90.29 K ops/s |
| **Server Streaming (SSE / Chunks)** | **12.13 µs / stream** | 412.15 K chunks/s |
| **Multiplexed Concurrency (4 Streams)** | **4.90 µs / op** | 204.13 K ops/s |
| **Multiplexed Concurrency (64 Streams)** | **5.31 µs / op** | 188.40 K ops/s |
| **Memory Leak (20,000 Ops)** | **0 B Net Heap Growth** | Verified Zero-Leak |

*For complete benchmarks and reproducible test methodologies, see [`benchmark.md`](benchmark.md).*

---

## Verification

```bash
cargo fmt --check -p velda-http2
cargo check -p velda-http2
cargo test -p velda-http2
cargo clippy -p velda-http2 --all-targets --all-features -- -D warnings
cargo bench -p velda-http2 --bench single_thread_bench
cargo bench -p velda-http2 --bench multi_thread_bench
cargo bench -p velda-http2 --bench adversarial_bench
cargo bench -p velda-http2 --bench memory_leak_bench
```
