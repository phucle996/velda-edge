# velda-http1

Dedicated Layer 7 HTTP/1.1 Protocol Engine for Velda Edge, implementing RFC 9112.

This crate is a **pure wire codec** — it translates between raw TCP bytes and native `Http1Request`/`Http1Response` domain types. It does not make routing decisions, manage connections, or talk to backends. Those responsibilities belong to other crates in the monorepo.

---

## What This Crate Does

When a client sends an HTTP/1.1 request to Velda Edge, the bytes arrive on a TCP stream. This crate's job is to:

1. **Decode** those bytes into structured parts (`Http1RequestHead` + `Body` or `Http1Request`).
2. **Encode** structured parts (`Http1ResponseHead` + `Body` or `Http1Response`) into wire bytes using **zero-copy body streaming**.
3. **Forward** a request upstream by encoding it onto a backend stream and decoding the response that comes back.

That's it. This crate intentionally has no knowledge of routing, TLS, load balancing, health checking, or connection pooling. It is a self-contained protocol engine that speaks HTTP/1.1 on byte streams.

---

## Where It Sits In The Pipeline

```
Client TCP bytes
  │
  ▼
velda-transport          accept loop, path classification
  │
  ▼
velda-edge pipeline      spawns per-connection task (pipeline/l7/http1.rs)
  │
  │   ┌────────────────────────────────────────────────────────┐
  │   │  velda-http1 (THIS CRATE)                              │
  │   │                                                        │
  │   │  Phase 1: conn.next_request_head()                     │
  │   │    → parses method, URI, version, HeaderMap, framing   │
  │   │    → hook point for Auth / RateLimit / WAF header      │
  │   │                                                        │
  │   │  Phase 2: conn.read_body(framing)                      │
  │   │    → reads body (Content-Length / Chunked)             │
  │   │    → hook point for WAF body / payload inspection      │
  │   │                                                        │
  │   │  Phase 3: pipe::forward_request()                      │
  │   │    → serializes RequestHead and streams Body           │
  │   │    → reads ResponseHead and Body from backend          │
  │   │                                                        │
  │   │  Phase 4: conn.send_response(&response)                │
  │   │    → encodes ResponseHead into small write buffer      │
  │   │    → streams Body slice directly (ZERO COPY)           │
  │   └────────────────────────────────────────────────────────┘
  │
  ▼
velda-router             route matching (separate crate, called by pipeline)
velda-upstream           backend topology & health (separate crate, no dependency on velda-http1)
```

**Important:** All connection and request processing is orchestrated by `velda-edge::pipeline::http1`, which creates `Http1ServerConnection` and passes upstream streams into `forward_request()`.

---

## Phased Decoding & Encoding (Plugin & WAF Ready)

HTTP/1.1 request processing is split into distinct phases:

### 1. Request Head Decoding (`decode_request_head`)
Parses the request line and headers using `httparse` on a stack array (up to 128 headers) without reading the entire body.
- Returns `(RequestHead, BodyFraming)`:
  - `BodyFraming::Empty`: Request has no body.
  - `BodyFraming::ContentLength(usize)`: Request has fixed body.
  - `BodyFraming::Chunked`: Request is transferred chunked.
- **Fail-Fast**: If auth fails, IP is blocked, or headers are malicious, the proxy rejects the request with 401/403/429 without reading or allocating memory for the body.

### 2. Body Decoding (`decode_body`)
Decodes the payload based on the detected `BodyFraming`:
- `Content-Length`: Sliced zero-copy from read buffer via `buf.split_to(len).freeze()`.
- `Chunked`: Optimized 2-pass decoder with **memoized stack offsets** (`[(usize, usize); 16]`). Pass 1 verifies framing and records chunk boundaries; Pass 2 copies bytes directly without reparsing hex numbers or scanning CRLF twice. Uses SIMD `memchr` for CRLF scan.

### 3. Zero-Copy Egress Encoding (`send_response`)
- Headers are serialized into `write_buf` (always small, ~150–300 bytes).
- Response body (if `Body::Bytes`) is streamed directly from its `Bytes` slice to the socket.
- **Result**: Zero memory copies of the body on egress, and `write_buf` never balloons in size.
- Pre-computed static status lines are used for common status codes (`200 OK`, `404 Not Found`, etc.), replacing 5 separate writes with a single instruction.

---

## Connection Management: `Http1ServerConnection`

Wraps a downstream stream and drives the request-response cycle:

```rust
let mut conn = Http1ServerConnection::new(stream, config);

// Phased approach (enables plugin hooks):
while let Some((head, framing)) = conn.next_request_head().await? {
    // [Hook Point: on_request_head] Auth, CORS, Rate Limit, WAF Headers
    let body = conn.read_body(framing).await?;
    // [Hook Point: on_request_body] WAF Payload Scan

    let req = Http1Request::from_parts(head, body);
    let resp = process(&req).await;

    conn.send_response(&resp).await?;
    if conn.is_closed() { break; }
}

// Or convenient single-call approach:
while let Some(req) = conn.next_request().await? {
    let resp = process(&req).await;
    conn.send_response(&resp).await?;
    if conn.is_closed() { break; }
}
```

### Buffer Compaction
On each request cycle, if read/write buffers have expanded beyond the configured shrink threshold and are currently empty, they are shrunk back to their initial capacity. This prevents memory bloat on idle keep-alive connections.

---

## Module Reference

```
src/
├── lib.rs                  Crate root, canonical public re-exports (flat entities)
├── config.rs               Http1Config scaled by hardware memory tier
├── error.rs                Http1Error enum
├── wire.rs                 RFC 9112 wire framing & chunked codec utilities
│
├── server/                 Downstream Ingress (Gateway <-> Client)
│   ├── mod.rs              Server subsystem exports
│   ├── request.rs          Http1Request, Http1RequestHead, Http1BodyFraming (Ingress entity)
│   ├── decode.rs           Wire parsing (RFC 9112) and stream decoding (&mut BytesMut)
│   ├── encode.rs           encode_status_line, encode_response, send_response_parts
│   └── connection.rs       Http1ServerConnection (downstream loop, keep-alive RFC 9112)
│
├── client/                 Upstream Ingress (Gateway <-> Backend Microservice)
│   ├── mod.rs              Client subsystem exports
│   ├── response.rs         Http1Response, Http1ResponseHead (Ingress entity)
│   ├── encode.rs           encode_request_line, encode_request_head, encode_request
│   ├── decode.rs           Wire parsing (RFC 9112) and stream decoding (&mut BytesMut)
│   ├── connector.rs        forward_request, read_response_head, read_next_chunk
│   └── stream.rs           UpstreamHttp1Stream (Plain/TLS), is_healthy(), connect_stream
│
└── pipe/                   Bidirectional stream pipes & hop-by-hop sanitization
    ├── mod.rs              Pipe strategy resolution & header filter
    ├── buffered.rs         Buffered request/response pipe
    ├── client_stream.rs    Chunked upload streaming pipe
    ├── server_stream.rs    Progressive chunked download streaming pipe
    └── duplex.rs           Full-duplex bidirectional streaming pipe

tests/
└── http1_test.rs           18 integration tests (phased decode, streaming, limits, keep-alive)

benches/
├── single_thread_bench.rs  Decode/encode throughput on a single core
├── multi_thread_bench.rs   Linear scaling verification across Tokio workers
├── adversarial_bench.rs    Malformed/oversized/smuggling payload robustness
└── memory_leak_bench.rs    Buffer compaction and zero-leak verification
```

---

## Running Tests & Benchmarks

```bash
cargo test -p velda-http1
cargo clippy -p velda-http1 --all-targets --all-features -- -D warnings
cargo fmt --check -p velda-http1

cargo bench -p velda-http1 --bench single_thread_bench
cargo bench -p velda-http1 --bench multi_thread_bench
cargo bench -p velda-http1 --bench adversarial_bench
cargo bench -p velda-http1 --bench memory_leak_bench
```
