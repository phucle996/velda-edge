//! # Layer 7 HTTP/1.1 Gateway Orchestration & Bridging Pipelines
//!
//! **Architectural Role**: In accordance with the Velda Edge Monorepo Contract:
//! Protocol crates (`velda-http1`, `velda-http2`, `velda-http3`) are pure **Protocol Engines**
//! providing protocol primitives, framing, codecs, and connection mechanics ("WHAT").
//!
//! The pipeline modules here in `velda-edge` own the **Gateway Request & Proxying Pipelines** ("HOW"):
//! 1. Managing Expect: 100-continue interim status transmission
//! 2. Consuming downstream bodies and protecting upstream pools from slowloris / slow uploads
//! 3. Enriching proxy forwarding headers (`X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, `Host`)
//! 4. Upstream connection lifecycle: acquiring / leasing connections from [`UpstreamTable`]
//! 5. Transparent 1-shot self-healing on stale pooled connections (`upstream.acquire_fresh(...)`)
//! 6. Executing self-contained, linear, zero-alloc forward/streaming workflows
//! 7. Emitting canonical error responses (400 Bad Request, 408 Request Timeout, 502 Bad Gateway)
//!
//! Contains all 12 dedicated, protocol-isolated pipeline implementations:
//! - 4 pipes for HTTP/1.1 to HTTP/1.1 backend forwarding (`*_h1_h1.rs`)
//! - 4 pipes for HTTP/1.1 to HTTP/2 Multiplexed backend forwarding (`*_h1_h2.rs`)
//! - 4 pipes for HTTP/1.1 to HTTP/3 QUIC backend forwarding (`*_h1_h3.rs`)

pub mod buffered_h1_h1;
pub mod buffered_h1_h2;
pub mod buffered_h1_h3;
pub mod client_stream_h1_h1;
pub mod client_stream_h1_h2;
pub mod client_stream_h1_h3;
pub mod duplex_h1_h1;
pub mod duplex_h1_h2;
pub mod duplex_h1_h3;
pub mod server_stream_h1_h1;
pub mod server_stream_h1_h2;
pub mod server_stream_h1_h3;
