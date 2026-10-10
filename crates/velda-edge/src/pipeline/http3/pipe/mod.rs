//! # Layer 7 HTTP/3 Gateway Orchestration & Bridging Pipelines
//!
//! **Architectural Role**: Unlike the low-level wire baselines in protocol crates
//! (`velda_http1::pipe`, `velda_http2::pipe`, `velda_http3::pipe`), these modules are
//! **Gateway-Level Pipelines**.
//!
//! While protocol baselines are pure wire-framing loops that operate on established
//! connection handles, the pipelines here own the **end-to-end Gateway request lifecycle**:
//! 1. Managing Expect: 100-continue interim status transmission
//! 2. Consuming downstream bodies and protecting upstream pools from slowloris / slow uploads
//! 3. Enriching proxy forwarding headers (`X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, `Host`)
//! 4. Upstream connection lifecycle: acquiring / leasing connections from [`UpstreamTable`]
//! 5. Transparent 1-shot self-healing on stale pooled connections (`upstream.acquire_fresh(...)`)
//! 6. Calling the underlying protocol wire baselines or executing multi-protocol bridging
//! 7. Emitting canonical error responses (400 Bad Request, 408 Request Timeout, 502 Bad Gateway)
//!
//! Contains all 12 dedicated, protocol-isolated pipeline implementations:
//! - 4 pipes for HTTP/3 to HTTP/3 backend forwarding (`*_h3_h3.rs`)
//! - 4 pipes for HTTP/3 to HTTP/2 Multiplexed backend forwarding (`*_h3_h2.rs`)
//! - 4 pipes for HTTP/3 to HTTP/1.1 backend forwarding (`*_h3_h1.rs`)

pub mod buffered_h3_h1;
pub mod buffered_h3_h2;
pub mod buffered_h3_h3;
pub mod client_stream_h3_h1;
pub mod client_stream_h3_h2;
pub mod client_stream_h3_h3;
pub mod duplex_h3_h1;
pub mod duplex_h3_h2;
pub mod duplex_h3_h3;
pub mod server_stream_h3_h1;
pub mod server_stream_h3_h2;
pub mod server_stream_h3_h3;
