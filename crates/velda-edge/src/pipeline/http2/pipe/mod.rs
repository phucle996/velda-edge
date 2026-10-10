//! # Layer 7 HTTP/2 Gateway Orchestration & Bridging Pipelines
//!
//! **Architectural Role**: Unlike the low-level wire baselines in protocol crates
//! (`velda_http2::pipe`, `velda_http1::pipe`, `velda_http3::pipe`), these modules are
//! **Gateway-Level Pipelines**.
//!
//! While protocol baselines are pure wire-framing loops that operate on established
//! connection handles, the pipelines here own the **end-to-end Gateway request lifecycle**:
//! 1. Consuming / streaming downstream frames and validating limits (e.g. 400 Bad Request)
//! 2. Enriching proxy forwarding headers (`X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, `Host`)
//! 3. Upstream connection lifecycle: acquiring / leasing connections from [`UpstreamTable`]
//! 4. Transparent 1-shot self-healing on connection drops (`upstream.acquire_fresh(...)`)
//! 5. Calling the underlying protocol wire baselines or executing multi-protocol bridging
//! 6. Emitting canonical error responses (400 Bad Request, 502 Bad Gateway)
//!
//! Contains all 12 dedicated, protocol-isolated pipeline implementations:
//! - 4 pipes for HTTP/2 to HTTP/2 Multiplexed backend forwarding (`*_h2_h2.rs`)
//! - 4 pipes for HTTP/2 to HTTP/1.1 Bridge backend forwarding (`*_h2_h1.rs`)
//! - 4 pipes for HTTP/2 to HTTP/3 QUIC Bridge backend forwarding (`*_h2_h3.rs`)

pub mod buffered_h2_h1;
pub mod buffered_h2_h2;
pub mod buffered_h2_h3;
pub mod client_stream_h2_h1;
pub mod client_stream_h2_h2;
pub mod client_stream_h2_h3;
pub mod duplex_h2_h1;
pub mod duplex_h2_h2;
pub mod duplex_h2_h3;
pub mod server_stream_h2_h1;
pub mod server_stream_h2_h2;
pub mod server_stream_h2_h3;
