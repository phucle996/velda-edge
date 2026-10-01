//! Crate `velda-http2`
//!
//! Dedicated high-performance Layer 7 HTTP/2 Protocol Engine for Velda Edge (RFC 9113).
//!
//! Architecture (Transparent Two-Way Ingress Model):
//! - `server/`: Downstream Ingress (Gateway acting as H2 Server). Manages multiplexed
//!   concurrent streams, flow control decoding, and per-stream response transmission.
//! - `client/`: Upstream Ingress (Gateway acting as H2 Client). Manages outbound request
//!   framing, flow-controlled response ingestion, and multiplexed backend connection reuse.
//! - `config`: Tunable parameters for flow control window sizes, frame limits, and concurrency.
//! - `error`: Strongly typed HTTP/2 and frame errors.

pub mod client;
pub mod config;
pub mod error;
pub mod server;

// Top-level public re-exports
pub use client::{
    Http2Response, Http2ResponseHead, Http2UpstreamConnector, decode_l7_response, decode_response,
    decode_streaming_response, send_h2_request, send_l7_request, start_streaming_request,
};
pub use config::Http2Config;
pub use error::Http2Error;
pub use server::{
    Http2Request, Http2RequestHead, Http2Responder, Http2ServerConnection, Http2StreamReceiver,
    Http2StreamSender, decode_request,
};
