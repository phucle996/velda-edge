//! Crate `velda-http2`
//!
//! Self-contained Layer 7 HTTP/2 Protocol Engine & Edge Integration (RFC 9113 & RFC 7541).
//!
//! Architecture:
//! - `proto/`: Sans-IO HTTP/2 protocol core (`frame`, `hpack`, `huffman`, `flow_control`, `guard`).
//! - `server/`: Edge Downstream Ingress (`Http2ServerConnection`, `Http2ServerRequest`, `Http2Responder`).
//! - `client/`: Edge Upstream Ingress (`Http2Client`, `connector`).
//! - `headers`, `config`, `error`, `strategy`: Shared Edge HTTP/2 helpers and tuning.

pub mod client;
pub mod config;
pub mod error;
pub mod headers;
pub mod proto;
pub mod server;
pub mod strategy;
