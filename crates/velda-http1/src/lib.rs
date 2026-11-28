//! Crate `velda-http1`
//!
//! Dedicated high-performance Layer 7 HTTP/1.1 Protocol Engine for Velda Edge (RFC 9112).
//!
//! Architectural Structure (Minh bạch 2 chiều dữ liệu theo góc nhìn Ingress):
//! - `server/`: Downstream Ingress (Gateway <-> Client). Owns `Http1ServerRequest` & `Http1ServerResponse`,
//!   downstream request wire decoding, response serialization, and `Http1ServerConnection`.
//! - `client/`: Upstream Ingress (Gateway <-> Backend). Owns `Http1ClientRequest` & `Http1ClientResponse`,
//!   outbound request serialization, response decoding, and upstream connection mechanics.
//! - `pipe/`: Bidirectional stream pump connecting downstream and upstream according to streaming policy.
//! - `config/`: Buffer sizing scaled continuously from host hardware topology.
//! - `outcome`: HTTP/1.1 protocol state transitions.

pub mod client;
pub mod config;
pub mod outcome;
pub mod server;
pub mod simd;
pub mod strategy;
pub mod wire;

pub use outcome::Http1Outcome;
