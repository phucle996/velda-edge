//! Downstream HTTP/1.1 Server Subsystem.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Owns the downstream connection lifecycle (Gateway <-> Client):
//! - `request`: Request entity model & stream decoding owned by ingress receiver.
//! - `response`: Response entity model & wire serialization to downstream clients.
//! - `connection`: Downstream connection worker and keep-alive loop.
//! - `header`: Downstream header enrichment and anti-spoofing.
//! - `path`: Downstream URI path normalization and security validation.

pub mod connection;
pub mod early_response;
pub mod header;
pub mod path;
pub mod request;
pub mod response;
