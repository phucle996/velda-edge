//! Downstream Ingress Subsystem (Gateway acting as HTTP/2 Server).
//!
//! Owns incoming requests from downstream clients, flow control decoding,
//! stream response dispatch, and server connection lifecycle.

pub mod connection;
pub mod early_response;
pub mod path;
pub mod request;
pub mod response;
