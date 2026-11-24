//! Upstream Ingress Subsystem (Gateway acting as HTTP/2 Client).
//!
//! Owns upstream connection establishment and multiplexed client transport.

pub mod connection;
pub mod connector;
