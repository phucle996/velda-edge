//! Server-side HTTP/3 protocol engine (RFC 9114).
//!
//! Provides the core packet-driven QUIC / HTTP/3 state machine for edge ingress,
//! frame parsing, zero-alloc QPACK decoding, and egress response transmission.

pub mod connection;
pub mod response;

pub use connection::{Http3Engine, Http3RequestEvent, OutgoingDatagram};
pub use response::{build_edge_response_frames, send_response};
