//! Server-side HTTP/3 protocol engine (RFC 9114).
//!
//! Provides the core packet-driven QUIC / HTTP/3 state machine for edge ingress,
//! frame parsing, zero-alloc QPACK decoding, and egress response transmission.

pub mod connection;
pub mod header;
pub mod path;
pub mod request;
pub mod response;

pub use connection::{Http3Engine, Http3RequestEvent, OutgoingDatagram};
pub use header::{enrich_headers, extract_host};
pub use path::{is_clean_path, normalize_path};
pub use request::{Http3ServerRequest, Http3ServerRequestHead};
pub use response::{
    Http3ServerResponse, Http3ServerResponseHead, build_edge_response_frames, send_response,
};
