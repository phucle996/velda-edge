//! Server-side HTTP/3 protocol engine (RFC 9114).
//!
//! Provides the core packet-driven QUIC / HTTP/3 state machine for edge ingress,
//! frame parsing, zero-alloc QPACK decoding, and egress response transmission.
//!
//! # Architectural Boundary Design
//! Unlike stream-oriented HTTP/1.1 and HTTP/2 (which use dedicated `request.rs` modules
//! to parse linear byte streams or drive incremental flow-controlled stream receivers),
//! HTTP/3 operates entirely as a packet-driven event state machine (`quinn_proto`).
//! Incoming UDP packets are ingested by [`Http3Engine`] in [`connection`], which directly
//! assembles frames, decodes QPACK headers, and emits canonical [`velda_core::L7Request`]
//! via [`Http3RequestEvent`].
//!
//! There is deliberately NO intermediate downstream `Http3ServerRequest` wrapper entity
//! (consistent with `velda-grpc::udp::server::engine`), eliminating unnecessary heap wrapper
//! allocations and preserving the flat-workflow, zero-overhead hot path.

pub mod connection;
pub mod header;
pub mod path;
pub mod response;

pub use connection::{Http3Engine, Http3RequestEvent, OutgoingDatagram};
pub use header::{enrich_headers, extract_host};
pub use path::{is_clean_path, normalize_path};
pub use response::{
    Http3ServerResponse, Http3ServerResponseHead, build_edge_response_frames, send_response,
};
