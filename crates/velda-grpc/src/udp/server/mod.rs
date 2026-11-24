//! Downstream gRPC server pipeline modules over UDP.
//!
//! Submodules:
//! - `engine`: Packet-driven QUIC state machine for datagram ingestion and event processing.
//! - `header`: Forwarding header enrichment and authority extraction.
//! - `path`: gRPC `/Service/Method` path validation.

pub mod engine;
pub mod header;
pub mod path;

pub use engine::{GrpcUdpEngine, GrpcUdpRequestEvent, OutgoingDatagram};
pub use header::extract_authority;
pub use path::parse_grpc_path;
