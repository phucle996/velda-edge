//! Downstream gRPC server pipeline modules over UDP.
//!
//! Submodules:
//! - `engine`: Packet-driven QUIC state machine for datagram ingestion and event processing.
//! - `responder`: Response encoder and trailers serializer (`GrpcUdpResponder`).
//! - `stream`: Downstream UDP stream representation (`GrpcUdpServerStream`).

pub mod engine;
pub mod header;
pub mod path;
pub mod responder;
pub mod stream;

pub use engine::{GrpcUdpEngine, GrpcUdpRequestEvent, OutgoingDatagram};
pub use header::{enrich_headers, extract_authority};
pub use path::{parse_grpc_path, validate_grpc_path};
pub use responder::{GrpcUdpResponder, GrpcUdpServerResponse, GrpcUdpServerResponseHead};
pub use stream::{GrpcUdpServerRequest, GrpcUdpServerRequestHead, GrpcUdpServerStream};
