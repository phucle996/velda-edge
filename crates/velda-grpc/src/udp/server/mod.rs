//! Downstream gRPC server pipeline modules over UDP.
//!
//! Submodules:
//! - `engine`: Packet-driven QUIC state machine for datagram ingestion and event processing.
//! - `responder`: Response encoder and trailers serializer (`GrpcUdpResponder`).
//! - `stream`: Downstream UDP stream representation (`GrpcUdpServerStream`).

pub mod engine;
pub mod responder;
pub mod stream;

pub use engine::{GrpcUdpEngine, GrpcUdpRequestEvent, OutgoingDatagram};
pub use responder::GrpcUdpResponder;
pub use stream::GrpcUdpServerStream;
