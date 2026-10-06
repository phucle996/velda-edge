//! Dedicated Layer 7 gRPC over UDP (QUIC packet-driven transport).
//!
//! Submodules:
//! - `server`: Downstream packet-driven state machine, stream parsing, and responder.
//! - `client`: Upstream backend connection management and Unary forwarding over UDP.
//! - `pipe`: Bidirectional message pumping and unary forwarding strategies over UDP.
//! - `wire`: Self-contained variable-length integer framing and QPACK header codec.

pub mod client;
pub mod pipe;
pub mod server;
pub mod wire;

pub use client::GrpcUdpUpstreamConnector;
pub use pipe::pipe_grpc_udp_unary;
pub use quinn_proto;
pub use server::{
    GrpcUdpEngine, GrpcUdpRequestEvent, GrpcUdpResponder, GrpcUdpServerStream, OutgoingDatagram,
};
