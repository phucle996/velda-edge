//! Dedicated Layer 7 gRPC over UDP (QUIC packet-driven transport).
//!
//! Submodules:
//! - `server`: Downstream packet-driven state machine, stream parsing, and responder.
//! - `client`: Upstream backend QUIC connection management and multiplexed client.
//! - `pipe`: Strategy-driven bidirectional stream pumping between downstream and upstream.
//! - `wire`: Self-contained variable-length integer framing and QPACK header codec.

pub mod client;
pub mod server;
pub mod strategy;
pub mod wire;

pub use strategy::GrpcUdpPipeStrategy;

pub use client::{
    GrpcUdpClient, GrpcUdpClientRequest, GrpcUdpClientRequestHead, GrpcUdpClientResponse,
    GrpcUdpClientResponseHead, connect as connect_udp, default_client_config,
};
pub use quinn_proto;
pub use server::{
    GrpcUdpEngine, GrpcUdpRequestEvent, GrpcUdpResponder, GrpcUdpServerRequest,
    GrpcUdpServerRequestHead, GrpcUdpServerResponse, GrpcUdpServerResponseHead,
    GrpcUdpServerStream, OutgoingDatagram,
};
