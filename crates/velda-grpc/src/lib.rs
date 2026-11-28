//! Dedicated high-performance gRPC protocol engine for the Velda Edge Data Plane.
//!
//! Protocols & Subsystems:
//! - `tcp/`: Dedicated gRPC over TCP (HTTP/2 transport): server, client, pipe.
//! - `udp/`: Dedicated gRPC over UDP (QUIC transport): server, client, pipe, wire.
//! - `frame`: 5-byte Length-Prefixed Message (LPM) encoder and decoder.
//! - `status`: Canonical gRPC status codes and trailers formatting.
//! - `wire`: Standard gRPC header constants and pseudo-header names.

pub mod config;
pub mod frame;
pub mod outcome;
pub mod slot;
pub mod status;
pub mod tcp;
pub mod udp;
pub mod wire;

// Common types, framing, config, outcome, status, and wire namespaces
pub use config::GrpcConfig;
pub use frame::{GrpcFrame, decode_grpc_frame, encode_grpc_frame};
pub use outcome::GrpcOutcome;
pub use slot::GrpcMetadataSlot;
pub use status::{GRPC_STATUS_CODE_STRINGS, GRPC_STATUS_NAMES, GrpcStatus};
pub use tcp::{
    GrpcClientRequest, GrpcClientRequestHead, GrpcClientResponse, GrpcClientResponseHead,
    GrpcPipeStrategy, GrpcResponder, GrpcServerConnection, GrpcServerRequest,
    GrpcServerRequestHead, GrpcServerResponse, GrpcServerResponseHead, GrpcServerStream,
    GrpcUpstreamConnector,
};
pub use udp::client::connector::{GrpcUdpClient, connect as connect_udp};
pub use udp::quinn_proto;
pub use udp::server::engine::{GrpcUdpEngine, GrpcUdpRequestEvent, OutgoingDatagram};
pub use udp::strategy::GrpcUdpPipeStrategy;
pub use wire::{GrpcWire, X_REQUEST_ID};
