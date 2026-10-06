//! Dedicated high-performance gRPC protocol engine for the Velda Edge Data Plane.
//!
//! Protocols & Subsystems:
//! - `tcp/`: Dedicated gRPC over TCP (HTTP/2 transport): server, client, pipe.
//! - `udp/`: Dedicated gRPC over UDP (QUIC transport): server, client, pipe, wire.
//! - `frame`: 5-byte Length-Prefixed Message (LPM) encoder and decoder.
//! - `status`: Canonical gRPC status codes and trailers formatting.
//! - `wire`: Standard gRPC header constants and pseudo-header names.

pub mod config;
pub mod error;
pub mod frame;
pub mod status;
pub mod tcp;
pub mod udp;
pub mod wire;

// Common types, framing, status, config, error, and wire namespaces
pub use config::GrpcConfig;
pub use error::GrpcError;
pub use frame::{GrpcFrame, decode_grpc_frame, encode_grpc_frame};
pub use status::GrpcStatus;
pub use wire::GrpcWire;
