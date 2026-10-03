//! Dedicated high-performance gRPC protocol engine for the Velda Edge Data Plane.
//!
//! Subsystems:
//! - `server/`: Downstream HTTP/2 connection handling, stream parsing, and responder.
//! - `client/`: Upstream backend connection management, stream opening, and Unary forwarding.
//! - `pipe/`: Strategy-driven bidirectional stream pumping between downstream and upstream.
//! - `frame`: 5-byte Length-Prefixed Message (LPM) encoder and decoder.
//! - `status`: Canonical gRPC status codes and trailers formatting.

pub mod client;
pub mod config;
pub mod error;
pub mod frame;
pub mod pipe;
pub mod server;
pub mod status;
pub mod wire;

// Re-exports for downstream server
pub use server::{GrpcResponder, GrpcServerConnection, GrpcServerStream};

// Re-exports for upstream client
pub use client::GrpcUpstreamConnector;

// Re-exports for streaming pipe and strategies
pub use pipe::{
    GrpcPipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_grpc_stream,
    pipe_server_stream,
};

// Common types, framing, status, config, error, and wire namespaces
pub use config::GrpcConfig;
pub use error::GrpcError;
pub use frame::{GrpcFrame, decode_grpc_frame, encode_grpc_frame};
pub use status::GrpcStatus;
pub use wire::GrpcWire;
