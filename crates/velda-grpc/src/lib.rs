//! Dedicated high-performance gRPC protocol engine for the Velda Edge Data Plane.
//!
//! Subsystems:
//! - `composer_parse/`: Ingress stream parsing and HTTP/2 request extraction for `velda-composer`.
//! - `upstream_connector/`: Upstream backend connection management for `velda-upstream` & pool.
//! - `stream_pipe`: Full-duplex bi-directional stream pump connecting downstream and upstream.
//! - `frame`: 5-byte Length-Prefixed Message (LPM) encoder and decoder.
//! - `status`: Canonical gRPC status codes and trailers formatting.

pub mod composer_parse;
pub mod edge_response;
pub mod error;
pub mod frame;
pub mod status;
pub mod stream_pipe;
pub mod upstream_connector;

// Re-exports for downstream
pub use composer_parse::{GrpcServerConnection, GrpcServerStream};
pub use edge_response::GrpcResponder;

// Re-exports for upstream (connector)
pub use upstream_connector::GrpcUpstreamConnector;

// Re-exports for streaming pipe
pub use stream_pipe::pipe_grpc_stream;

// Common framing, status and error types
pub use error::GrpcError;
pub use frame::{GRPC_FRAME_HEADER_SIZE, decode_grpc_frame, encode_grpc_frame};
pub use status::GrpcStatus;
