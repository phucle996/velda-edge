//! Error types for the velda-grpc engine.

use thiserror::Error;

use crate::status::GrpcStatus;

/// Errors produced during gRPC connection lifecycle, framing, or upstream proxying.
#[derive(Debug, Error)]
pub enum GrpcError {
    #[error("HTTP/2 framing error: {0}")]
    H2(#[from] h2::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP header or request construction error: {0}")]
    Http(#[from] http::Error),

    #[error("gRPC protocol error: {0}")]
    Protocol(String),

    #[error("gRPC status error: code {0:?}, message: {1}")]
    Status(GrpcStatus, String),

    #[error("Payload too large: {0} bytes exceeds max_body_size")]
    PayloadTooLarge(usize),

    #[error("Operation timed out")]
    Timeout,

    #[error("Internal gRPC error: {0}")]
    Internal(String),
}
