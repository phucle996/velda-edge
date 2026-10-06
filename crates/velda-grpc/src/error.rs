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

    #[error("Upstream TLS error: {0}")]
    Tls(String),

    #[error("gRPC status error: code {0:?}, message: {1}")]
    Status(GrpcStatus, String),

    #[error("Payload too large: {0} bytes exceeds max_body_size")]
    PayloadTooLarge(usize),

    #[error("Operation timed out")]
    Timeout,

    #[error("Internal gRPC error: {0}")]
    Internal(String),
}

impl GrpcError {
    /// Returns true if this error indicates that the connection or stream died/reset
    /// prior to processing or due to stale connection reuse, making it safe to perform
    /// transparent 1-shot self-healing.
    pub fn is_stale_or_refused(&self) -> bool {
        match self {
            GrpcError::H2(e) => {
                e.reason() == Some(h2::Reason::REFUSED_STREAM) || e.is_go_away() || e.is_io()
            }
            GrpcError::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::NotConnected
            ),
            GrpcError::Protocol(msg) => msg.contains("connection closed"),
            _ => false,
        }
    }
}
