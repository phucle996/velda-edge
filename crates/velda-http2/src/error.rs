//! Error types for HTTP/2 protocol handling.

use thiserror::Error;

/// Error conditions encountered during HTTP/2 processing.
#[derive(Debug, Error)]
pub enum Http2Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP/2 protocol error: {0}")]
    H2(#[from] h2::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),

    #[error("HTTP parsing error: {0}")]
    Parse(String),

    #[error("Payload too large: {0} bytes exceeds max_body_size")]
    PayloadTooLarge(usize),

    #[error("Stream reset by peer with reason: {0:?}")]
    StreamReset(h2::Reason),

    #[error("Connection closed unexpectedly")]
    ConnectionClosed,

    #[error("Upstream TLS error: {0}")]
    Tls(String),

    #[error("Streaming policy violation: {0}")]
    StreamingViolation(String),
}
