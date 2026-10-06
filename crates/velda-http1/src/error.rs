//! Error types for HTTP/1.1 protocol handling.

use thiserror::Error;

/// Error conditions encountered during HTTP/1.1 processing.
#[derive(Debug, Error)]
pub enum Http1Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP/1.1 parsing error: {0}")]
    Parse(String),

    #[error("Invalid HTTP header: {0}")]
    InvalidHeader(String),

    #[error("Invalid HTTP method: {0}")]
    InvalidMethod(String),

    #[error("Invalid HTTP URI: {0}")]
    InvalidUri(String),

    #[error("Payload too large: content length {0} exceeds limit")]
    PayloadTooLarge(usize),

    #[error("Header section too large: {0} bytes exceeds limit")]
    HeaderTooLarge(usize),

    #[error("Header count exceeds limit: {0}")]
    TooManyHeaders(usize),

    #[error("HTTP request smuggling detected: {0}")]
    SmugglingDetected(String),

    #[error("Invalid chunked transfer encoding: {0}")]
    InvalidChunkedEncoding(String),

    #[error("Connection closed by peer")]
    ConnectionClosed,

    #[error("Streaming policy violation: {0}")]
    StreamingViolation(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("I/O timeout exceeded: connection idle")]
    Timeout,
}
