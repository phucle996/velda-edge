//! Error types for HTTP/3 protocol handling.

use thiserror::Error;

/// Error conditions encountered during HTTP/3 processing.
#[derive(Debug, Error)]
pub enum Http3Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP/3 error: {0}")]
    H3(String),

    #[error("Frame parsing error: {0}")]
    Frame(String),

    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),

    #[error("Payload too large: {0} bytes exceeds max_body_size")]
    PayloadTooLarge(usize),

    #[error("Connection closed unexpectedly")]
    ConnectionClosed,

    #[error("Streaming policy violation: {0}")]
    StreamingViolation(String),
}
