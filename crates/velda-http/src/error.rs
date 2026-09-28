//! Error types for HTTP request parsing and response encoding.

use thiserror::Error;

/// Error conditions encountered during HTTP protocol processing.
#[derive(Debug, Error)]
pub enum HttpError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP parsing error: {0}")]
    Parse(String),

    #[error("Invalid HTTP header: {0}")]
    InvalidHeader(String),

    #[error("Invalid HTTP method: {0}")]
    InvalidMethod(String),

    #[error("Invalid HTTP URI: {0}")]
    InvalidUri(String),

    #[error("Payload too large: content length {0} exceeds limit")]
    PayloadTooLarge(usize),

    #[error("Connection closed by peer")]
    ConnectionClosed,

    #[error("HTTP/2 error: {0}")]
    H2(#[from] h2::Error),

    #[error("HTTP/3 error: {0}")]
    H3(String),

    #[error("Unsupported HTTP version: {0}")]
    UnsupportedVersion(String),
}
