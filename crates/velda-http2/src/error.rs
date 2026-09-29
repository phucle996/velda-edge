//! Error types for HTTP/2 protocol handling.

use thiserror::Error;

/// Error conditions encountered during HTTP/2 processing.
#[derive(Debug, Error)]
pub enum Http2Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP/2 error: {0}")]
    H2(#[from] h2::Error),

    #[error("HTTP parsing error: {0}")]
    Parse(String),
}
