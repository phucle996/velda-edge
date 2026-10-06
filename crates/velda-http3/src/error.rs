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

impl Http3Error {
    /// Returns true if this error indicates that the QUIC connection closed unexpectedly
    /// or remote peer rejected the request (e.g. H3_REQUEST_REJECTED / peer shutdown).
    pub fn is_connection_closed(&self) -> bool {
        match self {
            Http3Error::ConnectionClosed => true,
            Http3Error::H3(msg) => {
                msg.contains("connection closed")
                    || msg.contains("H3_REQUEST_REJECTED")
                    || msg.contains("reset")
            }
            Http3Error::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::NotConnected
            ),
            _ => false,
        }
    }
}
