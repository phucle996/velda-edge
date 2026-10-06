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

impl Http2Error {
    /// Returns true if this error indicates that the remote peer refused the stream,
    /// sent GOAWAY, or closed the connection prior to processing (RFC 9113 §8.1.4),
    /// making it safe to perform transparent 1-shot self-healing.
    pub fn is_refused_or_goaway(&self) -> bool {
        match self {
            Http2Error::ConnectionClosed => true,
            Http2Error::StreamReset(reason) => *reason == h2::Reason::REFUSED_STREAM,
            Http2Error::H2(e) => {
                e.reason() == Some(h2::Reason::REFUSED_STREAM) || e.is_go_away() || e.is_io()
            }
            Http2Error::Io(e) => matches!(
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
