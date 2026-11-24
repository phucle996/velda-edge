//! Error types for HTTP/2 protocol handling.

use thiserror::Error;

use crate::proto::H2ProtocolError;
use crate::proto::frame::H2ErrorCode;

/// Error conditions encountered during HTTP/2 processing.
#[derive(Debug, Error)]
pub enum Http2Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP/2 protocol error: {0}")]
    Protocol(#[from] H2ProtocolError),

    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),

    #[error("HTTP parsing error: {0}")]
    Parse(String),

    #[error("Payload too large: {0} bytes exceeds max_body_size")]
    PayloadTooLarge(usize),

    #[error("Stream reset by peer with error code: {0:?}")]
    StreamReset(H2ErrorCode),

    #[error("Connection GOAWAY received (last_stream_id={last_stream_id}, code={code:?})")]
    GoAway {
        last_stream_id: u32,
        code: H2ErrorCode,
    },

    #[error("Connection closed unexpectedly")]
    ConnectionClosed,

    #[error("Upstream TLS error: {0}")]
    Tls(String),

    #[error("Streaming policy violation: {0}")]
    StreamingViolation(String),

    #[error("HTTP/2 flood detected: control frame overhead exceeded safety threshold")]
    FloodDetected,
}

impl Http2Error {
    /// Returns true if this error indicates that the remote peer refused the stream,
    /// sent GOAWAY, or closed the connection prior to processing (RFC 9113 §8.1.4),
    /// making it safe to perform transparent 1-shot self-healing.
    pub fn is_refused_or_goaway(&self) -> bool {
        match self {
            Http2Error::ConnectionClosed | Http2Error::GoAway { .. } => true,
            Http2Error::StreamReset(code) => *code == H2ErrorCode::REFUSED_STREAM,
            Http2Error::Protocol(p) => p.code == H2ErrorCode::REFUSED_STREAM,
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
