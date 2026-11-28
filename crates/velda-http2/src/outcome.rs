//! HTTP/2 protocol state transitions (`velda_http2::outcome`).
//!
//! ```text
//!  Http2Outcome ──► internal_status() ──► InternalStatus::{BadRequest, PayloadTooLarge, UpstreamTimeout, UpstreamError}
//!               └──► is_refused_or_goaway() ──► Pool eviction / 1-shot retry decision
//! ```

use std::fmt;

use velda_core::InternalStatus;

use crate::proto::H2ProtocolError;
use crate::proto::frame::H2ErrorCode;

/// Outcome states encountered during HTTP/2 protocol processing.
#[derive(Debug)]
pub enum Http2Outcome {
    Io(std::io::Error),
    Protocol(H2ProtocolError),
    Http(http::Error),
    Parse(String),
    PayloadTooLarge(usize),
    StreamReset(H2ErrorCode),
    GoAway {
        last_stream_id: u32,
        code: H2ErrorCode,
    },
    ConnectionClosed,
    Tls(String),
    StreamingViolation(String),
    FloodDetected,
}

impl Http2Outcome {
    /// Returns true if this outcome indicates that the remote peer refused the stream,
    /// sent GOAWAY, or closed the connection prior to processing (RFC 9113 §8.1.4),
    /// making it safe to perform transparent 1-shot self-healing.
    pub fn is_refused_or_goaway(&self) -> bool {
        match self {
            Http2Outcome::ConnectionClosed | Http2Outcome::GoAway { .. } => true,
            Http2Outcome::StreamReset(code) => *code == H2ErrorCode::REFUSED_STREAM,
            Http2Outcome::Protocol(p) => p.code == H2ErrorCode::REFUSED_STREAM,
            Http2Outcome::Io(e) => matches!(
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

    /// Maps this HTTP/2 outcome to a canonical protocol-agnostic [`InternalStatus`].
    #[inline]
    pub fn internal_status(&self) -> InternalStatus {
        match self {
            Self::PayloadTooLarge(_) => InternalStatus::PayloadTooLarge,
            Self::Io(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                InternalStatus::UpstreamTimeout
            }
            Self::Parse(_) | Self::Http(_) | Self::StreamingViolation(_) | Self::FloodDetected => {
                InternalStatus::BadRequest
            }
            Self::Protocol(_)
            | Self::StreamReset(_)
            | Self::GoAway { .. }
            | Self::ConnectionClosed
            | Self::Tls(_)
            | Self::Io(_) => InternalStatus::UpstreamError,
        }
    }
}

impl fmt::Display for Http2Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O outcome: {e}"),
            Self::Protocol(p) => write!(f, "HTTP/2 protocol outcome: {p}"),
            Self::Http(e) => write!(f, "HTTP outcome: {e}"),
            Self::Parse(msg) => write!(f, "HTTP parse outcome: {msg}"),
            Self::PayloadTooLarge(n) => {
                write!(f, "Payload too large: {n} bytes exceeds max_body_size")
            }
            Self::StreamReset(code) => write!(f, "Stream reset by peer with error code: {code:?}"),
            Self::GoAway {
                last_stream_id,
                code,
            } => write!(
                f,
                "Connection GOAWAY received (last_stream_id={last_stream_id}, code={code:?})"
            ),
            Self::ConnectionClosed => write!(f, "Connection closed unexpectedly"),
            Self::Tls(msg) => write!(f, "Upstream TLS outcome: {msg}"),
            Self::StreamingViolation(msg) => write!(f, "Streaming policy violation: {msg}"),
            Self::FloodDetected => {
                write!(
                    f,
                    "HTTP/2 flood detected: control frame overhead exceeded safety threshold"
                )
            }
        }
    }
}
