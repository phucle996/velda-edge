//! HTTP/3 protocol state transitions (`velda_http3::outcome`).
//!
//! ```text
//!  Http3Outcome ──► internal_status() ──► InternalStatus::{BadRequest, PayloadTooLarge, UpstreamError}
//!               └──► is_connection_closed() ──► Pool eviction / retry decision
//! ```

use std::fmt;

use velda_core::InternalStatus;

/// Outcome states encountered during HTTP/3 protocol processing.
#[derive(Debug)]
pub enum Http3Outcome {
    Io(std::io::Error),
    H3(String),
    Frame(String),
    Http(http::Error),
    InvalidPath(String),
    PayloadTooLarge(usize),
    ConnectionClosed,
    StreamingViolation(String),
}

impl Http3Outcome {
    /// Returns true if this outcome indicates that the QUIC connection closed unexpectedly
    /// or remote peer rejected the request (e.g. `H3_REQUEST_REJECTED` / peer shutdown).
    pub fn is_connection_closed(&self) -> bool {
        match self {
            Http3Outcome::ConnectionClosed => true,
            Http3Outcome::H3(msg) => {
                msg.contains("connection closed")
                    || msg.contains("Connection lost")
                    || msg.contains("closed by peer")
                    || msg.contains("maximum number of requests")
                    || msg.contains("H3_REQUEST_REJECTED")
                    || msg.contains("reset")
            }
            Http3Outcome::Io(e) => matches!(
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

    /// Maps this HTTP/3 outcome to a canonical protocol-agnostic [`InternalStatus`].
    #[inline]
    pub fn internal_status(&self) -> InternalStatus {
        match self {
            Self::PayloadTooLarge(_) => InternalStatus::PayloadTooLarge,
            Self::Io(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                InternalStatus::UpstreamTimeout
            }
            Self::Frame(_) | Self::Http(_) | Self::InvalidPath(_) | Self::StreamingViolation(_) => {
                InternalStatus::BadRequest
            }
            Self::H3(_) | Self::ConnectionClosed | Self::Io(_) => InternalStatus::UpstreamError,
        }
    }
}

impl fmt::Display for Http3Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O outcome: {e}"),
            Self::H3(msg) => write!(f, "HTTP/3 outcome: {msg}"),
            Self::Frame(msg) => write!(f, "Frame parse outcome: {msg}"),
            Self::Http(e) => write!(f, "HTTP outcome: {e}"),
            Self::InvalidPath(msg) => write!(f, "Invalid URI path: {msg}"),
            Self::PayloadTooLarge(n) => {
                write!(f, "Payload too large: {n} bytes exceeds max_body_size")
            }
            Self::ConnectionClosed => write!(f, "Connection closed unexpectedly"),
            Self::StreamingViolation(msg) => write!(f, "Streaming policy violation: {msg}"),
        }
    }
}
