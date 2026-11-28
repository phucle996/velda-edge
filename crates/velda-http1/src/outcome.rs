//! HTTP/1.1 protocol state transitions (`velda_http1::outcome`).
//!
//! ```text
//!  Http1Outcome ──► internal_status() ──► InternalStatus::{BadRequest, PayloadTooLarge, HeaderFieldsTooLarge, UpstreamTimeout, UpstreamError}
//!               └──► is_stale_connection() ──► Pool eviction / retry decision
//! ```

use std::fmt;

use velda_core::InternalStatus;

/// Outcome states encountered during HTTP/1.1 protocol processing.
#[derive(Debug)]
pub enum Http1Outcome {
    Io(std::io::Error),
    Parse(String),
    InvalidHeader(String),
    InvalidMethod(String),
    InvalidUri(String),
    PayloadTooLarge(usize),
    HeaderTooLarge(usize),
    TooManyHeaders(usize),
    SmugglingDetected(String),
    InvalidChunkedEncoding(String),
    ConnectionClosed,
    StreamingViolation(String),
    InvalidConfig(String),
    Timeout,
}

impl Http1Outcome {
    /// Returns true if this outcome is caused by a closed or broken connection
    /// (e.g. stale keep-alive connection race).
    pub fn is_stale_connection(&self) -> bool {
        match self {
            Http1Outcome::ConnectionClosed => true,
            Http1Outcome::Io(e) => matches!(
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

    /// Maps this HTTP/1.1 outcome to a canonical protocol-agnostic [`InternalStatus`].
    #[inline]
    pub fn internal_status(&self) -> InternalStatus {
        match self {
            Self::PayloadTooLarge(_) => InternalStatus::PayloadTooLarge,
            Self::HeaderTooLarge(_) | Self::TooManyHeaders(_) => {
                InternalStatus::HeaderFieldsTooLarge
            }
            Self::Timeout => InternalStatus::UpstreamTimeout,
            Self::Io(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                InternalStatus::UpstreamTimeout
            }
            Self::Parse(_)
            | Self::InvalidHeader(_)
            | Self::InvalidMethod(_)
            | Self::InvalidUri(_)
            | Self::SmugglingDetected(_)
            | Self::InvalidChunkedEncoding(_)
            | Self::StreamingViolation(_) => InternalStatus::BadRequest,
            Self::InvalidConfig(_) => InternalStatus::InternalError,
            Self::Io(_) | Self::ConnectionClosed => InternalStatus::UpstreamError,
        }
    }
}

impl fmt::Display for Http1Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O outcome: {e}"),
            Self::Parse(msg) => write!(f, "HTTP/1.1 parse outcome: {msg}"),
            Self::InvalidHeader(msg) => write!(f, "Invalid HTTP header: {msg}"),
            Self::InvalidMethod(msg) => write!(f, "Invalid HTTP method: {msg}"),
            Self::InvalidUri(msg) => write!(f, "Invalid HTTP URI: {msg}"),
            Self::PayloadTooLarge(n) => {
                write!(f, "Payload too large: content length {n} exceeds limit")
            }
            Self::HeaderTooLarge(n) => {
                write!(f, "Header section too large: {n} bytes exceeds limit")
            }
            Self::TooManyHeaders(n) => write!(f, "Header count exceeds limit: {n}"),
            Self::SmugglingDetected(msg) => write!(f, "HTTP request smuggling detected: {msg}"),
            Self::InvalidChunkedEncoding(msg) => {
                write!(f, "Invalid chunked transfer encoding: {msg}")
            }
            Self::ConnectionClosed => write!(f, "Connection closed by peer"),
            Self::StreamingViolation(msg) => write!(f, "Streaming policy violation: {msg}"),
            Self::InvalidConfig(msg) => write!(f, "Invalid configuration: {msg}"),
            Self::Timeout => write!(f, "I/O timeout exceeded: connection idle"),
        }
    }
}
