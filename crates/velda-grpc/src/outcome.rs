//! gRPC protocol state transitions (`velda_grpc::outcome`).
//!
//! ```text
//!  GrpcOutcome ──► internal_status() ──► InternalStatus::{BadRequest, PayloadTooLarge, UpstreamTimeout, UpstreamError, InternalError}
//!              └──► is_stale_or_refused() ──► Pool eviction / 1-shot retry decision
//! ```

use std::fmt;

use velda_core::InternalStatus;

/// Outcome states produced during gRPC framing, connection lifecycle, or upstream proxying.
#[derive(Debug)]
pub enum GrpcOutcome {
    Io(std::io::Error),
    Http(http::Error),
    Transport {
        refused_or_goaway: bool,
        detail: String,
    },
    Protocol(String),
    Tls(String),
    PayloadTooLarge(usize),
    Timeout,
    Internal(String),
}

impl GrpcOutcome {
    /// Explicitly translates a transport-layer HTTP/2 outcome without `From` coercion.
    #[inline]
    pub fn from_http2(e: velda_http2::Http2Outcome) -> Self {
        match e {
            velda_http2::Http2Outcome::Io(io) => Self::Io(io),
            velda_http2::Http2Outcome::PayloadTooLarge(n) => Self::PayloadTooLarge(n),
            velda_http2::Http2Outcome::Tls(msg) => Self::Tls(msg),
            other => Self::Transport {
                refused_or_goaway: other.is_refused_or_goaway(),
                detail: other.to_string(),
            },
        }
    }

    /// Returns true if this outcome indicates that the connection or stream died/reset
    /// prior to processing or due to stale connection reuse, making it safe to perform
    /// transparent 1-shot self-healing.
    pub fn is_stale_or_refused(&self) -> bool {
        match self {
            Self::Transport {
                refused_or_goaway, ..
            } => *refused_or_goaway,
            Self::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::NotConnected
            ),
            Self::Protocol(msg) => msg.contains("connection closed"),
            _ => false,
        }
    }

    /// Maps this gRPC outcome to a canonical protocol-agnostic [`InternalStatus`].
    #[inline]
    pub fn internal_status(&self) -> InternalStatus {
        match self {
            Self::PayloadTooLarge(_) => InternalStatus::PayloadTooLarge,
            Self::Timeout => InternalStatus::UpstreamTimeout,
            Self::Io(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                InternalStatus::UpstreamTimeout
            }
            Self::Http(_) | Self::Protocol(_) => InternalStatus::BadRequest,
            Self::Internal(_) => InternalStatus::InternalError,
            Self::Io(_) | Self::Tls(_) | Self::Transport { .. } => InternalStatus::UpstreamError,
        }
    }
}

impl fmt::Display for GrpcOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O outcome: {e}"),
            Self::Http(e) => write!(f, "HTTP construction outcome: {e}"),
            Self::Transport { detail, .. } => write!(f, "gRPC transport outcome: {detail}"),
            Self::Protocol(msg) => write!(f, "gRPC protocol outcome: {msg}"),
            Self::Tls(msg) => write!(f, "Upstream TLS outcome: {msg}"),
            Self::PayloadTooLarge(n) => {
                write!(f, "Payload too large: {n} bytes exceeds max_body_size")
            }
            Self::Timeout => write!(f, "Operation timed out"),
            Self::Internal(msg) => write!(f, "Internal gRPC outcome: {msg}"),
        }
    }
}
