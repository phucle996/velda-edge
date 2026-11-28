//! Protocol-agnostic upstream state transitions (`velda_upstream::outcome`).
//!
//! ```text
//!  UpstreamOutcome ──► internal_status() ──► InternalStatus::{ServiceUnavailable, UpstreamTimeout, UpstreamError}
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use velda_core::InternalStatus;

/// Outcome variants produced by upstream endpoint selection, health filtering, and acquisition.
#[derive(Debug)]
pub enum UpstreamOutcome {
    /// No endpoints in the upstream are currently active or healthy.
    NoEndpointsAvailable(Arc<str>),
    /// Failed to connect to a backend endpoint.
    ConnectionFailed {
        ip: u128,
        port_be: u16,
        reason: String,
    },
    /// Timed out while attempting to connect or acquire a backend connection.
    AcquisitionTimeout(Duration),
    /// DNS discovery resolution failure.
    DnsResolutionFailed { host: String, reason: String },
    /// Invalid upstream configuration.
    InvalidConfig(String),
    /// TLS handshake failure with backend endpoint.
    Tls { sni: String, reason: String },
    /// Upstream protocol failure.
    Protocol(String),
    /// OS socket I/O failure during connection establishment or polling.
    Io(std::io::Error),
}

impl UpstreamOutcome {
    /// Maps this upstream outcome directly to a canonical [`InternalStatus`].
    #[inline]
    pub fn internal_status(&self) -> InternalStatus {
        match self {
            Self::NoEndpointsAvailable(_) => InternalStatus::ServiceUnavailable,
            Self::AcquisitionTimeout(_) => InternalStatus::UpstreamTimeout,
            Self::Io(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                InternalStatus::UpstreamTimeout
            }
            Self::InvalidConfig(_) => InternalStatus::InternalError,
            Self::ConnectionFailed { .. }
            | Self::DnsResolutionFailed { .. }
            | Self::Tls { .. }
            | Self::Protocol(_)
            | Self::Io(_) => InternalStatus::UpstreamError,
        }
    }
}

impl fmt::Display for UpstreamOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEndpointsAvailable(id) => {
                write!(f, "no usable endpoints available for upstream '{id}'")
            }
            Self::ConnectionFailed {
                ip,
                port_be,
                reason,
            } => {
                write!(
                    f,
                    "failed to connect to backend endpoint ({ip:#x}, port_be={port_be}): {reason}"
                )
            }
            Self::AcquisitionTimeout(d) => {
                write!(f, "connection acquisition timed out after {d:?}")
            }
            Self::DnsResolutionFailed { host, reason } => {
                write!(f, "DNS resolution failed for host '{host}': {reason}")
            }
            Self::InvalidConfig(msg) => write!(f, "invalid upstream configuration: {msg}"),
            Self::Tls { sni, reason } => {
                write!(f, "upstream TLS handshake failed for '{sni}': {reason}")
            }
            Self::Protocol(msg) => write!(f, "upstream protocol outcome: {msg}"),
            Self::Io(e) => write!(f, "upstream I/O outcome: {e}"),
        }
    }
}
