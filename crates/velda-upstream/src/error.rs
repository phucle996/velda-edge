//! Error taxonomy for the `velda-upstream` subsystem.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Result alias for upstream operations.
pub type Result<T> = std::result::Result<T, UpstreamError>;

/// Concrete error types occurring during upstream resolution and acquisition.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// No endpoints in the upstream are currently active or healthy.
    #[error("no usable endpoints available for upstream '{0}'")]
    NoEndpointsAvailable(Arc<str>),

    /// Failed to connect to a backend endpoint.
    #[error("failed to connect to backend endpoint '{endpoint}': {reason}")]
    ConnectionFailed {
        endpoint: SocketAddr,
        reason: String,
    },

    /// Timed out while attempting to connect or acquire a backend connection.
    #[error("connection acquisition timed out after {0:?}")]
    AcquisitionTimeout(Duration),

    /// DNS discovery resolution failure.
    #[error("DNS resolution failed for host '{host}': {reason}")]
    DnsResolutionFailed { host: String, reason: String },

    /// Configuration validation error.
    #[error("invalid upstream configuration: {0}")]
    InvalidConfig(String),

    /// TLS handshake failure with backend endpoint.
    #[error("upstream TLS handshake failed for '{sni}': {reason}")]
    Tls { sni: String, reason: String },

    /// Upstream protocol failure (e.g., HTTP/2, HTTP/3, gRPC).
    #[error("upstream protocol error: {0}")]
    Protocol(String),

    /// IO error during connection establishment or polling.
    #[error("upstream I/O error: {0}")]
    Io(#[from] std::io::Error),
}
