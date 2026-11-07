//! Ingress layer errors and standard conversions.

use std::net::SocketAddr;
use thiserror::Error;
use velda_core::{Error as CoreError, ErrorKind};
use velda_sock::SockError;
use velda_xdp::XdpError;

/// Errors originating in the ingress coordination subsystem.
#[derive(Debug, Error)]
pub enum IngressError {
    /// OS socket subsystem error.
    #[error("socket error: {0}")]
    Sock(#[from] SockError),

    /// Kernel-bypass XDP subsystem error.
    #[error("xdp error: {0}")]
    Xdp(#[from] XdpError),

    /// Generic I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to bind to a local socket address.
    #[error("failed to bind to address {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    /// Failed to accept an incoming client connection.
    #[error("failed to accept incoming connection: {0}")]
    Accept(std::io::Error),

    /// Failed to connect to a target address.
    #[error("failed to connect to target {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    /// Bidirectional byte forwarding failed.
    #[error("bidirectional forwarding error: {0}")]
    Forward(std::io::Error),

    /// The ingress connection is already closed.
    #[error("connection closed unexpectedly")]
    Closed,
}

/// Backwards compatibility alias for `IngressError`.
pub type TransportError = IngressError;

impl From<IngressError> for CoreError {
    fn from(err: IngressError) -> Self {
        match err {
            IngressError::Sock(e) => e.into(),
            IngressError::Xdp(e) => e.into(),
            IngressError::Io(e) => {
                CoreError::with_source(ErrorKind::Connection, "ingress I/O error", e)
            }
            IngressError::Bind { addr, source } => CoreError::with_source(
                ErrorKind::Connection,
                format!("failed to bind listener on {addr}"),
                source,
            ),
            IngressError::Accept(e) => {
                CoreError::with_source(ErrorKind::Connection, "failed to accept connection", e)
            }
            IngressError::Connect { addr, source } => CoreError::with_source(
                ErrorKind::UpstreamUnavailable,
                format!("failed to connect to target {addr}"),
                source,
            ),
            IngressError::Forward(e) => CoreError::with_source(
                ErrorKind::Connection,
                "ingress bidirectional forwarding error",
                e,
            ),
            IngressError::Closed => {
                CoreError::new(ErrorKind::Connection, "ingress connection closed")
            }
        }
    }
}

/// Specialized Result type for ingress operations.
pub type Result<T> = std::result::Result<T, IngressError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind as IoErrorKind};

    #[test]
    fn test_error_conversion_to_core_error() {
        let err = IngressError::Closed;
        let core_err: CoreError = err.into();
        assert_eq!(core_err.kind(), ErrorKind::Connection);
        assert_eq!(core_err.message(), "ingress connection closed");

        let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let bind_err = IngressError::Bind {
            addr,
            source: IoError::new(IoErrorKind::AddrInUse, "address in use"),
        };
        let core_bind: CoreError = bind_err.into();
        assert_eq!(core_bind.kind(), ErrorKind::Connection);
        assert!(core_bind.message().contains("127.0.0.1:9090"));

        let conn_err = IngressError::Connect {
            addr,
            source: IoError::new(IoErrorKind::ConnectionRefused, "refused"),
        };
        let core_conn: CoreError = conn_err.into();
        assert_eq!(core_conn.kind(), ErrorKind::UpstreamUnavailable);
    }
}
