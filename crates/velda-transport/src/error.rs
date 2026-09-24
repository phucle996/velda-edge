//! Transport layer errors.

use std::net::SocketAddr;
use thiserror::Error;
use velda_core::{Error as CoreError, ErrorKind};

/// Errors originating in the L4 transport subsystem.
#[derive(Debug, Error)]
pub enum TransportError {
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

    /// The transport connection is already closed.
    #[error("connection closed unexpectedly")]
    Closed,
}

impl From<TransportError> for CoreError {
    fn from(err: TransportError) -> Self {
        match err {
            TransportError::Io(e) => {
                CoreError::with_source(ErrorKind::Connection, "transport I/O error", e)
            }
            TransportError::Bind { addr, source } => CoreError::with_source(
                ErrorKind::Connection,
                format!("failed to bind listener on {addr}"),
                source,
            ),
            TransportError::Accept(e) => {
                CoreError::with_source(ErrorKind::Connection, "failed to accept connection", e)
            }
            TransportError::Connect { addr, source } => CoreError::with_source(
                ErrorKind::UpstreamUnavailable,
                format!("failed to connect to target {addr}"),
                source,
            ),
            TransportError::Forward(e) => CoreError::with_source(
                ErrorKind::Connection,
                "transport bidirectional forwarding error",
                e,
            ),
            TransportError::Closed => {
                CoreError::new(ErrorKind::Connection, "transport connection closed")
            }
        }
    }
}

/// Specialized Result type for L4 transport operations.
pub type Result<T> = std::result::Result<T, TransportError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind as IoErrorKind};

    #[test]
    fn test_error_conversion_to_core_error() {
        let err = TransportError::Closed;
        let core_err: CoreError = err.into();
        assert_eq!(core_err.kind(), ErrorKind::Connection);
        assert_eq!(core_err.message(), "transport connection closed");

        let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let bind_err = TransportError::Bind {
            addr,
            source: IoError::new(IoErrorKind::AddrInUse, "address in use"),
        };
        let core_bind: CoreError = bind_err.into();
        assert_eq!(core_bind.kind(), ErrorKind::Connection);
        assert!(core_bind.message().contains("127.0.0.1:9090"));

        let conn_err = TransportError::Connect {
            addr,
            source: IoError::new(IoErrorKind::ConnectionRefused, "refused"),
        };
        let core_conn: CoreError = conn_err.into();
        assert_eq!(core_conn.kind(), ErrorKind::UpstreamUnavailable);
    }
}
