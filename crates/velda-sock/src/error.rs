//! Socket subsystem errors and standard conversions.

use thiserror::Error;
use velda_core::{Error as CoreError, ErrorKind};

/// Errors originating in the OS socket subsystem.
#[derive(Debug, Error)]
pub enum SockError {
    /// Generic I/O error originating from kernel socket syscalls.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to bind to a local socket address.
    #[error("failed to bind to address {}: {source}", crate::connect::format_raw_addr(*ip, *port_be))]
    Bind {
        ip: u128,
        port_be: u16,
        #[source]
        source: std::io::Error,
    },

    /// Failed to accept an incoming client connection.
    #[error("failed to accept incoming connection: {0}")]
    Accept(std::io::Error),

    /// Failed to connect to a target address.
    #[error("failed to connect to target {}: {source}", crate::connect::format_raw_addr(*ip, *port_be))]
    Connect {
        ip: u128,
        port_be: u16,
        #[source]
        source: std::io::Error,
    },

    /// Bidirectional byte forwarding failed.
    #[error("bidirectional forwarding error: {0}")]
    Forward(std::io::Error),

    /// The socket connection is already closed.
    #[error("connection closed unexpectedly")]
    Closed,
}

impl From<SockError> for CoreError {
    fn from(err: SockError) -> Self {
        match err {
            SockError::Io(e) => {
                CoreError::with_source(ErrorKind::Connection, "socket I/O error", e)
            }
            SockError::Bind {
                ip,
                port_be,
                source,
            } => CoreError::with_source(
                ErrorKind::Connection,
                format!(
                    "failed to bind listener on {}",
                    crate::connect::format_raw_addr(ip, port_be)
                ),
                source,
            ),
            SockError::Accept(e) => {
                CoreError::with_source(ErrorKind::Connection, "failed to accept connection", e)
            }
            SockError::Connect {
                ip,
                port_be,
                source,
            } => CoreError::with_source(
                ErrorKind::UpstreamUnavailable,
                format!(
                    "failed to connect to target {}",
                    crate::connect::format_raw_addr(ip, port_be)
                ),
                source,
            ),
            SockError::Forward(e) => CoreError::with_source(
                ErrorKind::Connection,
                "socket bidirectional forwarding error",
                e,
            ),
            SockError::Closed => CoreError::new(ErrorKind::Connection, "socket connection closed"),
        }
    }
}

/// Specialized Result type for OS socket operations.
pub type Result<T> = std::result::Result<T, SockError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind as IoErrorKind};

    #[test]
    fn test_error_conversion_to_core_error() {
        let err = SockError::Closed;
        let core_err: CoreError = err.into();
        assert_eq!(core_err.kind(), ErrorKind::Connection);
        assert_eq!(core_err.message(), "socket connection closed");

        let ip = velda_core::lut::ip::v4_to_u128(0x7F00_0001);
        let port_be = 9090u16.to_be();
        let bind_err = SockError::Bind {
            ip,
            port_be,
            source: IoError::new(IoErrorKind::AddrInUse, "address in use"),
        };
        let core_bind: CoreError = bind_err.into();
        assert_eq!(core_bind.kind(), ErrorKind::Connection);
        assert!(core_bind.message().contains("127.0.0.1:9090"));

        let conn_err = SockError::Connect {
            ip,
            port_be,
            source: IoError::new(IoErrorKind::ConnectionRefused, "refused"),
        };
        let core_conn: CoreError = conn_err.into();
        assert_eq!(core_conn.kind(), ErrorKind::UpstreamUnavailable);
    }
}
