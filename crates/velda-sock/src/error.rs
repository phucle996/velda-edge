//! Socket subsystem errors.

use thiserror::Error;

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

/// Specialized Result type for OS socket operations.
pub type Result<T> = std::result::Result<T, SockError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind as IoErrorKind};

    #[test]
    fn test_sock_error_display_formatting() {
        let err = SockError::Closed;
        assert_eq!(err.to_string(), "connection closed unexpectedly");

        let ip = velda_core::lut::ip::v4_to_u128(0x7F00_0001);
        let port_be = 9090u16.to_be();
        let bind_err = SockError::Bind {
            ip,
            port_be,
            source: IoError::new(IoErrorKind::AddrInUse, "address in use"),
        };
        assert!(bind_err.to_string().contains("127.0.0.1:9090"));

        let conn_err = SockError::Connect {
            ip,
            port_be,
            source: IoError::new(IoErrorKind::ConnectionRefused, "refused"),
        };
        assert!(conn_err.to_string().contains("127.0.0.1:9090"));
    }
}
