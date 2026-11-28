//! Ingress layer errors.

use thiserror::Error;
use velda_sock::{SockError, format_raw_addr};
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
    #[error("failed to bind to address {}: {source}", format_raw_addr(*ip, *port_be))]
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
    #[error("failed to connect to target {}: {source}", format_raw_addr(*ip, *port_be))]
    Connect {
        ip: u128,
        port_be: u16,
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

/// Specialized Result type for ingress operations.
pub type Result<T> = std::result::Result<T, IngressError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind as IoErrorKind};

    #[test]
    fn test_ingress_error_display_formatting() {
        let err = IngressError::Closed;
        assert_eq!(err.to_string(), "connection closed unexpectedly");

        let ip = velda_core::lut::ip::v4_to_u128(u32::from_be_bytes([127, 0, 0, 1]));
        let port_be = 9090u16.to_be();
        let bind_err = IngressError::Bind {
            ip,
            port_be,
            source: IoError::new(IoErrorKind::AddrInUse, "address in use"),
        };
        assert!(bind_err.to_string().contains("127.0.0.1:9090"));

        let conn_err = IngressError::Connect {
            ip,
            port_be,
            source: IoError::new(IoErrorKind::ConnectionRefused, "refused"),
        };
        assert!(conn_err.to_string().contains("127.0.0.1:9090"));
    }
}
