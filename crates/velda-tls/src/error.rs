//! Strongly typed error definitions for TLS subsystem.

use thiserror::Error;
use velda_core::{Error as CoreError, ErrorKind};

/// Specific errors that can occur during TLS configuration, handshake, or execution.
#[derive(Debug, Error)]
pub enum TlsError {
    #[error("Invalid certificate: {0}")]
    InvalidCertificate(String),

    #[error("Invalid private key: {0}")]
    InvalidPrivateKey(String),

    #[error("Invalid CA bundle: {0}")]
    InvalidCaBundle(String),

    #[error("SNI not found or not recognized: {0}")]
    SniNotFound(String),

    #[error("No SNI was provided in ClientHello; handshake rejected")]
    NoSniProvided,

    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("Upstream target configuration not found for SNI: {0}")]
    UpstreamTargetNotFound(String),

    #[error("Rustls error: {0}")]
    Rustls(#[from] rustls::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<TlsError> for CoreError {
    fn from(err: TlsError) -> Self {
        match err {
            TlsError::NoSniProvided | TlsError::SniNotFound(_) => {
                CoreError::new(ErrorKind::Protocol, err.to_string())
            }
            TlsError::InvalidCertificate(_)
            | TlsError::InvalidPrivateKey(_)
            | TlsError::InvalidCaBundle(_)
            | TlsError::UpstreamTargetNotFound(_) => {
                CoreError::new(ErrorKind::Internal, err.to_string())
            }
            TlsError::HandshakeFailed(_) => CoreError::new(ErrorKind::Connection, err.to_string()),
            TlsError::Rustls(ref r) => CoreError::new(ErrorKind::Protocol, r.to_string()),
            TlsError::Io(ref io) => CoreError::new(ErrorKind::Connection, io.to_string()),
        }
    }
}
