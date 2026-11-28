//! Strongly typed error definitions for TLS subsystem.

use thiserror::Error;

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

    #[error("Unsupported TLS protocol version: {0}")]
    UnsupportedProtocolVersion(String),

    #[error("Rustls error: {0}")]
    Rustls(#[from] rustls::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
