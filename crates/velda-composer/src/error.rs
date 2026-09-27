//! Error types for the Velda Composer boundary.

use thiserror::Error;
use velda_core::{Error as CoreError, ErrorKind};

/// Specific error conditions encountered during protocol and stream composition.
#[derive(Debug, Error)]
pub enum ComposerError {
    /// Transport I/O failure or connection drop.
    #[error("Transport I/O error: {0}")]
    Transport(String),

    /// TLS processing, negotiation, or handshake failure.
    #[error("TLS processing failed: {0}")]
    Tls(String),

    /// TLS handshake exceeded deadline.
    #[error("TLS handshake timed out after {0}ms")]
    HandshakeTimeout(u64),

    /// Inconsistency between configured listener protocol and negotiated protocol.
    #[error(
        "Protocol mismatch on listener '{listener_id}': expected '{expected}', actual '{actual}'"
    )]
    ProtocolMismatch {
        listener_id: String,
        expected: String,
        actual: String,
    },

    /// Protocol declared in configuration is not supported.
    #[error("Unsupported application protocol '{0}'")]
    UnsupportedProtocol(String),

    /// Missing composition configuration for listener.
    #[error("No compiled composition configuration found for listener '{0}'")]
    MissingConfiguration(String),

    /// Connection was closed by peer before composition finished.
    #[error("Connection closed before composition completed")]
    Closed,

    /// General internal composition error.
    #[error("Internal composer error: {0}")]
    Internal(String),
}

impl From<ComposerError> for CoreError {
    fn from(err: ComposerError) -> Self {
        match err {
            ComposerError::Transport(msg) => CoreError::new(ErrorKind::Connection, msg),
            ComposerError::Tls(msg) => CoreError::new(ErrorKind::Protocol, msg),
            ComposerError::HandshakeTimeout(ms) => CoreError::new(
                ErrorKind::Timeout,
                format!("TLS handshake timed out ({ms}ms)"),
            ),
            ComposerError::ProtocolMismatch {
                listener_id,
                expected,
                actual,
            } => CoreError::new(
                ErrorKind::Protocol,
                format!("Protocol mismatch for {listener_id}: {expected} vs {actual}"),
            ),
            ComposerError::UnsupportedProtocol(proto) => CoreError::new(
                ErrorKind::InvalidRequest,
                format!("Unsupported protocol: {proto}"),
            ),
            ComposerError::MissingConfiguration(id) => CoreError::new(
                ErrorKind::RouteConfig,
                format!("Missing composition configuration for listener: {id}"),
            ),
            ComposerError::Closed => {
                CoreError::new(ErrorKind::Connection, "Connection closed prematurely")
            }
            ComposerError::Internal(msg) => CoreError::new(ErrorKind::Internal, msg),
        }
    }
}
