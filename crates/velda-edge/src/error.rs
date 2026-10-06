//! Error types for the `velda-edge` supervisor crate.

use thiserror::Error;

/// Error conditions encountered within `velda-edge`.
#[derive(Debug, Error)]
pub enum EdgeError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Sync domain error in '{domain}': {reason}")]
    SyncDomain { domain: String, reason: String },

    #[error("Invalid socket address '{addr}' for listener '{id}': {reason}")]
    InvalidAddress {
        id: String,
        addr: String,
        reason: String,
    },

    #[error("Transport error: {0}")]
    Transport(#[from] velda_transport::TransportError),

    #[error("Upstream error: {0}")]
    Upstream(#[from] velda_upstream::UpstreamError),

    #[error("TLS error: {0}")]
    Tls(#[from] velda_tls::TlsError),

    #[error("Invalid configuration: {detail}")]
    InvalidConfig { detail: String },

    #[error("Configuration reload failed: {0}")]
    Reload(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<velda_sync::SyncError> for EdgeError {
    fn from(err: velda_sync::SyncError) -> Self {
        match err {
            velda_sync::SyncError::Io(e) => EdgeError::Io(e),
            velda_sync::SyncError::Compile { domain, reason } => {
                EdgeError::SyncDomain { domain, reason }
            }
            velda_sync::SyncError::Validation { domain, reason } => {
                EdgeError::SyncDomain { domain, reason }
            }
            other => EdgeError::SyncDomain {
                domain: "edge".into(),
                reason: other.to_string(),
            },
        }
    }
}
