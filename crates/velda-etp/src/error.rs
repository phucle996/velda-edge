//! Error taxonomy for External Third-Party (`velda-etp`) connections and pools.
//!
//! ```text
//!   Caller / Sync / Pool Operation
//!         │
//!         ├─► Config / Endpoint / PEM parse ──► EtpError::Config / EtpError::Tls
//!         ├─► TcpStream / TlsConnector      ──► EtpError::Io / EtpError::Tls / Timeout
//!         ├─► Semaphore / Arena exhausted   ──► EtpError::PoolExhausted { id }
//!         └─► EtpRegistry lookup / cast     ──► EtpError::NotFound / KindMismatch
//!
//!   Error Variant Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ Config(String) | Io(std::io::Error) | Tls(String)                          │
//!   │ PoolExhausted { id } | Timeout { id, timeout_ms }                          │
//!   │ NotFound { id } | KindMismatch { id, expected, actual }                    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use thiserror::Error;

/// Unified error type for External Third-Party connection management.
#[derive(Debug, Error)]
pub enum EtpError {
    /// Invalid configuration schema, endpoint specification, or pool setting.
    #[error("ETP configuration error: {0}")]
    Config(String),

    /// Underlying transport I/O error.
    #[error("ETP I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// TLS configuration, PEM parsing, or handshake error.
    #[error("ETP TLS error: {0}")]
    Tls(String),

    /// Pool semaphore reached capacity and no slot became free before timeout.
    #[error("ETP pool '{id}' exhausted")]
    PoolExhausted { id: String },

    /// Connection establishment or acquisition exceeded configured deadline.
    #[error("ETP '{id}' timed out after {timeout_ms}ms")]
    Timeout { id: String, timeout_ms: u64 },

    /// Requested ETP identifier does not exist in the active `EtpRegistry`.
    #[error("ETP '{id}' not found")]
    NotFound { id: String },

    /// Requested ETP identifier exists but its `EtpKind` differs from expected.
    #[error("ETP '{id}' kind mismatch: expected '{expected}', got '{actual}'")]
    KindMismatch {
        id: String,
        expected: &'static str,
        actual: &'static str,
    },
}
