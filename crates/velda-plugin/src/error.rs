//! Plugin Subsystem Error Definitions.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("TLS configuration or handshake error: {0}")]
    Tls(String),

    #[error("HTTP protocol error: {0}")]
    Http(String),

    #[error("Scrape timeout reached ({0} ms)")]
    ScrapeTimeout(u64),

    #[error("Max scrape concurrency limit reached ({0})")]
    ConcurrencyLimit(usize),

    #[error("Formatted metrics payload exceeded max buffer size ({0} KB)")]
    BufferOverflow(usize),

    #[error("Plugin '{0}' referenced by route was not found in active plugins configuration")]
    NotFound(String),

    #[error(
        "Protocol mismatch for plugin '{plugin_id}' on route '{route_id}': route uses {route_protocol:?}, but plugin only supports {supported:?}"
    )]
    ProtocolMismatch {
        plugin_id: String,
        route_id: String,
        route_protocol: velda_core::ProtocolFamily,
        supported: crate::capability::SupportedProtocols,
    },

    #[error("Phase mismatch for plugin '{plugin_id}': expected {expected}, but found {actual}")]
    PhaseMismatch {
        plugin_id: String,
        expected: String,
        actual: String,
    },

    #[error("Streaming policy conflict for plugin '{plugin_id}' on route '{route_id}': {detail}")]
    StreamingConflict {
        plugin_id: String,
        route_id: String,
        detail: String,
    },

    #[error("Invalid configuration for plugin '{plugin_id}': {reason}")]
    InvalidPluginConfig { plugin_id: String, reason: String },
}
