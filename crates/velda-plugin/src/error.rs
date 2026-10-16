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
}
