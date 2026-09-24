//! Pure Logging Capability Provider for velda-sync daemon.
//!
//! Initializes non-blocking structured tracing with support for human-readable compact console
//! output or machine-parsable JSON format, filtered via environment variables.
//!
//! Log writes are offloaded to a dedicated background worker thread over a lock-free ring buffer
//! via `tracing-appender`, ensuring the main synchronization loop is NEVER blocked by stdout I/O.

use std::env;
pub use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;

/// Initializes the non-blocking global tracing subscriber for the velda-sync process.
///
/// Returns an `Option<WorkerGuard>`. The returned guard must be kept alive for the duration
/// of the process (e.g. bound in `main()`) so buffered logs are flushed upon shutdown.
///
/// Configuration via environment variables:
/// - `VELDA_SYNC_LOG_LEVEL` (default: "info", fallback: `RUST_LOG`)
/// - `VELDA_SYNC_LOG_FORMAT` ("json" for structured logs, default: compact text)
pub fn init_logger() -> Option<WorkerGuard> {
    let filter = env::var("VELDA_SYNC_LOG_LEVEL")
        .or_else(|_| env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".into());

    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let format = env::var("VELDA_SYNC_LOG_FORMAT")
        .unwrap_or_else(|_| "compact".into())
        .to_lowercase();

    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());

    let init_result = if format == "json" {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_current_span(false)
            .with_target(true)
            .with_writer(non_blocking)
            .try_init()
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false)
            .with_writer(non_blocking)
            .try_init()
    };

    if init_result.is_ok() {
        Some(guard)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_logger_idempotent() {
        let _g1 = init_logger();
        let _g2 = init_logger();
    }
}
