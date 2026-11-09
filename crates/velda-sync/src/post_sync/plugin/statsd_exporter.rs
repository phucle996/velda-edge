//! StatsD / DogStatsD Metrics Exporter Schema & Semantic Validation (`phase: "background"`).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the StatsD / DogStatsD Metrics Exporter plugin.
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted. Missing fields fail fast at configuration sync time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsdExporterConfig {
    /// Target StatsD daemon / DogStatsD / Telegraf UDP socket address (e.g. `"127.0.0.1:8125"`).
    pub endpoint: String,
    /// Periodic push interval in seconds (between 1 and 3600).
    pub interval_secs: u64,
    /// Metric name prefix (e.g. `"velda"`).
    pub prefix: String,
    /// Metric dialect format: explicitly declared as `"dogstatsd"` (with tags) or `"standard"` (dot notation).
    pub dialect: String,
    /// Maximum UDP packet payload size in bytes before flushing (256 to 65507, recommended: 1432).
    pub max_packet_size: usize,
}

/// Validates StatsD Metrics Exporter configuration strictly according to production standards.
pub fn validate_statsd_exporter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<StatsdExporterConfig, SyncError> {
    let cfg: StatsdExporterConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete statsd_exporter config: {e}"
            ),
        })?;

    // 1. Validate endpoint
    let trimmed_endpoint = cfg.endpoint.trim();
    if trimmed_endpoint.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': endpoint must not be empty"),
        });
    }

    if trimmed_endpoint.parse::<std::net::SocketAddr>().is_err() {
        // Also check host:port format if hostname is provided
        let parts: Vec<&str> = trimmed_endpoint.rsplitn(2, ':').collect();
        if parts.len() != 2 || parts[0].parse::<u16>().is_err() || parts[1].is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': endpoint '{trimmed_endpoint}' must be a valid 'IP:PORT' or 'HOST:PORT' socket address"
                ),
            });
        }
    }

    // 2. Validate interval_secs (1 to 3600 seconds)
    if cfg.interval_secs == 0 || cfg.interval_secs > 3600 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': interval_secs must be between 1 and 3600 seconds, found {}",
                cfg.interval_secs
            ),
        });
    }

    // 3. Validate prefix
    let trimmed_prefix = cfg.prefix.trim();
    if trimmed_prefix.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': prefix must not be empty"),
        });
    }
    if !trimmed_prefix
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': prefix '{trimmed_prefix}' contains invalid characters (allowed: [a-zA-Z0-9_.-])"
            ),
        });
    }

    // 4. Validate dialect
    let dialect = cfg.dialect.trim().to_ascii_lowercase();
    if dialect != "dogstatsd" && dialect != "standard" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': dialect must be explicitly declared as 'dogstatsd' or 'standard', found '{}'",
                cfg.dialect
            ),
        });
    }

    // 5. Validate max_packet_size (safe MTU bounds: 256 to 65507 bytes)
    if cfg.max_packet_size < 256 || cfg.max_packet_size > 65507 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_packet_size must be between 256 and 65507 bytes (recommended: 1432), found {}",
                cfg.max_packet_size
            ),
        });
    }

    Ok(StatsdExporterConfig {
        endpoint: trimmed_endpoint.to_string(),
        interval_secs: cfg.interval_secs,
        prefix: trimmed_prefix.to_string(),
        dialect,
        max_packet_size: cfg.max_packet_size,
    })
}
