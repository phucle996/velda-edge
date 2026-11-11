//! Standard Stream Log Shipper Schema & Validation (`phase: "background"`).
//!
//! Validates configuration for [`StdLogShipperPlugin`], which drains reference-counted
//! 64-byte [`velda_observability::AccessLogRecord`] batches across all protocols (HTTP/1, H2, H3,
//! gRPC, Raw L4) and formats them into standard text or JSON streams (`stdout`, `stderr`, or file).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Standard Log Shipper background plugin (`std_log_shipper`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StdLogShipperConfig {
    /// Destination output stream (`"stdout"`, `"stderr"`, or a filesystem file path).
    pub target: String,
    /// Serialization format (`"text"` or `"json"`).
    pub format: String,
    /// Periodic batch flush interval in milliseconds (50 to 60,000 ms).
    pub flush_interval_ms: u64,
    /// Maximum records grouped in a single drain cycle (1 to 10,000).
    pub max_batch_size: usize,
}

/// Validates [`StdLogShipperConfig`] at configuration compilation time.
pub fn validate_std_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<StdLogShipperConfig, SyncError> {
    let cfg: StdLogShipperConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete std_log_shipper config: {e}"
            ),
        })?;

    let target = cfg.target.trim();
    if target.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': target cannot be empty"),
        });
    }

    let format = cfg.format.trim().to_ascii_lowercase();
    if format != "text" && format != "json" && format != "clf" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': format must be 'text' or 'json', found '{format}'"
            ),
        });
    }

    if !(50..=60_000).contains(&cfg.flush_interval_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': flush_interval_ms ({}) must be between 50 and 60,000 ms",
                cfg.flush_interval_ms
            ),
        });
    }

    if !(1..=10_000).contains(&cfg.max_batch_size) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': max_batch_size ({}) must be between 1 and 10,000",
                cfg.max_batch_size
            ),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_std_log_shipper_config() {
        let raw = json!({
            "target": "stdout",
            "format": "json",
            "flush_interval_ms": 100,
            "max_batch_size": 512
        });
        let cfg = validate_std_log_shipper_config("log_sink", &raw).unwrap();
        assert_eq!(cfg.target, "stdout");
        assert_eq!(cfg.format, "json");
    }

    #[test]
    fn test_invalid_std_log_shipper_format() {
        let raw = json!({
            "target": "stdout",
            "format": "xml",
            "flush_interval_ms": 100,
            "max_batch_size": 512
        });
        assert!(validate_std_log_shipper_config("log_sink", &raw).is_err());
    }
}
