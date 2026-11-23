//! ClickHouse Log Shipper Schema & Semantic Validation (`clickhouse_log_shipper::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize ClickHouseLogShipperConfig
//!         │
//!         ├─► etp_id.trim().is_empty()               ──► Err(PluginError::Config)
//!         ├─► format not row_binary/json_each_row    ──► Err(PluginError::Config)
//!         ├─► flush_interval_ms outside 100..=60000  ──► Err(PluginError::Config)
//!         ├─► max_batch_size outside 1..=50000       ──► Err(PluginError::Config)
//!         └─► request_timeout_ms outside 100..=60000 ──► Err(PluginError::Config)
//! ```

use crate::error::PluginError;

pub use super::config::{ClickHouseLogShipperConfig, VELDA_ACCESS_LOGS_TABLE};

/// Validates [`ClickHouseLogShipperConfig`] at configuration compilation time.
pub fn validate_clickhouse_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<ClickHouseLogShipperConfig, PluginError> {
    let cfg: ClickHouseLogShipperConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has malformed or incomplete clickhouse_log_shipper config: {e}"
            ))
        })?;

    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': etp_id cannot be empty"
        )));
    }

    let fmt = cfg.format.trim().to_ascii_lowercase();
    if fmt != "row_binary"
        && fmt != "rowbinary"
        && fmt != "json"
        && fmt != "json_each_row"
        && fmt != "jsoneachrow"
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': format must be 'row_binary' or 'json', found '{fmt}'"
        )));
    }

    if cfg.flush_interval_ms < 100 || cfg.flush_interval_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': flush_interval_ms must be between 100 and 60,000 ms (aligned with velda_core coarse clock), got {}",
            cfg.flush_interval_ms
        )));
    }

    if cfg.max_batch_size == 0 || cfg.max_batch_size > 50_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_batch_size must be between 1 and 50,000, got {}",
            cfg.max_batch_size
        )));
    }

    if cfg.request_timeout_ms < 100 || cfg.request_timeout_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': request_timeout_ms must be between 100 and 60,000 ms, got {}",
            cfg.request_timeout_ms
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::clickhouse_log_shipper::ClickHouseFormat;

    #[test]
    fn test_validate_config_valid() {
        let json = serde_json::json!({
            "etp_id": "clickhouse-logs",
            "database": "velda_analytics",
            "format": "row_binary",
            "flush_interval_ms": 200,
            "max_batch_size": 5000,
            "request_timeout_ms": 3000
        });

        let cfg = validate_clickhouse_log_shipper_config("ch_test", &json).unwrap();
        assert_eq!(cfg.etp_id, "clickhouse-logs");
        assert_eq!(cfg.database, "velda_analytics");
        assert_eq!(cfg.format, "row_binary");
        assert_eq!(cfg.parsed_format(), ClickHouseFormat::RowBinary);
        assert_eq!(cfg.flush_interval_ms, 200);
        assert_eq!(cfg.max_batch_size, 5000);
        assert_eq!(cfg.request_timeout_ms, 3000);
    }

    #[test]
    fn test_validate_config_defaults_optional_database() {
        let json = serde_json::json!({
            "etp_id": "clickhouse-primary"
        });
        let cfg = validate_clickhouse_log_shipper_config("ch_test", &json).unwrap();
        assert_eq!(cfg.database, "");
        assert_eq!(cfg.format, "row_binary");
        assert_eq!(cfg.flush_interval_ms, 1000);
        assert_eq!(cfg.max_batch_size, 8000);
        assert_eq!(cfg.request_timeout_ms, 5000);
    }

    #[test]
    fn test_validate_config_empty_etp_id() {
        let json = serde_json::json!({
            "etp_id": "   ",
            "database": "default"
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());
    }

    #[test]
    fn test_validate_config_invalid_format() {
        let json = serde_json::json!({
            "etp_id": "clickhouse-logs",
            "database": "default",
            "format": "parquet"
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());
    }

    #[test]
    fn test_invalid_flush_interval_and_timeouts() {
        let low_flush = serde_json::json!({
            "etp_id": "clickhouse-logs",
            "flush_interval_ms": 50
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &low_flush).is_err());

        let zero_batch = serde_json::json!({
            "etp_id": "clickhouse-logs",
            "max_batch_size": 0
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &zero_batch).is_err());

        let low_req_timeout = serde_json::json!({
            "etp_id": "clickhouse-logs",
            "request_timeout_ms": 50
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &low_req_timeout).is_err());
    }
}
