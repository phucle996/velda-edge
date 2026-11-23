//! Configuration Validation for S3 Log Shipper (`s3_log_shipper::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize S3LogShipperConfig
//!         │
//!         ├─► etp_id.trim().is_empty()               ──► Err(PluginError::Config)
//!         ├─► prefix.contains("..")                  ──► Err(PluginError::Config)
//!         ├─► max_drain_batches outside 1..=4096     ──► Err(PluginError::Config)
//!         ├─► flush_interval_ms outside 100..=600000 ──► Err(PluginError::Config)
//!         └─► request_timeout_ms outside 100..=120000──► Err(PluginError::Config)
//! ```

use serde_json::Value;

use crate::error::PluginError;

use super::config::S3LogShipperConfig;

/// Parses and validates an [`S3LogShipperConfig`] from raw JSON config.
pub fn validate_s3_log_shipper_config(
    plugin_id: &str,
    raw: &Value,
) -> Result<S3LogShipperConfig, PluginError> {
    let cfg: S3LogShipperConfig = serde_json::from_value(raw.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): invalid config: {e}"
        ))
    })?;

    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): 'etp_id' must not be empty"
        )));
    }
    if cfg.prefix.contains("..") {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): 'prefix' must not contain '..'"
        )));
    }
    if !(1..=4096).contains(&cfg.max_drain_batches) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): max_drain_batches must be in 1..=4096, got {}",
            cfg.max_drain_batches
        )));
    }
    if !(100..=600_000).contains(&cfg.flush_interval_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): flush_interval_ms must be in 100..=600_000, got {}",
            cfg.flush_interval_ms
        )));
    }
    if !(100..=120_000).contains(&cfg.request_timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): request_timeout_ms must be in 100..=120_000, got {}",
            cfg.request_timeout_ms
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::s3_log_shipper::S3Compression;

    #[test]
    fn test_valid_s3_log_shipper_config_and_defaults() {
        let minimal = serde_json::json!({ "etp_id": "s3-archive" });
        let cfg = validate_s3_log_shipper_config("s3_shipper", &minimal).unwrap();
        assert_eq!(cfg.etp_id, "s3-archive");
        assert_eq!(cfg.prefix, "velda-logs");
        assert_eq!(cfg.compression, S3Compression::Zstd);
        assert_eq!(cfg.max_drain_batches, 256);
        assert_eq!(cfg.flush_interval_ms, 10_000);
        assert_eq!(cfg.request_timeout_ms, 10_000);

        let full = serde_json::json!({
            "etp_id": "s3-prod",
            "prefix": "",
            "compression": "gzip",
            "max_drain_batches": 512,
            "flush_interval_ms": 5000,
            "request_timeout_ms": 15000
        });
        let parsed = validate_s3_log_shipper_config("s3_shipper", &full).unwrap();
        assert_eq!(parsed.prefix, "");
        assert_eq!(parsed.compression, S3Compression::Gzip);
        assert_eq!(parsed.max_drain_batches, 512);
    }

    #[test]
    fn test_invalid_s3_log_shipper_config() {
        assert!(validate_s3_log_shipper_config("s3", &serde_json::json!({})).is_err());
        assert!(
            validate_s3_log_shipper_config("s3", &serde_json::json!({ "etp_id": "   " })).is_err()
        );
        assert!(
            validate_s3_log_shipper_config(
                "s3",
                &serde_json::json!({ "etp_id": "s3-ok", "prefix": "logs/../escape" })
            )
            .is_err()
        );
        assert!(
            validate_s3_log_shipper_config(
                "s3",
                &serde_json::json!({ "etp_id": "s3-ok", "max_drain_batches": 0 })
            )
            .is_err()
        );
        assert!(
            validate_s3_log_shipper_config(
                "s3",
                &serde_json::json!({ "etp_id": "s3-ok", "flush_interval_ms": 50 })
            )
            .is_err()
        );
        assert!(
            validate_s3_log_shipper_config(
                "s3",
                &serde_json::json!({ "etp_id": "s3-ok", "request_timeout_ms": 200_000 })
            )
            .is_err()
        );
    }
}
