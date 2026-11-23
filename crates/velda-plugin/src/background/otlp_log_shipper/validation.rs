//! OpenTelemetry Log Shipper Schema & Validation (`otlp_log_shipper::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize OtlpLogShipperConfig
//!         │
//!         ├─► etp_id.trim().is_empty()               ──► Err(PluginError::Config)
//!         ├─► service_name.trim().is_empty()         ──► Err(PluginError::Config)
//!         ├─► flush_interval_ms outside 100..=60000  ──► Err(PluginError::Config)
//!         ├─► max_batch_size outside 1..=10000       ──► Err(PluginError::Config)
//!         └─► request_timeout_ms outside 100..=60000 ──► Err(PluginError::Config)
//! ```

use crate::error::PluginError;

pub use super::config::OtlpLogShipperConfig;

/// Validates [`OtlpLogShipperConfig`] at configuration compilation time.
pub fn validate_otlp_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<OtlpLogShipperConfig, PluginError> {
    let cfg: OtlpLogShipperConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete otlp_log_shipper config: {e}"
        ))
    })?;

    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'etp_id' cannot be empty"
        )));
    }

    if cfg.service_name.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': service_name cannot be empty"
        )));
    }

    if !(100..=60_000).contains(&cfg.flush_interval_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': flush_interval_ms ({}) must be between 100 and 60,000 ms",
            cfg.flush_interval_ms
        )));
    }

    if !(1..=10_000).contains(&cfg.max_batch_size) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_batch_size ({}) must be between 1 and 10,000",
            cfg.max_batch_size
        )));
    }

    if !(100..=60_000).contains(&cfg.request_timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': request_timeout_ms ({}) must be between 100 and 60,000 ms",
            cfg.request_timeout_ms
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_otlp_log_shipper_config() {
        let raw = json!({
            "etp_id": "otlp-main",
            "service_name": "velda-edge",
            "flush_interval_ms": 500,
            "max_batch_size": 256,
            "request_timeout_ms": 5000
        });
        let cfg = validate_otlp_log_shipper_config("otlp_sink", &raw).unwrap();
        assert_eq!(cfg.etp_id, "otlp-main");
        assert_eq!(cfg.service_name, "velda-edge");

        let minimal =
            validate_otlp_log_shipper_config("otlp_min", &json!({ "etp_id": "otlp-main" }))
                .unwrap();
        assert_eq!(minimal.service_name, "velda-edge");
        assert_eq!(minimal.flush_interval_ms, 1000);
        assert_eq!(minimal.max_batch_size, 1000);
    }

    #[test]
    fn test_invalid_otlp_log_shipper_config() {
        assert!(validate_otlp_log_shipper_config("otlp", &json!({})).is_err());
        assert!(validate_otlp_log_shipper_config("otlp", &json!({ "etp_id": "  " })).is_err());
        assert!(
            validate_otlp_log_shipper_config(
                "otlp",
                &json!({ "etp_id": "otlp-main", "service_name": "" })
            )
            .is_err()
        );
        assert!(
            validate_otlp_log_shipper_config(
                "otlp",
                &json!({ "etp_id": "otlp-main", "flush_interval_ms": 10 })
            )
            .is_err()
        );
        assert!(
            validate_otlp_log_shipper_config(
                "otlp",
                &json!({ "etp_id": "otlp-main", "max_batch_size": 0 })
            )
            .is_err()
        );
        assert!(
            validate_otlp_log_shipper_config(
                "otlp",
                &json!({ "etp_id": "otlp-main", "request_timeout_ms": 100_000 })
            )
            .is_err()
        );
    }
}
