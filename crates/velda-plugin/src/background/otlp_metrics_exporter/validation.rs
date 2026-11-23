//! OpenTelemetry Metrics Exporter Schema & Semantic Validation (`otlp_metrics_exporter::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize OtlpMetricsExporterConfig
//!         │
//!         ├─► etp_id.trim().is_empty()               ──► Err(PluginError::Config)
//!         ├─► interval_secs outside 1..=3600         ──► Err(PluginError::Config)
//!         ├─► request_timeout_ms outside 100..=60000 ──► Err(PluginError::Config)
//!         └─► service_name.trim().is_empty()         ──► Err(PluginError::Config)
//! ```

use crate::error::PluginError;

pub use super::config::OtlpMetricsExporterConfig;

/// Validates OTLP Metrics Exporter configuration against ETP and bounded timer invariants.
pub fn validate_otlp_metrics_exporter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<OtlpMetricsExporterConfig, PluginError> {
    let cfg: OtlpMetricsExporterConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has malformed or incomplete otlp_metrics_exporter config: {e}"
            ))
        })?;

    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'etp_id' cannot be empty"
        )));
    }

    if !(1..=3600).contains(&cfg.interval_secs) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': interval_secs ({}) must be between 1 and 3600 seconds",
            cfg.interval_secs
        )));
    }

    if !(100..=60_000).contains(&cfg.request_timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': request_timeout_ms ({}) must be between 100 and 60,000 ms",
            cfg.request_timeout_ms
        )));
    }

    if cfg.service_name.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': service_name cannot be empty"
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_otlp_metrics_exporter_config() {
        let val = serde_json::json!({
            "etp_id": "otlp-main",
            "interval_secs": 15,
            "request_timeout_ms": 10000,
            "service_name": "edge-gateway"
        });
        let cfg = validate_otlp_metrics_exporter_config("metrics_exp", &val).unwrap();
        assert_eq!(cfg.etp_id, "otlp-main");
        assert_eq!(cfg.interval_secs, 15);

        let minimal = validate_otlp_metrics_exporter_config(
            "metrics_min",
            &serde_json::json!({ "etp_id": "otlp-main" }),
        )
        .unwrap();
        assert_eq!(minimal.interval_secs, 15);
        assert_eq!(minimal.request_timeout_ms, 5000);
        assert_eq!(minimal.service_name, "velda-edge");
    }

    #[test]
    fn test_invalid_otlp_metrics_exporter_config() {
        assert!(validate_otlp_metrics_exporter_config("m", &serde_json::json!({})).is_err());
        assert!(
            validate_otlp_metrics_exporter_config("m", &serde_json::json!({ "etp_id": "   " }))
                .is_err()
        );
        assert!(
            validate_otlp_metrics_exporter_config(
                "m",
                &serde_json::json!({ "etp_id": "otlp-main", "interval_secs": 0 })
            )
            .is_err()
        );
        assert!(
            validate_otlp_metrics_exporter_config(
                "m",
                &serde_json::json!({ "etp_id": "otlp-main", "request_timeout_ms": 50 })
            )
            .is_err()
        );
        assert!(
            validate_otlp_metrics_exporter_config(
                "m",
                &serde_json::json!({ "etp_id": "otlp-main", "service_name": "  " })
            )
            .is_err()
        );
    }
}
