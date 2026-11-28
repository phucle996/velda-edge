//! Trace Exporter Configuration Schema & Validation (`trace_exporter::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize TraceExporterConfig { targets }
//!         │
//!         ├─► targets.is_empty() ──► Err(PluginError::Config)
//!         └─► For each target:
//!               ├─► etp_id.trim().is_empty()               ──► Err
//!               ├─► service_name.trim().is_empty()         ──► Err
//!               ├─► flush_interval_ms outside 10..=60_000  ──► Err
//!               ├─► max_batch_size outside 1..=10_000      ──► Err
//!               └─► request_timeout_ms outside 100..=60000 ──► Err
//! ```

use serde_json::Value;

use super::config::{TraceExporterConfig, TraceTargetConfig};
use crate::error::PluginError;

fn validate_trace_target(
    plugin_id: &str,
    idx: usize,
    cfg: &TraceTargetConfig,
) -> Result<(), PluginError> {
    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: 'etp_id' cannot be empty"
        )));
    }
    if cfg.service_name.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: 'service_name' cannot be empty"
        )));
    }
    if !(10..=60_000).contains(&cfg.flush_interval_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: flush_interval_ms ({}) must be in 10..=60_000",
            cfg.flush_interval_ms
        )));
    }
    if !(1..=10_000).contains(&cfg.max_batch_size) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: max_batch_size ({}) must be in 1..=10_000",
            cfg.max_batch_size
        )));
    }
    if !(100..=60_000).contains(&cfg.request_timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: request_timeout_ms ({}) must be in 100..=60_000",
            cfg.request_timeout_ms
        )));
    }
    Ok(())
}

/// Validates [`TraceExporterConfig`] at configuration compilation time.
pub fn validate_trace_exporter_config(
    plugin_id: &str,
    config_val: &Value,
) -> Result<TraceExporterConfig, PluginError> {
    let cfg: TraceExporterConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed trace_exporter config: {e}"
        ))
    })?;

    if cfg.targets.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (trace_exporter): 'targets' must contain at least one target"
        )));
    }

    for (idx, target) in cfg.targets.iter().enumerate() {
        validate_trace_target(plugin_id, idx, target)?;
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_trace_exporter_config() {
        let raw = json!({
            "targets": [{
                "etp_id": "otlp-main",
                "service_name": "velda-edge",
                "flush_interval_ms": 500,
                "max_batch_size": 256,
                "request_timeout_ms": 5000
            }]
        });
        let cfg = validate_trace_exporter_config("trace_exporter", &raw).unwrap();
        assert_eq!(cfg.targets.len(), 1);
        assert_eq!(cfg.targets[0].etp_id, "otlp-main");
    }

    #[test]
    fn test_reject_invalid_trace_config() {
        assert!(
            validate_trace_exporter_config("trace_exporter", &json!({ "targets": [] })).is_err()
        );
        assert!(
            validate_trace_exporter_config(
                "trace_exporter",
                &json!({ "targets": [{ "etp_id": "" }] })
            )
            .is_err()
        );
    }
}
