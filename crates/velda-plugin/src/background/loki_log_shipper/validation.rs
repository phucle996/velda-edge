//! Grafana Loki Log Shipper Schema & Semantic Validation (`loki_log_shipper::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize LokiLogShipperConfig
//!         │
//!         ├─► etp_id.trim().is_empty()               ──► Err(PluginError::Config)
//!         ├─► service_name.trim().is_empty()         ──► Err(PluginError::Config)
//!         ├─► flush_interval_ms outside 100..=60000  ──► Err(PluginError::Config)
//!         ├─► max_batch_size outside 1..=10000       ──► Err(PluginError::Config)
//!         ├─► request_timeout_ms outside 100..=60000 ──► Err(PluginError::Config)
//!         └─► labels key not [a-zA-Z_][a-zA-Z0-9_]*  ──► Err(PluginError::Config)
//! ```

use crate::error::PluginError;

pub use super::config::LokiLogShipperConfig;

#[inline]
fn is_valid_prometheus_label_name(key: &str) -> bool {
    let mut bytes = key.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Parses and validates [`LokiLogShipperConfig`] at configuration compilation time.
pub fn validate_loki_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<LokiLogShipperConfig, PluginError> {
    let cfg: LokiLogShipperConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete loki_log_shipper config: {e}"
        ))
    })?;

    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': etp_id cannot be empty"
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

    for (k, v) in &cfg.labels {
        if !is_valid_prometheus_label_name(k) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': Loki label key '{k}' must match Prometheus identifier [a-zA-Z_][a-zA-Z0-9_]*"
            )));
        }
        if v.trim().is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': Loki label '{k}' value cannot be empty"
            )));
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_loki_log_shipper_config() {
        let raw = json!({
            "etp_id": "loki-primary",
            "service_name": "velda-edge",
            "flush_interval_ms": 500,
            "max_batch_size": 512,
            "request_timeout_ms": 5000,
            "labels": {
                "env": "production",
                "_cluster_01": "us_east"
            }
        });
        let cfg = validate_loki_log_shipper_config("loki_shipper", &raw).unwrap();
        assert_eq!(cfg.etp_id, "loki-primary");
        assert_eq!(cfg.service_name, "velda-edge");
        assert_eq!(cfg.max_batch_size, 512);

        let minimal = json!({ "etp_id": "loki-grpc" });
        let min_cfg = validate_loki_log_shipper_config("loki_min", &minimal).unwrap();
        assert_eq!(min_cfg.etp_id, "loki-grpc");
        assert_eq!(min_cfg.service_name, "velda-edge");
        assert_eq!(min_cfg.flush_interval_ms, 1000);
        assert_eq!(min_cfg.max_batch_size, 1000);
        assert_eq!(min_cfg.request_timeout_ms, 5000);
    }

    #[test]
    fn test_invalid_loki_log_shipper_config() {
        assert!(validate_loki_log_shipper_config("loki", &json!({ "etp_id": "   " })).is_err());
        assert!(
            validate_loki_log_shipper_config(
                "loki",
                &json!({ "etp_id": "loki-1", "service_name": "" })
            )
            .is_err()
        );
        assert!(
            validate_loki_log_shipper_config(
                "loki",
                &json!({ "etp_id": "loki-1", "flush_interval_ms": 50 })
            )
            .is_err()
        );
        assert!(
            validate_loki_log_shipper_config(
                "loki",
                &json!({ "etp_id": "loki-1", "max_batch_size": 0 })
            )
            .is_err()
        );
        assert!(
            validate_loki_log_shipper_config(
                "loki",
                &json!({ "etp_id": "loki-1", "request_timeout_ms": 99 })
            )
            .is_err()
        );

        for bad_label in ["", "1env", "bad-label", "bad label", "a:b"] {
            assert!(
                validate_loki_log_shipper_config(
                    "loki",
                    &json!({ "etp_id": "loki-1", "labels": { bad_label: "val" } })
                )
                .is_err(),
                "expected invalid Prometheus label key '{bad_label}' to fail"
            );
        }
        assert!(
            validate_loki_log_shipper_config(
                "loki",
                &json!({ "etp_id": "loki-1", "labels": { "env": "   " } })
            )
            .is_err()
        );
    }
}
