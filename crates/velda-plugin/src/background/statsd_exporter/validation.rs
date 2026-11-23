//! StatsD / DogStatsD Metrics Exporter Schema & Semantic Validation (`statsd_exporter::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize StatsdExporterConfig
//!         │
//!         ├─► etp_id.trim().is_empty()                    ──► Err(PluginError::Config)
//!         ├─► interval_secs outside 1..=3600              ──► Err(PluginError::Config)
//!         ├─► non-empty prefix invalid [a-zA-Z0-9_.-]     ──► Err(PluginError::Config)
//!         ├─► Some(dialect) not "standard" | "dogstatsd"  ──► Err(PluginError::Config)
//!         └─► max_packet_size outside 256..=65507         ──► Err(PluginError::Config)
//! ```

use crate::error::PluginError;

pub use super::config::StatsdExporterConfig;

/// Validates StatsD Metrics Exporter configuration against ETP & MTU bounds.
pub fn validate_statsd_exporter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<StatsdExporterConfig, PluginError> {
    let mut cfg: StatsdExporterConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has malformed or incomplete statsd_exporter config: {e}"
            ))
        })?;

    cfg.etp_id = cfg.etp_id.trim().to_string();
    if cfg.etp_id.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'etp_id' must not be empty"
        )));
    }

    if !(1..=3600).contains(&cfg.interval_secs) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': interval_secs must be between 1 and 3600 seconds, found {}",
            cfg.interval_secs
        )));
    }

    cfg.prefix = cfg.prefix.trim().to_string();
    if !cfg.prefix.is_empty()
        && !cfg
            .prefix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': prefix '{}' contains invalid characters (allowed: [a-zA-Z0-9_.-])",
            cfg.prefix
        )));
    }

    if let Some(ref raw_dialect) = cfg.dialect {
        let normalized = raw_dialect.trim().to_ascii_lowercase();
        if normalized != "dogstatsd" && normalized != "standard" {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': dialect must be 'dogstatsd' or 'standard', found '{raw_dialect}'"
            )));
        }
        cfg.dialect = Some(normalized);
    }

    if !(256..=65_507).contains(&cfg.max_packet_size) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_packet_size must be between 256 and 65507 bytes (recommended: 1432), found {}",
            cfg.max_packet_size
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_statsd_exporter_config_and_defaults() {
        let minimal = serde_json::json!({ "etp_id": "statsd-main" });
        let cfg = validate_statsd_exporter_config("statsd_exporter", &minimal).unwrap();
        assert_eq!(cfg.etp_id, "statsd-main");
        assert_eq!(cfg.interval_secs, 2);
        assert_eq!(cfg.prefix, "");
        assert_eq!(cfg.dialect, None);
        assert_eq!(cfg.max_packet_size, 1432);

        let full = serde_json::json!({
            "etp_id": "statsd-edge",
            "interval_secs": 10,
            "prefix": "velda_gw.v1",
            "dialect": "DogStatsD",
            "max_packet_size": 1024
        });
        let parsed = validate_statsd_exporter_config("statsd_exporter", &full).unwrap();
        assert_eq!(parsed.prefix, "velda_gw.v1");
        assert_eq!(parsed.dialect.as_deref(), Some("dogstatsd"));
        assert_eq!(parsed.max_packet_size, 1024);
    }

    #[test]
    fn test_invalid_statsd_exporter_config() {
        assert!(validate_statsd_exporter_config("sd", &serde_json::json!({})).is_err());
        assert!(
            validate_statsd_exporter_config("sd", &serde_json::json!({ "etp_id": "  " })).is_err()
        );
        assert!(
            validate_statsd_exporter_config(
                "sd",
                &serde_json::json!({ "etp_id": "ok", "interval_secs": 0 })
            )
            .is_err()
        );
        assert!(
            validate_statsd_exporter_config(
                "sd",
                &serde_json::json!({ "etp_id": "ok", "interval_secs": 3601 })
            )
            .is_err()
        );
        assert!(
            validate_statsd_exporter_config(
                "sd",
                &serde_json::json!({ "etp_id": "ok", "prefix": "bad prefix!" })
            )
            .is_err()
        );
        assert!(
            validate_statsd_exporter_config(
                "sd",
                &serde_json::json!({ "etp_id": "ok", "dialect": "prometheus" })
            )
            .is_err()
        );
        assert!(
            validate_statsd_exporter_config(
                "sd",
                &serde_json::json!({ "etp_id": "ok", "max_packet_size": 128 })
            )
            .is_err()
        );
        assert!(
            validate_statsd_exporter_config(
                "sd",
                &serde_json::json!({ "etp_id": "ok", "max_packet_size": 70000 })
            )
            .is_err()
        );
    }
}
