//! Configuration Validation for Elasticsearch / OpenSearch Log Shipper (`elasticsearch_log_shipper::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize ElasticsearchLogShipperConfig
//!         │
//!         ├─► etp_id.trim().is_empty()                          ──► Err(PluginError::Config)
//!         ├─► index empty or contains / * ? " < > | ' ' , # : \ ──► Err(PluginError::Config)
//!         ├─► Some(pipeline) empty or contains & ? ' ' \r \n    ──► Err(PluginError::Config)
//!         ├─► max_drain_batches outside 1..=4096                ──► Err(PluginError::Config)
//!         ├─► flush_interval_ms outside 10..=60000              ──► Err(PluginError::Config)
//!         └─► request_timeout_ms outside 100..=120000           ──► Err(PluginError::Config)
//! ```

use serde_json::Value;

use crate::error::PluginError;

use super::config::ElasticsearchLogShipperConfig;

/// Forbidden characters in Elasticsearch / OpenSearch index names and request targets.
const FORBIDDEN_INDEX_CHARS: [char; 14] = [
    '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':', '\\', '\n', '\r',
];

/// Parses and validates an [`ElasticsearchLogShipperConfig`] from raw JSON config.
pub fn validate_elasticsearch_log_shipper_config(
    plugin_id: &str,
    raw: &Value,
) -> Result<ElasticsearchLogShipperConfig, PluginError> {
    let cfg: ElasticsearchLogShipperConfig = serde_json::from_value(raw.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): invalid config: {e}"
        ))
    })?;

    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): 'etp_id' must not be empty"
        )));
    }

    if cfg.index.trim().is_empty() || cfg.index.contains(FORBIDDEN_INDEX_CHARS) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): invalid 'index' '{}'",
            cfg.index
        )));
    }

    if let Some(ref p) = cfg.pipeline
        && (p.trim().is_empty() || p.contains(['&', '?', ' ', '\r', '\n']))
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): invalid 'pipeline' '{p}'"
        )));
    }

    if !(1..=4096).contains(&cfg.max_drain_batches) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): max_drain_batches must be in 1..=4096, got {}",
            cfg.max_drain_batches
        )));
    }

    if !(10..=60_000).contains(&cfg.flush_interval_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): flush_interval_ms must be in 10..=60_000, got {}",
            cfg.flush_interval_ms
        )));
    }

    if !(100..=120_000).contains(&cfg.request_timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): request_timeout_ms must be in 100..=120_000, got {}",
            cfg.request_timeout_ms
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::elasticsearch_log_shipper::{EsBulkOpType, EsCompression};

    #[test]
    fn test_validate_elasticsearch_config_valid_and_defaults() {
        let minimal = serde_json::json!({
            "etp_id": "es-primary"
        });
        let cfg = validate_elasticsearch_log_shipper_config("es_shipper", &minimal).unwrap();
        assert_eq!(cfg.etp_id, "es-primary");
        assert_eq!(cfg.index, "velda-logs");
        assert_eq!(cfg.op_type, EsBulkOpType::Create);
        assert_eq!(cfg.pipeline, None);
        assert_eq!(cfg.compression, EsCompression::Gzip);
        assert_eq!(cfg.max_drain_batches, 64);
        assert_eq!(cfg.flush_interval_ms, 1000);
        assert_eq!(cfg.request_timeout_ms, 5000);

        let full = serde_json::json!({
            "etp_id": "es-prod",
            "index": "edge-access-2026.11",
            "op_type": "index",
            "pipeline": "geoip-enrich",
            "compression": "none",
            "max_drain_batches": 256,
            "flush_interval_ms": 250,
            "request_timeout_ms": 10000
        });
        let cfg_full = validate_elasticsearch_log_shipper_config("es_shipper", &full).unwrap();
        assert_eq!(cfg_full.index, "edge-access-2026.11");
        assert_eq!(cfg_full.op_type, EsBulkOpType::Index);
        assert_eq!(cfg_full.pipeline.as_deref(), Some("geoip-enrich"));
        assert_eq!(cfg_full.compression, EsCompression::None);
        assert_eq!(cfg_full.max_drain_batches, 256);
        assert_eq!(cfg_full.flush_interval_ms, 250);
        assert_eq!(cfg_full.request_timeout_ms, 10_000);
    }

    #[test]
    fn test_validate_elasticsearch_config_rejections() {
        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "   " })
            )
            .is_err()
        );
        assert!(validate_elasticsearch_log_shipper_config("es", &serde_json::json!({})).is_err());

        for bad_idx in [
            "", "   ", "logs/bad", "logs*", "logs?", "logs\"", "logs<", "logs>", "logs|",
            "logs bad", "logs,bad", "logs#bad", "logs:bad",
        ] {
            assert!(
                validate_elasticsearch_log_shipper_config(
                    "es",
                    &serde_json::json!({ "etp_id": "es-1", "index": bad_idx })
                )
                .is_err(),
                "expected index '{bad_idx}' to be rejected"
            );
        }

        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "pipeline": "  " })
            )
            .is_err()
        );
        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "pipeline": "bad?x=1" })
            )
            .is_err()
        );

        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "max_drain_batches": 0 })
            )
            .is_err()
        );
        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "max_drain_batches": 4097 })
            )
            .is_err()
        );
        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "flush_interval_ms": 9 })
            )
            .is_err()
        );
        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "flush_interval_ms": 60001 })
            )
            .is_err()
        );
        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "request_timeout_ms": 99 })
            )
            .is_err()
        );
        assert!(
            validate_elasticsearch_log_shipper_config(
                "es",
                &serde_json::json!({ "etp_id": "es-1", "request_timeout_ms": 120001 })
            )
            .is_err()
        );
    }
}
