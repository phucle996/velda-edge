//! Kafka Log Shipper Schema & Semantic Validation (`kafka_log_shipper::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize KafkaLogShipperConfig
//!         │
//!         ├─► etp_id.trim().is_empty()                    ──► Err(PluginError::Config)
//!         ├─► topic invalid (1..=249, [a-zA-Z0-9._-])     ──► Err(PluginError::Config)
//!         ├─► format | compression | acks | partitioner   ──► strict enum parse
//!         └─► flush_interval | max_batch | timeout | meta ──► bounded range validation
//! ```

use super::config::KafkaLogShipperConfig;
use crate::error::PluginError;

/// Validates topic name against Kafka's canonical naming rules.
fn validate_kafka_topic_name(plugin_id: &str, topic: &str) -> Result<(), PluginError> {
    let t = topic.trim();
    if t.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': topic cannot be empty"
        )));
    }
    if t.len() > 249 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': topic length ({}) exceeds Kafka maximum limit of 249 characters",
            t.len()
        )));
    }
    if t == "." || t == ".." {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': topic cannot be '.' or '..'"
        )));
    }
    for byte in t.bytes() {
        let is_valid = byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_' || byte == b'-';
        if !is_valid {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': topic contains invalid character '{}' (only [a-zA-Z0-9._-] allowed)",
                byte as char
            )));
        }
    }
    Ok(())
}

/// Validates [`KafkaLogShipperConfig`] at configuration compilation time.
pub fn validate_kafka_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<KafkaLogShipperConfig, PluginError> {
    let cfg: KafkaLogShipperConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed or incomplete kafka_log_shipper config: {e}"
        ))
    })?;

    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'etp_id' cannot be empty"
        )));
    }

    validate_kafka_topic_name(plugin_id, &cfg.topic)?;

    cfg.parsed_compression()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;

    cfg.parsed_format()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;

    cfg.parsed_partitioner()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;

    cfg.parsed_acks()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;

    if !(100..=60_000).contains(&cfg.flush_interval_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': flush_interval_ms must be between 100 and 60,000 ms, found {}",
            cfg.flush_interval_ms
        )));
    }

    if !(1..=50_000).contains(&cfg.max_batch_size) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_batch_size must be between 1 and 50,000, found {}",
            cfg.max_batch_size
        )));
    }

    if !(100..=60_000).contains(&cfg.request_timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': request_timeout_ms must be between 100 and 60,000 ms, found {}",
            cfg.request_timeout_ms
        )));
    }

    if !(1_000..=3_600_000).contains(&cfg.metadata_refresh_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': metadata_refresh_ms must be between 1,000 and 3,600,000 ms, found {}",
            cfg.metadata_refresh_ms
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::kafka_log_shipper::config::{KafkaAckMode, PartitionStrategy};

    #[test]
    fn test_valid_kafka_log_shipper_config() {
        let minimal = serde_json::json!({
            "etp_id": "kafka-main",
            "topic": "velda_logs"
        });
        let cfg = validate_kafka_log_shipper_config("kafka", &minimal).unwrap();
        assert_eq!(cfg.etp_id, "kafka-main");
        assert_eq!(cfg.topic, "velda_logs");
        assert_eq!(cfg.parsed_acks().unwrap(), KafkaAckMode::Leader);
        assert_eq!(
            cfg.parsed_partitioner().unwrap(),
            PartitionStrategy::RoundRobin
        );
    }

    #[test]
    fn test_invalid_kafka_log_shipper_config() {
        assert!(
            validate_kafka_log_shipper_config("kafka", &serde_json::json!({ "topic": "ok" }))
                .is_err()
        );
        assert!(
            validate_kafka_log_shipper_config(
                "kafka",
                &serde_json::json!({ "etp_id": "   ", "topic": "ok" })
            )
            .is_err()
        );
        for bad_topic in ["", "bad topic", "a/b", ".", ".."] {
            assert!(
                validate_kafka_log_shipper_config(
                    "kafka",
                    &serde_json::json!({ "etp_id": "kafka-main", "topic": bad_topic })
                )
                .is_err()
            );
        }
        assert!(
            validate_kafka_log_shipper_config(
                "kafka",
                &serde_json::json!({ "etp_id": "k", "topic": "t", "max_batch_size": 0 })
            )
            .is_err()
        );
        assert!(
            validate_kafka_log_shipper_config(
                "kafka",
                &serde_json::json!({ "etp_id": "k", "topic": "t", "flush_interval_ms": 50 })
            )
            .is_err()
        );
    }
}
