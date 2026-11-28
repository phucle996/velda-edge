//! Access Log Exporter Configuration Schema & Validation (`log_exporter::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize LogExporterConfig { targets }
//!         │
//!         ├─► targets.is_empty() ──► Err(PluginError::Config)
//!         └─► For each target:
//!               ├─► mode == "local" ──► target (stdout|stderr|/abs), format (text|json|clf), bounds
//!               └─► mode == "etp"   ──► etp_id != "", labels, index, prefix, topic syntax checks
//! ```

use serde_json::Value;

use super::config::{
    CompressionCodec, EsCompression, EtpLogTargetConfig, KafkaAckMode, KafkaLogFormat,
    LocalLogTargetConfig, LogExporterConfig, LogTargetConfig, PartitionStrategy, S3Compression,
};
use crate::error::PluginError;

const FORBIDDEN_INDEX_CHARS: [char; 14] = [
    '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':', '\\', '\n', '\r',
];

#[inline]
fn is_valid_prometheus_label_name(key: &str) -> bool {
    let mut bytes = key.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn validate_local_target(
    plugin_id: &str,
    idx: usize,
    cfg: &LocalLogTargetConfig,
) -> Result<(), PluginError> {
    let target = cfg.target.trim();
    if target.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: 'target' cannot be empty"
        )));
    }
    let target_lc = target.to_ascii_lowercase();
    if target_lc != "stdout" && target_lc != "stderr" && !target.starts_with('/') {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: 'target' must be 'stdout', 'stderr', or an absolute file path, found '{target}'"
        )));
    }
    let format = cfg.format.trim().to_ascii_lowercase();
    if format != "text" && format != "json" && format != "clf" {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: local format must be 'text', 'json', or 'clf', found '{format}'"
        )));
    }
    if !(50..=60_000).contains(&cfg.flush_interval_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: flush_interval_ms ({}) must be in 50..=60_000",
            cfg.flush_interval_ms
        )));
    }
    if !(1..=10_000).contains(&cfg.max_batch_size) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: max_batch_size ({}) must be in 1..=10_000",
            cfg.max_batch_size
        )));
    }
    Ok(())
}

fn validate_kafka_topic_name(plugin_id: &str, idx: usize, topic: &str) -> Result<(), PluginError> {
    let t = topic.trim();
    if t.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: topic cannot be empty"
        )));
    }
    if t.len() > 249 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: topic length ({}) exceeds 249",
            t.len()
        )));
    }
    if t == "." || t == ".." {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: topic cannot be '.' or '..'"
        )));
    }
    for byte in t.bytes() {
        if !(byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_' || byte == b'-') {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: invalid character '{}' in topic",
                byte as char
            )));
        }
    }
    Ok(())
}

fn validate_etp_target(
    plugin_id: &str,
    idx: usize,
    cfg: &EtpLogTargetConfig,
) -> Result<(), PluginError> {
    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: 'etp_id' cannot be empty when mode is 'etp'"
        )));
    }
    if let Some(ref svc) = cfg.service_name
        && svc.trim().is_empty()
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: service_name cannot be empty"
        )));
    }
    if let Some(ms) = cfg.flush_interval_ms
        && !(10..=600_000).contains(&ms)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: flush_interval_ms ({ms}) out of range 10..=600_000"
        )));
    }
    if let Some(n) = cfg.max_batch_size
        && !(1..=50_000).contains(&n)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: max_batch_size ({n}) must be in 1..=50_000"
        )));
    }
    if let Some(n) = cfg.max_drain_batches
        && !(1..=4096).contains(&n)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: max_drain_batches ({n}) must be in 1..=4096"
        )));
    }
    if let Some(ms) = cfg.request_timeout_ms
        && !(100..=120_000).contains(&ms)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: request_timeout_ms ({ms}) must be in 100..=120_000"
        )));
    }
    if let Some(ms) = cfg.metadata_refresh_ms
        && !(1_000..=3_600_000).contains(&ms)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: metadata_refresh_ms ({ms}) must be in 1_000..=3_600_000"
        )));
    }
    if let Some(ref index) = cfg.index
        && !index.is_empty()
        && (index.trim().is_empty() || index.contains(FORBIDDEN_INDEX_CHARS))
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: invalid Elasticsearch 'index' '{index}'"
        )));
    }
    if let Some(ref p) = cfg.pipeline
        && (p.trim().is_empty() || p.contains(['&', '?', ' ', '\r', '\n']))
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: invalid Elasticsearch 'pipeline' '{p}'"
        )));
    }
    if let Some(ref prefix) = cfg.prefix
        && prefix.contains("..")
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: S3 'prefix' must not contain '..'"
        )));
    }
    if let Some(ref topic) = cfg.topic {
        validate_kafka_topic_name(plugin_id, idx, topic)?;
    }
    for (k, v) in &cfg.labels {
        if !is_valid_prometheus_label_name(k) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: invalid Loki label key '{k}'"
            )));
        }
        if v.trim().is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: empty Loki label value for '{k}'"
            )));
        }
    }
    if let Some(ref fmt) = cfg.format {
        let f = fmt.trim().to_ascii_lowercase();
        let valid_ch = matches!(
            f.as_str(),
            "row_binary" | "rowbinary" | "json" | "json_each_row" | "jsoneachrow"
        );
        let valid_kafka = KafkaLogFormat::parse(&f).is_ok();
        if !valid_ch && !valid_kafka {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: unsupported format '{fmt}'"
            )));
        }
    }
    if let Some(ref comp) = cfg.compression {
        let valid = EsCompression::parse(comp).is_ok()
            || S3Compression::parse(comp).is_ok()
            || CompressionCodec::parse(comp).is_ok();
        if !valid {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: unsupported compression '{comp}'"
            )));
        }
    }
    if let Some(ref acks) = cfg.acks {
        KafkaAckMode::parse(acks)?;
    }
    if let Some(ref part) = cfg.partitioner {
        PartitionStrategy::parse(part)?;
    }
    Ok(())
}

/// Validates [`LogExporterConfig`] at configuration compilation time.
pub fn validate_log_exporter_config(
    plugin_id: &str,
    config_val: &Value,
) -> Result<LogExporterConfig, PluginError> {
    let cfg: LogExporterConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed log_exporter config: {e}"
        ))
    })?;

    if cfg.targets.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (log_exporter): 'targets' must contain at least one target"
        )));
    }

    for (idx, target) in cfg.targets.iter().enumerate() {
        match target {
            LogTargetConfig::Local(local) => validate_local_target(plugin_id, idx, local)?,
            LogTargetConfig::Etp(etp) => validate_etp_target(plugin_id, idx, etp)?,
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_mixed_local_and_etp_targets() {
        let raw = json!({
            "targets": [
                {
                    "mode": "local",
                    "target": "stdout",
                    "format": "json",
                    "flush_interval_ms": 500,
                    "max_batch_size": 1000
                },
                {
                    "mode": "etp",
                    "etp_id": "clickhouse-logs",
                    "database": "velda_analytics",
                    "format": "row_binary",
                    "flush_interval_ms": 1000,
                    "max_batch_size": 8000,
                    "request_timeout_ms": 5000
                },
                {
                    "mode": "etp",
                    "etp_id": "loki-logs",
                    "service_name": "velda-edge",
                    "labels": { "env": "prod" }
                }
            ]
        });

        let cfg = validate_log_exporter_config("log_exporter", &raw).unwrap();
        assert_eq!(cfg.targets.len(), 3);
    }

    #[test]
    fn test_reject_empty_targets_and_missing_etp_id() {
        assert!(validate_log_exporter_config("log_exporter", &json!({ "targets": [] })).is_err());
        assert!(
            validate_log_exporter_config(
                "log_exporter",
                &json!({ "targets": [{ "mode": "etp", "etp_id": "" }] })
            )
            .is_err()
        );
    }
}
