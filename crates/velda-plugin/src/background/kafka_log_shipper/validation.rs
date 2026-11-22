//! Kafka Log Shipper Schema & Semantic Validation.

use super::config::{KafkaLogShipperConfig, KafkaTransportSecurity};
use crate::error::PluginError;

/// Validates topic name against Kafka's canonical naming rules:
/// - Maximum length 249 characters
/// - Contains only ASCII alphanumeric characters, periods (.), underscores (_), and hyphens (-)
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

/// Validates individual broker address formatted as `host:port` or `[ipv6]:port`.
fn validate_broker_address(plugin_id: &str, broker: &str) -> Result<(), PluginError> {
    let b = broker.trim();
    if b.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': broker address entry cannot be empty"
        )));
    }

    let (host, port_str) = if b.starts_with('[') {
        // Bracketed IPv6: [::1]:9092
        let Some(close_bracket) = b.find(']') else {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid IPv6 broker address '{b}' (missing closing bracket)"
            )));
        };
        let h = &b[1..close_bracket];
        let rest = &b[close_bracket + 1..];
        if !rest.starts_with(':') {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid IPv6 broker address '{b}' (missing port separator ':')"
            )));
        }
        (h, &rest[1..])
    } else {
        // Hostname or IPv4: host:9092
        let Some(colon_pos) = b.rfind(':') else {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid broker address '{b}': expected 'host:port'"
            )));
        };
        (&b[..colon_pos], &b[colon_pos + 1..])
    };

    if host.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': broker host cannot be empty in '{b}'"
        )));
    }

    match port_str.trim().parse::<u16>() {
        Ok(port) if port > 0 => Ok(()),
        _ => Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': invalid broker port in '{b}' (must be 1..=65535)"
        ))),
    }
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

    if cfg.brokers.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': 'brokers' array cannot be empty (at least one seed broker required)"
        )));
    }

    for broker in &cfg.brokers {
        validate_broker_address(plugin_id, broker)?;
    }

    validate_kafka_topic_name(plugin_id, &cfg.topic)?;

    let sec = cfg
        .parsed_transport_security()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;

    match sec {
        KafkaTransportSecurity::Plain => {
            if cfg.tls.client_cert_pem.is_some() || cfg.tls.client_key_pem.is_some() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security is 'plain' but client certificates are configured in 'tls'."
                )));
            }
        }
        KafkaTransportSecurity::Tls => {
            if cfg.tls.client_cert_pem.is_some() || cfg.tls.client_key_pem.is_some() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security is 'tls' (server auth) but client certificates are provided in 'tls'. Use transport_security 'mtls' for mutual TLS."
                )));
            }
            cfg.tls.build_client_config(&[]).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid inline TLS configuration: {e}"
                ))
            })?;
        }
        KafkaTransportSecurity::Mtls => {
            if cfg.tls.client_cert_pem.is_none() || cfg.tls.client_key_pem.is_none() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security 'mtls' requires both 'client_cert_pem' and 'client_key_pem' in 'tls' config"
                )));
            }
            cfg.tls.build_client_config(&[]).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid inline mTLS configuration: {e}"
                ))
            })?;
        }
    }

    let sasl = cfg
        .parsed_sasl_mechanism()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;

    if sasl.is_auth_required() {
        if cfg.sasl_username.trim().is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': sasl_username cannot be empty when sasl_mechanism is '{}'",
                sasl.as_str()
            )));
        }
        if cfg.sasl_password.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': sasl_password cannot be empty when sasl_mechanism is '{}'",
                sasl.as_str()
            )));
        }
    }

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

    if !(100..=60_000).contains(&cfg.connect_timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': connect_timeout_ms must be between 100 and 60,000 ms, found {}",
            cfg.connect_timeout_ms
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
