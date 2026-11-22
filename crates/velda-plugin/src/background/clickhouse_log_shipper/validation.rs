//! ClickHouse Log Shipper Schema & Semantic Validation.

use crate::error::PluginError;

pub use super::config::{ClickHouseLogShipperConfig, TransportSecurity, VELDA_ACCESS_LOGS_TABLE};

/// Validates [`ClickHouseLogShipperConfig`] at configuration compilation time.
pub fn validate_clickhouse_log_shipper_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<ClickHouseLogShipperConfig, PluginError> {
    let cfg: ClickHouseLogShipperConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has malformed or incomplete clickhouse_log_shipper config: {e}"
            ))
        })?;

    let endpoint = cfg.endpoint.trim();
    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': endpoint must start with 'http://' or 'https://', found '{endpoint}'"
        )));
    }

    let sec = cfg
        .parsed_transport_security()
        .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}': {e}")))?;

    match sec {
        TransportSecurity::Plain => {
            if endpoint.starts_with("https://") {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security is 'plain' but endpoint '{endpoint}' uses 'https://' scheme. Use 'http://' for plain transport."
                )));
            }
            if cfg.tls.client_cert_pem.is_some() || cfg.tls.client_key_pem.is_some() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security is 'plain' but client certificates are configured in 'tls'."
                )));
            }
        }
        TransportSecurity::Tls => {
            if endpoint.starts_with("http://") {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security is 'tls' but endpoint '{endpoint}' uses unencrypted 'http://' scheme. Use 'https://' for TLS transport."
                )));
            }
            if cfg.tls.client_cert_pem.is_some() || cfg.tls.client_key_pem.is_some() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security is 'tls' (server auth) but client certificates are provided in 'tls'. Use transport_security 'mtls' for mutual TLS."
                )));
            }
            cfg.tls.build_client_config(&["http/1.1"]).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid inline TLS configuration: {e}"
                ))
            })?;
        }
        TransportSecurity::Mtls => {
            if endpoint.starts_with("http://") {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security is 'mtls' but endpoint '{endpoint}' uses unencrypted 'http://' scheme. Use 'https://' for mTLS transport."
                )));
            }
            if cfg.tls.client_cert_pem.is_none() || cfg.tls.client_key_pem.is_none() {
                return Err(PluginError::Config(format!(
                    "Plugin '{plugin_id}': transport_security 'mtls' requires both 'client_cert_pem' and 'client_key_pem' in 'tls' config"
                )));
            }
            cfg.tls.build_client_config(&["http/1.1"]).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{plugin_id}': invalid inline mTLS configuration: {e}"
                ))
            })?;
        }
    }

    if cfg.database.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': database cannot be empty"
        )));
    }

    let fmt = cfg.format.trim().to_ascii_lowercase();
    if fmt != "row_binary"
        && fmt != "rowbinary"
        && fmt != "json"
        && fmt != "json_each_row"
        && fmt != "jsoneachrow"
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': format must be 'row_binary' or 'json', found '{fmt}'"
        )));
    }

    if cfg.flush_interval_ms < 100 || cfg.flush_interval_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': flush_interval_ms must be between 100 and 60,000 ms (aligned with velda_core coarse clock), got {}",
            cfg.flush_interval_ms
        )));
    }

    if cfg.max_batch_size == 0 || cfg.max_batch_size > 50_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': max_batch_size must be between 1 and 50,000, got {}",
            cfg.max_batch_size
        )));
    }

    if cfg.connect_timeout_ms < 100 || cfg.connect_timeout_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': connect_timeout_ms must be between 100 and 60,000 ms, got {}",
            cfg.connect_timeout_ms
        )));
    }

    if cfg.request_timeout_ms < 100 || cfg.request_timeout_ms > 60_000 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': request_timeout_ms must be between 100 and 60,000 ms, got {}",
            cfg.request_timeout_ms
        )));
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::clickhouse_log_shipper::ClickHouseFormat;

    #[test]
    fn test_validate_config_valid() {
        let json = serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "database": "velda_analytics",
            "format": "row_binary",
            "username": "writer",
            "password": "secret_password",
            "flush_interval_ms": 200,
            "max_batch_size": 5000,
            "connect_timeout_ms": 1500,
            "request_timeout_ms": 3000
        });

        let cfg = validate_clickhouse_log_shipper_config("ch_test", &json).unwrap();
        assert_eq!(cfg.endpoint, "http://127.0.0.1:8123");
        assert_eq!(cfg.database, "velda_analytics");
        assert_eq!(cfg.format, "row_binary");
        assert_eq!(cfg.parsed_format(), ClickHouseFormat::RowBinary);
        assert_eq!(cfg.username, "writer");
        assert_eq!(cfg.flush_interval_ms, 200);
        assert_eq!(cfg.max_batch_size, 5000);
    }

    #[test]
    fn test_validate_config_invalid_endpoint() {
        let json = serde_json::json!({
            "endpoint": "tcp://127.0.0.1:9000",
            "database": "default"
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());
    }

    #[test]
    fn test_validate_config_invalid_format() {
        let json = serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "database": "default",
            "format": "parquet"
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());
    }

    #[test]
    fn test_invalid_flush_interval() {
        let json = serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "database": "default",
            "flush_interval_ms": 50 // Below 100ms coarse threshold
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());
    }

    #[test]
    fn test_validate_config_invalid_inline_tls() {
        let json = serde_json::json!({
            "endpoint": "https://127.0.0.1:8443",
            "transport_security": "tls",
            "database": "default",
            "tls": {
                "ca_pem": "NOT_A_VALID_PEM_CERTIFICATE"
            }
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());
    }

    #[test]
    fn test_security_conflicts() {
        // Plain with https://
        let json = serde_json::json!({
            "endpoint": "https://127.0.0.1:8443",
            "transport_security": "plain",
            "database": "default"
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());

        // TLS with http://
        let json = serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "transport_security": "tls",
            "database": "default"
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());

        // mTLS with http://
        let json = serde_json::json!({
            "endpoint": "http://127.0.0.1:8123",
            "transport_security": "mtls",
            "database": "default"
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());

        // mTLS with missing client_key_pem
        let json = serde_json::json!({
            "endpoint": "https://127.0.0.1:8443",
            "transport_security": "mtls",
            "database": "default",
            "tls": {
                "client_cert_pem": "-----BEGIN CERTIFICATE-----\nMIIB...-----END CERTIFICATE-----\n"
            }
        });
        assert!(validate_clickhouse_log_shipper_config("ch_test", &json).is_err());
    }
}
