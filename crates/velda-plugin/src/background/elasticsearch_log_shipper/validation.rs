//! Configuration validation for Elasticsearch / OpenSearch Log Shipper.

use serde_json::Value;

use crate::error::PluginError;

use super::config::{ElasticsearchLogShipperConfig, EsAuthConfig};

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

    if cfg.endpoints.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): 'endpoints' must not be empty"
        )));
    }
    for ep in &cfg.endpoints {
        let trimmed = ep.trim();
        if trimmed.is_empty()
            || (!trimmed.starts_with("http://") && !trimmed.starts_with("https://"))
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (elasticsearch_log_shipper): endpoint '{ep}' must start with http:// or https://"
            )));
        }
    }

    if cfg.index.trim().is_empty() || cfg.index.contains(['"', '\\', '\n', '\r']) {
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

    match &cfg.auth {
        EsAuthConfig::ApiKey { key } if key.trim().is_empty() => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (elasticsearch_log_shipper): auth.key cannot be empty"
            )));
        }
        EsAuthConfig::Basic { username, .. } if username.trim().is_empty() => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (elasticsearch_log_shipper): auth.username cannot be empty"
            )));
        }
        EsAuthConfig::Bearer { token } if token.trim().is_empty() => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' (elasticsearch_log_shipper): auth.token cannot be empty"
            )));
        }
        _ => {}
    }

    if cfg.flush_interval_ms == 0 || cfg.connect_timeout_ms == 0 || cfg.request_timeout_ms == 0 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (elasticsearch_log_shipper): timeouts and flush_interval_ms must be > 0"
        )));
    }

    Ok(cfg)
}
