//! Configuration validation for S3 Log Shipper.

use serde_json::Value;

use crate::error::PluginError;

use super::config::S3LogShipperConfig;

/// Parses and validates an [`S3LogShipperConfig`] from raw JSON config.
pub fn validate_s3_log_shipper_config(
    plugin_id: &str,
    raw: &Value,
) -> Result<S3LogShipperConfig, PluginError> {
    let cfg: S3LogShipperConfig = serde_json::from_value(raw.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): invalid config: {e}"
        ))
    })?;

    let ep = cfg.endpoint.trim();
    if ep.is_empty() || (!ep.starts_with("http://") && !ep.starts_with("https://")) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): 'endpoint' must start with http:// or https://"
        )));
    }
    if cfg.bucket.trim().is_empty() || cfg.bucket.contains(['/', ' ']) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): invalid 'bucket' '{}'",
            cfg.bucket
        )));
    }
    if cfg.region.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): 'region' must not be empty"
        )));
    }
    if cfg.access_key_id.trim().is_empty() || cfg.secret_access_key.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): 'access_key_id' and 'secret_access_key' are required"
        )));
    }
    if cfg.flush_interval_ms == 0 || cfg.connect_timeout_ms == 0 || cfg.request_timeout_ms == 0 {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (s3_log_shipper): timeouts and flush_interval_ms must be > 0"
        )));
    }

    Ok(cfg)
}
