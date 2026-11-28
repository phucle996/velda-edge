//! Metrics Exporter Configuration Schema & Validation (`metrics_exporter::validation`).
//!
//! ```text
//!   serde_json::Value ──► Deserialize MetricsExporterConfig { targets }
//!         │
//!         ├─► targets.is_empty() ──► Err(PluginError::Config)
//!         └─► For each target:
//!               ├─► mode == "pull" ──► validate_prometheus_pull (listen, path, proto, tls, cors, auth, scrape)
//!               └─► mode == "push" ──► validate_push_target (etp_id, interval_secs, timeout, service_name, dialect, mtu)
//! ```

use serde_json::Value;

use super::config::{
    MetricsExporterConfig, MetricsPushTargetConfig, MetricsTargetConfig, PrometheusPullConfig,
};
use super::providers::statsd::dialect::StatsdDialect;
use crate::error::PluginError;

fn validate_prometheus_pull(
    plugin_id: &str,
    idx: usize,
    cfg: &PrometheusPullConfig,
) -> Result<(), PluginError> {
    let trimmed_listen = cfg.listen.trim();
    if trimmed_listen.is_empty() || trimmed_listen.parse::<std::net::SocketAddr>().is_err() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: invalid listen SocketAddr '{}'",
            cfg.listen
        )));
    }

    if !cfg.path.trim().starts_with('/') {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: path '{}' must start with '/'",
            cfg.path
        )));
    }

    let protocol = cfg.protocol.trim().to_ascii_lowercase();
    if protocol != "http" && protocol != "https" {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: protocol must be 'http' or 'https', got '{protocol}'"
        )));
    }

    let tls_mode = cfg.tls.mode.trim().to_ascii_lowercase();
    let has_cert = cfg
        .tls
        .cert_file
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty());
    let has_key = cfg
        .tls
        .key_file
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty());
    let has_ca = cfg
        .tls
        .client_ca_file
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty());

    match tls_mode.as_str() {
        "none" if protocol == "https" => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: protocol 'https' requires tls.mode 'tls' or 'mtls'"
            )));
        }
        "none" => {}
        "tls" if !has_cert || !has_key => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: tls.mode 'tls' requires 'cert_file' and 'key_file'"
            )));
        }
        "tls" => {}
        "mtls" if !has_cert || !has_key || !has_ca => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: tls.mode 'mtls' requires 'cert_file', 'key_file', and 'client_ca_file'"
            )));
        }
        "mtls" => {}
        _ => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: invalid tls.mode '{tls_mode}'"
            )));
        }
    }

    if cfg.cors.enabled && cfg.cors.allow_origins.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: cors enabled but 'allow_origins' is empty"
        )));
    }

    let auth_mode = cfg.auth.mode.trim().to_ascii_lowercase();
    match auth_mode.as_str() {
        "none" => {}
        "bearer"
            if !cfg
                .auth
                .token
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty()) =>
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: auth.mode 'bearer' requires 'token'"
            )));
        }
        "bearer" => {}
        "basic"
            if !cfg
                .auth
                .username
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
                || !cfg
                    .auth
                    .password
                    .as_deref()
                    .is_some_and(|s| !s.trim().is_empty()) =>
        {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: auth.mode 'basic' requires 'username' and 'password'"
            )));
        }
        "basic" => {}
        _ => {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}' target[{idx}]: invalid auth.mode '{auth_mode}'"
            )));
        }
    }

    if !(100..=120_000).contains(&cfg.scrape.timeout_ms) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: scrape.timeout_ms out of range [100, 120000]"
        )));
    }
    if !(1..=1024).contains(&cfg.scrape.max_concurrency) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: scrape.max_concurrency out of range [1, 1024]"
        )));
    }
    if !(8..=4096).contains(&cfg.scrape.max_buffer_size_kb) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: scrape.max_buffer_size_kb out of range [8, 4096]"
        )));
    }
    Ok(())
}

fn validate_push_target(
    plugin_id: &str,
    idx: usize,
    cfg: &MetricsPushTargetConfig,
) -> Result<(), PluginError> {
    if cfg.etp_id.trim().is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: 'etp_id' cannot be empty when mode is 'push'"
        )));
    }
    if let Some(secs) = cfg.interval_secs
        && !(1..=3600).contains(&secs)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: interval_secs ({secs}) must be in 1..=3600"
        )));
    }
    if let Some(ms) = cfg.request_timeout_ms
        && !(100..=60_000).contains(&ms)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: request_timeout_ms ({ms}) must be in 100..=60_000"
        )));
    }
    if let Some(ref svc) = cfg.service_name
        && svc.trim().is_empty()
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: service_name cannot be empty"
        )));
    }
    if let Some(ref d) = cfg.dialect
        && !d.trim().is_empty()
    {
        d.parse::<StatsdDialect>()
            .map_err(|e| PluginError::Config(format!("Plugin '{plugin_id}' target[{idx}]: {e}")))?;
    }
    if let Some(mtu) = cfg.max_packet_size
        && !(256..=65507).contains(&mtu)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' target[{idx}]: max_packet_size ({mtu}) must be in 256..=65507"
        )));
    }
    Ok(())
}

/// Validates [`MetricsExporterConfig`] at configuration compilation time.
pub fn validate_metrics_exporter_config(
    plugin_id: &str,
    config_val: &Value,
) -> Result<MetricsExporterConfig, PluginError> {
    let cfg: MetricsExporterConfig = serde_json::from_value(config_val.clone()).map_err(|e| {
        PluginError::Config(format!(
            "Plugin '{plugin_id}' has malformed metrics_exporter config: {e}"
        ))
    })?;

    if cfg.targets.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}' (metrics_exporter): 'targets' must contain at least one target"
        )));
    }

    for (idx, target) in cfg.targets.iter().enumerate() {
        match target {
            MetricsTargetConfig::Pull(pull) => validate_prometheus_pull(plugin_id, idx, pull)?,
            MetricsTargetConfig::Push(push) => validate_push_target(plugin_id, idx, push)?,
        }
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_mixed_pull_and_push_targets() {
        let raw = json!({
            "targets": [
                {
                    "mode": "pull",
                    "listen": "0.0.0.0:9090",
                    "path": "/metrics",
                    "protocol": "http",
                    "scrape": { "timeout_ms": 5000, "max_concurrency": 16, "max_buffer_size_kb": 256 },
                    "tls": { "mode": "none", "cert_file": null, "key_file": null, "client_ca_file": null },
                    "cors": { "enabled": false, "allow_origins": [], "allow_methods": [], "allow_headers": [] },
                    "auth": { "mode": "none", "token": null, "username": null, "password": null }
                },
                {
                    "mode": "push",
                    "etp_id": "otlp-main",
                    "interval_secs": 15,
                    "service_name": "velda-edge"
                },
                {
                    "mode": "push",
                    "etp_id": "statsd-main",
                    "interval_secs": 2,
                    "dialect": "dogstatsd"
                }
            ]
        });

        let cfg = validate_metrics_exporter_config("metrics_exporter", &raw).unwrap();
        assert_eq!(cfg.targets.len(), 3);
    }

    #[test]
    fn test_reject_invalid_targets() {
        assert!(
            validate_metrics_exporter_config("metrics_exporter", &json!({ "targets": [] }))
                .is_err()
        );
        assert!(
            validate_metrics_exporter_config(
                "metrics_exporter",
                &json!({ "targets": [{ "mode": "push", "etp_id": "" }] })
            )
            .is_err()
        );
    }
}
