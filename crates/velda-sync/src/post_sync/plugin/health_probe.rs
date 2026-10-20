//! Health Probe Configuration Schema & Semantic Validation (`phase: "background"`).
//!
//! Enforces a strict declarative contract for background auxiliary health probes.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Valid SocketAddr**: `listen` must be a valid IP:PORT socket address.
//! - **Valid Paths**: `liveness_path` and `readiness_path` must start with `/`.
//! - **Safe Intervals**: `check_interval_ms` and `timeout_ms` must lie within safe boundaries.

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Health Probe plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthProbeConfig {
    /// Socket address to bind and serve auxiliary health endpoints (e.g. "0.0.0.0:8081").
    pub listen: String,
    /// HTTP URI path for container liveness probe (e.g. "/livez").
    pub liveness_path: String,
    /// HTTP URI path for container readiness probe (e.g. "/readyz").
    pub readiness_path: String,
    /// Background health assessment interval in milliseconds (100ms to 60,000ms).
    pub check_interval_ms: u64,
    /// Probe execution timeout threshold in milliseconds (100ms to 30,000ms).
    pub timeout_ms: u64,
}

/// Validates Health Probe configuration strictly according to production standards.
pub fn validate_health_probe_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<HealthProbeConfig, SyncError> {
    let cfg: HealthProbeConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete health_probe config: {e}"
            ),
        })?;

    // 1. Validate Listen Address
    let trimmed_listen = cfg.listen.trim();
    if trimmed_listen.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': listen address cannot be empty"),
        });
    }
    if trimmed_listen.parse::<std::net::SocketAddr>().is_err() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': listen address '{trimmed_listen}' is not a valid SocketAddr (expected IP:PORT, e.g. 0.0.0.0:8081)"
            ),
        });
    }

    // 2. Validate Paths
    if !cfg.liveness_path.trim().starts_with('/') {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': liveness_path '{}' must start with '/'",
                cfg.liveness_path
            ),
        });
    }
    if !cfg.readiness_path.trim().starts_with('/') {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': readiness_path '{}' must start with '/'",
                cfg.readiness_path
            ),
        });
    }

    // 3. Validate Timing
    if !(100..=60_000).contains(&cfg.check_interval_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': check_interval_ms ({}) must be between 100 and 60,000ms",
                cfg.check_interval_ms
            ),
        });
    }
    if !(100..=30_000).contains(&cfg.timeout_ms) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': timeout_ms ({}) must be between 100 and 30,000ms",
                cfg.timeout_ms
            ),
        });
    }
    if cfg.timeout_ms > cfg.check_interval_ms {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': timeout_ms ({}) cannot exceed check_interval_ms ({})",
                cfg.timeout_ms, cfg.check_interval_ms
            ),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_health_probe() {
        let val = json!({
            "listen": "0.0.0.0:8081",
            "liveness_path": "/livez",
            "readiness_path": "/readyz",
            "check_interval_ms": 5000,
            "timeout_ms": 2000
        });
        let res = validate_health_probe_config("hp_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_listen_addr_rejected() {
        let val = json!({
            "listen": "invalid_not_socket_addr",
            "liveness_path": "/livez",
            "readiness_path": "/readyz",
            "check_interval_ms": 5000,
            "timeout_ms": 2000
        });
        let res = validate_health_probe_config("hp_bad_addr", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("not a valid SocketAddr")
        );
    }

    #[test]
    fn test_timeout_exceeds_interval_rejected() {
        let val = json!({
            "listen": "0.0.0.0:8081",
            "liveness_path": "/livez",
            "readiness_path": "/readyz",
            "check_interval_ms": 1000,
            "timeout_ms": 2000 // > 1000
        });
        let res = validate_health_probe_config("hp_bad_timeout", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("cannot exceed check_interval_ms")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "listen": "0.0.0.0:8081"
            // Missing paths and timings
        });
        let res = validate_health_probe_config("hp_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
