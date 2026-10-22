//! HTTP Fault Injection Plugin (`http_fault_injection`).
//!
//! Provides enterprise-grade Chaos Engineering and Microservice Resilience Testing
//! specifically engineered for HTTP traffic (HTTP/1.1, HTTP/2 REST/Web, and HTTP/3).
//!
//! # Architecture & Submodule Organization
//! - [`condition`]: Disjoint interval IP CIDR matching, header matching, and safe dynamic overrides.
//! - [`delay`]: Range `[min_ms, max_ms]` delay modeling with ~1ns thread-local PRNG.
//! - [`abort`]: Pre-allocated plain-text response builder and header injection.
//! - [`plugin`]: Lean sequential workflow owner (`HttpFaultInjectionPlugin`).

pub mod abort;
pub mod condition;
pub mod delay;
pub mod plugin;

use velda_sync::post_sync::plugin::PluginConfig;

pub use abort::HttpAbortConfig;
pub use condition::{HeaderMatchRule, HttpFaultCondition, IpRangeMatcher};
pub use delay::HttpDelayConfig;
pub use plugin::HttpFaultInjectionPlugin;

use crate::error::PluginError;

impl HttpFaultInjectionPlugin {
    /// Instantiates a new [`HttpFaultInjectionPlugin`] from compiled [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_http_fault_injection_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid http_fault_injection configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let delay_cfg = if cfg.delay.enabled && cfg.delay.percentage > 0 {
            Some(
                HttpDelayConfig::new(
                    cfg.delay.percentage as f64,
                    cfg.delay.fixed_delay_ms,
                    cfg.delay.fixed_delay_ms,
                )
                .map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': delay error: {e}", config.id))
                })?,
            )
        } else {
            None
        };

        let abort_cfg = if cfg.abort.enabled && cfg.abort.percentage > 0 {
            Some(
                HttpAbortConfig::new(
                    cfg.abort.percentage as f64,
                    cfg.abort.http_status,
                    None,
                    None,
                )
                .map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': abort error: {e}", config.id))
                })?,
            )
        } else {
            None
        };

        Ok(Self::new(
            &config.id,
            HttpFaultCondition::default(),
            delay_cfg,
            abort_cfg,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_from_config_complete() {
        let json_cfg = json!({
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 200,
                "percentage": 15
            },
            "abort": {
                "enabled": true,
                "http_status": 503,
                "percentage": 10
            }
        });

        let config = PluginConfig {
            id: "http_fault_injection".to_string(),
            phase: "pre_upstream".to_string(),
            config: json_cfg,
        };

        let plugin = HttpFaultInjectionPlugin::from_config(&config).unwrap();
        assert_eq!(plugin.id, "http_fault_injection");
        assert!(plugin.delay.is_some());
        assert!(plugin.abort.is_some());
    }
}
