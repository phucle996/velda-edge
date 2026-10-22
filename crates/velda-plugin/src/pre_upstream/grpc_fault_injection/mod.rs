//! gRPC Fault Injection Plugin (`grpc_fault_injection`).
//!
//! Provides enterprise-grade Chaos Engineering and Microservice Resilience Testing
//! specifically engineered for gRPC RPC traffic over TCP (HTTP/2) and UDP (QUIC).
//!
//! # Architecture & Submodule Organization
//! - [`condition`]: Disjoint interval IP matching, metadata header matching, and RPC service/method target matching.
//! - [`delay`]: Range `[min_ms, max_ms]` delay modeling with ~1ns thread-local PRNG.
//! - [`abort`]: Canonical gRPC status code (`0..=16`) and Trailers-Only response builder.
//! - [`plugin`]: Lean sequential workflow owner (`GrpcFaultInjectionPlugin`).

pub mod abort;
pub mod condition;
pub mod delay;
pub mod plugin;

use velda_sync::PluginConfig;

pub use abort::GrpcAbortConfig;
pub use condition::{GrpcFaultCondition, GrpcTargetMatcher, HeaderMatchRule, IpRangeMatcher};
pub use delay::GrpcDelayConfig;
pub use plugin::GrpcFaultInjectionPlugin;

use crate::error::PluginError;

impl GrpcFaultInjectionPlugin {
    /// Instantiates a new [`GrpcFaultInjectionPlugin`] from compiled [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_grpc_fault_injection_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid grpc_fault_injection configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let delay_cfg = if cfg.delay.enabled && cfg.delay.percentage > 0 {
            Some(
                GrpcDelayConfig::new(
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
                GrpcAbortConfig::new(
                    cfg.abort.percentage as f64,
                    cfg.abort.grpc_status,
                    Some(cfg.abort.grpc_message),
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
            GrpcFaultCondition::default(),
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
    fn test_grpc_from_config_complete() {
        let json_cfg = json!({
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 100,
                "percentage": 20
            },
            "abort": {
                "enabled": true,
                "grpc_status": 14,
                "grpc_message": "service unavailable",
                "percentage": 5
            }
        });

        let config = PluginConfig {
            id: "grpc_fault_injection".to_string(),
            phase: "pre_upstream".to_string(),
            config: json_cfg,
        };

        let plugin = GrpcFaultInjectionPlugin::from_config(&config).unwrap();
        assert_eq!(plugin.id, "grpc_fault_injection");
        assert!(plugin.delay.is_some());
        assert!(plugin.abort.is_some());
    }
}
