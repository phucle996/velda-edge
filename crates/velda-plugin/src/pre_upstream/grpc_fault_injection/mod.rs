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

use http::header::HeaderName;
use serde::Deserialize;
use velda_sync::post_sync::plugin::PluginConfig;

pub use abort::GrpcAbortConfig;
pub use condition::{GrpcFaultCondition, GrpcTargetMatcher, HeaderMatchRule, IpRangeMatcher};
pub use delay::GrpcDelayConfig;
pub use plugin::GrpcFaultInjectionPlugin;

use crate::error::PluginError;

#[derive(Debug, Deserialize, Default)]
struct RawPluginConfig {
    #[serde(default)]
    delay: Option<RawDelayConfig>,
    #[serde(default)]
    abort: Option<RawAbortConfig>,
    #[serde(default)]
    condition: Option<RawConditionConfig>,
}

#[derive(Debug, Deserialize)]
struct RawDelayConfig {
    #[serde(default)]
    percentage: f64,
    #[serde(default)]
    range_ms: Option<[u64; 2]>,
    #[serde(default)]
    fixed_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawAbortConfig {
    #[serde(default)]
    percentage: f64,
    #[serde(default = "default_abort_status")]
    grpc_status: u32,
    #[serde(default)]
    message: Option<String>,
}

fn default_abort_status() -> u32 {
    14 // UNAVAILABLE
}

#[derive(Debug, Deserialize, Default)]
struct RawConditionConfig {
    #[serde(default)]
    client_ips: Option<Vec<String>>,
    #[serde(default)]
    header_match: Option<RawHeaderMatch>,
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    allow_header_override: bool,
    #[serde(default = "default_max_delay_override")]
    max_override_delay_ms: u64,
}

fn default_max_delay_override() -> u64 {
    60_000
}

#[derive(Debug, Deserialize)]
struct RawHeaderMatch {
    name: String,
    #[serde(default)]
    value: Option<String>,
}

impl GrpcFaultInjectionPlugin {
    /// Instantiates a new [`GrpcFaultInjectionPlugin`] from compiled [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let raw: RawPluginConfig = serde_json::from_value(config.config.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Invalid grpc_fault_injection configuration for '{}': {e}",
                config.id
            ))
        })?;

        // 1. Compile Delay configuration
        let delay_cfg = if let Some(d) = raw.delay {
            let (min_ms, max_ms) = if let Some([min, max]) = d.range_ms {
                (min, max)
            } else if let Some(fixed) = d.fixed_ms {
                (fixed, fixed)
            } else {
                return Err(PluginError::Config(format!(
                    "Plugin '{}': delay must specify either 'range_ms: [min, max]' or 'fixed_ms'",
                    config.id
                )));
            };

            Some(
                GrpcDelayConfig::new(d.percentage, min_ms, max_ms).map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': delay error: {e}", config.id))
                })?,
            )
        } else {
            None
        };

        // 2. Compile Abort configuration
        let abort_cfg = if let Some(a) = raw.abort {
            Some(
                GrpcAbortConfig::new(a.percentage, a.grpc_status, a.message).map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': abort error: {e}", config.id))
                })?,
            )
        } else {
            None
        };

        // 3. Compile Condition configuration
        let condition_cfg = if let Some(c) = raw.condition {
            let ip_matcher = if let Some(cidrs) = c.client_ips {
                if !cidrs.is_empty() {
                    Some(IpRangeMatcher::from_cidrs(&cidrs).map_err(|e| {
                        PluginError::Config(format!(
                            "Plugin '{}': invalid client_ips: {e}",
                            config.id
                        ))
                    })?)
                } else {
                    None
                }
            } else {
                None
            };

            let header_match = if let Some(hm) = c.header_match {
                let name = HeaderName::try_from(hm.name.as_str()).map_err(|e| {
                    PluginError::Config(format!(
                        "Plugin '{}': invalid condition header name '{}': {e}",
                        config.id, hm.name
                    ))
                })?;
                match hm.value {
                    Some(val) => Some(HeaderMatchRule::Exact(name, val)),
                    None => Some(HeaderMatchRule::Present(name)),
                }
            } else {
                None
            };

            let target_matcher = if let Some(method_path) = c.method {
                Some(GrpcTargetMatcher::ExactPath(method_path))
            } else {
                c.service.map(GrpcTargetMatcher::ServicePrefix)
            };

            GrpcFaultCondition {
                ip_matcher,
                header_match,
                target_matcher,
                allow_header_override: c.allow_header_override,
                max_override_delay_ms: c.max_override_delay_ms,
            }
        } else {
            GrpcFaultCondition::default()
        };

        Ok(Self::new(&config.id, condition_cfg, delay_cfg, abort_cfg))
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
                "percentage": 20.0,
                "range_ms": [50, 200]
            },
            "abort": {
                "percentage": 5.0,
                "grpc_status": 14,
                "message": "service unavailable"
            },
            "condition": {
                "client_ips": ["10.0.0.0/8"],
                "service": "/order.v1.OrderService/",
                "allow_header_override": true,
                "max_override_delay_ms": 20000
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
        assert!(plugin.condition.ip_matcher.is_some());
        assert!(plugin.condition.target_matcher.is_some());
        assert!(plugin.condition.allow_header_override);
    }
}
