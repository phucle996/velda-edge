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

use http::header::{HeaderName, HeaderValue};
use serde::Deserialize;
use std::collections::HashMap;
use velda_sync::post_sync::plugin::PluginConfig;

pub use abort::HttpAbortConfig;
pub use condition::{HeaderMatchRule, HttpFaultCondition, IpRangeMatcher};
pub use delay::HttpDelayConfig;
pub use plugin::HttpFaultInjectionPlugin;

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
    http_status: u16,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

fn default_abort_status() -> u16 {
    503
}

#[derive(Debug, Deserialize, Default)]
struct RawConditionConfig {
    #[serde(default)]
    client_ips: Option<Vec<String>>,
    #[serde(default)]
    header_match: Option<RawHeaderMatch>,
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

impl HttpFaultInjectionPlugin {
    /// Instantiates a new [`HttpFaultInjectionPlugin`] from compiled [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let raw: RawPluginConfig = serde_json::from_value(config.config.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Invalid http_fault_injection configuration for '{}': {e}",
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
                HttpDelayConfig::new(d.percentage, min_ms, max_ms).map_err(|e| {
                    PluginError::Config(format!("Plugin '{}': delay error: {e}", config.id))
                })?,
            )
        } else {
            None
        };

        // 2. Compile Abort configuration
        let abort_cfg = if let Some(a) = raw.abort {
            let custom_headers = if let Some(map) = a.headers {
                let mut list = Vec::with_capacity(map.len());
                for (k, v) in map {
                    let name = HeaderName::try_from(k.as_str()).map_err(|e| {
                        PluginError::Config(format!(
                            "Plugin '{}': invalid abort header name '{k}': {e}",
                            config.id
                        ))
                    })?;
                    let val = HeaderValue::try_from(v.as_str()).map_err(|e| {
                        PluginError::Config(format!(
                            "Plugin '{}': invalid abort header value '{v}': {e}",
                            config.id
                        ))
                    })?;
                    list.push((name, val));
                }
                Some(list)
            } else {
                None
            };

            Some(
                HttpAbortConfig::new(a.percentage, a.http_status, a.body, custom_headers).map_err(
                    |e| PluginError::Config(format!("Plugin '{}': abort error: {e}", config.id)),
                )?,
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

            HttpFaultCondition {
                ip_matcher,
                header_match,
                allow_header_override: c.allow_header_override,
                max_override_delay_ms: c.max_override_delay_ms,
            }
        } else {
            HttpFaultCondition::default()
        };

        Ok(Self::new(&config.id, condition_cfg, delay_cfg, abort_cfg))
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
                "percentage": 15.0,
                "range_ms": [100, 400]
            },
            "abort": {
                "percentage": 10.0,
                "http_status": 503,
                "body": "service unavailable\n",
                "headers": {
                    "x-chaos": "true"
                }
            },
            "condition": {
                "client_ips": ["10.0.0.0/8"],
                "header_match": {
                    "name": "x-qa-role",
                    "value": "tester"
                },
                "allow_header_override": true,
                "max_override_delay_ms": 30000
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
        assert!(plugin.condition.ip_matcher.is_some());
        assert!(plugin.condition.allow_header_override);
    }
}
