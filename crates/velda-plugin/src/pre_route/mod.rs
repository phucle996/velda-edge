//! Pre-Route Execution Phase Plugins (`phase: "pre_route"`).
//!
//! Evaluated on downstream requests immediately following L7 protocol decoding
//! and before route lookup (e.g., JWT authentication, IP blacklisting, WAF inspection).
//!
//! # Execution Invariants
//! - **Fail-Fast**: Unauthorized or malicious requests must be rejected here to avoid
//!   wasting router trie lookups, body streaming, or upstream connection leasing.
//! - **Bounded Authority**: Returns [`Action::Continue`], [`Action::Respond`] (401/403/429), or [`Action::Reject`].

use std::sync::Arc;
use velda_core::{Action, L7Request};

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod cors_preflight;
pub mod ip_filter;
pub mod resource_limit;
pub mod waf_lite;

pub use cors_preflight::{CorsPreflightPlugin, OriginPattern};
pub use ip_filter::{IpFilterMode, IpFilterPlugin};
pub use resource_limit::{ResourceLimitPlugin, ResourceLimits};
pub use waf_lite::{ThreatVector, WafLitePlugin, WafMode, WafScope};

/// Contract for plugins evaluated prior to route matching.
pub trait PreRoutePlugin: Send + Sync {
    /// Unique identifier of this plugin instance.
    fn id(&self) -> &str;

    /// Evaluated on downstream request head before route lookup.
    fn on_request(&self, req: &mut L7Request) -> Action;

    /// Declares the reverse-permission capabilities required by this plugin.
    fn capability(&self) -> PluginCapability;

    /// Hardcoded network/application protocols supported by this plugin.
    fn supported_protocols(&self) -> SupportedProtocols;

    /// Execution direction. Always [`Direction::Request`] for pre-route plugins.
    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Request
    }
}

/// Executes a linear chain of pre-route plugins.
///
/// Short-circuits immediately on the first non-[`Action::Continue`] result.
#[inline]
pub fn run_pre_route_plugins(chain: &[Arc<dyn PreRoutePlugin>], req: &mut L7Request) -> Action {
    for plugin in chain {
        match plugin.on_request(req) {
            Action::Continue => continue,
            action => return action,
        }
    }
    Action::Continue
}

/// Sub-dispatcher: instantiates concrete [`PreRoutePlugin`] instances for this phase.
///
/// # Open-Closed Principle (OCP)
/// Adding a new pre-route plugin requires adding exactly 1 match arm here, delegating
/// config extraction and validation to the plugin's own `from_config` implementation.
pub fn instantiate(
    config: &velda_sync::post_sync::plugin::PluginConfig,
) -> Result<Arc<dyn PreRoutePlugin>, crate::error::PluginError> {
    match config.id.as_str() {
        "ip_filter" => Ok(Arc::new(IpFilterPlugin::from_config(config)?)),
        "cors_preflight" => Ok(Arc::new(CorsPreflightPlugin::from_config(config)?)),
        "resource_limit" => Ok(Arc::new(ResourceLimitPlugin::from_config(config)?)),
        "waf_lite" => Ok(Arc::new(WafLitePlugin::from_config(config)?)),
        other => Err(crate::error::PluginError::Config(format!(
            "Unsupported pre_route plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, StatusCode, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_pre_route_ip_filter_execution() {
        let config = velda_sync::post_sync::plugin::PluginConfig {
            id: "ip_filter".into(),
            phase: "pre_route".into(),
            config: serde_json::json!({
                "mode": "deny",
                "allow_cidrs": [],
                "deny_cidrs": ["192.168.1.100/32"],
                "client_ip_source": "x_forwarded_for"
            }),
        };

        let plugin = instantiate(&config).expect("instantiate ip_filter");
        let chain: Vec<Arc<dyn PreRoutePlugin>> = vec![plugin];

        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "192.168.1.100".parse().unwrap());

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/v1/resource"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let action = run_pre_route_plugins(&chain, &mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::FORBIDDEN);
            }
            _ => panic!("Expected Action::Respond(403)"),
        }
    }
}
