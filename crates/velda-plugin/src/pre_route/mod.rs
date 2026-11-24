//! Pre-Route Execution Stage Plugins (`PreRoutePlugin`).
//!
//! Evaluated on downstream requests immediately following L7 protocol decoding
//! and before route lookup (e.g., JWT authentication, IP blacklisting, WAF inspection).
//!
//! # Execution Invariants
//! - **Fail-Fast**: Unauthorized or malicious requests must be rejected here to avoid
//!   wasting router table lookups, body streaming, or upstream connection leasing.
//! - **Bounded Authority**: Returns [`Action::Continue`], [`Action::Respond`] (401/403/429), or [`Action::Reject`].

use std::sync::Arc;
use velda_core::{Action, L7Request};

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod cors_preflight;
pub mod https_redirect;
pub mod ip_filter;
pub mod method_filter;
pub mod resource_limit;
pub mod waf_lite;

pub use cors_preflight::{CorsPreflightPlugin, OriginPattern};
pub use https_redirect::{HttpsRedirectConfig, HttpsRedirectPlugin};
pub use ip_filter::{IpFilterMode, IpFilterPlugin};
pub use method_filter::{MethodFilterConfig, MethodFilterPlugin};
pub use resource_limit::{ResourceLimitPlugin, ResourceLimits};
pub use waf_lite::{WafLiteConfig, WafLitePlugin};

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

    /// Plugin Background Workload Weight (`0` = hot-path only, `1..=100` = shadow daemon weight).
    fn pbww(&self) -> u32;

    /// Starts optional background workload daemon on the assigned Shadow Worker (`pbww() > 0`).
    #[inline]
    fn run_background(
        self: Arc<Self>,
        _shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<crate::BackgroundTaskFuture> {
        None
    }
}

/// Executes a linear chain of pre-route plugins.
///
/// Short-circuits immediately on the first non-[`Action::Continue`] result and batch-renders
/// any pending arithmetic header mutations into `req.headers`.
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

/// Compile-time registry check for plugins owned by the `pre_route` stage.
#[inline]
pub fn owns_id(id: &str) -> bool {
    matches!(
        id,
        "ip_filter"
            | "cors_preflight"
            | "resource_limit"
            | "waf_lite"
            | "method_filter"
            | "https_redirect"
    )
}

/// Sub-dispatcher: instantiates concrete [`PreRoutePlugin`] instances for this stage.
///
/// # Open-Closed Principle (OCP)
/// Adding a new pre-route plugin requires adding its ID to [`owns_id`] and 1 match arm here,
/// delegating config extraction and validation to the plugin's own `from_config` implementation.
pub fn instantiate(
    config: &crate::config::PluginConfig,
) -> Result<Arc<dyn PreRoutePlugin>, crate::error::PluginError> {
    match config.id.as_str() {
        "ip_filter" => Ok(Arc::new(IpFilterPlugin::from_config(config)?)),
        "cors_preflight" => Ok(Arc::new(CorsPreflightPlugin::from_config(config)?)),
        "resource_limit" => Ok(Arc::new(ResourceLimitPlugin::from_config(config)?)),
        "waf_lite" => Ok(Arc::new(WafLitePlugin::from_config(config)?)),
        "method_filter" => Ok(Arc::new(MethodFilterPlugin::from_config(config)?)),
        "https_redirect" => Ok(Arc::new(HttpsRedirectPlugin::from_config(config)?)),
        other => Err(crate::error::PluginError::Config(format!(
            "Unsupported pre_route plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_pre_route_ip_filter_execution() {
        let config = crate::config::PluginConfig {
            id: "ip_filter".into(),
            config: serde_json::json!({
                "mode": "deny",
                "allow_cidrs": [],
                "deny_cidrs": ["192.168.1.100/32"],
                "client_ip_source": "x_forwarded_for"
            }),
        };

        let plugin = instantiate(&config).expect("instantiate ip_filter");
        let chain: Vec<Arc<dyn PreRoutePlugin>> = vec![plugin];

        let mut headers = velda_core::HeaderWorkspace::empty(2);
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
            Action::Reject(reason) => {
                assert_eq!(reason, velda_core::RejectReason::IpBlocked);
            }
            _ => panic!("Expected Action::Reject(IpBlocked)"),
        }
    }
}
