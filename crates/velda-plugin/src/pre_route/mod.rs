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

pub mod ip_filter;
pub mod jwt_auth;

pub use ip_filter::{IpFilterMode, IpFilterPlugin};
pub use jwt_auth::JwtAuthPlugin;

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
    match config.plugin_type.as_str() {
        "jwt_auth" | "jwt" => Ok(Arc::new(JwtAuthPlugin::from_config(config)?)),
        "ip_filter" => Ok(Arc::new(IpFilterPlugin::from_config(config)?)),
        other => Err(crate::error::PluginError::Config(format!(
            "Unsupported pre_route plugin type '{other}' for plugin '{}'",
            config.id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::AUTHORIZATION;
    use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_pre_route_jwt_auth_rejects_missing_token() {
        let plugin = Arc::new(JwtAuthPlugin::new("jwt-test", "https://auth.internal"));
        let chain: Vec<Arc<dyn PreRoutePlugin>> = vec![plugin];

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/v1/resource"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let action = run_pre_route_plugins(&chain, &mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            }
            _ => panic!("Expected Action::Respond(401)"),
        }
    }

    #[test]
    fn test_pre_route_jwt_auth_accepts_valid_header() {
        let plugin = Arc::new(JwtAuthPlugin::new("jwt-test", "https://auth.internal"));
        let chain: Vec<Arc<dyn PreRoutePlugin>> = vec![plugin];

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer header.payload.signature"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/v1/resource"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let action = run_pre_route_plugins(&chain, &mut req);
        assert!(matches!(action, Action::Continue));
        assert!(req.headers.contains_key("x-authenticated-user"));
    }
}
