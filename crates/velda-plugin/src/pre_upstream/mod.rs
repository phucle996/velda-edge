//! Pre-Upstream Execution Phase Plugins (`phase: "pre_upstream"`).
//!
//! Evaluated after route and endpoint selection, immediately before forwarding the
//! request to the target backend (e.g., token injection, request header mutation, upstream rate limiting).
//!
//! # Execution Invariants
//! - **Bounded Authority**: Returns [`Action::Continue`], [`Action::Respond`] (e.g. 429), or [`Action::Reject`].
//! - **Target Context Available**: The router has already resolved the target upstream pool and endpoint.

use std::sync::Arc;
use velda_core::{Action, L7Request};

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod header_mutation;
pub mod rate_limit;

pub use header_mutation::HeaderMutationPlugin;
pub use rate_limit::RateLimitPlugin;

/// Contract for plugins evaluated prior to forwarding to upstream backend.
pub trait PreUpstreamPlugin: Send + Sync {
    /// Unique identifier of this plugin instance.
    fn id(&self) -> &str;

    /// Evaluated on routed request before sending to upstream backend.
    fn on_upstream_request(&self, req: &mut L7Request) -> Action;

    /// Declares the reverse-permission capabilities required by this plugin.
    fn capability(&self) -> PluginCapability;

    /// Hardcoded network/application protocols supported by this plugin.
    fn supported_protocols(&self) -> SupportedProtocols;

    /// Execution direction. Always [`Direction::Request`] for pre-upstream plugins.
    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Request
    }
}

/// Executes a linear chain of pre-upstream plugins.
///
/// Short-circuits immediately on the first non-[`Action::Continue`] result.
#[inline]
pub fn run_pre_upstream_plugins(
    chain: &[Arc<dyn PreUpstreamPlugin>],
    req: &mut L7Request,
) -> Action {
    for plugin in chain {
        match plugin.on_upstream_request(req) {
            Action::Continue => continue,
            action => return action,
        }
    }
    Action::Continue
}

/// Sub-dispatcher: instantiates concrete [`PreUpstreamPlugin`] instances for this phase.
///
/// # Open-Closed Principle (OCP)
/// Adding a new pre-upstream plugin requires adding exactly 1 match arm here, delegating
/// config extraction and validation to the plugin's own `from_config` implementation.
pub fn instantiate(
    config: &velda_sync::post_sync::plugin::PluginConfig,
) -> Result<Arc<dyn PreUpstreamPlugin>, crate::error::PluginError> {
    match config.plugin_type.as_str() {
        "rate_limit" => Ok(Arc::new(RateLimitPlugin::from_config(config)?)),
        "header_mutation" => Ok(Arc::new(HeaderMutationPlugin::from_config(config)?)),
        other => Err(crate::error::PluginError::Config(format!(
            "Unsupported pre_upstream plugin type '{other}' for plugin '{}'",
            config.id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderName;
    use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_pre_upstream_header_mutation() {
        let set_hdrs = vec![(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("api.example.com"),
        )];
        let remove_hdrs = vec![HeaderName::from_static("x-internal-secret")];

        let plugin = Arc::new(HeaderMutationPlugin::new(
            "mutation-1",
            set_hdrs,
            remove_hdrs,
        ));
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-internal-secret"),
            HeaderValue::from_static("classified"),
        );

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/v1/payments"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let action = run_pre_upstream_plugins(&chain, &mut req);
        assert!(matches!(action, Action::Continue));
        assert!(!req.headers.contains_key("x-internal-secret"));
        assert_eq!(
            req.headers.get("x-forwarded-host").unwrap(),
            "api.example.com"
        );
    }

    #[test]
    fn test_pre_upstream_rate_limiting_exhaustion() {
        // Limit to 1 request per second
        let plugin = Arc::new(RateLimitPlugin::new("rate-limit-1", 1));
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        let mut req1 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/items"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        let action1 = run_pre_upstream_plugins(&chain, &mut req1);
        assert!(matches!(action1, Action::Continue));

        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/items"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        let action2 = run_pre_upstream_plugins(&chain, &mut req2);
        match action2 {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(resp.headers.get("retry-after").unwrap(), "1");
            }
            _ => panic!("Expected Action::Respond(429)"),
        }
    }
}
