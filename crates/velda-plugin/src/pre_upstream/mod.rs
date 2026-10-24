//! Pre-Upstream Execution Phase Plugins (`phase: "pre_upstream"`).
//!
//! Evaluated after route and endpoint selection, immediately before forwarding the
//! request to the target backend (e.g., token injection, request header mutation, upstream rate limiting).
//!
//! # Execution Invariants
//! - **Bounded Authority**: Returns [`Action::Continue`], [`Action::Respond`] (e.g. 429), or [`Action::Reject`].
//! - **Target Context Available**: The router has already resolved the target upstream pool and endpoint.

pub use std::sync::Arc;
use velda_core::{Action, L7Request};

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod api_key_auth;
pub mod canary;
pub mod csrf;
pub mod grpc_fault_injection;
pub mod header_mutation;
pub mod hmac_auth;
pub mod http_cache_lookup;
pub mod http_fault_injection;
pub mod jwt_auth;
pub mod oauth2_introspection;
pub mod rate_limit;
pub mod request_mirror;
pub mod request_termination;
pub mod url_rewrite;
pub mod w3c_trace_context;

pub use api_key_auth::{ApiKeyAuthPlugin, ApiKeyCache, ApiKeyEntry, KeySource};
pub use canary::{CanaryConfig, CanaryPlugin, HashSource, HashStrategy, HeaderRule, RuleOperator};
pub use csrf::{CsrfConfig, CsrfPlugin};
pub use grpc_fault_injection::{GrpcAbortConfig, GrpcDelayConfig, GrpcFaultInjectionPlugin};
pub use header_mutation::{
    DynamicVar, HeaderMutationConfig, HeaderMutationPlugin, HeaderValueSpec,
};
pub use hmac_auth::{HmacAuthConfig, HmacAuthPlugin, HmacCredentialConfig};
pub use http_cache_lookup::{HttpCacheLookupConfig, HttpCacheLookupPlugin};
pub use http_fault_injection::{
    HeaderMatchRule, HttpAbortConfig, HttpDelayConfig, HttpFaultCondition,
    HttpFaultInjectionPlugin, IpRangeMatcher,
};
pub use jwt_auth::{JwtAlgorithm, JwtAuthPlugin, TokenSource};
pub use oauth2_introspection::{
    ClaimToHeaderConfig, IdpKeepAliveClient, IntrospectionCache, Oauth2IntrospectionConfig,
    Oauth2IntrospectionPlugin,
};
pub use rate_limit::{
    FallbackBehavior, KeyStrategy, RateLimitAlgorithm, RateLimitBackend, RateLimitConfig,
    RateLimitPlugin, TlsSource,
};
pub use request_mirror::{
    MirroredRequest, RequestMirrorConfig, RequestMirrorPlugin, get_mirror_sender, init_mirror_queue,
};
pub use request_termination::{RequestTerminationConfig, RequestTerminationPlugin};
pub use url_rewrite::{
    CompiledPathStrategy, PathRewriteStrategy, QueryStrategy, ReplacementToken, UrlRewriteConfig,
    UrlRewritePlugin,
};
pub use w3c_trace_context::{W3cTraceContextConfig, W3cTraceContextPlugin};

/// Contract for plugins evaluated prior to forwarding to upstream backend.
pub trait PreUpstreamPlugin: Send + Sync {
    /// Unique identifier of this plugin instance.
    fn id(&self) -> &str;

    /// Evaluated on routed request before sending to upstream backend.
    ///
    /// Allows mutating the request, short-circuiting, or dynamically re-routing to an
    /// alternate target upstream (e.g. Canary traffic splitting) by mutating `target_upstream`.
    fn on_upstream_request(&self, req: &mut L7Request, target_upstream: &mut Arc<str>) -> Action;

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
    target_upstream: &mut Arc<str>,
) -> Action {
    for (idx, plugin) in chain.iter().enumerate() {
        match plugin.on_upstream_request(req, target_upstream) {
            Action::Continue => continue,
            Action::Delay { duration, and_then } => {
                if and_then.is_continue() {
                    let next = run_pre_upstream_plugins(&chain[idx + 1..], req, target_upstream);
                    return Action::delay(duration, next);
                } else {
                    return Action::Delay { duration, and_then };
                }
            }
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
    match config.id.as_str() {
        "api_key_auth" => Ok(Arc::new(ApiKeyAuthPlugin::from_config(config)?)),
        "jwt_auth" => Ok(Arc::new(JwtAuthPlugin::from_config(config)?)),
        "rate_limit" => Ok(Arc::new(RateLimitPlugin::from_config(config)?)),
        "header_mutation" => Ok(Arc::new(HeaderMutationPlugin::from_config(config)?)),
        "canary" => Ok(Arc::new(CanaryPlugin::from_config(config)?)),
        "request_mirror" => Ok(Arc::new(RequestMirrorPlugin::from_config(config)?)),
        "url_rewrite" => Ok(Arc::new(UrlRewritePlugin::from_config(config)?)),
        "http_cache_lookup" => Ok(Arc::new(HttpCacheLookupPlugin::from_config(config)?)),
        "http_fault_injection" => Ok(Arc::new(HttpFaultInjectionPlugin::from_config(config)?)),
        "grpc_fault_injection" => Ok(Arc::new(GrpcFaultInjectionPlugin::from_config(config)?)),
        "request_termination" => Ok(Arc::new(RequestTerminationPlugin::from_config(config)?)),
        "w3c_trace_context" => Ok(Arc::new(W3cTraceContextPlugin::from_config(config)?)),
        "csrf" | "csrf_protection" => Ok(Arc::new(CsrfPlugin::from_config(config)?)),
        "hmac_auth" => Ok(Arc::new(HmacAuthPlugin::from_config(config)?)),
        "oauth2_introspection" => Ok(Arc::new(Oauth2IntrospectionPlugin::from_config(config)?)),
        other => Err(crate::error::PluginError::Config(format!(
            "Unsupported pre_upstream plugin '{other}' for plugin '{}'",
            config.id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderName;
    use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
    use velda_core::{Body, RejectReason};

    #[test]
    fn test_pre_upstream_header_mutation() {
        let set_hdrs = vec![(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("api.example.com"),
        )];
        let remove_hdrs = vec![HeaderName::from_static("x-internal-secret")];

        let config = HeaderMutationConfig {
            strip_hop_by_hop: true,
            remove: remove_hdrs,
            rename: Vec::new(),
            set: set_hdrs
                .into_iter()
                .map(|(k, v)| (k, HeaderValueSpec::Static(v)))
                .collect(),
            append: Vec::new(),
        };
        let plugin = Arc::new(HeaderMutationPlugin::new("mutation-1", config));
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

        let mut target = Arc::from("default-upstream");
        let action = run_pre_upstream_plugins(&chain, &mut req, &mut target);
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
        let mut target1 = Arc::from("default-upstream");
        let action1 = run_pre_upstream_plugins(&chain, &mut req1, &mut target1);
        assert!(matches!(action1, Action::Continue));

        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/items"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );
        let mut target2 = Arc::from("default-upstream");
        let action2 = run_pre_upstream_plugins(&chain, &mut req2, &mut target2);
        match action2 {
            Action::Reject(reason) => {
                assert_eq!(reason, RejectReason::TooManyRequests);
                assert_eq!(req2.headers.get("retry-after").unwrap(), "1");
                assert_eq!(req2.headers.get("x-ratelimit-remaining").unwrap(), "0");
                assert!(req2.headers.has_bypass_headers());
            }
            _ => panic!("Expected Action::Reject(TooManyRequests)"),
        }
    }

    #[test]
    fn test_pre_upstream_url_rewrite() {
        let plugin = Arc::new(
            UrlRewritePlugin::new(
                "rewrite-chain",
                UrlRewriteConfig {
                    path: Some(PathRewriteStrategy::StripPrefix {
                        prefix: "/api/v1".into(),
                    }),
                    query: QueryStrategy::Append {
                        query: "cluster=primary".into(),
                    },
                    sanitize_path: true,
                },
            )
            .unwrap(),
        );
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/v1/customers/100?tab=overview"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = run_pre_upstream_plugins(&chain, &mut req, &mut target);
        assert!(matches!(action, Action::Continue));
        assert_eq!(req.uri.path(), "/customers/100");
        assert_eq!(req.uri.query(), Some("tab=overview&cluster=primary"));
    }

    #[test]
    fn test_pre_upstream_request_termination() {
        let plugin = Arc::new(
            RequestTerminationPlugin::new(
                "termination-1",
                RequestTerminationConfig {
                    status_code: 503,
                    headers: vec![(
                        HeaderName::from_static("retry-after"),
                        HeaderValue::from_static("60"),
                    )],
                    body: None,
                },
            )
            .unwrap(),
        );
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_static("req-term-999"),
        );

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/api/v1/checkout"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target = Arc::from("default-upstream");
        let action = run_pre_upstream_plugins(&chain, &mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(resp.headers.get("retry-after").unwrap(), "60");
                assert_eq!(resp.headers.get("x-request-id").unwrap(), "req-term-999");
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "text/plain; charset=utf-8"
                );
            }
            _ => panic!("Expected Action::Respond(503)"),
        }
    }
}
