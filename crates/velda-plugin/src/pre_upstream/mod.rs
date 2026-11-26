//! Pre-Upstream Execution Stage Plugins (`PreUpstreamPlugin`).
//!
//! Evaluated after route and endpoint selection, immediately before forwarding the
//! request to the target backend (e.g., token injection, request header mutation, upstream rate limiting).
//!
//! # Execution Invariants
//! - **Bounded Authority**: Returns [`Action::Continue`], [`Action::Respond`] (e.g. 429), or [`Action::Reject`].
//! - **Target Context Available**: The router has already resolved the target upstream pool and endpoint.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use velda_core::{Action, L7Request};

use crate::capability::{Direction, PluginCapability, SupportedProtocols};

pub mod api_key_auth;
pub mod api_metering;
pub mod canary;
pub mod csrf;
pub mod grpc_fault_injection;
pub mod header_mutation;
pub mod hmac_auth;
pub mod http_cache_lookup;
pub mod http_fault_injection;
pub mod ja4_filter;
pub mod jwt_auth;
pub mod oauth2_introspection;
pub mod rate_limit;
pub mod request_mirror;
pub mod request_termination;
pub mod targets;
pub mod url_rewrite;

pub use api_key_auth::{ApiKeyAuthConfig, ApiKeyAuthPlugin, ApiKeyEntryConfig};
pub use api_metering::{
    AccountState, ApiMeteringConfig, ApiMeteringPlugin, ExhaustedAction, MICRO_CREDITS_PER_UNIT,
    MeteringDecision, QuotaAlgorithm, QuotaWindowConfig, ShardedMeteringState, TierConfig,
};
pub use canary::{CanaryConfig, CanaryPlugin, HeaderRule, RuleOperator};
pub use csrf::{CsrfConfig, CsrfPlugin};
pub use grpc_fault_injection::{GrpcAbortConfig, GrpcDelayConfig, GrpcFaultInjectionPlugin};
pub use header_mutation::{HeaderMutationConfig, HeaderMutationPlugin};
pub use hmac_auth::{HmacAuthConfig, HmacAuthPlugin, HmacCredentialConfig};
pub use http_cache_lookup::{HttpCacheLookupConfig, HttpCacheLookupPlugin};
pub use http_fault_injection::{
    HeaderMatchRule, HttpAbortConfig, HttpDelayConfig, HttpFaultCondition,
    HttpFaultInjectionPlugin, IpRangeMatcher,
};
pub use ja4_filter::{Ja4FilterConfig, Ja4FilterPlugin};
pub use jwt_auth::{JwtAlgorithm, JwtAuthPlugin, TokenSource};
pub use oauth2_introspection::{
    ClaimToHeaderConfig, IntrospectionCache, Oauth2IntrospectionConfig, Oauth2IntrospectionPlugin,
};
pub use rate_limit::{
    FallbackBehavior, KeyStrategy, RateLimitAlgorithm, RateLimitConfig, RateLimitPlugin,
};
pub use request_mirror::{RequestMirrorConfig, RequestMirrorPlugin};
pub use request_termination::{RequestTerminationConfig, RequestTerminationPlugin};
pub use targets::{
    TARGET_UNASSIGNED, TARGETS_CANCELLED, target_cancel_all, target_init, target_mirror,
    target_pair, target_primary, target_set_mirror, target_set_primary,
};
pub use url_rewrite::{PathRewriteRule, QueryRewriteMode, UrlRewriteConfig, UrlRewritePlugin};

/// Contract for plugins evaluated prior to forwarding to upstream backend.
pub trait PreUpstreamPlugin: Send + Sync {
    /// Unique identifier of this plugin instance.
    fn id(&self) -> &str;

    /// Synchronous evaluation used by pure-CPU pre-upstream plugins.
    ///
    /// `targets` is a bit-packed `u32` routing word: low 16 bits hold the primary
    /// upstream index and high 16 bits hold the optional mirror upstream index (`0xFFFF` = unassigned).
    fn on_upstream_request(&self, req: &mut L7Request, targets: &mut u32) -> Action;

    /// Non-blocking hot-path evaluation invoked first by async downstream pipelines.
    ///
    /// Plugins whose warm cache hit path is synchronous return `Some(action)` with zero future
    /// overhead; on a cold network miss requiring async I/O (e.g. `oauth2_introspection`),
    /// returning `None` hands off to [`on_upstream_request_async`].
    #[inline(always)]
    fn try_on_upstream_request(&self, req: &mut L7Request, targets: &mut u32) -> Option<Action> {
        Some(self.on_upstream_request(req, targets))
    }

    /// Asynchronous cold-path evaluation invoked when `try_on_upstream_request` returns `None`.
    fn on_upstream_request_async<'a>(
        &'a self,
        req: &'a mut L7Request,
        targets: &'a mut u32,
    ) -> Pin<Box<dyn Future<Output = Action> + Send + 'a>> {
        let action = self.on_upstream_request(req, targets);
        Box::pin(async move { action })
    }

    /// Declares the reverse-permission capabilities required by this plugin.
    fn capability(&self) -> PluginCapability;

    /// Hardcoded network/application protocols supported by this plugin.
    fn supported_protocols(&self) -> SupportedProtocols;

    /// Execution direction. Always [`Direction::Request`] for pre-upstream plugins.
    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Request
    }

    /// Plugin Background Workload Weight (`0` = hot-path only, `1..=100` = shadow daemon weight).
    fn pbww(&self) -> u32;

    /// Optional `etp_id` referencing an external third-party pool in `EtpRegistry`.
    #[inline]
    fn etp_id(&self) -> Option<&str> {
        None
    }

    /// Attaches the resolved [`velda_etp::EtpInstance`] from `EtpRegistry` before
    /// launching background workers.
    #[inline]
    fn attach_etp(
        &self,
        _instance: &velda_etp::EtpInstance,
    ) -> Result<(), crate::error::PluginError> {
        Ok(())
    }

    /// Starts optional background workload daemon on the assigned Shadow Worker (`pbww() > 0`).
    #[inline]
    fn run_background(
        self: Arc<Self>,
        _shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<crate::BackgroundTaskFuture> {
        None
    }
}

/// Executes a linear chain of pre-upstream plugins synchronously (for pure-CPU tests & callers).
#[inline]
pub fn run_pre_upstream_plugins(
    chain: &[Arc<dyn PreUpstreamPlugin>],
    req: &mut L7Request,
    targets: &mut u32,
) -> Action {
    for (idx, plugin) in chain.iter().enumerate() {
        match plugin.on_upstream_request(req, targets) {
            Action::Continue => continue,
            Action::Delay { duration, and_then } => {
                if and_then.is_continue() {
                    let next = run_pre_upstream_plugins(&chain[idx + 1..], req, targets);
                    return Action::delay(duration, next);
                } else {
                    target_cancel_all(targets);
                    return Action::Delay { duration, and_then };
                }
            }
            action => {
                target_cancel_all(targets);
                return action;
            }
        }
    }
    Action::Continue
}

/// Executes a linear chain of pre-upstream plugins across warm sync and cold `.await` paths.
///
/// Warm cache hits execute synchronously via `try_on_upstream_request` with zero `Future` pinning;
/// only plugins returning `None` (cold cache miss requiring async IdP I/O) yield via `.await`.
/// Automatically clears both primary and mirror halves of `targets` (`0xFFFF_FFFF`) whenever
/// any plugin short-circuits with `Action::Reject` or `Action::Respond`.
#[inline]
pub async fn run_pre_upstream_plugins_async(
    chain: &[Arc<dyn PreUpstreamPlugin>],
    req: &mut L7Request,
    targets: &mut u32,
) -> Action {
    for plugin in chain {
        let res = match plugin.try_on_upstream_request(req, targets) {
            Some(action) => action,
            None => plugin.on_upstream_request_async(req, targets).await,
        };
        match res {
            Action::Continue => continue,
            Action::Delay { duration, and_then } => {
                tokio::time::sleep(duration).await;
                match *and_then {
                    Action::Continue => continue,
                    other => {
                        target_cancel_all(targets);
                        return other;
                    }
                }
            }
            action => {
                target_cancel_all(targets);
                return action;
            }
        }
    }
    Action::Continue
}

/// Compile-time registry check for plugins owned by the `pre_upstream` stage.
#[inline]
pub fn owns_id(id: &str) -> bool {
    matches!(
        id,
        "api_key_auth"
            | "jwt_auth"
            | "rate_limit"
            | "header_mutation"
            | "canary"
            | "request_mirror"
            | "url_rewrite"
            | "http_cache_lookup"
            | "http_fault_injection"
            | "grpc_fault_injection"
            | "request_termination"
            | "csrf"
            | "csrf_protection"
            | "hmac_auth"
            | "oauth2_introspection"
            | "api_metering"
            | "ja4_filter"
    )
}

/// Sub-dispatcher: instantiates concrete [`PreUpstreamPlugin`] instances for this stage.
///
/// # Open-Closed Principle (OCP)
/// Adding a new pre-upstream plugin requires adding its ID to [`owns_id`] and 1 match arm here,
/// delegating config extraction and validation to the plugin's own `from_config` implementation.
pub fn instantiate(
    config: &crate::config::PluginConfig,
    upstream_ids: &[&str],
) -> Result<Arc<dyn PreUpstreamPlugin>, crate::error::PluginError> {
    match config.id.as_str() {
        "api_key_auth" => Ok(Arc::new(ApiKeyAuthPlugin::from_config(config)?)),
        "jwt_auth" => Ok(Arc::new(JwtAuthPlugin::from_config(config)?)),
        "rate_limit" => Ok(Arc::new(RateLimitPlugin::from_config(config)?)),
        "header_mutation" => Ok(Arc::new(HeaderMutationPlugin::from_config(config)?)),
        "canary" => Ok(Arc::new(CanaryPlugin::from_config(config, upstream_ids)?)),
        "request_mirror" => Ok(Arc::new(RequestMirrorPlugin::from_config(
            config,
            upstream_ids,
        )?)),
        "url_rewrite" => Ok(Arc::new(UrlRewritePlugin::from_config(config)?)),
        "http_cache_lookup" => Ok(Arc::new(HttpCacheLookupPlugin::from_config(config)?)),
        "http_fault_injection" => Ok(Arc::new(HttpFaultInjectionPlugin::from_config(config)?)),
        "grpc_fault_injection" => Ok(Arc::new(GrpcFaultInjectionPlugin::from_config(config)?)),
        "request_termination" => Ok(Arc::new(RequestTerminationPlugin::from_config(config)?)),
        "csrf" | "csrf_protection" => Ok(Arc::new(CsrfPlugin::from_config(config)?)),
        "hmac_auth" => Ok(Arc::new(HmacAuthPlugin::from_config(config)?)),
        "oauth2_introspection" => Ok(Arc::new(Oauth2IntrospectionPlugin::from_config(config)?)),
        "api_metering" => Ok(Arc::new(ApiMeteringPlugin::from_config(config)?)),
        "ja4_filter" => Ok(Arc::new(Ja4FilterPlugin::from_config(config)?)),
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
    use http::{HeaderValue, Method, StatusCode, Uri, Version};
    use velda_core::{Body, HeaderWorkspace, RejectReason};

    #[test]
    fn test_pre_upstream_header_mutation() {
        let set_hdrs = vec![(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("api.example.com"),
        )];
        let remove_hdrs = vec![HeaderName::from_static("x-internal-secret")];

        let config = HeaderMutationConfig {
            remove: remove_hdrs,
            set: set_hdrs,
            append: Vec::new(),
        };
        let plugin = Arc::new(HeaderMutationPlugin::new("mutation-1", config));
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        let mut headers = HeaderWorkspace::empty(4);
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

        let mut target = target_init(1);
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
            HeaderWorkspace::empty(4),
            Body::Empty,
        );
        let mut target1 = target_init(1);
        let action1 = run_pre_upstream_plugins(&chain, &mut req1, &mut target1);
        assert!(matches!(action1, Action::Continue));

        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/items"),
            Version::HTTP_11,
            HeaderWorkspace::empty(4),
            Body::Empty,
        );
        let mut target2 = target_init(1);
        let action2 = run_pre_upstream_plugins(&chain, &mut req2, &mut target2);
        assert_eq!(target2, TARGETS_CANCELLED);
        match action2 {
            Action::Reject(reason) => {
                assert_eq!(reason, RejectReason::TooManyRequests);
                assert_eq!(req2.headers.get("retry-after").unwrap(), "1");
                assert_eq!(req2.headers.get("x-ratelimit-remaining").unwrap(), "0");
                assert!(req2.headers.bypass_headers().count() >= 2);
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
                    path_rules: vec![PathRewriteRule {
                        find: "/api/v1/".into(),
                        replace: "/".into(),
                    }],
                    query_mode: QueryRewriteMode::Keep,
                    query_replace: None,
                    remove_params: Vec::new(),
                    query_append: Some("cluster=primary".into()),
                },
            )
            .unwrap(),
        );
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/v1/customers/100?tab=overview"),
            Version::HTTP_11,
            HeaderWorkspace::empty(4),
            Body::Empty,
        );

        let mut target = 1;
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
                    headers: std::collections::HashMap::from([("retry-after".into(), "60".into())]),
                    body: "Service Unavailable".into(),
                    content_type: "text/plain; charset=utf-8".into(),
                },
            )
            .unwrap(),
        );
        let chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![plugin];

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/api/v1/checkout"),
            Version::HTTP_11,
            HeaderWorkspace::empty(0),
            Body::Empty,
        );

        let mut target = target_init(1);
        let action = run_pre_upstream_plugins(&chain, &mut req, &mut target);
        assert_eq!(target, TARGETS_CANCELLED);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(resp.headers.get("retry-after").unwrap(), "60");
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "text/plain; charset=utf-8"
                );
                assert_eq!(resp.body.as_slice(), b"Service Unavailable");
            }
            _ => panic!("Expected Action::Respond(503)"),
        }
    }

    #[test]
    fn test_canary_and_mirror_pack_u32_and_cancel_on_reject() {
        let canary = Arc::new(
            CanaryPlugin::new(
                "canary",
                CanaryConfig {
                    canary_upstream: "prod-v2".into(),
                    ratio: 500,
                    rules: vec![HeaderRule {
                        header: HeaderName::from_static("x-canary"),
                        operator: RuleOperator::Equals,
                        pattern: (*b"true").into(),
                    }],
                    hash_headers: vec![HeaderName::from_static("x-tenant")],
                },
                2,
            )
            .unwrap(),
        );
        let mirror = Arc::new(
            RequestMirrorPlugin::new(
                "mirror",
                RequestMirrorConfig {
                    mirror_upstream: "staging".into(),
                    sample_percent: 100.0,
                    ignore_methods: vec![],
                },
                5,
            )
            .unwrap(),
        );
        let rate_limit = Arc::new(RateLimitPlugin::new("rl", 1));

        // 1. Canary + Mirror both update independent halves of targets u32; original HeaderWorkspace untouched
        let allow_chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![canary, mirror.clone()];
        let mut h = HeaderWorkspace::empty(2);
        h.insert("x-canary", HeaderValue::from_static("true"));
        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/orders"),
            Version::HTTP_11,
            h,
            Body::Bytes(bytes::Bytes::from_static(b"{}")),
        );
        let mut targets = target_init(0);
        let act = run_pre_upstream_plugins(&allow_chain, &mut req, &mut targets);
        assert!(matches!(act, Action::Continue));
        assert_eq!(target_primary(targets), Some(2));
        assert_eq!(target_mirror(targets), Some(5));
        assert!(!req.headers.contains_key("x-velda-mirror"));

        // 2. Mirror -> Reject clears BOTH halves to TARGETS_CANCELLED (0xFFFF_FFFF)
        let reject_chain: Vec<Arc<dyn PreUpstreamPlugin>> = vec![mirror, rate_limit];
        let mut req1 = L7Request::new(
            Method::GET,
            Uri::from_static("/orders"),
            Version::HTTP_11,
            HeaderWorkspace::empty(2),
            Body::Empty,
        );
        let mut t1 = target_init(0);
        assert!(matches!(
            run_pre_upstream_plugins(&reject_chain, &mut req1, &mut t1),
            Action::Continue
        ));
        assert_eq!(target_primary(t1), Some(0));
        assert_eq!(target_mirror(t1), Some(5));

        let mut req2 = L7Request::new(
            Method::GET,
            Uri::from_static("/orders"),
            Version::HTTP_11,
            HeaderWorkspace::empty(2),
            Body::Empty,
        );
        let mut t2 = target_init(0);
        assert!(matches!(
            run_pre_upstream_plugins(&reject_chain, &mut req2, &mut t2),
            Action::Reject(RejectReason::TooManyRequests)
        ));
        assert_eq!(t2, TARGETS_CANCELLED);
        assert_eq!(target_primary(t2), None);
        assert_eq!(target_mirror(t2), None);
    }
}
