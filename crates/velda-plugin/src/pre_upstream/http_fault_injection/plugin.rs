//! HTTP Fault Injection Plugin Workflow Owner.
//!
//! # Architecture & Pipeline Invariants
//! - **Phase**: Pre-Upstream (`pre_upstream`). Evaluated after routing, immediately before backend dispatch.
//! - **Reverse Capability**: Declares `header: Read, payload: None`, enabling 100% Zero-Copy streaming
//!   passthrough for all regular, non-aborted production requests.
//! - **Strict Order**: When a request matches both delay and abort criteria, the gateway delays
//!   execution first, then aborts with the pre-compiled response (`Action::delay(dur, Action::Respond(resp))`),
//!   accurately simulating real-world backend timeouts and cascading failures.
//! - **Header Sanitization**: When dynamic overrides are allowed, `x-fault-delay-ms` and `x-fault-abort-code`
//!   are purged from request headers to prevent internal chaos tokens from leaking to upstream.

use std::sync::Arc;

use http::header::HeaderName;
use velda_core::{Action, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

use super::abort::HttpAbortConfig;
use super::condition::{HeaderMatchRule, HttpFaultCondition, IpRangeMatcher};
use super::delay::HttpDelayConfig;

/// Plugin for injecting artificial delays and HTTP abort responses.
#[derive(Debug, Clone)]
pub struct HttpFaultInjectionPlugin {
    pub id: String,
    pub condition: HttpFaultCondition,
    pub delay: Option<HttpDelayConfig>,
    pub abort: Option<HttpAbortConfig>,
}

impl HttpFaultInjectionPlugin {
    /// Creates a new HTTP Fault Injection plugin instance.
    pub fn new(
        id: impl Into<String>,
        condition: HttpFaultCondition,
        delay: Option<HttpDelayConfig>,
        abort: Option<HttpAbortConfig>,
    ) -> Self {
        Self {
            id: id.into(),
            condition,
            delay,
            abort,
        }
    }

    /// Instantiates a new [`HttpFaultInjectionPlugin`] from compiled [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_http_fault_injection_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid http_fault_injection configuration for '{}': {e}",
                    config.id
                ))
            })?;

        // 1. Build Scope & Condition Matchers
        let ip_matcher = if !cfg.scope.client_ips.is_empty() {
            Some(
                IpRangeMatcher::from_cidrs(&cfg.scope.client_ips).map_err(|e| {
                    PluginError::Config(format!(
                        "Plugin '{}': IP range matcher error: {e}",
                        config.id
                    ))
                })?,
            )
        } else {
            None
        };

        let header_match = if let Some(hm) = &cfg.scope.header_match {
            let name = HeaderName::from_bytes(hm.name.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{}': invalid header name '{}' in scope.header_match: {e}",
                    config.id, hm.name
                ))
            })?;
            Some(match &hm.exact_match {
                Some(val) => HeaderMatchRule::Exact(name, val.clone()),
                None => HeaderMatchRule::Present(name),
            })
        } else {
            None
        };

        let condition = HttpFaultCondition {
            ip_matcher,
            header_match,
            allow_header_override: cfg.scope.allow_header_override,
            max_override_delay_ms: cfg.scope.max_override_delay_ms,
        };

        // 2. Build Delay
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
        } else if cfg.scope.allow_header_override {
            // Enable delay evaluator with 0% static hit rate so dynamic x-fault-delay-ms override can trigger
            Some(
                HttpDelayConfig::new(0.0, 0, cfg.scope.max_override_delay_ms).map_err(|e| {
                    PluginError::Config(format!(
                        "Plugin '{}': delay override init error: {e}",
                        config.id
                    ))
                })?,
            )
        } else {
            None
        };

        // 3. Build Abort
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
        } else if cfg.scope.allow_header_override {
            // Enable abort evaluator with 0% static hit rate so dynamic x-fault-abort-code override can trigger
            Some(HttpAbortConfig::new(0.0, 500, None, None).map_err(|e| {
                PluginError::Config(format!(
                    "Plugin '{}': abort override init error: {e}",
                    config.id
                ))
            })?)
        } else {
            None
        };

        Ok(Self::new(&config.id, condition, delay_cfg, abort_cfg))
    }
}

impl PreUpstreamPlugin for HttpFaultInjectionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    /// Evaluates the request against fault injection rules.
    ///
    /// # Flat Sequential Workflow
    /// 1. Evaluates scope matching (`client_ips` disjoint interval ranges and `header_match`).
    ///    Non-matching production traffic bypasses in ~1-5ns (`Action::Continue`).
    /// 2. Extracts dynamic header overrides (`x-fault-delay-ms`, `x-fault-abort-code`) if enabled.
    /// 3. Purges override headers from `req.headers` to prevent leakage to upstream backends.
    /// 4. Evaluates delay injection criteria.
    /// 5. Evaluates abort injection criteria.
    /// 6. Composes non-blocking [`Action`] (Delay first, then Abort or Continue).
    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        // Security Hygiene: Always extract and purge chaos testing headers so testing tokens
        // NEVER leak to upstream backends, regardless of whether request matches scope or overrides are enabled.
        let (override_delay, override_abort) = if self.condition.allow_header_override {
            let overrides = self.condition.extract_overrides(req);
            req.headers.remove("x-fault-delay-ms");
            req.headers.remove("x-fault-abort-code");
            overrides
        } else {
            req.headers.remove("x-fault-delay-ms");
            req.headers.remove("x-fault-abort-code");
            (None, None)
        };

        // Step 1: Scope filter. If condition does not match, real users are 100% unaffected.
        if !self.condition.matches(req) {
            return Action::Continue;
        }

        // Step 2: Evaluate Delay
        let delay_dur = self
            .delay
            .as_ref()
            .and_then(|d| d.calculate_delay(override_delay));

        // Step 3: Evaluate Abort
        let abort_resp = self.abort.as_ref().and_then(|a| a.evaluate(override_abort));

        // Step 4: Compose Action
        match (delay_dur, abort_resp) {
            (Some(dur), Some(resp)) => Action::delay(dur, Action::Respond(resp)),
            (Some(dur), None) => Action::delay(dur, Action::Continue),
            (None, Some(resp)) => Action::Respond(resp),
            (None, None) => Action::Continue,
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
    use std::time::Duration;
    use velda_core::Body;

    use crate::pre_upstream::http_fault_injection::condition::{HeaderMatchRule, IpRangeMatcher};

    #[test]
    fn test_fault_injection_scope_bypass() {
        let condition = HttpFaultCondition {
            ip_matcher: Some(IpRangeMatcher::from_cidrs(&["10.0.0.0/8".to_string()]).unwrap()),
            ..Default::default()
        };
        let abort = HttpAbortConfig::new(100.0, 503, None, None).unwrap();

        let plugin = HttpFaultInjectionPlugin::new("fault-test", condition, None, Some(abort));

        // Request from public Internet (203.0.113.1) -> Must bypass immediately!
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("203.0.113.1"),
        );
        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target: Arc<str> = Arc::from("http_upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));
    }

    #[test]
    fn test_fault_injection_delay_only() {
        let condition = HttpFaultCondition::default();
        let delay = HttpDelayConfig::new(100.0, 200, 200).unwrap();

        let plugin = HttpFaultInjectionPlugin::new("fault-test", condition, Some(delay), None);

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let mut target: Arc<str> = Arc::from("http_upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(200));
                assert!(matches!(*and_then, Action::Continue));
            }
            _ => panic!("Expected Action::Delay with Continue"),
        }
    }

    #[test]
    fn test_fault_injection_abort_only() {
        let condition = HttpFaultCondition::default();
        let abort = HttpAbortConfig::new(100.0, 503, None, None).unwrap();

        let plugin = HttpFaultInjectionPlugin::new("fault-test", condition, None, Some(abort));

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let mut target: Arc<str> = Arc::from("http_upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "text/plain; charset=utf-8"
                );
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_fault_injection_delay_and_abort() {
        let condition = HttpFaultCondition::default();
        let delay = HttpDelayConfig::new(100.0, 300, 300).unwrap();
        let abort = HttpAbortConfig::new(100.0, 504, None, None).unwrap();

        let plugin =
            HttpFaultInjectionPlugin::new("fault-test", condition, Some(delay), Some(abort));

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let mut target: Arc<str> = Arc::from("http_upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(300));
                match *and_then {
                    Action::Respond(resp) => {
                        assert_eq!(resp.status, StatusCode::GATEWAY_TIMEOUT);
                    }
                    _ => panic!("Expected Action::Respond inside Delay"),
                }
            }
            _ => panic!("Expected Action::Delay with Respond"),
        }
    }

    #[test]
    fn test_fault_injection_dynamic_override() {
        let condition = HttpFaultCondition {
            allow_header_override: true,
            max_override_delay_ms: 5000,
            header_match: Some(HeaderMatchRule::Exact(
                HeaderName::from_static("x-chaos-test"),
                "enabled".to_string(),
            )),
            ..Default::default()
        };
        // 0% configured in static config
        let delay = HttpDelayConfig::new(0.0, 100, 200).unwrap();
        let abort = HttpAbortConfig::new(0.0, 500, None, None).unwrap();

        let plugin =
            HttpFaultInjectionPlugin::new("fault-test", condition, Some(delay), Some(abort));

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-chaos-test"),
            HeaderValue::from_static("enabled"),
        );
        headers.insert(
            HeaderName::from_static("x-fault-delay-ms"),
            HeaderValue::from_static("400"),
        );
        headers.insert(
            HeaderName::from_static("x-fault-abort-code"),
            HeaderValue::from_static("503"),
        );

        let mut req = L7Request::new(
            Method::GET,
            Uri::from_static("/api/users"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let mut target: Arc<str> = Arc::from("http_upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(400));
                match *and_then {
                    Action::Respond(resp) => {
                        assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
                    }
                    _ => panic!("Expected Action::Respond inside Delay"),
                }
            }
            _ => panic!("Expected Action::Delay with Respond"),
        }

        // Verify headers were stripped to prevent leakage upstream
        assert!(!req.headers.contains_key("x-fault-delay-ms"));
        assert!(!req.headers.contains_key("x-fault-abort-code"));
        assert!(req.headers.contains_key("x-chaos-test"));
    }

    #[test]
    fn test_from_config_complete_with_scope() {
        let json_cfg = serde_json::json!({
            "scope": {
                "client_ips": ["10.0.0.0/8"],
                "header_match": {
                    "name": "x-canary-test",
                    "exact_match": "active"
                },
                "allow_header_override": true,
                "max_override_delay_ms": 4000
            },
            "delay": {
                "enabled": true,
                "fixed_delay_ms": 150,
                "percentage": 20
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
        assert!(plugin.condition.ip_matcher.is_some());
        assert!(plugin.condition.header_match.is_some());
        assert!(plugin.condition.allow_header_override);
        assert_eq!(plugin.condition.max_override_delay_ms, 4000);
        assert!(plugin.delay.is_some());
        assert!(plugin.abort.is_some());
    }
}
