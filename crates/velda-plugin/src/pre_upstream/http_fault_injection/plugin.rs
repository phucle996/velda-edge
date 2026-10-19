//! HTTP Fault Injection Plugin Workflow Owner.
//!
//! # Architecture & Pipeline Invariants
//! - **Phase**: Pre-Upstream (`pre_upstream`). Evaluated after routing, immediately before backend dispatch.
//! - **Reverse Capability**: Declares `header: Read, payload: None`, enabling 100% Zero-Copy streaming
//!   passthrough for all regular, non-aborted production requests.
//! - **Strict Order**: When a request matches both delay and abort criteria, the gateway delays
//!   execution first, then aborts with the pre-compiled response (`Action::delay(dur, Action::Respond(resp))`),
//!   accurately simulating real-world backend timeouts and cascading failures.

use velda_core::{Action, L7Request};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::pre_upstream::PreUpstreamPlugin;

use super::abort::HttpAbortConfig;
use super::condition::HttpFaultCondition;
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
    /// 3. Evaluates delay injection criteria.
    /// 4. Evaluates abort injection criteria.
    /// 5. Composes non-blocking [`Action`] (Delay first, then Abort or Continue).
    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        // Step 1: Scope filter. If condition does not match, real users are 100% unaffected.
        if !self.condition.matches(req) {
            return Action::Continue;
        }

        // Step 2: Extract dynamic header overrides (if explicitly allowed in config)
        let (override_delay, override_abort) = self.condition.extract_overrides(req);

        // Step 3: Evaluate Delay
        let delay_dur = self
            .delay
            .as_ref()
            .and_then(|d| d.calculate_delay(override_delay));

        // Step 4: Evaluate Abort
        let abort_resp = self.abort.as_ref().and_then(|a| a.evaluate(override_abort));

        // Step 5: Compose Action
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
    use http::header::HeaderName;
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

        let action = plugin.on_upstream_request(&mut req);
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

        let action = plugin.on_upstream_request(&mut req);
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

        let action = plugin.on_upstream_request(&mut req);
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

        let action = plugin.on_upstream_request(&mut req);
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

        let action = plugin.on_upstream_request(&mut req);
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
    }
}
