//! gRPC Fault Injection Plugin Workflow Owner.
//!
//! # Architecture & Protocol Separation Invariants
//! - **Protocol Isolation (Rule 2.7)**: Hardcoded to `SupportedProtocols::GRPC`. Does not evaluate
//!   or execute on HTTP REST routes.
//! - **Transport Transparent**: Seamlessly intercepts and injects faults into both gRPC over TCP
//!   (HTTP/2 multiplexing) and gRPC over UDP (QUIC datagrams).
//! - **Reverse Capability**: Declares `header: Read, payload: None`, guaranteeing Zero-Copy
//!   direct streaming for unaffected RPC calls.

use velda_core::{Action, L7Request};

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::pre_upstream::PreUpstreamPlugin;

use super::abort::GrpcAbortConfig;
use super::condition::GrpcFaultCondition;
use super::delay::GrpcDelayConfig;

/// Plugin for injecting artificial delays and canonical gRPC abort responses.
#[derive(Debug, Clone)]
pub struct GrpcFaultInjectionPlugin {
    pub id: String,
    pub condition: GrpcFaultCondition,
    pub delay: Option<GrpcDelayConfig>,
    pub abort: Option<GrpcAbortConfig>,
}

impl GrpcFaultInjectionPlugin {
    /// Creates a new gRPC Fault Injection plugin instance.
    pub fn new(
        id: impl Into<String>,
        condition: GrpcFaultCondition,
        delay: Option<GrpcDelayConfig>,
        abort: Option<GrpcAbortConfig>,
    ) -> Self {
        Self {
            id: id.into(),
            condition,
            delay,
            abort,
        }
    }
}

impl PreUpstreamPlugin for GrpcFaultInjectionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    /// Evaluates the gRPC request against fault injection rules.
    ///
    /// # Flat Sequential Workflow
    /// 1. Scope filter: client IP CIDR, metadata headers, and RPC service/method match.
    ///    Non-matching traffic bypasses in ~1-5ns (`Action::Continue`).
    /// 2. Extracts dynamic header overrides (`x-grpc-fault-delay-ms`, `x-grpc-fault-abort-code`).
    /// 3. Evaluates delay criteria.
    /// 4. Evaluates abort criteria.
    /// 5. Composes non-blocking [`Action`] (Delay first, then Abort or Continue).
    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        // Step 1: Scope filter
        if !self.condition.matches(req) {
            return Action::Continue;
        }

        // Step 2: Dynamic header overrides
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
        SupportedProtocols::GRPC
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderName;
    use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
    use std::time::Duration;
    use velda_core::Body;

    use crate::pre_upstream::grpc_fault_injection::condition::GrpcTargetMatcher;

    #[test]
    fn test_grpc_fault_target_bypass() {
        let condition = GrpcFaultCondition {
            target_matcher: Some(GrpcTargetMatcher::ServicePrefix(
                "/order.v1.OrderService/".to_string(),
            )),
            ..Default::default()
        };
        let abort = GrpcAbortConfig::new(100.0, 14, None).unwrap();

        let plugin = GrpcFaultInjectionPlugin::new("grpc-fault-test", condition, None, Some(abort));

        // Call to payment service -> should bypass!
        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/payment.v1.PaymentService/Charge"),
            Version::HTTP_2,
            HeaderMap::new(),
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        assert!(matches!(action, Action::Continue));
    }

    #[test]
    fn test_grpc_fault_abort_trailers_only() {
        let condition = GrpcFaultCondition::default();
        let abort = GrpcAbortConfig::new(100.0, 14, None).unwrap();

        let plugin = GrpcFaultInjectionPlugin::new("grpc-fault-test", condition, None, Some(abort));

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/order.v1.OrderService/CreateOrder"),
            Version::HTTP_2,
            HeaderMap::new(),
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::OK);
                assert_eq!(
                    resp.headers.get("content-type").unwrap(),
                    "application/grpc"
                );
                assert_eq!(resp.headers.get("grpc-status").unwrap(), "14");
                assert_eq!(
                    resp.headers.get("grpc-message").unwrap(),
                    "fault injected by edge"
                );
            }
            _ => panic!("Expected Action::Respond"),
        }
    }

    #[test]
    fn test_grpc_fault_delay_and_abort() {
        let condition = GrpcFaultCondition::default();
        let delay = GrpcDelayConfig::new(100.0, 250, 250).unwrap();
        let abort = GrpcAbortConfig::new(100.0, 13, None).unwrap(); // INTERNAL

        let plugin =
            GrpcFaultInjectionPlugin::new("grpc-fault-test", condition, Some(delay), Some(abort));

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/order.v1.OrderService/CreateOrder"),
            Version::HTTP_2,
            HeaderMap::new(),
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(250));
                match *and_then {
                    Action::Respond(resp) => {
                        assert_eq!(resp.headers.get("grpc-status").unwrap(), "13");
                    }
                    _ => panic!("Expected Action::Respond inside Delay"),
                }
            }
            _ => panic!("Expected Action::Delay with Respond"),
        }
    }

    #[test]
    fn test_grpc_fault_dynamic_overrides() {
        let condition = GrpcFaultCondition {
            allow_header_override: true,
            max_override_delay_ms: 10_000,
            ..Default::default()
        };
        // 0% statically configured
        let delay = GrpcDelayConfig::new(0.0, 100, 200).unwrap();
        let abort = GrpcAbortConfig::new(0.0, 14, None).unwrap();

        let plugin =
            GrpcFaultInjectionPlugin::new("grpc-fault-test", condition, Some(delay), Some(abort));

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-grpc-fault-delay-ms"),
            HeaderValue::from_static("350"),
        );
        headers.insert(
            HeaderName::from_static("x-grpc-fault-abort-code"),
            HeaderValue::from_static("8"), // RESOURCE_EXHAUSTED
        );

        let mut req = L7Request::new(
            Method::POST,
            Uri::from_static("/order.v1.OrderService/CreateOrder"),
            Version::HTTP_2,
            headers,
            Body::Empty,
        );

        let action = plugin.on_upstream_request(&mut req);
        match action {
            Action::Delay { duration, and_then } => {
                assert_eq!(duration, Duration::from_millis(350));
                match *and_then {
                    Action::Respond(resp) => {
                        assert_eq!(resp.headers.get("grpc-status").unwrap(), "8");
                    }
                    _ => panic!("Expected Action::Respond inside Delay"),
                }
            }
            _ => panic!("Expected Action::Delay with Respond"),
        }
    }
}
