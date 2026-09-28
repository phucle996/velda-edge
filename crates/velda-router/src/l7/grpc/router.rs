//! High-performance, in-memory gRPC route lookup table.
//!
//! Provides $O(1)$ zero-allocation route resolution for gRPC services and methods.

use std::collections::HashMap;

use super::route::{GrpcRoute, GrpcRouteRequest};
use crate::error::RouterError;

/// gRPC route table for a single listener.
#[derive(Debug, Default, Clone)]
pub struct ListenerGrpcRouter {
    services: HashMap<String, Vec<GrpcRoute>>,
}

impl ListenerGrpcRouter {
    pub fn new(routes: Vec<GrpcRoute>) -> Self {
        let mut services: HashMap<String, Vec<GrpcRoute>> = HashMap::new();

        for r in routes {
            services.entry(r.service.clone()).or_default().push(r);
        }

        Self { services }
    }

    /// Resolves a gRPC route for the given request without allocations.
    #[inline]
    pub fn route(&self, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        let candidates = self.services.get(req.service)?;
        candidates
            .iter()
            .find(|r| r.matches_method(req.method) && r.matches_authority(req.authority))
    }
}

/// Unified, thread-safe gRPC routing table coordinating all gRPC listeners.
#[derive(Debug, Default, Clone)]
pub struct GrpcRouter {
    listeners: HashMap<String, ListenerGrpcRouter>,
}

impl GrpcRouter {
    /// Builds a new [`GrpcRouter`] from a list of compiled [`GrpcRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = GrpcRoute>) -> Result<Self, RouterError> {
        let mut grouped: HashMap<String, Vec<GrpcRoute>> = HashMap::new();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = HashMap::with_capacity(grouped.len());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerGrpcRouter::new(list));
        }

        Ok(Self { listeners })
    }

    /// Resolves a gRPC route for the given listener and request view.
    ///
    /// Performs zero allocations on the request serving hot path.
    #[inline]
    pub fn route(&self, listener_id: &str, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        let listener_router = self.listeners.get(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the number of configured listeners.
    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{RouteId, UpstreamId};

    #[test]
    fn test_grpc_router_service_and_method_matching() {
        let routes = vec![
            GrpcRoute::new(
                RouteId::new(1),
                "grpc-in",
                "payment.PaymentService",
                UpstreamId::new(10),
                "payment-all",
            ),
            GrpcRoute::new(
                RouteId::new(2),
                "grpc-in",
                "order.OrderService",
                UpstreamId::new(20),
                "order-create",
            )
            .with_method("CreateOrder"),
            GrpcRoute::new(
                RouteId::new(3),
                "grpc-in",
                "order.OrderService",
                UpstreamId::new(30),
                "order-cancel",
            )
            .with_method("CancelOrder"),
        ];

        let router = GrpcRouter::new(routes).unwrap();

        // 1. Any method under payment.PaymentService matches payment-all
        let req1 = GrpcRouteRequest::new("payment.PaymentService", Some("Refund"));
        let r1 = router.route("grpc-in", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "payment-all");

        // 2. Specific method CreateOrder matches order-create
        let req2 = GrpcRouteRequest::new("order.OrderService", Some("CreateOrder"));
        let r2 = router.route("grpc-in", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));

        // 3. Specific method CancelOrder matches order-cancel
        let req3 = GrpcRouteRequest::new("order.OrderService", Some("CancelOrder"));
        let r3 = router.route("grpc-in", &req3).unwrap();
        assert_eq!(r3.id, RouteId::new(3));

        // 4. Unknown method under order.OrderService returns None
        let req_unknown = GrpcRouteRequest::new("order.OrderService", Some("UnknownMethod"));
        assert!(router.route("grpc-in", &req_unknown).is_none());
    }

    #[test]
    fn test_grpc_from_path_zero_alloc() {
        let path = "/user.UserService/GetUserProfile";
        let req = GrpcRouteRequest::from_path(path, Some("grpc.example.com")).unwrap();

        assert_eq!(req.service, "user.UserService");
        assert_eq!(req.method, Some("GetUserProfile"));
        assert_eq!(req.authority, Some("grpc.example.com"));

        // Invalid gRPC path formats return None
        assert!(GrpcRouteRequest::from_path("/invalid", None).is_none());
        assert!(GrpcRouteRequest::from_path("no-leading-slash/Method", None).is_none());
    }

    #[test]
    fn test_grpc_router_authority_matching() {
        let routes = vec![
            GrpcRoute::new(
                RouteId::new(1),
                "grpc-in",
                "service.Api",
                UpstreamId::new(10),
                "prod-backend",
            )
            .with_authority("prod.example.com"),
            GrpcRoute::new(
                RouteId::new(2),
                "grpc-in",
                "service.Api",
                UpstreamId::new(20),
                "staging-backend",
            )
            .with_authority("*.staging.example.com"),
        ];

        let router = GrpcRouter::new(routes).unwrap();

        let req_prod = GrpcRouteRequest::new("service.Api", Some("Call"))
            .with_authority("prod.example.com:443");
        assert_eq!(
            router.route("grpc-in", &req_prod).unwrap().id,
            RouteId::new(1)
        );

        let req_stage = GrpcRouteRequest::new("service.Api", Some("Call"))
            .with_authority("cluster1.staging.example.com");
        assert_eq!(
            router.route("grpc-in", &req_stage).unwrap().id,
            RouteId::new(2)
        );

        let req_stage_caps = GrpcRouteRequest::new("service.Api", Some("Call"))
            .with_authority("CLUSTER1.STAGING.EXAMPLE.COM");
        assert_eq!(
            router.route("grpc-in", &req_stage_caps).unwrap().id,
            RouteId::new(2)
        );

        let req_evil = GrpcRouteRequest::new("service.Api", Some("Call"))
            .with_authority("fakestaging.example.com");
        assert!(router.route("grpc-in", &req_evil).is_none());
    }
}
