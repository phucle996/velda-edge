//! Unified top-level Router coordinating L4 and L7 (HTTP / gRPC) routing.

use velda_core::TransportProtocol;

use crate::error::RouterError;
use crate::l4::{L4Route, L4Router};
use crate::l7::grpc::{GrpcRoute, GrpcRouteRequest, GrpcRouter};
use crate::l7::http::{HttpRoute, HttpRouteRequest, HttpRouter};

/// Unified runtime router holding L4, HTTP, and gRPC routing structures.
///
/// Designed to be stored in an [`Arc`] or [`ArcSwap`] inside the in-memory runtime snapshot,
/// supporting lock-free concurrent lookups on the request serving hot path.
#[derive(Debug, Default, Clone)]
pub struct Router {
    l4: L4Router,
    http: HttpRouter,
    grpc: GrpcRouter,
}

impl Router {
    /// Creates a new router from individual protocol routers.
    pub fn new(l4: L4Router, http: HttpRouter, grpc: GrpcRouter) -> Self {
        Self { l4, http, grpc }
    }

    /// Resolves an L4 route by `listener_id` and `protocol`.
    #[inline]
    pub fn route_l4(&self, listener_id: &str, protocol: TransportProtocol) -> Option<&L4Route> {
        self.l4.route(listener_id, protocol)
    }

    /// Resolves an HTTP route by `listener_id` and request view.
    ///
    /// Performs zero allocations on the request serving hot path.
    #[inline]
    pub fn route_http(&self, listener_id: &str, req: &HttpRouteRequest<'_>) -> Option<&HttpRoute> {
        self.http.route(listener_id, req)
    }

    /// Resolves a gRPC route by `listener_id` and request view.
    ///
    /// Performs zero allocations on the request serving hot path.
    #[inline]
    pub fn route_grpc(&self, listener_id: &str, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        self.grpc.route(listener_id, req)
    }

    /// Returns a reference to the underlying [`L4Router`].
    #[inline]
    pub fn l4(&self) -> &L4Router {
        &self.l4
    }

    /// Returns a reference to the underlying [`HttpRouter`].
    #[inline]
    pub fn http(&self) -> &HttpRouter {
        &self.http
    }

    /// Returns a reference to the underlying [`GrpcRouter`].
    #[inline]
    pub fn grpc(&self) -> &GrpcRouter {
        &self.grpc
    }
}

/// Builder for constructing a compiled [`Router`].
#[derive(Debug, Default)]
pub struct RouterBuilder {
    l4_routes: Vec<L4Route>,
    http_routes: Vec<HttpRoute>,
    grpc_routes: Vec<GrpcRoute>,
}

impl RouterBuilder {
    /// Creates a new empty [`RouterBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an L4 route rule.
    pub fn add_l4_route(mut self, route: L4Route) -> Self {
        self.l4_routes.push(route);
        self
    }

    /// Adds an HTTP route rule.
    pub fn add_http_route(mut self, route: HttpRoute) -> Self {
        self.http_routes.push(route);
        self
    }

    /// Adds a gRPC route rule.
    pub fn add_grpc_route(mut self, route: GrpcRoute) -> Self {
        self.grpc_routes.push(route);
        self
    }

    /// Compiles the router into its in-memory lookup representation.
    pub fn build(self) -> Result<Router, RouterError> {
        let l4 = L4Router::new(self.l4_routes)?;
        let http = HttpRouter::new(self.http_routes)?;
        let grpc = GrpcRouter::new(self.grpc_routes)?;
        Ok(Router::new(l4, http, grpc))
    }
}
