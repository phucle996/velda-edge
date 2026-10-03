//! Unified top-level Router coordinating L4 (TCP, UDP) and L7 (HTTP/1.1, HTTP/2, HTTP/3, and gRPC) routing.

use crate::error::RouterError;
use crate::l4::tcp::{TcpRoute, TcpRouter};
use crate::l4::udp::{UdpRoute, UdpRouter};
use crate::l7::grpc::{GrpcRoute, GrpcRouteRequest, GrpcRouter};
use crate::l7::http1::{Http1Route, Http1RouteRequest, Http1Router};
use crate::l7::http2::{Http2Route, Http2RouteRequest, Http2Router};
use crate::l7::http3::{Http3Route, Http3RouteRequest, Http3Router};

/// Unified runtime router holding dedicated routing structures for each supported protocol.
///
/// Designed to be stored in an [`Arc`] or [`ArcSwap`] inside the in-memory runtime snapshot,
/// supporting lock-free concurrent lookups on the request serving hot path.
#[derive(Debug, Default, Clone)]
pub struct Router {
    tcp: TcpRouter,
    udp: UdpRouter,
    http1: Http1Router,
    http2: Http2Router,
    http3: Http3Router,
    grpc: GrpcRouter,
}

impl Router {
    /// Creates a new router from individual protocol routers.
    pub fn new(
        tcp: TcpRouter,
        udp: UdpRouter,
        http1: Http1Router,
        http2: Http2Router,
        http3: Http3Router,
        grpc: GrpcRouter,
    ) -> Self {
        Self {
            tcp,
            udp,
            http1,
            http2,
            http3,
            grpc,
        }
    }

    /// Resolves an L4 TCP route by `listener_id`.
    #[inline]
    pub fn route_tcp(&self, listener_id: &str) -> Option<&TcpRoute> {
        self.tcp.route(listener_id)
    }

    /// Resolves an L4 UDP route by `listener_id`.
    #[inline]
    pub fn route_udp(&self, listener_id: &str) -> Option<&UdpRoute> {
        self.udp.route(listener_id)
    }

    /// Resolves an HTTP/1.1 route by `listener_id` and request view.
    #[inline]
    pub fn route_http1(
        &self,
        listener_id: &str,
        req: &Http1RouteRequest<'_>,
    ) -> Option<&Http1Route> {
        self.http1.route(listener_id, req)
    }

    /// Resolves an HTTP/2 route by `listener_id` and request view.
    #[inline]
    pub fn route_http2(
        &self,
        listener_id: &str,
        req: &Http2RouteRequest<'_>,
    ) -> Option<&Http2Route> {
        self.http2.route(listener_id, req)
    }

    /// Resolves an HTTP/3 route by `listener_id` and request view.
    #[inline]
    pub fn route_http3(
        &self,
        listener_id: &str,
        req: &Http3RouteRequest<'_>,
    ) -> Option<&Http3Route> {
        self.http3.route(listener_id, req)
    }

    /// Resolves a gRPC route by `listener_id` and request view.
    #[inline]
    pub fn route_grpc(&self, listener_id: &str, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        self.grpc.route(listener_id, req)
    }

    /// Returns a reference to the underlying [`TcpRouter`].
    #[inline]
    pub fn tcp(&self) -> &TcpRouter {
        &self.tcp
    }

    /// Returns a reference to the underlying [`UdpRouter`].
    #[inline]
    pub fn udp(&self) -> &UdpRouter {
        &self.udp
    }

    /// Returns a reference to the underlying [`Http1Router`].
    #[inline]
    pub fn http1(&self) -> &Http1Router {
        &self.http1
    }

    /// Returns a reference to the underlying [`Http2Router`].
    #[inline]
    pub fn http2(&self) -> &Http2Router {
        &self.http2
    }

    /// Returns a reference to the underlying [`Http3Router`].
    #[inline]
    pub fn http3(&self) -> &Http3Router {
        &self.http3
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
    tcp_routes: Vec<TcpRoute>,
    udp_routes: Vec<UdpRoute>,
    http1_routes: Vec<Http1Route>,
    http2_routes: Vec<Http2Route>,
    http3_routes: Vec<Http3Route>,
    grpc_routes: Vec<GrpcRoute>,
}

impl RouterBuilder {
    /// Creates a new empty [`RouterBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an L4 TCP route rule.
    pub fn add_tcp_route(mut self, route: TcpRoute) -> Self {
        self.tcp_routes.push(route);
        self
    }

    /// Adds an L4 UDP route rule.
    pub fn add_udp_route(mut self, route: UdpRoute) -> Self {
        self.udp_routes.push(route);
        self
    }

    /// Adds an HTTP/1.1 route rule.
    pub fn add_http1_route(mut self, route: Http1Route) -> Self {
        self.http1_routes.push(route);
        self
    }

    /// Adds an HTTP/2 route rule.
    pub fn add_http2_route(mut self, route: Http2Route) -> Self {
        self.http2_routes.push(route);
        self
    }

    /// Adds an HTTP/3 route rule.
    pub fn add_http3_route(mut self, route: Http3Route) -> Self {
        self.http3_routes.push(route);
        self
    }

    /// Adds a gRPC route rule.
    pub fn add_grpc_route(mut self, route: GrpcRoute) -> Self {
        self.grpc_routes.push(route);
        self
    }

    /// Compiles the router into its in-memory lookup representation.
    pub fn build(self) -> Result<Router, RouterError> {
        let tcp = TcpRouter::new(self.tcp_routes)?;
        let udp = UdpRouter::new(self.udp_routes)?;
        let http1 = Http1Router::new(self.http1_routes)?;
        let http2 = Http2Router::new(self.http2_routes)?;
        let http3 = Http3Router::new(self.http3_routes)?;
        let grpc = GrpcRouter::new(self.grpc_routes)?;
        Ok(Router::new(tcp, udp, http1, http2, http3, grpc))
    }
}
