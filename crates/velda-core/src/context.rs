//! Request and connection context shared across the Velda data plane.
//!
//! Contexts carry state through the request lifecycle. They do not
//! implement routing, plugin execution, upstream communication,
//! protocol parsing, or transport handling.
//!
//! The context layer connects the L4 and L7 models:
//!
//! ```text
//!     L4 connection
//!          |
//!          v
//!     RequestContext
//!          |
//!          +---- L4Request
//!          |
//!          +---- L7Request
//!          |
//!          +---- RequestState
//! ```
//!
//! The actual processing is performed by higher-level crates such as
//! `velda-proxy`, `velda-router`, `velda-plugin`, and `velda-upstream`.

use crate::l4::request::{ConnectionId, L4Request};
use crate::l7::request::L7Request;
use crate::types::{RequestId, RouteId, UpstreamId};

/// Context associated with a network connection.
///
/// A connection may contain multiple L7 requests, especially when
/// HTTP keep-alive or HTTP/2 multiplexing is used.
///
/// Therefore, connection-level information must not be confused
/// with request-level state.
#[derive(Debug)]
pub struct ConnectionContext {
    /// L4 connection information.
    pub connection: L4Request,
}

impl ConnectionContext {
    /// Creates a new connection context.
    pub fn new(connection: L4Request) -> Self {
        Self { connection }
    }

    /// Returns the unique connection identifier.
    pub fn id(&self) -> ConnectionId {
        self.connection.connection_id
    }

    /// Returns the client IP address.
    pub fn client_ip(&self) -> std::net::IpAddr {
        self.connection.client_ip()
    }
}

/// Context associated with a single request.
///
/// `RequestContext` is the main object passed through the Velda
/// request lifecycle and plugin hooks.
///
/// The L4 context is retained because higher-level policy may still
/// need transport information such as the client address.
#[derive(Debug)]
pub struct RequestContext {
    /// L4 connection information.
    pub l4: ConnectionContext,

    /// L7 HTTP request.
    ///
    /// This is present when the connection has been decoded into
    /// an HTTP request.
    pub l7: L7Request,

    /// Mutable state accumulated during request processing.
    pub state: RequestState,
}

impl RequestContext {
    /// Creates a new request context.
    pub fn new(l4: ConnectionContext, l7: L7Request, request_id: RequestId) -> Self {
        Self {
            l4,
            l7,
            state: RequestState::new(request_id),
        }
    }

    /// Returns the request identifier.
    pub fn request_id(&self) -> RequestId {
        self.state.request_id
    }

    /// Returns the client IP address.
    pub fn client_ip(&self) -> std::net::IpAddr {
        self.l4.client_ip()
    }

    /// Returns the selected route, if routing has already completed.
    pub fn route(&self) -> Option<RouteId> {
        self.state.route
    }

    /// Returns the selected upstream, if one has already been selected.
    pub fn upstream(&self) -> Option<UpstreamId> {
        self.state.upstream
    }
}

/// Mutable state accumulated throughout the request lifecycle.
///
/// `RequestState` contains information produced by one phase and
/// consumed by later phases.
///
/// Example:
///
/// ```text
///     PRE_ROUTE
///         │
///         ▼
///     ROUTE
///         │
///         ├── route = Some(...)
///         │
///         ▼
///     PRE_UPSTREAM
///         │
///         ├── upstream = Some(...)
///         │
///         ▼
///     UPSTREAM
/// ```
///
/// Keep this structure explicit and strongly typed. Avoid using
/// `HashMap<String, Box<dyn Any>>` as a generic plugin state store.
#[derive(Debug)]
pub struct RequestState {
    /// Unique identifier for this request.
    pub request_id: RequestId,

    /// Route selected by the router.
    pub route: Option<RouteId>,

    /// Upstream selected for the request.
    pub upstream: Option<UpstreamId>,

    /// Whether routing has completed.
    pub routed: bool,

    /// Whether the request has been sent to an upstream.
    pub upstream_started: bool,

    /// Whether a response has been received from the upstream.
    pub upstream_completed: bool,
}

impl RequestState {
    /// Creates a new request state.
    pub fn new(request_id: RequestId) -> Self {
        Self {
            request_id,
            route: None,
            upstream: None,
            routed: false,
            upstream_started: false,
            upstream_completed: false,
        }
    }

    /// Stores the route selected by the router.
    pub fn set_route(&mut self, route: RouteId) {
        self.route = Some(route);
        self.routed = true;
    }

    /// Stores the upstream selected for the request.
    pub fn set_upstream(&mut self, upstream: UpstreamId) {
        self.upstream = Some(upstream);
    }

    /// Marks the upstream request as started.
    pub fn mark_upstream_started(&mut self) {
        self.upstream_started = true;
    }

    /// Marks the upstream request as completed.
    pub fn mark_upstream_completed(&mut self) {
        self.upstream_completed = true;
    }
}
