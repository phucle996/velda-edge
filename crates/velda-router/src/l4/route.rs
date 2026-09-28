use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use velda_core::{RouteId, TransportProtocol, UpstreamId};

/// Concrete compiled L4 routing rule for a listener and transport protocol.
#[derive(Debug, Clone)]
pub struct L4Route {
    /// Unique identifier of this route.
    pub id: RouteId,
    /// Ingress listener identifier this route attaches to.
    pub listener_id: String,
    /// Expected transport protocol (TCP or UDP).
    pub protocol: TransportProtocol,
    /// Logical upstream destination for forwarding traffic.
    pub upstream_id: UpstreamId,
    /// Upstream target name as declared in configuration.
    pub upstream_name: String,
    /// Pre-compiled target backend socket addresses.
    pub target_endpoints: Vec<SocketAddr>,
    /// Optional idle timeout for stateful bidirectional L4 UDP sessions.
    /// If `None`, UDP is unidirectional (1 chiều fire-and-forget).
    /// If `Some`, UDP is bidirectional (2 chiều stateful proxying).
    pub udp_idle_timeout: Option<Duration>,
    /// Plugin hook identifiers associated with this route.
    pub plugins: Vec<String>,
    rr_index: Arc<AtomicUsize>,
}

impl PartialEq for L4Route {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.listener_id == other.listener_id
            && self.protocol == other.protocol
            && self.upstream_id == other.upstream_id
            && self.upstream_name == other.upstream_name
            && self.target_endpoints == other.target_endpoints
            && self.udp_idle_timeout == other.udp_idle_timeout
            && self.plugins == other.plugins
    }
}

impl Eq for L4Route {}

impl L4Route {
    /// Creates a new L4 route rule.
    pub fn new(
        id: RouteId,
        listener_id: impl Into<String>,
        protocol: TransportProtocol,
        upstream_id: UpstreamId,
        upstream_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            listener_id: listener_id.into(),
            protocol,
            upstream_id,
            upstream_name: upstream_name.into(),
            target_endpoints: Vec::new(),
            udp_idle_timeout: None,
            plugins: Vec::new(),
            rr_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Attaches target physical backend socket addresses to this route.
    pub fn with_target_endpoints(mut self, endpoints: Vec<SocketAddr>) -> Self {
        self.target_endpoints = endpoints;
        self
    }

    /// Sets the optional idle timeout for stateful bidirectional L4 UDP sessions.
    pub fn with_udp_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.udp_idle_timeout = timeout;
        self
    }

    /// Returns whether this route is configured for unidirectional (fire-and-forget) UDP.
    #[inline]
    pub fn is_unidirectional(&self) -> bool {
        self.udp_idle_timeout.is_none()
    }

    /// Attaches plugin identifiers to this route.
    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Selects an eligible target backend address using atomic round-robin.
    ///
    /// Performs zero allocations on the request serving hot path.
    #[inline]
    pub fn select_target(&self) -> Option<SocketAddr> {
        if self.target_endpoints.is_empty() {
            return None;
        }
        if self.target_endpoints.len() == 1 {
            return Some(self.target_endpoints[0]);
        }
        let idx = self.rr_index.fetch_add(1, Ordering::Relaxed);
        Some(self.target_endpoints[idx % self.target_endpoints.len()])
    }
}
