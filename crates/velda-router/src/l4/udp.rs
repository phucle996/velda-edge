//! Layer 4 UDP routing rules and in-memory lookup table.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! UDP routing operates strictly on L4 datagram flows, managing optional idle session timeouts
//! for bidirectional stateful proxying or unidirectional fire-and-forget.

use rustc_hash::FxHashMap;
use std::time::Duration;
use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;

/// Concrete compiled Layer 4 UDP routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpRoute {
    /// Unique identifier of this route.
    pub id: RouteId,
    /// Ingress listener identifier this route attaches to.
    pub listener_id: String,
    /// Logical upstream destination for forwarding traffic.
    pub upstream_id: UpstreamId,
    /// Upstream target name as declared in configuration.
    pub upstream_name: String,
    /// Optional idle timeout for stateful bidirectional L4 UDP sessions.
    /// If `None`, UDP is unidirectional (1 chiều fire-and-forget).
    /// If `Some`, UDP is bidirectional (2 chiều stateful proxying).
    pub udp_idle_timeout: Option<Duration>,
    /// Plugin hook identifiers associated with this route.
    pub plugins: Vec<String>,
}

impl UdpRoute {
    /// Creates a new L4 UDP route rule.
    pub fn new(
        id: RouteId,
        listener_id: impl Into<String>,
        upstream_id: UpstreamId,
        upstream_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            listener_id: listener_id.into(),
            upstream_id,
            upstream_name: upstream_name.into(),
            udp_idle_timeout: None,
            plugins: Vec::new(),
        }
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
}

/// High-performance, in-memory L4 UDP route lookup table.
///
/// Provides zero-allocation $O(1)$ route resolution from `listener_id` to [`UdpRoute`] via [`FxHashMap`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UdpRouter {
    listeners: FxHashMap<String, UdpRoute>,
}

impl UdpRouter {
    /// Builds a new [`UdpRouter`] from a list of compiled [`UdpRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = UdpRoute>) -> Result<Self, RouterError> {
        let mut listeners: FxHashMap<String, UdpRoute> = FxHashMap::default();

        for r in routes {
            if listeners.contains_key(&r.listener_id) {
                return Err(RouterError::DuplicateRoute {
                    listener: r.listener_id,
                    protocol: "Udp".to_string(),
                });
            }
            listeners.insert(r.listener_id.clone(), r);
        }

        Ok(Self { listeners })
    }

    /// Resolves a UDP route for an incoming datagram on a listener.
    #[inline]
    pub fn route(&self, listener_id: &str) -> Option<&UdpRoute> {
        self.listeners.get(listener_id)
    }

    /// Returns the total number of configured UDP routes.
    #[inline]
    pub fn len(&self) -> usize {
        self.listeners.len()
    }

    /// Returns whether the router has no configured routes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.listeners.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udp_router_lookup() {
        let routes = vec![
            UdpRoute::new(
                RouteId::new(1),
                "dns-in",
                UpstreamId::new(10),
                "dns-backend",
            )
            .with_udp_idle_timeout(Some(Duration::from_secs(30))),
            UdpRoute::new(
                RouteId::new(2),
                "syslog-in",
                UpstreamId::new(20),
                "syslog-backend",
            ),
        ];

        let router = UdpRouter::new(routes).unwrap();
        assert_eq!(router.len(), 2);
        assert!(!router.is_empty());

        let r1 = router.route("dns-in").unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "dns-backend");
        assert!(!r1.is_unidirectional());

        let r2 = router.route("syslog-in").unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_name, "syslog-backend");
        assert!(r2.is_unidirectional());

        assert!(router.route("unknown").is_none());
    }

    #[test]
    fn test_udp_router_duplicate_fails() {
        let routes = vec![
            UdpRoute::new(RouteId::new(1), "dns", UpstreamId::new(10), "d1"),
            UdpRoute::new(RouteId::new(2), "dns", UpstreamId::new(20), "d2"),
        ];

        let err = UdpRouter::new(routes).unwrap_err();
        assert!(matches!(err, RouterError::DuplicateRoute { .. }));
    }
}
