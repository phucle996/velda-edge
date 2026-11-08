//! Layer 4 TCP routing rules and in-memory lookup table.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! TCP routing operates strictly on L4 byte streams and holds zero UDP session timeout logic
//! or L7 framing constructs. Maintains dedicated, lock-free $O(1)$ listener resolution.

use rustc_hash::FxHashMap;
use velda_core::{ListenerId, RouteId, UpstreamId};

use crate::error::RouterError;

/// Concrete compiled Layer 4 TCP routing rule (12 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpRoute {
    /// Unique identifier of this route.
    pub id: RouteId,
    /// Ingress listener identifier this route attaches to.
    pub listener_id: ListenerId,
    /// Logical upstream destination for forwarding traffic.
    pub upstream_id: UpstreamId,
}

impl TcpRoute {
    /// Creates a new L4 TCP route rule.
    #[inline]
    pub fn new(id: RouteId, listener_id: impl Into<ListenerId>, upstream_id: UpstreamId) -> Self {
        Self {
            id,
            listener_id: listener_id.into(),
            upstream_id,
        }
    }
}

/// High-performance, in-memory L4 TCP route lookup table.
///
/// Provides zero-allocation $O(1)$ route resolution from [`ListenerId`] to [`TcpRoute`] via [`FxHashMap`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TcpRouter {
    listeners: FxHashMap<ListenerId, TcpRoute>,
}

impl TcpRouter {
    /// Builds a new [`TcpRouter`] from a list of compiled [`TcpRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = TcpRoute>) -> Result<Self, RouterError> {
        let mut listeners: FxHashMap<ListenerId, TcpRoute> = FxHashMap::default();

        for r in routes {
            if listeners.contains_key(&r.listener_id) {
                return Err(RouterError::DuplicateRoute {
                    listener: r.listener_id.to_string(),
                    protocol: "Tcp".to_string(),
                });
            }
            listeners.insert(r.listener_id, r);
        }

        Ok(Self { listeners })
    }

    /// Resolves a TCP route for an incoming connection on a listener.
    #[inline]
    pub fn route(&self, listener_id: impl Into<ListenerId>) -> Option<&TcpRoute> {
        self.listeners.get(&listener_id.into())
    }

    /// Returns the total number of configured TCP routes.
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
    fn test_tcp_router_lookup() {
        let routes = vec![
            TcpRoute::new(RouteId::new(1), "postgres-in", UpstreamId::new(10)),
            TcpRoute::new(RouteId::new(2), "redis-in", UpstreamId::new(20)),
        ];

        let router = TcpRouter::new(routes).unwrap();
        assert_eq!(router.len(), 2);
        assert!(!router.is_empty());

        let r1 = router.route("postgres-in").unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_id, UpstreamId::new(10));

        let r2 = router.route("redis-in").unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_id, UpstreamId::new(20));

        assert!(router.route("unknown").is_none());
    }

    #[test]
    fn test_tcp_router_duplicate_fails() {
        let routes = vec![
            TcpRoute::new(RouteId::new(1), "redis", UpstreamId::new(10)),
            TcpRoute::new(RouteId::new(2), "redis", UpstreamId::new(20)),
        ];

        let err = TcpRouter::new(routes).unwrap_err();
        assert!(matches!(err, RouterError::DuplicateRoute { .. }));
    }
}
