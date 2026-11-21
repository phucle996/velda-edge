//! Layer 4 TCP routing rules and in-memory lookup table.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! TCP routing operates strictly on L4 byte streams and holds zero UDP session timeout logic
//! or L7 framing constructs. Maintains dedicated, lock-free $O(1)$ listener resolution.

use crate::error::RouterError;

/// Concrete compiled Layer 4 TCP routing rule (8 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpRoute {
    /// Unique identifier of this route (`u16`).
    pub id: u16,
    /// Logical upstream destination for forwarding traffic (`u16`).
    pub upstream_id: u16,
    /// Ingress listener identifier (`u32` FNV-1a) this route attaches to.
    pub listener_id: u32,
}

impl TcpRoute {
    /// Creates a new L4 TCP route rule.
    #[inline]
    pub const fn new(id: u16, listener_id: u32, upstream_id: u16) -> Self {
        Self {
            id,
            upstream_id,
            listener_id,
        }
    }
}

/// High-performance, in-memory L4 TCP route lookup table backed by a flat contiguous `Vec<TcpRoute>`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TcpRouter {
    routes: Vec<TcpRoute>,
}

impl TcpRouter {
    /// Builds a new [`TcpRouter`] from a list of compiled [`TcpRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = TcpRoute>) -> Result<Self, RouterError> {
        let mut vec: Vec<TcpRoute> = routes.into_iter().collect();
        vec.sort_by_key(|r| r.listener_id);
        for w in vec.windows(2) {
            if w[0].listener_id == w[1].listener_id {
                return Err(RouterError::DuplicateRoute {
                    listener: w[0].listener_id.to_string(),
                    protocol: "Tcp".to_string(),
                });
            }
        }
        Ok(Self { routes: vec })
    }

    /// Resolves a TCP route for an incoming connection on a listener.
    #[inline]
    pub fn route(&self, listener_id: u32) -> Option<&TcpRoute> {
        if self.routes.len() <= 16 {
            self.routes.iter().find(|r| r.listener_id == listener_id)
        } else {
            self.routes
                .binary_search_by_key(&listener_id, |r| r.listener_id)
                .ok()
                .map(|idx| &self.routes[idx])
        }
    }

    /// Returns the total number of configured TCP routes.
    #[inline]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Returns whether the router has no configured routes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::fnv1a_hash_32;

    #[test]
    fn test_tcp_router_lookup() {
        let l1 = fnv1a_hash_32("postgres-in");
        let l2 = fnv1a_hash_32("redis-in");
        let routes = vec![TcpRoute::new(1, l1, 10), TcpRoute::new(2, l2, 20)];

        let router = TcpRouter::new(routes).unwrap();
        assert_eq!(router.len(), 2);
        assert!(!router.is_empty());

        let r1 = router.route(l1).unwrap();
        assert_eq!(r1.id, 1);
        assert_eq!(r1.upstream_id, 10);

        let r2 = router.route(l2).unwrap();
        assert_eq!(r2.id, 2);
        assert_eq!(r2.upstream_id, 20);

        assert!(router.route(fnv1a_hash_32("unknown")).is_none());
    }

    #[test]
    fn test_tcp_router_duplicate_fails() {
        let l1 = fnv1a_hash_32("redis");
        let routes = vec![TcpRoute::new(1, l1, 10), TcpRoute::new(2, l1, 20)];

        let err = TcpRouter::new(routes).unwrap_err();
        assert!(matches!(err, RouterError::DuplicateRoute { .. }));
    }
}
