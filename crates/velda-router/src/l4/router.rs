use std::collections::HashMap;

use velda_core::TransportProtocol;

use super::route::L4Route;
use crate::error::RouterError;

/// Route table for a single L4 listener holding dedicated slots for TCP and UDP.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ListenerL4Router {
    pub tcp: Option<L4Route>,
    pub udp: Option<L4Route>,
}

/// High-performance, in-memory L4 route lookup table.
///
/// Provides zero-allocation $O(1)$ route resolution
/// from `(listener_id, protocol)` to [`L4Route`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct L4Router {
    listeners: HashMap<String, ListenerL4Router>,
    total_routes: usize,
}

impl L4Router {
    /// Builds a new [`L4Router`] from a list of compiled [`L4Route`] rules.
    pub fn new(routes: impl IntoIterator<Item = L4Route>) -> Result<Self, RouterError> {
        let mut listeners: HashMap<String, ListenerL4Router> = HashMap::new();
        let mut total_routes = 0;

        for r in routes {
            let entry = listeners.entry(r.listener_id.clone()).or_default();
            match r.protocol {
                TransportProtocol::Tcp => {
                    if entry.tcp.is_some() {
                        return Err(RouterError::DuplicateRoute {
                            listener: r.listener_id,
                            protocol: "Tcp".to_string(),
                        });
                    }
                    entry.tcp = Some(r);
                }
                TransportProtocol::Udp => {
                    if entry.udp.is_some() {
                        return Err(RouterError::DuplicateRoute {
                            listener: r.listener_id,
                            protocol: "Udp".to_string(),
                        });
                    }
                    entry.udp = Some(r);
                }
            }
            total_routes += 1;
        }

        Ok(Self {
            listeners,
            total_routes,
        })
    }

    /// Resolves an L4 route for an incoming connection on a listener with protocol.
    ///
    /// Performs zero allocations on the request serving hot path.
    #[inline]
    pub fn route(&self, listener_id: &str, protocol: TransportProtocol) -> Option<&L4Route> {
        let listener = self.listeners.get(listener_id)?;
        match protocol {
            TransportProtocol::Tcp => listener.tcp.as_ref(),
            TransportProtocol::Udp => listener.udp.as_ref(),
        }
    }

    /// Returns the total number of configured L4 routes.
    #[inline]
    pub fn len(&self) -> usize {
        self.total_routes
    }

    /// Returns whether the router has no configured routes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_routes == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{RouteId, UpstreamId};

    #[test]
    fn test_l4_router_lookup() {
        let routes = vec![
            L4Route::new(
                RouteId::new(1),
                "postgres-in",
                TransportProtocol::Tcp,
                UpstreamId::new(10),
                "postgres-backend",
            ),
            L4Route::new(
                RouteId::new(2),
                "dns-in",
                TransportProtocol::Udp,
                UpstreamId::new(20),
                "dns-backend",
            ),
        ];

        let router = L4Router::new(routes).unwrap();

        assert_eq!(router.len(), 2);
        assert!(!router.is_empty());

        let tcp_route = router.route("postgres-in", TransportProtocol::Tcp).unwrap();
        assert_eq!(tcp_route.id, RouteId::new(1));
        assert_eq!(tcp_route.upstream_id, UpstreamId::new(10));
        assert_eq!(tcp_route.upstream_name, "postgres-backend");

        let udp_route = router.route("dns-in", TransportProtocol::Udp).unwrap();
        assert_eq!(udp_route.id, RouteId::new(2));
        assert_eq!(udp_route.upstream_id, UpstreamId::new(20));
        assert_eq!(udp_route.upstream_name, "dns-backend");

        // Wrong protocol on same listener returns None
        assert!(
            router
                .route("postgres-in", TransportProtocol::Udp)
                .is_none()
        );
        // Unknown listener returns None
        assert!(router.route("unknown", TransportProtocol::Tcp).is_none());
    }

    #[test]
    fn test_l4_router_duplicate_fails() {
        let routes = vec![
            L4Route::new(
                RouteId::new(1),
                "redis",
                TransportProtocol::Tcp,
                UpstreamId::new(10),
                "redis-1",
            ),
            L4Route::new(
                RouteId::new(2),
                "redis",
                TransportProtocol::Tcp,
                UpstreamId::new(20),
                "redis-2",
            ),
        ];

        let err = L4Router::new(routes).unwrap_err();
        assert!(matches!(err, RouterError::DuplicateRoute { .. }));
    }
}
