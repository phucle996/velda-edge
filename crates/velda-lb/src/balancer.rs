//! Unified load balancer contract and target backend decision making.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::context::SelectionContext;
use velda_core::Endpoint;

/// Unified contract for load-balancing strategies across Velda Edge.
///
/// Guaranteed zero-copy and zero heap allocation on the request serving hot path.
pub trait LoadBalancer: Send + Sync {
    /// Selects an endpoint index from the slice of candidate endpoints.
    ///
    /// Returning `usize` allows the caller to index directly with zero allocations or data copies.
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize>;

    /// Decides and returns the physical backend socket address (`IP:Port`) directly.
    ///
    /// This is the primary zero-copy method used by Stage 2 (`velda-upstream`)
    /// to immediately construct a `ConnectionKey` for Stage 4 (`velda-pool`).
    #[inline]
    fn select_addr(
        &self,
        endpoints: &[Endpoint],
        ctx: &SelectionContext<'_>,
    ) -> Option<SocketAddr> {
        self.select_index(endpoints, ctx)
            .and_then(|idx| endpoints.get(idx).map(|ep| ep.address))
    }

    /// Zero-copy convenience method returning a borrowed reference to the selected endpoint.
    #[inline]
    fn select<'a>(
        &self,
        endpoints: &'a [Endpoint],
        ctx: &SelectionContext<'_>,
    ) -> Option<&'a Endpoint> {
        self.select_index(endpoints, ctx)
            .and_then(|idx| endpoints.get(idx))
    }
}

impl<T: LoadBalancer + ?Sized> LoadBalancer for Arc<T> {
    #[inline]
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        (**self).select_index(endpoints, ctx)
    }

    #[inline]
    fn select_addr(
        &self,
        endpoints: &[Endpoint],
        ctx: &SelectionContext<'_>,
    ) -> Option<SocketAddr> {
        (**self).select_addr(endpoints, ctx)
    }

    #[inline]
    fn select<'a>(
        &self,
        endpoints: &'a [Endpoint],
        ctx: &SelectionContext<'_>,
    ) -> Option<&'a Endpoint> {
        (**self).select(endpoints, ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm::RoundRobin;

    #[test]
    fn test_select_addr_returns_physical_socket_addr() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let endpoints = vec![Endpoint::new("e1", ep1, 1), Endpoint::new("e2", ep2, 1)];

        let lb = RoundRobin::new();
        let ctx = SelectionContext::NONE;

        let addr1 = lb.select_addr(&endpoints, &ctx).unwrap();
        let addr2 = lb.select_addr(&endpoints, &ctx).unwrap();

        assert_eq!(addr1, ep1);
        assert_eq!(addr2, ep2);
    }
}
