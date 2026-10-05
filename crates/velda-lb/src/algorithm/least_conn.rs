//! Least Connections, Least Requests, and Weighted Least Requests load balancing.

use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

/// Chooses the endpoint with the smallest number of active connections.
#[derive(Debug, Default)]
pub struct LeastConnections;

impl LeastConnections {
    /// Creates a new least-connections load balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for LeastConnections {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }

        // Contiguous metrics slice path: direct array indexing eliminates SipHash computation.
        if let Some(metrics) = ctx.metrics_slice {
            let limit = endpoints.len().min(metrics.len());
            if limit == 0 {
                return Some(0);
            }
            let mut best_idx = 0;
            let mut min_load = metrics[0].active_connections();
            for (i, m) in metrics[..limit].iter().enumerate().skip(1) {
                let load = m.active_connections();
                if load < min_load {
                    min_load = load;
                    best_idx = i;
                }
            }
            return Some(best_idx);
        }

        let Some(metrics_map) = ctx.metrics_map else {
            return Some(0);
        };

        let mut best_idx = 0;
        let mut min_load = metrics_map
            .get(&endpoints[0].address)
            .map(|m| m.active_connections())
            .unwrap_or(0);

        for (i, ep) in endpoints.iter().enumerate().skip(1) {
            let load = metrics_map
                .get(&ep.address)
                .map(|m| m.active_connections())
                .unwrap_or(0);

            if load < min_load {
                min_load = load;
                best_idx = i;
            }
        }

        Some(best_idx)
    }
}

/// Chooses the endpoint with the smallest number of currently in-flight requests.
#[derive(Debug, Default)]
pub struct LeastRequests;

impl LeastRequests {
    /// Creates a new least-requests load balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for LeastRequests {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }

        // Contiguous metrics slice path: direct array indexing eliminates SipHash computation.
        if let Some(metrics) = ctx.metrics_slice {
            let limit = endpoints.len().min(metrics.len());
            if limit == 0 {
                return Some(0);
            }
            let mut best_idx = 0;
            let mut min_inflight = metrics[0].inflight_requests();
            for (i, m) in metrics[..limit].iter().enumerate().skip(1) {
                let inflight = m.inflight_requests();
                if inflight < min_inflight {
                    min_inflight = inflight;
                    best_idx = i;
                }
            }
            return Some(best_idx);
        }

        let Some(metrics_map) = ctx.metrics_map else {
            return Some(0);
        };

        let mut best_idx = 0;
        let mut min_inflight = metrics_map
            .get(&endpoints[0].address)
            .map(|m| m.inflight_requests())
            .unwrap_or(0);

        for (i, ep) in endpoints.iter().enumerate().skip(1) {
            let inflight = metrics_map
                .get(&ep.address)
                .map(|m| m.inflight_requests())
                .unwrap_or(0);

            if inflight < min_inflight {
                min_inflight = inflight;
                best_idx = i;
            }
        }

        Some(best_idx)
    }
}

/// Weighted Least Requests load balancer (in-flight load normalized by endpoint weight).
#[derive(Debug, Default)]
pub struct WeightedLeastRequests;

impl WeightedLeastRequests {
    /// Creates a new weighted least-requests load balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for WeightedLeastRequests {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }

        // Direct array indexing eliminates SipHash computation and pointer chasing in HashMap.
        if let Some(metrics) = ctx.metrics_slice {
            let limit = endpoints.len().min(metrics.len());
            if limit == 0 {
                return Some(0);
            }
            let mut best_idx = 0;
            let mut min_inflight = metrics[0].inflight_requests() as u64;
            let mut min_weight = endpoints[0].weight.max(1) as u64;

            for i in 1..limit {
                let inflight = metrics[i].inflight_requests() as u64;
                let weight = endpoints[i].weight.max(1) as u64;
                // Comparing (inflight / weight) < (min_inflight / min_weight) using integer cross-multiplication.
                // Eliminates floating-point conversion (cvtsi2sd) and division (divsd) instructions.
                if inflight * min_weight < min_inflight * weight {
                    min_inflight = inflight;
                    min_weight = weight;
                    best_idx = i;
                }
            }
            return Some(best_idx);
        }

        let Some(metrics_map) = ctx.metrics_map else {
            return Some(0);
        };

        let mut best_idx = 0;
        let mut min_inflight = metrics_map
            .get(&endpoints[0].address)
            .map_or(0, |m| m.inflight_requests()) as u64;
        let mut min_weight = endpoints[0].weight.max(1) as u64;

        for (i, ep) in endpoints.iter().enumerate().skip(1) {
            let inflight = metrics_map
                .get(&ep.address)
                .map_or(0, |m| m.inflight_requests()) as u64;
            let weight = ep.weight.max(1) as u64;
            if inflight * min_weight < min_inflight * weight {
                min_inflight = inflight;
                min_weight = weight;
                best_idx = i;
            }
        }

        Some(best_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::EndpointMetrics;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    #[test]
    fn test_least_connections_selects_minimum() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let ep3: SocketAddr = "10.0.0.3:8080".parse().unwrap();

        let endpoints = vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
            Endpoint::new("e3", ep3, 1),
        ];

        let mut metrics = HashMap::new();
        let m1 = EndpointMetrics::new();
        let m2 = EndpointMetrics::new();
        let m3 = EndpointMetrics::new();

        m1.inc_active();
        m1.inc_active(); // 2 active
        m2.inc_active(); // 1 active
        m3.inc_active();
        m3.inc_active();
        m3.inc_active(); // 3 active

        metrics.insert(ep1, m1);
        metrics.insert(ep2, m2);
        metrics.insert(ep3, m3);

        let ctx = SelectionContext::NONE.with_metrics(&metrics);
        let balancer = LeastConnections::new();

        let selected = balancer.select(&endpoints, &ctx).unwrap();
        assert_eq!(selected.address, ep2);
    }
}
