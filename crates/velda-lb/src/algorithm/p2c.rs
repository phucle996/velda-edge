//! Power of Two Choices (P2C) load balancing.
//!
//! Mitigates herd behavior inherent in classic Least Connections by sampling 2 random
//! candidates and picking the least loaded. Proven $O(1)$ constant time performance.

use super::random::fast_random_u64;
use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

/// Power of Two Random Choices load balancer.
#[derive(Debug, Default)]
pub struct PowerOfTwoChoices;

impl PowerOfTwoChoices {
    /// Creates a new P2C load balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for PowerOfTwoChoices {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        let n = endpoints.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(0);
        }

        // Pick two distinct random indices
        let r1 = (fast_random_u64() as usize) % n;
        let mut r2 = (fast_random_u64() as usize) % (n - 1);
        if r2 >= r1 {
            r2 += 1;
        }

        let Some(metrics_map) = ctx.metrics_map else {
            return Some(r1);
        };

        let ep1 = &endpoints[r1];
        let ep2 = &endpoints[r2];

        let load_of = |ep: &Endpoint| -> f64 {
            if let Some(m) = metrics_map.get(&ep.address) {
                let load = m.active_connections() + m.inflight_requests();
                load as f64 / ep.weight.max(1) as f64
            } else {
                0.0
            }
        };

        if load_of(ep1) <= load_of(ep2) {
            Some(r1)
        } else {
            Some(r2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::EndpointMetrics;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    #[test]
    fn test_p2c_selection_prefers_lower_load() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let endpoints = vec![Endpoint::new("e1", ep1, 1), Endpoint::new("e2", ep2, 1)];

        let mut metrics = HashMap::new();
        let m1 = EndpointMetrics::new();
        let m2 = EndpointMetrics::new();

        // ep1 is heavily loaded (100 in-flight)
        for _ in 0..100 {
            m1.inc_inflight();
        }

        metrics.insert(ep1, m1);
        metrics.insert(ep2, m2);

        let ctx = SelectionContext::NONE.with_metrics(&metrics);
        let balancer = PowerOfTwoChoices::new();

        // P2C should choose the unloaded ep2
        for _ in 0..50 {
            let selected = balancer.select(&endpoints, &ctx).unwrap();
            assert_eq!(selected.address, ep2);
        }
    }
}
