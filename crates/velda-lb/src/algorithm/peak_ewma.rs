//! Peak EWMA (Least Response Time) load balancing.
//!
//! Uses Exponentially Weighted Moving Average (EWMA) of observed response latency,
//! multiplied by the in-flight penalty, to route to the lowest latency backend.

use super::random::fast_random_u64;
use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

/// Peak EWMA / Least Response Time load balancer using P2C sampling.
#[derive(Debug, Default)]
pub struct PeakEwma;

impl PeakEwma {
    /// Creates a new Peak EWMA load balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for PeakEwma {
    fn select_index(&self, endpoints: &[Endpoint], ctx: &SelectionContext<'_>) -> Option<usize> {
        let n = endpoints.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(0);
        }

        // P2C sampling for O(1) performance
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

        let cost_of = |ep: &Endpoint| -> f64 {
            if let Some(m) = metrics_map.get(&ep.address) {
                let latency = m.latency_ewma_nanos().max(1_000); // minimum 1µs baseline
                let inflight = m.inflight_requests() as u64;
                // BLOCKER FIX: Prevent integer overflow panic under degraded backend conditions.
                // High latency (in nanos) * high inflight count can exceed u64::MAX.
                // Use saturating arithmetic to safely cap at u64::MAX.
                let cost = latency.saturating_mul(inflight.saturating_add(1));
                cost as f64 / ep.weight.max(1) as f64
            } else {
                1_000.0 / ep.weight.max(1) as f64
            }
        };

        if cost_of(ep1) <= cost_of(ep2) {
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
    fn test_peak_ewma_prefers_lower_latency() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let endpoints = vec![Endpoint::new("e1", ep1, 1), Endpoint::new("e2", ep2, 1)];

        let mut metrics = HashMap::new();
        let m1 = EndpointMetrics::new();
        let m2 = EndpointMetrics::new();

        // ep1 has high latency (100 ms)
        m1.record_latency_nanos(100_000_000);

        // ep2 has fast latency (5 ms)
        m2.record_latency_nanos(5_000_000);

        metrics.insert(ep1, m1);
        metrics.insert(ep2, m2);

        let ctx = SelectionContext::NONE.with_metrics(&metrics);
        let balancer = PeakEwma::new();

        for _ in 0..100 {
            let chosen = balancer.select(&endpoints, &ctx).unwrap();
            assert_eq!(chosen.address, ep2);
        }
    }

    #[test]
    fn test_peak_ewma_overflow_resilience() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let endpoints = vec![Endpoint::new("e1", ep1, 1), Endpoint::new("e2", ep2, 1)];

        let mut metrics = HashMap::new();
        let m1 = EndpointMetrics::new();
        let m2 = EndpointMetrics::new();

        // Extreme degraded conditions: high latency and near-max inflight
        m1.record_latency_nanos(u64::MAX / 2);
        m1.inc_inflight();

        m2.record_latency_nanos(10_000);

        metrics.insert(ep1, m1);
        metrics.insert(ep2, m2);

        let ctx = SelectionContext::NONE.with_metrics(&metrics);
        let balancer = PeakEwma::new();

        // Must not panic on arithmetic overflow
        let chosen = balancer.select(&endpoints, &ctx);
        assert!(chosen.is_some());
    }
}
