//! Peak EWMA (Least Response Time) load balancing.
//!
//! Uses Exponentially Weighted Moving Average (EWMA) of observed response latency,
//! multiplied by the in-flight penalty, to route to the lowest latency backend.

use super::random::fast_random_pair;
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

        // Extracts two distinct random candidate indices in 0..n from a single 64-bit PRNG invocation.
        let (r1, r2) = fast_random_pair(n);

        // Direct array indexing eliminates SipHash computation in HashMap.
        if let Some(metrics) = ctx.metrics_slice {
            let cost_at = |idx: usize| -> (u64, u64) {
                let w = endpoints[idx].weight.max(1) as u64;
                if let Some(m) = metrics.get(idx) {
                    let latency = m.latency_ewma_nanos().max(1_000);
                    let inflight = m.inflight_requests() as u64;
                    let cost = latency.saturating_mul(inflight.saturating_add(1));
                    (cost, w)
                } else {
                    (1_000, w)
                }
            };

            let (cost1, w1) = cost_at(r1);
            let (cost2, w2) = cost_at(r2);

            // Comparing (cost1 / w1) <= (cost2 / w2) using 128-bit integer cross-multiplication.
            // Avoids floating-point conversions (cvtsi2sd) and divisions (divsd).
            return if (cost1 as u128) * (w2 as u128) <= (cost2 as u128) * (w1 as u128) {
                Some(r1)
            } else {
                Some(r2)
            };
        }

        let Some(metrics_map) = ctx.metrics_map else {
            return Some(r1);
        };

        let ep1 = &endpoints[r1];
        let ep2 = &endpoints[r2];

        let cost_of = |ep: &Endpoint| -> (u64, u64) {
            let w = ep.weight.max(1) as u64;
            if let Some(m) = metrics_map.get(&(ep.ip, ep.port_be)) {
                let latency = m.latency_ewma_nanos().max(1_000);
                let inflight = m.inflight_requests() as u64;
                let cost = latency.saturating_mul(inflight.saturating_add(1));
                (cost, w)
            } else {
                (1_000, w)
            }
        };

        let (cost1, w1) = cost_of(ep1);
        let (cost2, w2) = cost_of(ep2);

        if (cost1 as u128) * (w2 as u128) <= (cost2 as u128) * (w1 as u128) {
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

    #[test]
    fn test_peak_ewma_prefers_lower_latency() {
        let ep1 = Endpoint::from_v4([10, 0, 0, 1], 8080, 1);
        let ep2 = Endpoint::from_v4([10, 0, 0, 2], 8080, 1);

        let endpoints = vec![ep1, ep2];

        let mut metrics = HashMap::new();
        let m1 = EndpointMetrics::new();
        let m2 = EndpointMetrics::new();

        // ep1 has high latency (100 ms)
        m1.record_latency_nanos(100_000_000);

        // ep2 has fast latency (5 ms)
        m2.record_latency_nanos(5_000_000);

        metrics.insert((ep1.ip, ep1.port_be), m1);
        metrics.insert((ep2.ip, ep2.port_be), m2);

        let ctx = SelectionContext::NONE.with_metrics(&metrics);
        let balancer = PeakEwma::new();

        for _ in 0..100 {
            let chosen = balancer.select(&endpoints, &ctx).unwrap();
            assert_eq!((chosen.ip, chosen.port_be), (ep2.ip, ep2.port_be));
        }
    }

    #[test]
    fn test_peak_ewma_overflow_resilience() {
        let ep1 = Endpoint::from_v4([10, 0, 0, 1], 8080, 1);
        let ep2 = Endpoint::from_v4([10, 0, 0, 2], 8080, 1);
        let endpoints = vec![ep1, ep2];

        let mut metrics = HashMap::new();
        let m1 = EndpointMetrics::new();
        let m2 = EndpointMetrics::new();

        // Extreme degraded conditions: high latency and near-max inflight
        m1.record_latency_nanos(u64::MAX / 2);
        m1.inc_inflight();

        m2.record_latency_nanos(10_000);

        metrics.insert((ep1.ip, ep1.port_be), m1);
        metrics.insert((ep2.ip, ep2.port_be), m2);

        let ctx = SelectionContext::NONE.with_metrics(&metrics);
        let balancer = PeakEwma::new();

        // Must not panic on arithmetic overflow
        let chosen = balancer.select(&endpoints, &ctx);
        assert!(chosen.is_some());
    }
}
