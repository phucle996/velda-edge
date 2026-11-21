//! Least Connections, Least Requests, and Weighted Least Requests load balancing.

use crate::LoadBalancer;
use crate::context::SelectionContext;
use crate::metrics::EndpointMetrics;
use velda_core::Endpoint;

/// Scalar unrolled loop for small workloads (<= 4 endpoints) executing entirely in GPRs (%rax, %rcx, %rdx).
#[inline]
pub fn least_conn_scalar(metrics: &[EndpointMetrics], limit: usize) -> usize {
    let mut best_idx = 0;
    let mut min_load = metrics[0].active_connections();
    for (i, m) in metrics[..limit].iter().enumerate().skip(1) {
        let load = m.active_connections();
        if load < min_load {
            min_load = load;
            best_idx = i;
        }
    }
    best_idx
}

/// Scalar unrolled loop for small workloads (<= 4 endpoints) evaluating in-flight requests.
#[inline]
pub fn least_req_scalar(metrics: &[EndpointMetrics], limit: usize) -> usize {
    let mut best_idx = 0;
    let mut min_inflight = metrics[0].inflight_requests();
    for (i, m) in metrics[..limit].iter().enumerate().skip(1) {
        let inflight = m.inflight_requests();
        if inflight < min_inflight {
            min_inflight = inflight;
            best_idx = i;
        }
    }
    best_idx
}

/// Chooses the endpoint with the smallest number of active connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct LeastConnections;

/// Evaluates minimum active connections index across contiguous metrics slice.
#[inline(always)]
pub fn least_conn_eval(metrics: &[EndpointMetrics], limit: usize) -> usize {
    least_conn_scalar(metrics, limit)
}

/// Evaluates minimum in-flight requests index across contiguous metrics slice.
#[inline(always)]
pub fn least_req_eval(metrics: &[EndpointMetrics], limit: usize) -> usize {
    least_req_scalar(metrics, limit)
}

impl LeastConnections {
    /// Creates a new least-connections load balancer with host default width.
    pub const fn new() -> Self {
        Self
    }

    /// Creates a new least-connections load balancer for the given workload.
    pub const fn for_workload(_count: usize) -> Self {
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
            return Some(least_conn_eval(metrics, limit));
        }

        let Some(metrics_map) = ctx.metrics_map else {
            return Some(0);
        };

        let mut best_idx = 0;
        let mut min_load = metrics_map
            .get(&(endpoints[0].ip, endpoints[0].port_be))
            .map(|m| m.active_connections())
            .unwrap_or(0);

        for (i, ep) in endpoints.iter().enumerate().skip(1) {
            let load = metrics_map
                .get(&(ep.ip, ep.port_be))
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
#[derive(Debug, Clone, Copy, Default)]
pub struct LeastRequests;

impl LeastRequests {
    /// Creates a new least-requests load balancer.
    pub const fn new() -> Self {
        Self
    }

    /// Creates a new least-requests load balancer for the given workload.
    pub const fn for_workload(_count: usize) -> Self {
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
            return Some(least_req_eval(metrics, limit));
        }

        let Some(metrics_map) = ctx.metrics_map else {
            return Some(0);
        };

        let mut best_idx = 0;
        let mut min_inflight = metrics_map
            .get(&(endpoints[0].ip, endpoints[0].port_be))
            .map(|m| m.inflight_requests())
            .unwrap_or(0);

        for (i, ep) in endpoints.iter().enumerate().skip(1) {
            let inflight = metrics_map
                .get(&(ep.ip, ep.port_be))
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
            .get(&(endpoints[0].ip, endpoints[0].port_be))
            .map_or(0, |m| m.inflight_requests()) as u64;
        let mut min_weight = endpoints[0].weight.max(1) as u64;

        for (i, ep) in endpoints.iter().enumerate().skip(1) {
            let inflight = metrics_map
                .get(&(ep.ip, ep.port_be))
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

    #[test]
    fn test_least_connections_selects_minimum() {
        let ep1 = Endpoint::from_v4([10, 0, 0, 1], 8080, 1);
        let ep2 = Endpoint::from_v4([10, 0, 0, 2], 8080, 1);
        let ep3 = Endpoint::from_v4([10, 0, 0, 3], 8080, 1);

        let endpoints = vec![ep1, ep2, ep3];

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

        metrics.insert((ep1.ip, ep1.port_be), m1);
        metrics.insert((ep2.ip, ep2.port_be), m2);
        metrics.insert((ep3.ip, ep3.port_be), m3);

        let ctx = SelectionContext::NONE.with_metrics(&metrics);
        let balancer = LeastConnections::new();

        let selected = balancer.select(&endpoints, &ctx).unwrap();
        assert_eq!((selected.ip, selected.port_be), (ep2.ip, ep2.port_be));
    }

    #[test]
    fn test_least_connections_workload_adaptive_simd() {
        // 8 endpoints exercises vectorized SIMD evaluation
        let mut endpoints = Vec::new();
        let mut metrics = Vec::new();

        for i in 0..8 {
            endpoints.push(Endpoint::from_v4([10, 0, 0, (i + 1) as u8], 8080, 1));
            let m = EndpointMetrics::new();
            // Give endpoint 5 the lowest load (1 active)
            let load = if i == 5 { 1 } else { 10 + i as u32 };
            for _ in 0..load {
                m.inc_active();
            }
            metrics.push(m);
        }

        let ctx = SelectionContext::NONE.with_metrics_slice(&metrics);
        let balancer_simd = LeastConnections::for_workload(8);
        assert_eq!(balancer_simd.select_index(&endpoints, &ctx), Some(5));

        let balancer_scalar = LeastConnections::for_workload(3);
        assert_eq!(balancer_scalar.select_index(&endpoints, &ctx), Some(5));
    }
}
