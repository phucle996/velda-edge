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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_min_index_u32_avx2(loads: &[u32]) -> usize {
    use std::arch::x86_64::*;
    let len = loads.len();
    if len <= 1 {
        return 0;
    }

    let ptr = loads.as_ptr();
    unsafe {
        let mut min_vec = _mm256_set1_epi32(i32::MAX);
        let chunks = len / 8;

        for i in 0..chunks {
            let v = _mm256_loadu_si256(ptr.add(i * 8) as *const __m256i);
            min_vec = _mm256_min_epu32(min_vec, v);
        }

        let low = _mm256_castsi256_si128(min_vec);
        let high = _mm256_extracti128_si256(min_vec, 1);
        let min128 = _mm_min_epu32(low, high);
        let min64 = _mm_min_epu32(min128, _mm_shuffle_epi32(min128, 0x4E));
        let min32 = _mm_min_epu32(min64, _mm_shuffle_epi32(min64, 0xB1));
        let mut min_val = _mm_cvtsi128_si32(min32) as u32;

        let remainder_start = chunks * 8;
        for i in remainder_start..len {
            let val = *ptr.add(i);
            if val < min_val {
                min_val = val;
            }
        }

        let min_broadcast = _mm256_set1_epi32(min_val as i32);
        for i in 0..chunks {
            let v = _mm256_loadu_si256(ptr.add(i * 8) as *const __m256i);
            let eq = _mm256_cmpeq_epi32(v, min_broadcast);
            let mask = _mm256_movemask_ps(_mm256_castsi256_ps(eq));
            if mask != 0 {
                return i * 8 + (mask.trailing_zeros() as usize);
            }
        }

        for i in remainder_start..len {
            if *ptr.add(i) == min_val {
                return i;
            }
        }

        0
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn find_min_index_u32_sse41(loads: &[u32]) -> usize {
    use std::arch::x86_64::*;
    let len = loads.len();
    if len <= 1 {
        return 0;
    }

    let ptr = loads.as_ptr();
    unsafe {
        let mut min_vec = _mm_set1_epi32(i32::MAX);
        let chunks = len / 4;

        for i in 0..chunks {
            let v = _mm_loadu_si128(ptr.add(i * 4) as *const __m128i);
            min_vec = _mm_min_epu32(min_vec, v);
        }

        let min64 = _mm_min_epu32(min_vec, _mm_shuffle_epi32(min_vec, 0x4E));
        let min32 = _mm_min_epu32(min64, _mm_shuffle_epi32(min64, 0xB1));
        let mut min_val = _mm_cvtsi128_si32(min32) as u32;

        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            let val = *ptr.add(i);
            if val < min_val {
                min_val = val;
            }
        }

        let min_broadcast = _mm_set1_epi32(min_val as i32);
        for i in 0..chunks {
            let v = _mm_loadu_si128(ptr.add(i * 4) as *const __m128i);
            let eq = _mm_cmpeq_epi32(v, min_broadcast);
            let mask = _mm_movemask_ps(_mm_castsi128_ps(eq));
            if mask != 0 {
                return i * 4 + (mask.trailing_zeros() as usize);
            }
        }

        for i in remainder_start..len {
            if *ptr.add(i) == min_val {
                return i;
            }
        }

        0
    }
}

#[cfg(target_arch = "aarch64")]
unsafe fn find_min_index_u32_neon(loads: &[u32]) -> usize {
    use std::arch::aarch64::*;
    let len = loads.len();
    if len <= 1 {
        return 0;
    }
    let ptr = loads.as_ptr();
    unsafe {
        let mut min_vec = vdupq_n_u32(u32::MAX);
        let chunks = len / 4;
        for i in 0..chunks {
            let v = vld1q_u32(ptr.add(i * 4));
            min_vec = vminq_u32(min_vec, v);
        }
        let mut min_val = vminvq_u32(min_vec);
        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            let val = *ptr.add(i);
            if val < min_val {
                min_val = val;
            }
        }
        for i in 0..len {
            if *ptr.add(i) == min_val {
                return i;
            }
        }
        0
    }
}

/// 256-bit AVX2-accelerated minimum search across active connection counters.
#[inline]
pub fn least_conn_avx2(metrics: &[EndpointMetrics], limit: usize) -> usize {
    if limit <= 4 {
        return least_conn_scalar(metrics, limit);
    }
    let mut loads = [0u32; 64];
    let count = limit.min(64);
    for i in 0..count {
        loads[i] = metrics[i].active_connections();
    }
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { find_min_index_u32_avx2(&loads[..count]) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        least_conn_scalar(metrics, limit)
    }
}

/// 128-bit SSE4.1/NEON-accelerated minimum search across active connection counters.
#[inline]
pub fn least_conn_sse2(metrics: &[EndpointMetrics], limit: usize) -> usize {
    if limit <= 4 {
        return least_conn_scalar(metrics, limit);
    }
    let mut loads = [0u32; 64];
    let count = limit.min(64);
    for i in 0..count {
        loads[i] = metrics[i].active_connections();
    }
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { find_min_index_u32_sse41(&loads[..count]) }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { find_min_index_u32_neon(&loads[..count]) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        least_conn_scalar(metrics, limit)
    }
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

/// 256-bit AVX2-accelerated minimum search across in-flight request counters.
#[inline]
pub fn least_req_avx2(metrics: &[EndpointMetrics], limit: usize) -> usize {
    if limit <= 4 {
        return least_req_scalar(metrics, limit);
    }
    let mut loads = [0u32; 64];
    let count = limit.min(64);
    for i in 0..count {
        loads[i] = metrics[i].inflight_requests();
    }
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { find_min_index_u32_avx2(&loads[..count]) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        least_req_scalar(metrics, limit)
    }
}

/// 128-bit SSE4.1/NEON-accelerated minimum search across in-flight request counters.
#[inline]
pub fn least_req_sse2(metrics: &[EndpointMetrics], limit: usize) -> usize {
    if limit <= 4 {
        return least_req_scalar(metrics, limit);
    }
    let mut loads = [0u32; 64];
    let count = limit.min(64);
    for i in 0..count {
        loads[i] = metrics[i].inflight_requests();
    }
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { find_min_index_u32_sse41(&loads[..count]) }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { find_min_index_u32_neon(&loads[..count]) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        least_req_scalar(metrics, limit)
    }
}

/// Chooses the endpoint with the smallest number of active connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct LeastConnections;

/// Evaluates minimum active connections index across contiguous metrics slice.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn least_conn_eval(metrics: &[EndpointMetrics], limit: usize) -> usize {
    if limit <= 4 {
        return least_conn_scalar(metrics, limit);
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            least_conn_avx2(metrics, limit)
        } else {
            least_conn_sse2(metrics, limit)
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        least_conn_sse2(metrics, limit)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        least_conn_scalar(metrics, limit)
    }
}

/// Evaluates minimum in-flight requests index across contiguous metrics slice.
///
/// Inlined direct execution eliminating indirect function pointer overhead.
#[inline(always)]
pub fn least_req_eval(metrics: &[EndpointMetrics], limit: usize) -> usize {
    if limit <= 4 {
        return least_req_scalar(metrics, limit);
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            least_req_avx2(metrics, limit)
        } else {
            least_req_sse2(metrics, limit)
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        least_req_sse2(metrics, limit)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        least_req_scalar(metrics, limit)
    }
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

    #[test]
    fn test_least_connections_workload_adaptive_simd() {
        // 8 endpoints exercises vectorized SIMD evaluation
        let mut endpoints = Vec::new();
        let mut metrics = Vec::new();

        for i in 0..8 {
            let ep: SocketAddr = format!("10.0.0.{}:8080", i + 1).parse().unwrap();
            endpoints.push(Endpoint::new(format!("e{i}"), ep, 1));
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
