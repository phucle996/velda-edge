//! Uniform Random and Weighted Random load balancing.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

static GLOBAL_SEED: AtomicU64 = AtomicU64::new(0x853c49e6748fea9b);

thread_local! {
    static RNG_STATE: Cell<u64> = const { Cell::new(0) };
}

/// Fast thread-local XorShift64* PRNG running in ~1 nanosecond with zero dependencies.
/// Uses const-initialized TLS on Linux x86_64, eliminating all runtime initialization checks.
#[inline(always)]
pub(crate) fn fast_random_u64() -> u64 {
    RNG_STATE.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            let seed = GLOBAL_SEED.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
            let mut z = seed ^ 0xbf58476d1ce4e5b9;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            x = z ^ (z >> 31);
            if x == 0 {
                x = 0x853c49e6748fea9b;
            }
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545f4914f6cdd1d)
    })
}

/// Daniel Lemire's fast alternative to modulo reduction for uniform bounded random integers.
///
/// Replaces the expensive 64-bit hardware integer division (`idivq`, 15-25 CPU cycles)
/// with a 128-bit multiplication and right shift (3-4 CPU cycles), executing with zero pipeline stalls.
#[inline(always)]
pub(crate) fn fast_reduce(val: u64, n: usize) -> usize {
    ((val as u128 * n as u128) >> 64) as usize
}

/// Extracts two distinct random indices in `0..n` from a single 64-bit PRNG invocation.
///
/// Splits the 64-bit entropy into two independent 32-bit halves and applies 32-bit Lemire
/// reduction. Reduces PRNG generation cost by 50% for P2C and PeakEWMA sampling.
#[inline(always)]
pub(crate) fn fast_random_pair(n: usize) -> (usize, usize) {
    let rand = fast_random_u64();
    let low = rand as u32;
    let high = (rand >> 32) as u32;
    let r1 = ((low as u64 * n as u64) >> 32) as usize;
    let mut r2 = ((high as u64 * (n - 1) as u64) >> 32) as usize;
    if r2 >= r1 {
        r2 += 1;
    }
    (r1, r2)
}

/// Uniform Random load balancer.
#[derive(Debug, Default)]
pub struct Random;

impl Random {
    /// Creates a new uniform random load balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for Random {
    fn select_index(&self, endpoints: &[Endpoint], _ctx: &SelectionContext<'_>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        if endpoints.len() == 1 {
            return Some(0);
        }
        Some(fast_reduce(fast_random_u64(), endpoints.len()))
    }
}

/// Weighted Random load balancer.
#[derive(Debug, Default)]
pub struct WeightedRandom;

impl WeightedRandom {
    /// Creates a new weighted random load balancer.
    pub const fn new() -> Self {
        Self
    }
}

impl LoadBalancer for WeightedRandom {
    fn select_index(&self, endpoints: &[Endpoint], _ctx: &SelectionContext<'_>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        if endpoints.len() == 1 {
            return Some(0);
        }

        let total_weight: u64 = endpoints.iter().map(|e| e.weight as u64).sum();
        if total_weight == 0 {
            return Some(0);
        }

        let mut target = ((fast_random_u64() as u128 * total_weight as u128) >> 64) as u64;
        for (i, ep) in endpoints.iter().enumerate() {
            let w = ep.weight as u64;
            if target < w {
                return Some(i);
            }
            target -= w;
        }

        Some(endpoints.len() - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    #[test]
    fn test_random_distribution() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let endpoints = vec![Endpoint::new("e1", ep1, 1), Endpoint::new("e2", ep2, 1)];

        let balancer = Random::new();
        let ctx = SelectionContext::NONE;

        let mut count1 = 0;
        let mut count2 = 0;

        for _ in 0..10_000 {
            let selected = balancer.select(&endpoints, &ctx).unwrap();
            if selected.address == ep1 {
                count1 += 1;
            } else {
                count2 += 1;
            }
        }

        // Both endpoints should receive roughly half of the traffic
        assert!(count1 > 4_000 && count1 < 6_000);
        assert!(count2 > 4_000 && count2 < 6_000);
    }

    #[test]
    fn test_weighted_random_distribution() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        // 3:1 ratio
        let endpoints = vec![Endpoint::new("e1", ep1, 3), Endpoint::new("e2", ep2, 1)];

        let balancer = WeightedRandom::new();
        let ctx = SelectionContext::NONE;

        let mut counts = HashMap::new();
        for _ in 0..10_000 {
            let selected = balancer.select(&endpoints, &ctx).unwrap();
            *counts.entry(selected.address).or_insert(0) += 1;
        }

        let c1 = *counts.get(&ep1).unwrap();
        let c2 = *counts.get(&ep2).unwrap();

        // c1 should be ~7500, c2 ~2500
        assert!(c1 > 6_800 && c1 < 8_200);
        assert!(c2 > 1_800 && c2 < 3_200);
    }
}
