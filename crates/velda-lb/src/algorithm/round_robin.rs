//! Standard Round-Robin load balancing.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

thread_local! {
    /// Worker thread monotonic identifier for shard routing.
    /// Const-initialized TLS compiles to a single %fs:[offset] read without runtime init guards.
    static RR_SHARD_ID: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[inline(always)]
fn get_rr_shard_id() -> usize {
    RR_SHARD_ID.with(|cell| {
        let mut id = cell.get();
        if id == usize::MAX {
            static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
            id = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
            cell.set(id);
        }
        id
    })
}

// 64-byte CPU cache alignment ensures each counter shard sits on its own cache line,
// completely eliminating inter-core Cache Line Bouncing and false sharing across worker threads.
#[repr(align(64))]
#[derive(Debug, Default)]
struct ShardCounter {
    index: AtomicUsize,
}

/// Sharded Round-Robin load balancer.
///
/// Divides atomic counter state across cache-aligned shards. Each worker thread operates
/// on an independent cache line, scaling to hundreds of millions of ops/s with zero cross-core contention.
#[derive(Debug)]
pub struct RoundRobin {
    shards: Box<[ShardCounter]>,
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobin {
    /// Creates a new sharded round-robin load balancer reading worker concurrency from RAM.
    #[inline]
    pub fn new() -> Self {
        Self::with_workers(velda_core::global_hardware_topology().worker_threads)
    }

    /// Creates a new sharded round-robin load balancer configured with explicit worker count
    /// probed by the composition root (`velda-edge`).
    ///
    /// Because RoundRobin has zero read-sum overhead, shard count scales 1:1 with
    /// worker concurrency to completely eliminate multi-core cache invalidation.
    pub fn with_workers(workers: usize) -> Self {
        let count = workers.max(1).next_power_of_two();
        let shards = (0..count)
            .map(|_| ShardCounter {
                index: AtomicUsize::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { shards }
    }
}

impl LoadBalancer for RoundRobin {
    fn select_index(&self, endpoints: &[Endpoint], _ctx: &SelectionContext<'_>) -> Option<usize> {
        let n = endpoints.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(0);
        }

        // Shard count is guaranteed to be a power of two, enabling a 1-cycle bitmask
        // to replace 64-bit hardware integer division (`idivq`).
        let shard_idx = get_rr_shard_id() & (self.shards.len() - 1);
        let idx = self.shards[shard_idx].index.fetch_add(1, Ordering::Relaxed);

        // When endpoint count is a power of two (common in edge clusters: 2, 4, 8, 16),
        // bitmasking achieves zero-division round-robin distribution.
        if n & (n - 1) == 0 {
            Some(idx & (n - 1))
        } else {
            Some(idx % n)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_round_robin_distribution() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let ep3: SocketAddr = "10.0.0.3:8080".parse().unwrap();

        let endpoints = vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
            Endpoint::new("e3", ep3, 1),
        ];

        let balancer = RoundRobin::new();
        let ctx = SelectionContext::NONE;

        let s1 = balancer.select(&endpoints, &ctx).unwrap();
        let s2 = balancer.select(&endpoints, &ctx).unwrap();
        let s3 = balancer.select(&endpoints, &ctx).unwrap();
        let s4 = balancer.select(&endpoints, &ctx).unwrap();

        assert_eq!(s1.address, ep1);
        assert_eq!(s2.address, ep2);
        assert_eq!(s3.address, ep3);
        assert_eq!(s4.address, ep1);
    }

    #[test]
    fn test_worker_probed_scaling() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let endpoints = vec![Endpoint::new("e1", ep1, 1)];
        let ctx = SelectionContext::NONE;

        // Verify custom probed worker scales
        for workers in [1, 2, 4, 8, 16, 64] {
            let rr = RoundRobin::with_workers(workers);
            assert_eq!(rr.shards.len(), workers.next_power_of_two());
            assert!(rr.select(&endpoints, &ctx).is_some());
        }
    }
}
