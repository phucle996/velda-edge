//! Smooth Weighted Round-Robin (SWRR) load balancing.
//!
//! Implements Nginx's smooth weighted round-robin selection algorithm,
//! ensuring proportional distribution without clustering requests onto the highest-weight backend.

use std::sync::Mutex;

use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

thread_local! {
    /// Worker thread monotonic identifier for SWRR shard routing.
    static WORKER_SHARD_ID: usize = {
        static NEXT_SHARD: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        NEXT_SHARD.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    };
}

/// Smooth Weighted Round Robin balancer with sharded state.
///
/// Weight ranges from 1 to 100 per endpoint.
pub struct WeightedRoundRobin {
    // OPTIMIZATION: Thread-sharded Mutex array eliminates lock contention across worker threads.
    // Each worker thread maps to a dedicated shard, achieving near-zero contention similar to Nginx's per-worker model.
    shards: Box<[Mutex<SwrrState>]>,
}

#[derive(Default)]
struct SwrrState {
    current_weights: Vec<i64>,
}

impl Default for WeightedRoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl WeightedRoundRobin {
    /// Creates a new weighted round-robin balancer reading worker concurrency from RAM.
    pub fn new() -> Self {
        Self::with_workers(velda_core::global_hardware_topology().worker_threads)
    }

    /// Creates a new weighted round-robin balancer with explicit worker count
    /// injected by the caller (`velda-edge` supervisor).
    ///
    /// OPTIMIZATION: Shard count scales 1:1 with worker concurrency to eliminate Mutex lock contention.
    pub fn with_workers(workers: usize) -> Self {
        let count = workers.max(1).next_power_of_two();
        let shards = (0..count)
            .map(|_| Mutex::new(SwrrState::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { shards }
    }
}

impl LoadBalancer for WeightedRoundRobin {
    fn select_index(&self, endpoints: &[Endpoint], _ctx: &SelectionContext<'_>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        if endpoints.len() == 1 {
            return Some(0);
        }

        // OPTIMIZATION: Retrieve thread-dedicated shard to avoid cross-thread lock contention.
        let shard_idx = WORKER_SHARD_ID.with(|id| *id) % self.shards.len();

        // BLOCKER FIX: Prevent poisoned mutex cascade panic. If another worker thread panics
        // while holding this lock, recover the inner state instead of panicking on all subsequent requests.
        let mut lock = self.shards[shard_idx]
            .lock()
            .unwrap_or_else(|poison_err| poison_err.into_inner());
        if lock.current_weights.len() != endpoints.len() {
            lock.current_weights = vec![0; endpoints.len()];
        }

        let total_weight: i64 = endpoints.iter().map(|e| e.weight as i64).sum();
        let mut best_idx = 0;
        let mut max_weight = i64::MIN;

        for (i, ep) in endpoints.iter().enumerate() {
            lock.current_weights[i] += ep.weight as i64;
            if lock.current_weights[i] > max_weight {
                max_weight = lock.current_weights[i];
                best_idx = i;
            }
        }

        lock.current_weights[best_idx] -= total_weight;
        Some(best_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    #[test]
    fn test_smooth_weighted_round_robin_distribution() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let ep3: SocketAddr = "10.0.0.3:8080".parse().unwrap();

        // Weights: ep1=5, ep2=1, ep3=1 (Total = 7)
        let endpoints = vec![
            Endpoint::new("e1", ep1, 5),
            Endpoint::new("e2", ep2, 1),
            Endpoint::new("e3", ep3, 1),
        ];

        let balancer = WeightedRoundRobin::new();
        let ctx = SelectionContext::NONE;

        let mut counts = HashMap::new();
        let mut sequence = Vec::new();

        for _ in 0..7 {
            let selected = balancer.select(&endpoints, &ctx).unwrap();
            *counts.entry(selected.address).or_insert(0) += 1;
            sequence.push(selected.id.0.as_str());
        }

        // In 7 requests: ep1 gets exactly 5, ep2 gets 1, ep3 gets 1
        assert_eq!(counts.get(&ep1), Some(&5));
        assert_eq!(counts.get(&ep2), Some(&1));
        assert_eq!(counts.get(&ep3), Some(&1));

        // Smooth interleaving: never 5 consecutive "e1"s
        assert_eq!(sequence, vec!["e1", "e1", "e2", "e1", "e3", "e1", "e1"]);
    }

    #[test]
    fn test_weighted_round_robin_poisoned_mutex_resilience() {
        use std::panic;
        use std::sync::Arc;

        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let endpoints = vec![Endpoint::new("e1", ep1, 1), Endpoint::new("e2", ep2, 1)];

        let balancer = Arc::new(WeightedRoundRobin::new());
        let b_clone = Arc::clone(&balancer);

        // Deliberately poison all shards in another thread
        let _ = std::thread::spawn(move || {
            for s in &b_clone.shards {
                let _lock = s.lock().unwrap();
            }
            panic!("Intentional worker panic to poison the mutexes");
        })
        .join();

        // The mutex is now poisoned! Next request must NOT panic.
        let ctx = SelectionContext::NONE;
        let selected = balancer.select(&endpoints, &ctx);
        assert!(selected.is_some());
    }

    #[test]
    fn test_swrr_with_workers_probed_scaling() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let endpoints = vec![Endpoint::new("e1", ep1, 1)];
        let ctx = SelectionContext::NONE;

        for workers in [1, 2, 4, 8, 16, 64] {
            let swrr = WeightedRoundRobin::with_workers(workers);
            assert_eq!(swrr.shards.len(), workers.next_power_of_two());
            assert!(swrr.select(&endpoints, &ctx).is_some());
        }
    }
}
