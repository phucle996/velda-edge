//! Smooth Weighted Round-Robin (SWRR) load balancing.
//!
//! Implements smooth weighted round-robin selection algorithm,
//! ensuring proportional distribution without clustering requests onto the highest-weight backend.

use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::LoadBalancer;
use crate::context::SelectionContext;
use velda_core::Endpoint;

thread_local! {
    /// Worker thread monotonic identifier for SWRR shard routing.
    static WORKER_SHARD_ID: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[inline(always)]
fn get_worker_shard_id() -> usize {
    WORKER_SHARD_ID.with(|cell| {
        let mut id = cell.get();
        if id == usize::MAX {
            static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
            id = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
            cell.set(id);
        }
        id
    })
}

#[repr(align(64))]
struct SwrrShard {
    mutex: Mutex<SwrrState>,
}

/// Function pointer signature for Smooth Weighted Round-Robin weight update and max selection.
pub type SwrrUpdateFn = fn(&mut [i64], &[Endpoint]) -> usize;

/// Scalar unrolled loop for small workloads (<= 4 endpoints) executing in GPRs.
#[inline]
pub fn swrr_update_scalar(current_weights: &mut [i64], endpoints: &[Endpoint]) -> usize {
    let mut best_idx = 0;
    let mut max_weight = i64::MIN;
    for (i, ep) in endpoints.iter().enumerate() {
        current_weights[i] += ep.weight as i64;
        if current_weights[i] > max_weight {
            max_weight = current_weights[i];
            best_idx = i;
        }
    }
    best_idx
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn swrr_update_avx2(current_weights: &mut [i64], endpoints: &[Endpoint]) -> usize {
    use std::arch::x86_64::*;
    let len = endpoints.len();
    if len <= 4 {
        return swrr_update_scalar(current_weights, endpoints);
    }

    let mut weights_raw = [0i64; 64];
    let count = len.min(64);
    for i in 0..count {
        weights_raw[i] = endpoints[i].weight as i64;
    }

    let chunks = count / 4;
    let cw_ptr = current_weights.as_mut_ptr();
    let w_ptr = weights_raw.as_ptr();

    unsafe {
        for i in 0..chunks {
            let cw_vec = _mm256_loadu_si256(cw_ptr.add(i * 4) as *const __m256i);
            let w_vec = _mm256_loadu_si256(w_ptr.add(i * 4) as *const __m256i);
            let sum_vec = _mm256_add_epi64(cw_vec, w_vec);
            _mm256_storeu_si256(cw_ptr.add(i * 4) as *mut __m256i, sum_vec);
        }

        let remainder_start = chunks * 4;
        for i in remainder_start..count {
            *cw_ptr.add(i) += *w_ptr.add(i);
        }

        let mut best_idx = 0;
        let mut max_weight = *cw_ptr;
        for i in 1..count {
            let w = *cw_ptr.add(i);
            if w > max_weight {
                max_weight = w;
                best_idx = i;
            }
        }
        best_idx
    }
}

/// Vector-accelerated SWRR weight accumulator with runtime CPU feature fallback.
#[inline]
pub fn swrr_update_vector(current_weights: &mut [i64], endpoints: &[Endpoint]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { swrr_update_avx2(current_weights, endpoints) };
        }
    }
    swrr_update_scalar(current_weights, endpoints)
}

/// Smooth Weighted Round Robin balancer with sharded state.
///
/// Weight ranges from 1 to 100 per endpoint.
pub struct WeightedRoundRobin {
    // Thread-sharded Mutex array padded to 64-byte cache lines completely eliminates
    // cross-core false sharing and lock contention across worker threads.
    shards: Box<[SwrrShard]>,
    update_fn: SwrrUpdateFn,
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
        Self::for_workload(velda_core::global_hardware_topology().worker_threads, 0)
    }

    /// Creates a new weighted round-robin balancer with explicit worker count
    /// injected by the caller (`velda-edge` supervisor).
    pub fn with_workers(workers: usize) -> Self {
        Self::for_workload(workers, 0)
    }

    /// Creates a new weighted round-robin balancer with workload-adaptive register binding.
    ///
    /// - `endpoint_count <= 4`: Bound to scalar unrolled loop in GPRs.
    /// - `endpoint_count > 4`: Bound to 256-bit AVX2 vector SIMD weight accumulator.
    pub fn for_workload(workers: usize, endpoint_count: usize) -> Self {
        let count = workers.max(1).next_power_of_two();
        let shards = (0..count)
            .map(|_| SwrrShard {
                mutex: Mutex::new(SwrrState::default()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let update_fn = if endpoint_count != 0 && endpoint_count <= 4 {
            swrr_update_scalar
        } else {
            use velda_core::hardware::RegisterWidth;
            let caps = velda_core::global_hardware_topology()
                .cpu
                .register_capabilities;
            let width = if endpoint_count == 0 {
                caps.select_for_workload(RegisterWidth::Bits256)
            } else {
                caps.select_for_workload_size(endpoint_count)
            };
            match width {
                RegisterWidth::Bits256 => swrr_update_vector,
                _ => swrr_update_scalar,
            }
        };

        Self { shards, update_fn }
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

        // Shard count is guaranteed to be a power of two, enabling a 1-cycle bitmask.
        let shard_idx = get_worker_shard_id() & (self.shards.len() - 1);

        let mut lock = self.shards[shard_idx]
            .mutex
            .lock()
            .unwrap_or_else(|poison_err| poison_err.into_inner());
        if lock.current_weights.len() != endpoints.len() {
            lock.current_weights.clear();
            lock.current_weights.resize(endpoints.len(), 0);
        }

        let total_weight: i64 = endpoints.iter().map(|e| e.weight as i64).sum();
        if total_weight <= 0 {
            static FALLBACK_COUNTER: AtomicUsize = AtomicUsize::new(0);
            return Some(FALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed) % endpoints.len());
        }

        let best_idx = (self.update_fn)(&mut lock.current_weights, endpoints);
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
    fn test_smooth_wrr_distribution() {
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
    fn test_poisoned_mutex_resilience() {
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
                let _lock = s.mutex.lock().unwrap();
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

    #[test]
    fn test_swrr_zero_weight_fallback() {
        let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
        let endpoints = vec![Endpoint::new("e1", ep1, 0), Endpoint::new("e2", ep2, 0)];
        let balancer = WeightedRoundRobin::new();
        let ctx = SelectionContext::NONE;

        let res1 = balancer.select_index(&endpoints, &ctx);
        let res2 = balancer.select_index(&endpoints, &ctx);
        assert!(res1.is_some());
        assert!(res2.is_some());
    }
}
