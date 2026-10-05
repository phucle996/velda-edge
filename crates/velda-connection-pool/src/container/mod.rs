//! Sharded container architecture, LIFO subpools, and hardware topology scaling.
//!
//! # Container Organization
//! - [`subpool::SubPool`]: Isolated LIFO queue for transactional single-borrower connections.
//! - [`multiplexed::MultiplexedPool`]: Sharded table for concurrent multi-stream connections (HTTP/2, HTTP/3).
//! - [`shard::ShardTable`]: Cache-line aligned (`#[repr(align(64))]`) partitioned concurrency infrastructure.

pub mod hasher;
pub mod multiplexed;
pub mod shard;
pub mod subpool;

pub use hasher::{FastBuildHasher, FastHasher};
pub use multiplexed::{MultiplexedConnection, MultiplexedPool, MuxShard};
pub use shard::{PoolShard, ShardMetrics, ShardTable, current_thread_lane};
pub use subpool::SubPool;

use velda_core::hardware::{CpuTier, MemoryTier};

/// Minimum number of idle connections retained per key container.
pub const MIN_IDLE_PER_KEY: usize = 8;

/// Maximum number of idle connections retained per key container to protect from OS `EMFILE`.
pub const MAX_IDLE_PER_KEY: usize = 128;

/// Minimum shard count for concurrency distribution.
pub const MIN_SHARDS: usize = 8;

/// Maximum shard count for concurrency distribution on ultra-high-core server hardware.
///
/// # Rationale
/// Capped at 1024 to prevent excessive memory overhead and thread synchronization storms
/// on high-end NUMA machines (e.g., dual-socket 256-core/512-thread servers).
pub const MAX_SHARDS: usize = 1024;

/// Returns the recommended concurrency shard count for a given [`CpuTier`].
///
/// Scales strictly with CPU parallelism as a power of two to guarantee minimal lock contention.
#[inline]
pub fn concurrency_shards_for_cpu_tier(tier: CpuTier) -> usize {
    match tier {
        CpuTier::Constrained => 1,
        CpuTier::Small => 2,
        CpuTier::Medium => 4,
        CpuTier::Large => 8,
        CpuTier::XLarge => 16,
        CpuTier::TwoXLarge => 32,
        CpuTier::Ultra => 64,
    }
}

/// Returns the recommended maximum idle connections per key container for a given [`MemoryTier`].
///
/// Protects against File Descriptor exhaustion (`EMFILE`) and memory bloat on constrained hardware.
#[inline]
pub fn max_idle_per_key_for_mem_tier(tier: MemoryTier) -> usize {
    match tier {
        MemoryTier::Constrained => 8,
        MemoryTier::Small => 16,
        MemoryTier::Medium => 32,
        MemoryTier::Large => 64,
        MemoryTier::XLarge => 128,
        MemoryTier::TwoXLarge => 256,
        MemoryTier::Ultra => 512,
    }
}

/// Returns the recommended maximum concurrent streams per multiplexed connection for a given [`MemoryTier`].
#[inline]
pub fn max_concurrent_streams_for_mem_tier(tier: MemoryTier) -> u32 {
    match tier {
        MemoryTier::Constrained => 32,
        MemoryTier::Small => 64,
        MemoryTier::Medium => 100,
        MemoryTier::Large => 128,
        MemoryTier::XLarge => 256,
        MemoryTier::TwoXLarge => 256,
        MemoryTier::Ultra => 512,
    }
}

/// Computes the optimal maximum idle connections per key container based on worker concurrency.
///
/// Scales with worker count (2 connections per worker to absorb microbursts without closing),
/// clamped within `[MIN_IDLE_PER_KEY, MAX_IDLE_PER_KEY]` to prevent File Descriptor exhaustion (`EMFILE`).
#[inline]
pub fn optimal_max_idle_per_key(workers: usize) -> usize {
    (workers.max(1) * 2).clamp(MIN_IDLE_PER_KEY, MAX_IDLE_PER_KEY)
}

/// Reads the optimal maximum idle connections per key from global hardware topology cached in RAM.
#[inline]
pub fn probed_max_idle_per_key() -> usize {
    optimal_max_idle_per_key(velda_core::global_hardware_topology().worker_threads())
}

/// Computes the optimal shard count dynamically scaled to the number of worker threads.
///
/// Overprovisions shards (2x workers) to minimize hash collisions under concurrent traffic,
/// clamped within `[MIN_SHARDS, MAX_SHARDS]` to prevent thread contention storms on high-core hardware.
#[inline]
pub fn optimal_shard_count(workers: usize) -> usize {
    (workers.max(1) * 2)
        .next_power_of_two()
        .clamp(MIN_SHARDS, MAX_SHARDS)
}

/// Reads the optimal shard count from global hardware topology cached in RAM.
#[inline]
pub fn probed_shard_count() -> usize {
    optimal_shard_count(velda_core::global_hardware_topology().worker_threads())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use crate::resource::PoolableResource;

    #[derive(Debug)]
    struct Dummy(usize, Instant, AtomicBool);

    impl PoolableResource for Dummy {
        fn is_healthy(&self) -> bool {
            self.2.load(Ordering::Acquire)
        }
        fn created_at(&self) -> Instant {
            self.1
        }
        fn last_used_at(&self) -> Instant {
            self.1
        }
        fn touch(&mut self) {
            self.1 = Instant::now();
        }
        fn close(&mut self) {
            self.2.store(false, Ordering::Release);
        }
    }

    #[test]
    fn test_subpool_lifo_order_and_capacity() {
        let mut subpool = SubPool::new(2);
        assert_eq!(subpool.max_idle(), 2);
        assert!(subpool.is_empty());

        let d1 = Dummy(1, Instant::now(), AtomicBool::new(true));
        let d2 = Dummy(2, Instant::now(), AtomicBool::new(true));
        let d3 = Dummy(3, Instant::now(), AtomicBool::new(true));

        subpool.release(d1);
        subpool.release(d2);
        assert_eq!(subpool.len(), 2);

        // d3 exceeds capacity (max 2), must be closed
        subpool.release(d3);
        assert_eq!(subpool.len(), 2);

        // LIFO: pops d2 first
        let popped = subpool.acquire(Duration::from_secs(60), None).unwrap();
        assert_eq!(popped.0, 2);

        let popped2 = subpool.acquire(Duration::from_secs(60), None).unwrap();
        assert_eq!(popped2.0, 1);

        assert!(subpool.is_empty());
    }

    #[test]
    fn test_subpool_empty_duration_tracking() {
        let mut subpool: SubPool<Dummy> = SubPool::new(10);
        assert!(subpool.empty_duration() <= Duration::from_millis(50));

        let d = Dummy(1, Instant::now(), AtomicBool::new(true));
        subpool.release(d);
        assert_eq!(subpool.empty_duration(), Duration::ZERO);

        let _ = subpool.acquire(Duration::from_secs(60), None);
        assert!(subpool.is_empty());
        assert!(subpool.empty_duration() <= Duration::from_millis(50));
    }

    #[test]
    fn test_pool_shard_alignment_and_poison_recovery() {
        assert_eq!(std::mem::align_of::<PoolShard<String, Dummy>>(), 64);

        let shard = PoolShard::<String, Dummy>::new();
        {
            let mut guard = shard.lock();
            guard.insert("k1".to_string(), SubPool::new(10));
        }

        // Simulate catch_unwind panic to poison mutex
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = shard.lock();
            panic!("deliberate panic to poison lock");
        }));

        // lock() should automatically recover from poison and not panic
        let guard = shard.lock();
        assert!(guard.contains_key("k1"));
    }

    #[test]
    fn test_shard_table_bitmask_and_workers() {
        let table = ShardTable::<String, Dummy>::with_workers(8);
        assert_eq!(table.shard_count(), 16);
        assert_eq!(table.all_shards().len(), 16);

        let s1 = table.shard_index(&"route_a".to_string());
        let s2 = table.shard_index(&"route_a".to_string());
        assert_eq!(s1, s2);
        assert!(s1 < 16);
    }

    #[test]
    fn test_subpool_set_max_idle_evicts_excess() {
        let mut subpool = SubPool::new(5);
        for i in 0..5 {
            subpool.release(Dummy(i, Instant::now(), AtomicBool::new(true)));
        }
        assert_eq!(subpool.len(), 5);

        // Shrink capacity to 2
        subpool.set_max_idle(2);
        assert_eq!(subpool.len(), 2);
        assert_eq!(subpool.max_idle(), 2);
    }

    #[test]
    fn test_subpool_evict_expired_idle_and_lifetime() {
        let mut subpool = SubPool::new(10);
        let past = Instant::now() - Duration::from_secs(100);

        subpool.release(Dummy(1, past, AtomicBool::new(true)));
        subpool.release(Dummy(2, Instant::now(), AtomicBool::new(true)));
        assert_eq!(subpool.len(), 2);

        let evicted = subpool.evict_expired(Duration::from_secs(10), None);
        assert_eq!(evicted, 1);
        assert_eq!(subpool.len(), 1);

        let conn = subpool.acquire(Duration::from_secs(60), None).unwrap();
        assert_eq!(conn.0, 2);
    }

    #[test]
    fn test_subpool_clear_and_drain_all() {
        let mut subpool = SubPool::new(10);
        subpool.release(Dummy(1, Instant::now(), AtomicBool::new(true)));
        subpool.release(Dummy(2, Instant::now(), AtomicBool::new(true)));
        assert_eq!(subpool.len(), 2);

        subpool.clear();
        assert_eq!(subpool.len(), 0);
        assert!(subpool.is_empty());
    }

    #[test]
    fn test_optimal_shard_count_clamp_and_power_of_two() {
        assert_eq!(optimal_shard_count(0), 8);
        assert_eq!(optimal_shard_count(1), 8);
        assert_eq!(optimal_shard_count(3), 8);
        assert_eq!(optimal_shard_count(4), 8);
        assert_eq!(optimal_shard_count(5), 16);
        assert_eq!(optimal_shard_count(9), 32);
        assert_eq!(optimal_shard_count(17), 64);
        assert_eq!(optimal_shard_count(33), 128);
        assert_eq!(optimal_shard_count(65), 256);
        assert_eq!(optimal_shard_count(128), 256);
        assert_eq!(optimal_shard_count(256), 512);
        assert_eq!(optimal_shard_count(512), 1024);
        assert_eq!(optimal_shard_count(1024), 1024);
    }

    #[test]
    fn test_optimal_max_idle_per_key_scaling_and_clamp() {
        assert_eq!(optimal_max_idle_per_key(0), 8);
        assert_eq!(optimal_max_idle_per_key(1), 8);
        assert_eq!(optimal_max_idle_per_key(2), 8);
        assert_eq!(optimal_max_idle_per_key(4), 8);
        assert_eq!(optimal_max_idle_per_key(8), 16);
        assert_eq!(optimal_max_idle_per_key(16), 32);
        assert_eq!(optimal_max_idle_per_key(32), 64);
        assert_eq!(optimal_max_idle_per_key(64), 128);
        assert_eq!(optimal_max_idle_per_key(128), 128);
        assert_eq!(optimal_max_idle_per_key(256), 128);

        let probed = probed_max_idle_per_key();
        assert!((8..=128).contains(&probed));
    }
}
