//! Core Buffer Pool Implementations (`velda_core::buffer::pool`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                    PER-CORE PAYLOAD SLAB POOL SHARDING & LIFO BURST                     │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  L1D (_MM_HINT_T0) : Reserved for velda-router tables                                   │
//! │  L2  (_MM_HINT_T1) : HeaderWorkspace (thread_local! LIFO in header.rs)                  │
//! │  L3  (_MM_HINT_T2) : Core-jailed PayloadSlabPool arenas (4K / 16K / 32K / 64K)          │
//! │                                                                                         │
//! │  ShardedBufferPool ──► core_pool(lane_id % N) ──► CoreBufferPool.slabs                  │
//! │    ├─ acquire(class)        : baseline floor -> LIFO burst -> mmap arena                │
//! │    └─ maintain_lane(lane_id): shadow worker TTL eviction + low-watermark replenish      │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;

use super::slab::{AdaptiveSlabLimits, PayloadSlabPool};

/// Configuration dimensions for an adaptive two-tier slab buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePoolConfig {
    pub small_slabs: usize,
    pub small_max_cap: usize,
    pub medium_slabs: usize,
    pub medium_max_cap: usize,
    pub standard_slabs: usize,
    pub standard_max_cap: usize,
    pub large_slabs: usize,
    pub large_max_cap: usize,
    pub idle_ttl_secs: u64,
}

impl AdaptivePoolConfig {
    /// Extracts the slab allocator limits from this configuration.
    #[inline(always)]
    pub fn slab_limits(&self) -> AdaptiveSlabLimits {
        AdaptiveSlabLimits {
            small_baseline: self.small_slabs,
            small_max_cap: self.small_max_cap,
            medium_baseline: self.medium_slabs,
            medium_max_cap: self.medium_max_cap,
            standard_baseline: self.standard_slabs,
            standard_max_cap: self.standard_max_cap,
            large_baseline: self.large_slabs,
            large_max_cap: self.large_max_cap,
            idle_ttl_secs: self.idle_ttl_secs,
        }
    }
}

/// Core-local buffer pool holding pre-warmed payload slab arenas.
#[derive(Clone, Debug)]
pub struct CoreBufferPool {
    pub core_id: usize,
    pub slabs: Arc<PayloadSlabPool>,
}

impl CoreBufferPool {
    /// Creates and eagerly warms up a core-local payload slab pool with default 3x burst ceilings.
    pub fn new(
        core_id: usize,
        small_slabs: usize,
        medium_slabs: usize,
        standard_slabs: usize,
        large_slabs: usize,
    ) -> Self {
        Self {
            core_id,
            slabs: PayloadSlabPool::new(small_slabs, medium_slabs, standard_slabs, large_slabs),
        }
    }

    /// Creates an adaptive core-local payload slab pool with explicit burst ceilings and idle TTL.
    pub fn with_adaptive_limits(core_id: usize, config: AdaptivePoolConfig) -> Self {
        Self {
            core_id,
            slabs: PayloadSlabPool::with_adaptive_limits(config.slab_limits()),
        }
    }

    /// Performs non-blocking maintenance on this core's payload slabs.
    #[inline(always)]
    pub fn maintain(&self) -> (usize, usize) {
        self.slabs.maintain()
    }

    /// Evicts expired burst slabs using `coarse_sec()`.
    #[inline(always)]
    pub fn sweep_expired(&self) -> usize {
        self.slabs.sweep_expired()
    }
}

/// Sharded collection of core-local buffer pools partitioned across available traffic worker cores.
#[derive(Clone, Debug)]
pub struct ShardedBufferPool {
    pools: Arc<[CoreBufferPool]>,
}

impl Default for ShardedBufferPool {
    fn default() -> Self {
        Self::new(1, 16, 16, 16, 16)
    }
}

impl ShardedBufferPool {
    /// Creates a sharded buffer pool across `num_cores` with identical per-core slab dimensions.
    pub fn new(
        num_cores: usize,
        small_slabs: usize,
        medium_slabs: usize,
        standard_slabs: usize,
        large_slabs: usize,
    ) -> Self {
        let count = num_cores.max(1);
        let mut vec = Vec::with_capacity(count);
        for id in 0..count {
            vec.push(CoreBufferPool::new(
                id,
                small_slabs,
                medium_slabs,
                standard_slabs,
                large_slabs,
            ));
        }

        Self { pools: vec.into() }
    }

    /// Creates an adaptive sharded buffer pool across `num_cores` with explicit burst limits and TTL.
    pub fn with_adaptive_limits(num_cores: usize, config: AdaptivePoolConfig) -> Self {
        let count = num_cores.max(1);
        let mut vec = Vec::with_capacity(count);
        for id in 0..count {
            vec.push(CoreBufferPool::with_adaptive_limits(id, config));
        }

        Self { pools: vec.into() }
    }

    /// Accesses the buffer pool dedicated to the specified core index.
    #[inline(always)]
    pub fn core_pool(&self, core_id: usize) -> &CoreBufferPool {
        &self.pools[core_id % self.pools.len()]
    }

    /// Returns the total number of partitioned core pools.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// Returns `true` if empty (always false).
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }

    /// Autonomous maintenance on the core-local buffer pool dedicated to `lane_id`.
    #[inline(always)]
    pub fn maintain_lane(&self, lane_id: usize) -> (usize, usize) {
        self.core_pool(lane_id).maintain()
    }

    /// Sweeps expired burst slabs across all core pools using `coarse_sec()`.
    pub fn sweep_all_cores(&self) -> usize {
        let mut total_slabs = 0;
        for pool in self.pools.iter() {
            total_slabs += pool.sweep_expired();
        }
        total_slabs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::slab::SlabClass;

    #[test]
    fn test_sharded_buffer_pool_distribution() {
        let sharded = ShardedBufferPool::new(4, 4, 4, 4, 4);
        assert_eq!(sharded.len(), 4);
        assert!(!sharded.is_empty());

        let pool_0 = sharded.core_pool(0);
        let pool_1 = sharded.core_pool(1);
        assert_eq!(pool_0.core_id, 0);
        assert_eq!(pool_1.core_id, 1);

        // Core 4 wraps around modulo to core 0
        let pool_4 = sharded.core_pool(4);
        assert_eq!(pool_4.core_id, 0);

        let s_lease = pool_0
            .slabs
            .acquire(SlabClass::Medium16K)
            .expect("acquire medium slab lease");
        assert_eq!(s_lease.len(), 16384);

        let std_lease = pool_0
            .slabs
            .acquire(SlabClass::Standard32K)
            .expect("acquire standard 32k slab lease");
        assert_eq!(std_lease.len(), 32768);
    }

    #[test]
    fn test_sharded_buffer_pool_sweep_all_cores() {
        crate::clock::init_clock();
        let config = AdaptivePoolConfig {
            small_slabs: 2,
            small_max_cap: 4,
            medium_slabs: 2,
            medium_max_cap: 4,
            standard_slabs: 2,
            standard_max_cap: 4,
            large_slabs: 2,
            large_max_cap: 4,
            idle_ttl_secs: 15,
        };
        let sharded = ShardedBufferPool::with_adaptive_limits(2, config);

        let pool_0 = sharded.core_pool(0);
        let s1 = pool_0.slabs.acquire(SlabClass::Small4K).unwrap();
        let s2 = pool_0.slabs.acquire(SlabClass::Small4K).unwrap();
        let s3 = pool_0.slabs.acquire(SlabClass::Small4K).unwrap();
        drop(s1);
        drop(s2);
        drop(s3);

        // Before TTL expires, burst slab stays pooled
        assert_eq!(sharded.sweep_all_cores(), 0);
        assert_eq!(pool_0.slabs.total_pooled(SlabClass::Small4K), 3);
    }

    #[test]
    fn test_sharded_buffer_pool_maintain_lane_strict_core_jailing() {
        let config = AdaptivePoolConfig {
            small_slabs: 4,
            small_max_cap: 8,
            medium_slabs: 4,
            medium_max_cap: 8,
            standard_slabs: 4,
            standard_max_cap: 8,
            large_slabs: 4,
            large_max_cap: 8,
            idle_ttl_secs: 15,
        };
        let sharded = ShardedBufferPool::with_adaptive_limits(2, config);

        // Drain core 0's standard slabs so available == 0 (<= low watermark)
        let pool_0 = sharded.core_pool(0);
        let s0_1 = pool_0.slabs.acquire(SlabClass::Standard32K).unwrap();
        let s0_2 = pool_0.slabs.acquire(SlabClass::Standard32K).unwrap();
        let s0_3 = pool_0.slabs.acquire(SlabClass::Standard32K).unwrap();
        let s0_4 = pool_0.slabs.acquire(SlabClass::Standard32K).unwrap();

        // Companion shadow worker on core 0 calls maintain_lane(0)
        let s_stats = sharded.maintain_lane(0);
        // Core 0 should proactively replenish slabs into burst queue
        assert!(s_stats.1 > 0);

        // Core 1 was untouched and its pool is fully intact
        let pool_1 = sharded.core_pool(1);
        assert_eq!(pool_1.slabs.active_leases(SlabClass::Standard32K), 0);
        assert_eq!(pool_1.slabs.total_pooled(SlabClass::Standard32K), 4);

        drop(s0_1);
        drop(s0_2);
        drop(s0_3);
        drop(s0_4);
    }
}
