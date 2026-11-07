//! Core buffer pool implementations: HeaderPool, CoreBufferPool, and ShardedBufferPool.
//!
//! # Deliberate Hardware Cache Hierarchy Decision
//! - **L1D Cache (`_MM_HINT_T0`)** is strictly reserved for `velda-router` hot routing tables.
//! - **L2 Cache (`_MM_HINT_T1` / `pldl2keep`)** houses [`HeaderPool`] management structures,
//!   [`HeaderWorkspace`] instances, and [`PayloadSlabPool`] 24-byte descriptor queues.
//! - **LIFO Burst Acquisition (`guard.burst.pop_back()`)**: During traffic bursts, workers pop from
//!   the back of the `VecDeque` (most recently recycled workspace) rather than the front (FIFO).
//!   This keeps the active working set of 1-2 burst workspaces warm in L2 cache instead of rotating
//!   through hundreds of cold workspaces in DRAM, while `maintain()` / `sweep_expired()` continues
//!   evicting the oldest untouched items from `guard.burst.front()`.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::header::HeaderWorkspace;
use super::slab::{AdaptiveSlabLimits, PayloadSlabPool};

pub(crate) struct HeaderBucket {
    pub(crate) baseline: Vec<HeaderWorkspace>,
    pub(crate) burst: VecDeque<(u64, HeaderWorkspace)>,
    pub(crate) baseline_limit: usize,
    pub(crate) max_burst_limit: usize,
}

/// Pre-warmed two-tier adaptive object pool of [`HeaderWorkspace`] instances.
pub struct HeaderPool {
    pub(crate) bucket: Mutex<HeaderBucket>,
    pub(crate) active_leases: AtomicUsize,
    pub(crate) idle_ttl_secs: u64,
}

impl std::fmt::Debug for HeaderPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderPool")
            .field("active_leases", &self.active_leases.load(Ordering::Relaxed))
            .finish()
    }
}

impl HeaderPool {
    pub fn new(baseline_slots: usize) -> Arc<Self> {
        let max_burst_cap = baseline_slots.saturating_mul(3).max(baseline_slots + 32);
        Self::with_adaptive_limits(baseline_slots, max_burst_cap, 15)
    }

    pub fn with_adaptive_limits(
        baseline_slots: usize,
        max_burst_cap: usize,
        idle_ttl_secs: u64,
    ) -> Arc<Self> {
        let mut baseline = Vec::with_capacity(baseline_slots);
        for _ in 0..baseline_slots {
            baseline.push(HeaderWorkspace::default());
        }
        Arc::new(Self {
            bucket: Mutex::new(HeaderBucket {
                baseline,
                burst: VecDeque::new(),
                baseline_limit: baseline_slots,
                max_burst_limit: max_burst_cap.max(baseline_slots),
            }),
            active_leases: AtomicUsize::new(0),
            idle_ttl_secs,
        })
    }

    pub fn acquire(self: &Arc<Self>) -> Option<HeaderWorkspaceLease> {
        let mut guard = self.lock_bucket();
        let ws = if let Some(ws) = guard.baseline.pop() {
            ws
        } else if let Some((_, ws)) = guard.burst.pop_back() {
            // Priority 2 (LIFO Burst): Warmest recently recycled workspace reused first (keeps in L2/L1D)
            ws
        } else {
            if self.active_leases.load(Ordering::Relaxed) >= guard.max_burst_limit {
                return None;
            }
            HeaderWorkspace::default()
        };
        self.active_leases.fetch_add(1, Ordering::Relaxed);
        Some(HeaderWorkspaceLease {
            workspace: Some(ws),
            pool: Arc::clone(self),
        })
    }

    /// Performs non-blocking maintenance on this core's header pool using [`crate::coarse_now_sec`].
    ///
    /// 1. Evicts expired burst workspaces (> idle_ttl_secs) using non-blocking `try_lock()`.
    /// 2. If available pooled workspaces drop <= 25% of baseline (low watermark),
    ///    proactively pre-allocates a batch of workspaces into the burst queue with coarse timestamp.
    ///
    /// Returns `(evicted, replenished)`.
    #[inline(always)]
    pub fn maintain(&self) -> (usize, usize) {
        let now_sec = crate::coarse_now_sec();
        let mut evicted = 0;
        let mut replenished = 0;
        let guard = match self.bucket.try_lock() {
            Ok(g) => Some(g),
            Err(std::sync::TryLockError::Poisoned(p)) => Some(p.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        if let Some(mut guard) = guard {
            // 1. Evict expired burst workspaces
            while guard.burst.front().is_some_and(|(returned_at_sec, _)| {
                now_sec.saturating_sub(*returned_at_sec) >= self.idle_ttl_secs
            }) {
                drop(guard.burst.pop_front());
                evicted += 1;
            }

            // 2. Low-watermark proactive replenishment
            let available = guard.baseline.len() + guard.burst.len();
            let active = self.active_leases.load(Ordering::Relaxed);
            let low_watermark = (guard.baseline_limit / 4).max(1);

            if available <= low_watermark {
                let batch_size = (guard.baseline_limit / 2).max(2);
                let current_total = available + active;
                let allowed = guard.max_burst_limit.saturating_sub(current_total);
                let to_replenish = batch_size.min(allowed);
                for _ in 0..to_replenish {
                    guard.burst.push_back((now_sec, HeaderWorkspace::default()));
                    replenished += 1;
                }
            }
        }
        (evicted, replenished)
    }

    /// Evicts expired burst workspaces across all cores that have been idle longer than `idle_ttl_secs`.
    #[inline(always)]
    pub fn sweep_expired(&self) -> usize {
        let now_sec = crate::coarse_now_sec();
        let mut evicted = 0;
        let guard = match self.bucket.try_lock() {
            Ok(g) => Some(g),
            Err(std::sync::TryLockError::Poisoned(p)) => Some(p.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        if let Some(mut guard) = guard {
            while guard.burst.front().is_some_and(|(returned_at_sec, _)| {
                now_sec.saturating_sub(*returned_at_sec) >= self.idle_ttl_secs
            }) {
                drop(guard.burst.pop_front());
                evicted += 1;
            }
        }
        evicted
    }

    #[inline(always)]
    pub(crate) fn lock_bucket(&self) -> std::sync::MutexGuard<'_, HeaderBucket> {
        self.bucket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn recycle(&self, mut ws: HeaderWorkspace) {
        ws.reset();
        self.active_leases.fetch_sub(1, Ordering::Relaxed);
        let mut guard = self.lock_bucket();
        if guard.baseline.len() < guard.baseline_limit {
            guard.baseline.push(ws);
        } else if guard.baseline.len() + guard.burst.len() < guard.max_burst_limit {
            let now_sec = crate::coarse_now_sec();
            guard.burst.push_back((now_sec, ws));
        }
    }
}

/// RAII Lease for a [`HeaderWorkspace`]. Automatically cleans and returns to the pool on drop.
pub struct HeaderWorkspaceLease {
    workspace: Option<HeaderWorkspace>,
    pool: Arc<HeaderPool>,
}

impl HeaderWorkspaceLease {
    /// Issues hardware Tier-2 (`T1` / L2 Cache) Prefetch hints (`_MM_HINT_T1` / `pldl2keep`)
    /// on the underlying workspace.
    #[inline(always)]
    pub fn prefetch_l2_keep(&self) {
        self.workspace
            .as_ref()
            .expect("HeaderWorkspaceLease held invalid state")
            .prefetch_l2_keep();
    }
}

impl Deref for HeaderWorkspaceLease {
    type Target = HeaderWorkspace;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.workspace
            .as_ref()
            .expect("HeaderWorkspaceLease held invalid state")
    }
}

impl DerefMut for HeaderWorkspaceLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.workspace
            .as_mut()
            .expect("HeaderWorkspaceLease held invalid state")
    }
}

impl Drop for HeaderWorkspaceLease {
    fn drop(&mut self) {
        if let Some(ws) = self.workspace.take() {
            self.pool.recycle(ws);
        }
    }
}

/// Configuration dimensions for an adaptive two-tier buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePoolConfig {
    pub header_slots: usize,
    pub header_max_cap: usize,
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

/// Combined core-local buffer pool containing pre-warmed headers and payload slabs.
#[derive(Clone, Debug)]
pub struct CoreBufferPool {
    pub core_id: usize,
    pub headers: Arc<HeaderPool>,
    pub slabs: Arc<PayloadSlabPool>,
}

impl CoreBufferPool {
    /// Creates and eagerly warms up a complete core-local buffer pool with default 3x burst ceilings and 15s TTL.
    pub fn new(
        core_id: usize,
        header_slots: usize,
        small_slabs: usize,
        medium_slabs: usize,
        standard_slabs: usize,
        large_slabs: usize,
    ) -> Self {
        Self {
            core_id,
            headers: HeaderPool::new(header_slots),
            slabs: PayloadSlabPool::new(small_slabs, medium_slabs, standard_slabs, large_slabs),
        }
    }

    /// Creates an adaptive core-local buffer pool with explicit burst ceilings and idle TTL in seconds.
    pub fn with_adaptive_limits(core_id: usize, config: AdaptivePoolConfig) -> Self {
        Self {
            core_id,
            headers: HeaderPool::with_adaptive_limits(
                config.header_slots,
                config.header_max_cap,
                config.idle_ttl_secs,
            ),
            slabs: PayloadSlabPool::with_adaptive_limits(config.slab_limits()),
        }
    }

    /// Performs non-blocking maintenance on this core's headers and slabs.
    #[inline(always)]
    pub fn maintain(&self) -> ((usize, usize), (usize, usize)) {
        let h_stats = self.headers.maintain();
        let s_stats = self.slabs.maintain();
        (h_stats, s_stats)
    }

    /// Evicts expired burst workspaces and slabs using `coarse_now_sec()`.
    #[inline(always)]
    pub fn sweep_expired(&self) -> (usize, usize) {
        let headers_evicted = self.headers.sweep_expired();
        let slabs_evicted = self.slabs.sweep_expired();
        (headers_evicted, slabs_evicted)
    }
}

/// Sharded collection of core-local buffer pools partitioned across available traffic worker cores.
#[derive(Clone, Debug)]
pub struct ShardedBufferPool {
    pools: Arc<[CoreBufferPool]>,
}

impl Default for ShardedBufferPool {
    fn default() -> Self {
        Self::new(1, 64, 16, 16, 16, 16)
    }
}

impl ShardedBufferPool {
    /// Creates a sharded buffer pool across `num_cores` with identical per-core dimensions.
    pub fn new(
        num_cores: usize,
        header_slots: usize,
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
                header_slots,
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
    pub fn maintain_lane(&self, lane_id: usize) -> ((usize, usize), (usize, usize)) {
        self.core_pool(lane_id).maintain()
    }

    /// Sweeps expired burst buffers across all core pools using `coarse_now_sec()`.
    pub fn sweep_all_cores(&self) -> (usize, usize) {
        let mut total_headers = 0;
        let mut total_slabs = 0;
        for pool in self.pools.iter() {
            let (h, s) = pool.sweep_expired();
            total_headers += h;
            total_slabs += s;
        }
        (total_headers, total_slabs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::slab::SlabClass;

    #[test]
    fn test_sharded_buffer_pool_distribution() {
        let sharded = ShardedBufferPool::new(4, 8, 4, 4, 4, 4);
        assert_eq!(sharded.len(), 4);
        assert!(!sharded.is_empty());

        let pool_0 = sharded.core_pool(0);
        let pool_1 = sharded.core_pool(1);
        assert_eq!(pool_0.core_id, 0);
        assert_eq!(pool_1.core_id, 1);

        // Core 4 wraps around modulo to core 0
        let pool_4 = sharded.core_pool(4);
        assert_eq!(pool_4.core_id, 0);

        let h_lease = pool_0.headers.acquire().expect("acquire header lease");
        assert!(h_lease.capacity() >= 32);

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
        let config = AdaptivePoolConfig {
            header_slots: 2,
            header_max_cap: 4,
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
        let h1 = pool_0.headers.acquire().unwrap();
        let h2 = pool_0.headers.acquire().unwrap();
        let h3 = pool_0.headers.acquire().unwrap(); // burst
        drop(h1);
        drop(h2);
        drop(h3);

        // Before TTL -> 0 evicted
        let (h_evicted, s_evicted) = sharded.sweep_all_cores();
        assert_eq!(h_evicted, 0);
        assert_eq!(s_evicted, 0);

        // Simulate TTL expiration (> 15s) by backdating burst timestamps on pool 0
        {
            let mut guard = pool_0.headers.bucket.lock().unwrap();
            for (ts, _) in guard.burst.iter_mut() {
                *ts = ts.saturating_sub(20);
            }
        }

        // After TTL -> burst evicted
        let (h_evicted, s_evicted) = sharded.sweep_all_cores();
        assert_eq!(h_evicted, 1);
        assert_eq!(s_evicted, 0);
    }

    #[test]
    fn test_sharded_buffer_pool_maintain_lane_strict_core_jailing() {
        let config = AdaptivePoolConfig {
            header_slots: 4,
            header_max_cap: 8,
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
        let (_h_stats, s_stats) = sharded.maintain_lane(0);
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
