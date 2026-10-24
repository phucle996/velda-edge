//! Pre-warmed User-Space Buffer and Object Pooling Subsystem (`velda_core::buffer`).
//!
//! # Architectural Mindset & Zero-Quantization Contract
//! - **RAM as Compute Capacity**: Pre-warms and commits memory pages at process startup.
//! - **Zero-Syscall in Serving**: Allocation and recycling occurs entirely in user space.
//! - **Anti-Page-Fault**: Touches each 4 KB page during cold-start to eliminate minor page faults.
//! - **Two-Tier Adaptive Memory Management**: Pre-warmed permanent floor + burst standby queue with 15s TTL.
//! - **Anti-OOM Protection**: Mathematically bounded burst ceiling based on available container RAM.
//! - **Non-Blocking Auxiliary Sweeper**: Sweeping performed via `try_lock()` by Auxiliary Core.
//!
//! # Defensive Memory Invariants (Anti-UAF & Anti-Contamination)
//! 1. **Zero Use-After-Free via Compiler Borrow Checker**:
//!    Buffer leases (`HeaderWorkspaceLease`, `PayloadSlabLease`) bind dereferenced slice lifetimes
//!    strictly to the lease guard. Accessing memory after lease drop is statically rejected by `rustc`.
//! 2. **Zero Double-Free / Double-Recycle**:
//!    Lease drop handlers extract inner slabs/workspaces via `Option::take()`, guaranteeing that
//!    pooled memory is returned to internal freelists exactly once.
//! 3. **Anti-Data-Leakage via Sanitization**:
//!    Recycled `HeaderMap` instances are stripped of 100% of entries (`len == 0`) before reuse,
//!    preventing cross-session header leakage (e.g., auth tokens, cookies).
//! 4. **Anti-Contamination Sub-Slicing**:
//!    Payload slabs expose strictly bounded slices `&slab[..bytes_read]` during network I/O,
//!    preventing readers from viewing stale bytes from previous requests.
//! 5. **Prohibition of Unsafe Pointer Aliasing**:
//!    Data plane code MUST NEVER use `unsafe` raw pointers to convert lease slices into unmanaged
//!    `Bytes` instances or static slices, preventing detached use-after-free corruptions.

pub mod header;

pub mod slab;

pub use header::{
    HeaderPool, HeaderWorkspace, HeaderWorkspaceLease, MAX_BYPASS_HEADERS, X_REQUEST_ID,
};
pub use slab::{PayloadSlab, PayloadSlabLease, PayloadSlabPool, SlabClass};

use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration dimensions for an adaptive two-tier buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePoolConfig {
    pub header_slots: usize,
    pub header_max_cap: usize,
    pub headers_per_map: usize,
    pub small_slabs: usize,
    pub small_max_cap: usize,
    pub medium_slabs: usize,
    pub medium_max_cap: usize,
    pub large_slabs: usize,
    pub large_max_cap: usize,
    pub idle_ttl: Duration,
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
        headers_per_map: usize,
        small_slabs: usize,
        medium_slabs: usize,
        large_slabs: usize,
    ) -> Self {
        Self {
            core_id,
            headers: HeaderPool::new(header_slots, headers_per_map),
            slabs: PayloadSlabPool::new(small_slabs, medium_slabs, large_slabs),
        }
    }

    /// Creates an adaptive core-local buffer pool with explicit burst ceilings and idle TTL.
    pub fn with_adaptive_limits(core_id: usize, config: AdaptivePoolConfig) -> Self {
        Self {
            core_id,
            headers: HeaderPool::with_adaptive_limits(
                config.header_slots,
                config.headers_per_map,
                config.header_max_cap,
                config.idle_ttl,
            ),
            slabs: PayloadSlabPool::with_adaptive_limits(
                config.small_slabs,
                config.small_max_cap,
                config.medium_slabs,
                config.medium_max_cap,
                config.large_slabs,
                config.large_max_cap,
                config.idle_ttl,
            ),
        }
    }

    /// Evicts expired burst workspaces and slabs using non-blocking `try_lock()`.
    ///
    /// Returns `(headers_evicted, slabs_evicted)`.
    #[inline]
    pub fn sweep_expired(&self, now: Instant) -> (usize, usize) {
        let headers_evicted = self.headers.sweep_expired(now);
        let slabs_evicted = self.slabs.sweep_expired(now);
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
        Self::new(1, 64, 32, 16, 16, 16)
    }
}

impl ShardedBufferPool {
    /// Creates a sharded buffer pool across `num_cores` with identical per-core dimensions.
    pub fn new(
        num_cores: usize,
        header_slots: usize,
        headers_per_map: usize,
        small_slabs: usize,
        medium_slabs: usize,
        large_slabs: usize,
    ) -> Self {
        let count = num_cores.max(1);
        let mut vec = Vec::with_capacity(count);
        for id in 0..count {
            vec.push(CoreBufferPool::new(
                id,
                header_slots,
                headers_per_map,
                small_slabs,
                medium_slabs,
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

    /// Sweeps expired burst buffers across all core pools.
    ///
    /// # Auxiliary Core Invariant
    /// Designed to be executed periodically by the Auxiliary Core (e.g. every 5s).
    /// Uses non-blocking `try_lock()`, completely preventing traffic core lock contention.
    pub fn sweep_all_cores(&self, now: Instant) -> (usize, usize) {
        let mut total_headers = 0;
        let mut total_slabs = 0;
        for pool in self.pools.iter() {
            let (h, s) = pool.sweep_expired(now);
            total_headers += h;
            total_slabs += s;
        }
        (total_headers, total_slabs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharded_buffer_pool_distribution() {
        let sharded = ShardedBufferPool::new(4, 8, 32, 4, 4, 4);
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
        assert!(h_lease.headers().capacity() >= 32);

        let s_lease = pool_0
            .slabs
            .acquire(SlabClass::Medium16K)
            .expect("acquire slab lease");
        assert_eq!(s_lease.len(), 16384);
    }

    #[test]
    fn test_sharded_buffer_pool_sweep_all_cores() {
        let config = AdaptivePoolConfig {
            header_slots: 2,
            header_max_cap: 4,
            headers_per_map: 32,
            small_slabs: 2,
            small_max_cap: 4,
            medium_slabs: 2,
            medium_max_cap: 4,
            large_slabs: 2,
            large_max_cap: 4,
            idle_ttl: Duration::from_millis(50),
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
        let (h_evicted, s_evicted) = sharded.sweep_all_cores(Instant::now());
        assert_eq!(h_evicted, 0);
        assert_eq!(s_evicted, 0);

        // After TTL -> burst evicted
        let future = Instant::now() + Duration::from_millis(100);
        let (h_evicted, s_evicted) = sharded.sweep_all_cores(future);
        assert_eq!(h_evicted, 1);
        assert_eq!(s_evicted, 0);
    }

    #[test]
    fn test_adaptive_sizing_formula() {
        use crate::hardware::sizing::HardwareSizing;

        // Baseline = 100, 1GB RAM, 4 cores, 16KB item
        // 20% of 1GB = 200MB = 209,715,200 bytes
        // Per core headroom = 209,715,200 / 4 = 52,428,800 bytes
        // In 16KB items: 52,428,800 / 16384 = 3200 items
        // Baseline * 3 = 300
        // min(300, 100 + 3200) = 300 (bounded by 3x burst multiplier)
        let total_ram = 1024 * 1024 * 1024;
        let cap = HardwareSizing::adaptive_burst_cap(100, total_ram, 4, 16384);
        assert_eq!(cap, 300);

        // Very constrained memory scenario: 10MB RAM, 4 cores, 64KB item
        // 20% of 10MB = 2MB = 2,097,152 bytes
        // Per core headroom = 524,288 bytes
        // In 64KB items: 524,288 / 65536 = 8 items
        // Baseline = 50, Baseline * 3 = 150
        // Headroom max = 50 + 8 = 58
        // min(150, 58) = 58 (strictly bounded by 20% headroom to prevent OOM!)
        let constrained_ram = 10 * 1024 * 1024;
        let cap_constrained = HardwareSizing::adaptive_burst_cap(50, constrained_ram, 4, 65536);
        assert_eq!(cap_constrained, 58);
    }
}
