//! Pre-warmed User-Space Payload Slab Allocator.
//!
//! # Architecture and Invariants
//! 1. **Continuous Mathematical Sizing & Anti-OOM Cap**:
//!    - Baseline and maximum burst capacities are mathematically bounded via
//!      [`crate::hardware::sizing::HardwareSizing`].
//!    - Prevents Out-Of-Memory (OOM) crashes by strictly enforcing `max_burst_cap`
//!      (Dual-Constraint formula: $\le 3\times$ baseline and $\le 20\%$ headroom RAM).
//! 2. **Eager Physical Page Commit (Anti-Page-Fault)**:
//!    - During cold-start initialization, every 4096-byte memory page in the baseline slabs
//!      is touched (written to) to force the Linux Kernel to establish MMU PTE mappings upfront.
//! 3. **Two-Tier Adaptive Memory Management**:
//!    - **Tier 1 (Hot Baseline Floor)**: Permanent pre-warmed physical slabs, never evicted.
//!    - **Tier 2 (Burst Standby Queue with 15s TTL)**: Spills over dynamically during traffic spikes.
//!      Excess slabs are stored with a return timestamp and reused across multi-wave micro-bursts,
//!      eliminating libc/jemalloc heap churn (`malloc`/`free` thrashing).
//! 4. **Non-Blocking Auxiliary Sweeper (`try_lock`)**:
//!    - Eviction of expired burst slabs is performed periodically by the Auxiliary Core using
//!      non-blocking `try_lock()`. If a Traffic Worker core is acquiring or releasing a slab,
//!      the sweeper immediately skips that cycle without introducing any lock contention.
//! 5. **Linux RSS Memory Retention**:
//!    - When burst slabs are evicted after 15s of inactivity, they are returned to `jemalloc`.
//!      Because jemalloc retains dirty pages in process Resident Set Size (RSS) across its decay
//!      window, subsequent traffic bursts reacquire warm physical pages without kernel faults.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Canonical slab size classes for payload processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlabClass {
    /// 4 KB slab for small REST JSON payloads, auth tokens, and small RPC messages.
    Small4K = 4096,
    /// 16 KB slab for standard API payloads and buffered chunk processing.
    Medium16K = 16384,
    /// 64 KB slab for large uploads, media streaming, and full-frame buffering.
    Large64K = 65536,
}

impl SlabClass {
    /// Returns the exact size in bytes for this slab class.
    #[inline(always)]
    pub const fn byte_size(self) -> usize {
        self as usize
    }
}

/// A contiguous pre-allocated byte buffer slab.
pub struct PayloadSlab {
    buffer: Box<[u8]>,
    class: SlabClass,
}

impl PayloadSlab {
    /// Allocates and eagerly warms up a payload slab, touching every 4KB page.
    pub fn new(class: SlabClass) -> Self {
        let size = class.byte_size();
        let mut buf = vec![0u8; size].into_boxed_slice();

        // Eagerly commit physical pages: write 1 byte per 4096-byte Linux memory page
        // to force kernel page allocation and eliminate Minor Page Faults during traffic spikes.
        let page_size = 4096;
        let mut offset = 0;
        while offset < size {
            buf[offset] = 0;
            offset += page_size;
        }

        Self { buffer: buf, class }
    }

    /// Borrows the slab as an immutable byte slice.
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    /// Borrows the slab as a mutable byte slice.
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    /// Returns the slab class.
    #[inline(always)]
    pub const fn class(&self) -> SlabClass {
        self.class
    }

    /// Returns the size of this slab in bytes.
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.class.byte_size()
    }

    /// Returns `true` if empty (always false for pre-warmed slabs).
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        false
    }
}

/// Internal bucket segregating permanent baseline slabs from transient burst slabs.
struct SlabBucket {
    /// Baseline pre-warmed physical slabs. Permanent, never evicted.
    baseline: Vec<PayloadSlab>,
    /// Burst standby slabs paired with their return timestamp.
    /// Ordered chronologically: oldest at front, newest at back.
    burst: VecDeque<(Instant, PayloadSlab)>,
    /// Maximum number of permanent baseline slabs.
    baseline_limit: usize,
    /// Absolute ceiling of active + pooled slabs (anti-OOM protection).
    max_burst_limit: usize,
}

/// Pre-warmed two-tier adaptive slab pool managing Small (4KB), Medium (16KB), and Large (64KB) slabs.
pub struct PayloadSlabPool {
    small_bucket: Mutex<SlabBucket>,
    medium_bucket: Mutex<SlabBucket>,
    large_bucket: Mutex<SlabBucket>,
    active_small: AtomicUsize,
    active_medium: AtomicUsize,
    active_large: AtomicUsize,
    idle_ttl: Duration,
}

impl PayloadSlabPool {
    /// Creates and eagerly warms up all slab classes with default 3x burst ceilings and 15s TTL.
    pub fn new(small_baseline: usize, medium_baseline: usize, large_baseline: usize) -> Arc<Self> {
        Self::with_adaptive_limits(
            small_baseline,
            small_baseline.saturating_mul(3).max(small_baseline + 16),
            medium_baseline,
            medium_baseline.saturating_mul(3).max(medium_baseline + 8),
            large_baseline,
            large_baseline.saturating_mul(3).max(large_baseline + 4),
            Duration::from_secs(15),
        )
    }

    /// Creates an adaptive slab pool with explicit baselines, max burst ceilings, and idle TTL.
    pub fn with_adaptive_limits(
        small_baseline: usize,
        small_max_cap: usize,
        medium_baseline: usize,
        medium_max_cap: usize,
        large_baseline: usize,
        large_max_cap: usize,
        idle_ttl: Duration,
    ) -> Arc<Self> {
        let mut small_vec = Vec::with_capacity(small_baseline);
        for _ in 0..small_baseline {
            small_vec.push(PayloadSlab::new(SlabClass::Small4K));
        }

        let mut medium_vec = Vec::with_capacity(medium_baseline);
        for _ in 0..medium_baseline {
            medium_vec.push(PayloadSlab::new(SlabClass::Medium16K));
        }

        let mut large_vec = Vec::with_capacity(large_baseline);
        for _ in 0..large_baseline {
            large_vec.push(PayloadSlab::new(SlabClass::Large64K));
        }

        Arc::new(Self {
            small_bucket: Mutex::new(SlabBucket {
                baseline: small_vec,
                burst: VecDeque::new(),
                baseline_limit: small_baseline,
                max_burst_limit: small_max_cap.max(small_baseline),
            }),
            medium_bucket: Mutex::new(SlabBucket {
                baseline: medium_vec,
                burst: VecDeque::new(),
                baseline_limit: medium_baseline,
                max_burst_limit: medium_max_cap.max(medium_baseline),
            }),
            large_bucket: Mutex::new(SlabBucket {
                baseline: large_vec,
                burst: VecDeque::new(),
                baseline_limit: large_baseline,
                max_burst_limit: large_max_cap.max(large_baseline),
            }),
            active_small: AtomicUsize::new(0),
            active_medium: AtomicUsize::new(0),
            active_large: AtomicUsize::new(0),
            idle_ttl,
        })
    }

    /// Returns references to the bucket mutex and active counter for a class.
    #[inline(always)]
    fn bucket_and_counter(&self, class: SlabClass) -> (&Mutex<SlabBucket>, &AtomicUsize) {
        match class {
            SlabClass::Small4K => (&self.small_bucket, &self.active_small),
            SlabClass::Medium16K => (&self.medium_bucket, &self.active_medium),
            SlabClass::Large64K => (&self.large_bucket, &self.active_large),
        }
    }

    /// Borrows a slab of the requested class wrapped in an RAII lease.
    ///
    /// Resolution Order:
    /// 1. **Tier 1 (Hot Baseline)**: Pops from pre-warmed physical slabs (0 alloc, 0 page fault).
    /// 2. **Tier 2 (Burst Standby Queue)**: Pops from warm user-space burst queue (0 alloc).
    /// 3. **Spillover Dynamic Allocation**: Dynamically allocates if below `max_burst_cap`.
    /// 4. **Anti-OOM Protection**: Returns `None` if `max_burst_cap` has been reached.
    pub fn acquire(self: &Arc<Self>, class: SlabClass) -> Option<PayloadSlabLease> {
        let (bucket_mutex, active_counter) = self.bucket_and_counter(class);
        let mut guard = bucket_mutex.lock().unwrap();

        // 1. Try baseline (fastest path, hot pre-warmed)
        let slab = if let Some(slab) = guard.baseline.pop() {
            slab
        } else if let Some((_, slab)) = guard.burst.pop_front() {
            // 2. Try burst standby (warm in user space within 15s TTL)
            slab
        } else {
            // 3. Both pools empty: check anti-OOM ceiling before allocating on heap
            let current_active = active_counter.load(Ordering::Relaxed);
            if current_active >= guard.max_burst_limit {
                return None;
            }
            PayloadSlab::new(class)
        };

        active_counter.fetch_add(1, Ordering::Relaxed);

        Some(PayloadSlabLease {
            slab: Some(slab),
            pool: Arc::clone(self),
        })
    }

    /// Returns the number of currently checked-out active leases for a class.
    #[inline(always)]
    pub fn active_leases(&self, class: SlabClass) -> usize {
        let (_, counter) = self.bucket_and_counter(class);
        counter.load(Ordering::Relaxed)
    }

    /// Returns the permanent baseline capacity for a class.
    #[inline(always)]
    pub fn baseline_slots(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        bucket.lock().unwrap().baseline_limit
    }

    /// Returns the absolute max burst ceiling for a class.
    #[inline(always)]
    pub fn max_burst_cap(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        bucket.lock().unwrap().max_burst_limit
    }

    /// Returns the count of burst slabs currently held in standby for a class.
    #[inline(always)]
    pub fn burst_standby_count(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        bucket.lock().unwrap().burst.len()
    }

    /// Returns the total number of slabs currently sitting in the pool for a class (baseline + burst).
    #[inline(always)]
    pub fn total_pooled(&self, class: SlabClass) -> usize {
        let (bucket, _) = self.bucket_and_counter(class);
        let guard = bucket.lock().unwrap();
        guard.baseline.len() + guard.burst.len()
    }

    /// Evicts burst slabs across all classes that have been idle longer than `idle_ttl`.
    ///
    /// # Auxiliary Core Invariant
    /// Uses non-blocking `try_lock()` to ensure that the sweeper running on the Auxiliary Core
    /// NEVER blocks or delays a Traffic Worker core servicing requests.
    pub fn sweep_expired(&self, now: Instant) -> usize {
        let mut evicted = 0;
        for class in [
            SlabClass::Small4K,
            SlabClass::Medium16K,
            SlabClass::Large64K,
        ] {
            let (bucket_mutex, _) = self.bucket_and_counter(class);
            if let Ok(mut guard) = bucket_mutex.try_lock() {
                while let Some((returned_at, _)) = guard.burst.front() {
                    if now.saturating_duration_since(*returned_at) >= self.idle_ttl {
                        let (_, slab) = guard.burst.pop_front().unwrap();
                        drop(slab); // Evicted back to allocator arena (retained in process RSS)
                        evicted += 1;
                    } else {
                        // Slabs were pushed chronologically; front is oldest
                        break;
                    }
                }
            }
        }
        evicted
    }

    /// Recycles a returned slab back into its respective class pool.
    fn recycle(&self, slab: PayloadSlab) {
        let (bucket_mutex, active_counter) = self.bucket_and_counter(slab.class);
        active_counter.fetch_sub(1, Ordering::Relaxed);

        let mut guard = bucket_mutex.lock().unwrap();
        // Priority 1: Refill permanent baseline floor (Zero clock overhead)
        if guard.baseline.len() < guard.baseline_limit {
            guard.baseline.push(slab);
        } else if guard.baseline.len() + guard.burst.len() < guard.max_burst_limit {
            // Priority 2: Retain in burst standby queue with timestamp for 15s cooldown
            guard.burst.push_back((Instant::now(), slab));
        }
        // Priority 3: Above max_burst_limit -> slab drops and frees back to allocator
    }
}

/// RAII Lease for a [`PayloadSlab`]. Automatically returns the slab to the pool on drop.
///
/// # DEFENSIVE MEMORY INVARIANT: Anti-Use-After-Free, Anti-Double-Free & Sub-Slice Safety
/// 1. **Zero Use-After-Free via Compiler Borrow Checker**:
///    Deref to `[u8]` produces slice references `&'a [u8]` strictly bounded to the lifetime of
///    this lease. The moment this lease is dropped, any outstanding slice references are
///    invalidated statically by `rustc`.
/// 2. **Prohibition of "Faux-Zero-Copy" Pointer Aliasing**:
///    Data plane code MUST NEVER use `unsafe` raw pointer casting to convert a lease slice
///    into an unmanaged `bytes::Bytes` or detached pointer. Slices must remain bounded to this
///    lease, or explicitly copied when sent across uncoordinated task boundaries.
/// 3. **Anti-Data-Leakage via Explicit Sub-Slicing**:
///    When reading network I/O into this slab, request handlers MUST ONLY expose the sub-slice
///    of bytes actually read (`&lease[..bytes_read]`). Never expose the full 16KB/64KB slab buffer,
///    preventing cross-request dirty data contamination.
/// 4. **Zero Double-Free via `Option::take()`**:
///    During `drop()`, `self.slab.take()` extracts the slab out of `self`. The slab is returned
///    to the pool exactly once, eliminating double-recycle hazards.
/// 5. **Async Cancellation Resilience**:
///    If a client drops a TCP connection or Tokio cancels an in-flight stream future, stack
///    unwinding drops this lease and returns the slab cleanly without resource leaks.
pub struct PayloadSlabLease {
    slab: Option<PayloadSlab>,
    pool: Arc<PayloadSlabPool>,
}

impl Deref for PayloadSlabLease {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.slab
            .as_ref()
            .expect("PayloadSlabLease held invalid state")
            .as_slice()
    }
}

impl DerefMut for PayloadSlabLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slab
            .as_mut()
            .expect("PayloadSlabLease held invalid state")
            .as_mut_slice()
    }
}

impl Drop for PayloadSlabLease {
    fn drop(&mut self) {
        if let Some(slab) = self.slab.take() {
            self.pool.recycle(slab);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_slab_warmup_and_sizes() {
        let small = PayloadSlab::new(SlabClass::Small4K);
        assert_eq!(small.len(), 4096);
        assert_eq!(small.class(), SlabClass::Small4K);

        let med = PayloadSlab::new(SlabClass::Medium16K);
        assert_eq!(med.len(), 16384);
        assert_eq!(med.class(), SlabClass::Medium16K);

        let large = PayloadSlab::new(SlabClass::Large64K);
        assert_eq!(large.len(), 65536);
        assert_eq!(large.class(), SlabClass::Large64K);
    }

    #[test]
    fn test_payload_slab_pool_acquire_and_recycle() {
        let pool = PayloadSlabPool::new(2, 2, 2);
        assert_eq!(pool.baseline_slots(SlabClass::Small4K), 2);
        assert_eq!(pool.active_leases(SlabClass::Small4K), 0);

        {
            let mut lease = pool
                .acquire(SlabClass::Small4K)
                .expect("acquire small slab");
            assert_eq!(pool.active_leases(SlabClass::Small4K), 1);
            assert_eq!(lease.len(), 4096);

            lease[0] = 0xAA;
            lease[4095] = 0xBB;
            assert_eq!(lease[0], 0xAA);
            assert_eq!(lease[4095], 0xBB);
        }

        // Lease dropped -> active reset to 0
        assert_eq!(pool.active_leases(SlabClass::Small4K), 0);

        // Next acquire reuses the slot
        let lease2 = pool
            .acquire(SlabClass::Small4K)
            .expect("acquire small slab 2");
        assert_eq!(pool.active_leases(SlabClass::Small4K), 1);
        assert_eq!(lease2.len(), 4096);
    }

    #[test]
    fn test_payload_slab_pool_adaptive_burst_and_decay() {
        // Pool with baseline=2, max_burst_cap=4, idle_ttl=50ms
        let pool =
            PayloadSlabPool::with_adaptive_limits(2, 4, 2, 4, 2, 4, Duration::from_millis(50));
        assert_eq!(pool.baseline_slots(SlabClass::Medium16K), 2);
        assert_eq!(pool.max_burst_cap(SlabClass::Medium16K), 4);

        // 1. Acquire baseline (2 slots)
        let l1 = pool
            .acquire(SlabClass::Medium16K)
            .expect("lease 1 (baseline)");
        let l2 = pool
            .acquire(SlabClass::Medium16K)
            .expect("lease 2 (baseline)");
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 2);

        // 2. Acquire burst (2 slots spillover)
        let l3 = pool.acquire(SlabClass::Medium16K).expect("lease 3 (burst)");
        let l4 = pool.acquire(SlabClass::Medium16K).expect("lease 4 (burst)");
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 4);

        // 3. Attempt 5th acquire -> Anti-OOM cap reached -> returns None!
        assert!(pool.acquire(SlabClass::Medium16K).is_none());

        // 4. Drop all leases
        drop(l1);
        drop(l2);
        drop(l3);
        drop(l4);
        assert_eq!(pool.active_leases(SlabClass::Medium16K), 0);

        // 2 refilled baseline, 2 in burst standby queue
        assert_eq!(pool.total_pooled(SlabClass::Medium16K), 4);
        assert_eq!(pool.burst_standby_count(SlabClass::Medium16K), 2);

        // Immediate sweep should evict 0 because 50ms TTL hasn't elapsed
        let evicted = pool.sweep_expired(Instant::now());
        assert_eq!(evicted, 0);
        assert_eq!(pool.total_pooled(SlabClass::Medium16K), 4);

        // Simulate time passage (> 50ms)
        let future_time = Instant::now() + Duration::from_millis(100);
        let evicted = pool.sweep_expired(future_time);
        assert_eq!(evicted, 2);

        // Pool cleanly decayed back to baseline floor (2)
        assert_eq!(pool.total_pooled(SlabClass::Medium16K), 2);
        assert_eq!(pool.burst_standby_count(SlabClass::Medium16K), 0);
    }
}
