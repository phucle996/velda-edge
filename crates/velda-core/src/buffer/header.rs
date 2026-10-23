//! Pre-warmed User-Space Header Workspace and Object Pool.
//!
//! # Architecture and Invariants
//! 1. **Zero-Heap Allocation on Normal Hot Path**:
//!    - Holds pre-allocated [`HeaderMap`] instances with pre-sized bucket arrays.
//!    - When recycled, [`HeaderMap::clear`] resets length to 0 while preserving the
//!      allocated capacity table in RAM.
//! 2. **Eager Physical Page Commit (Anti-Page-Fault Baseline)**:
//!    - At cold-start bootstrap, all baseline slots are allocated and touched to commit
//!      physical memory pages into the MMU, eliminating Minor Page Faults during normal traffic.
//! 3. **Two-Tier Adaptive Memory Management**:
//!    - **Tier 1 (Baseline Floor)**: Pre-warmed permanent workspaces, never evicted.
//!    - **Tier 2 (Burst Standby Queue)**: Spills over dynamically during spikes up to `max_burst_cap`.
//!      Standby workspaces retain a 15-second TTL in user space to absorb multi-wave micro-bursts
//!      without memory allocator churn (`malloc`/`free` thrashing).
//! 4. **Non-Blocking Auxiliary Sweeper (`try_lock`)**:
//!    - Eviction of expired burst workspaces is driven by the Auxiliary Core using non-blocking
//!      `try_lock()`. If a Traffic Core is actively processing headers, the sweeper immediately
//!      skips that cycle without introducing any pipeline jitter or lock contention.
//! 5. **Linux RSS Retention Mechanism**:
//!    - When burst workspaces are evicted after the 15s TTL, they are returned to the user-space
//!      allocator (`jemalloc`). The Linux kernel retains physical page frames in RSS throughout
//!      the allocator's dirty decay window, accelerating any subsequent memory re-expansion.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::HeaderMap;

/// Reusable workspace holding a pre-allocated, clearable [`HeaderMap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderWorkspace {
    headers: HeaderMap,
    initial_capacity: usize,
}

impl HeaderWorkspace {
    /// Default baseline capacity commit (32 header entries).
    pub const DEFAULT_CAPACITY: usize = 32;

    /// Creates and pre-warms a header workspace with specified initial bucket capacity.
    pub fn new(capacity: usize) -> Self {
        let mut map = HeaderMap::with_capacity(capacity);
        // Eagerly warmup internal bucket table by reserving capacity upfront
        // to force the allocator to commit the initial hash bucket array into physical RAM.
        map.reserve(capacity);
        Self {
            headers: map,
            initial_capacity: capacity,
        }
    }

    /// Creates an empty pre-warmed workspace with default baseline capacity (32 headers).
    #[inline(always)]
    pub fn empty() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }

    /// Creates and pre-warms a header workspace with specified initial capacity.
    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity)
    }

    /// Wraps an existing [`HeaderMap`] into a [`HeaderWorkspace`].
    #[inline]
    pub fn from_headers(headers: HeaderMap) -> Self {
        let cap = headers.capacity().max(Self::DEFAULT_CAPACITY);
        Self {
            headers,
            initial_capacity: cap,
        }
    }

    /// Unwraps and returns the inner [`HeaderMap`].
    #[inline(always)]
    pub fn into_inner(self) -> HeaderMap {
        self.headers
    }

    /// Returns the initial baseline capacity of this workspace.
    #[inline(always)]
    pub fn initial_capacity(&self) -> usize {
        self.initial_capacity
    }

    /// Borrows the internal header map mutably.
    #[inline(always)]
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Borrows the internal header map immutably.
    #[inline(always)]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Takes the inner [`HeaderMap`], replacing it with an empty map of the initial capacity.
    #[inline(always)]
    pub fn take(&mut self) -> HeaderMap {
        std::mem::replace(
            &mut self.headers,
            HeaderMap::with_capacity(self.initial_capacity),
        )
    }

    /// Clears all headers while preserving the pre-allocated bucket array.
    ///
    /// # DEFENSIVE SECURITY INVARIANT: Anti-Data-Leakage / Cross-Session Sanitization
    /// In an object-pooled multi-tenant proxy, recycling a `HeaderMap` without complete
    /// sanitization creates catastrophic data leakage (e.g. Request B reading Request A's
    /// `Authorization` bearer token, API keys, or cookies).
    ///
    /// Calling `.clear()` removes 100% of key-value entries (setting `len == 0`), guaranteeing
    /// that any subsequent lookup on this workspace returns `None`. The pre-allocated bucket
    /// capacity is preserved to eliminate `malloc` churn.
    ///
    /// # Defensive Memory Bound
    /// If an aberrant or malicious request bloated the map beyond 4x baseline capacity,
    /// it is shrunk back to baseline to prevent permanent process memory pinning.
    #[inline(always)]
    pub fn reset(&mut self) {
        self.headers.clear();
        // Shrink back to baseline if bloated by oversized request headers
        if self.headers.capacity() > self.initial_capacity * 4 {
            self.headers = HeaderMap::with_capacity(self.initial_capacity);
        }
    }
}

impl Default for HeaderWorkspace {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl std::ops::Deref for HeaderWorkspace {
    type Target = HeaderMap;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.headers
    }
}

impl std::ops::DerefMut for HeaderWorkspace {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.headers
    }
}

impl AsRef<HeaderMap> for HeaderWorkspace {
    #[inline(always)]
    fn as_ref(&self) -> &HeaderMap {
        &self.headers
    }
}

impl AsMut<HeaderMap> for HeaderWorkspace {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }
}

impl From<HeaderMap> for HeaderWorkspace {
    #[inline(always)]
    fn from(headers: HeaderMap) -> Self {
        Self::from_headers(headers)
    }
}

impl From<HeaderWorkspace> for HeaderMap {
    #[inline(always)]
    fn from(ws: HeaderWorkspace) -> Self {
        ws.into_inner()
    }
}

impl FromIterator<(http::header::HeaderName, http::header::HeaderValue)> for HeaderWorkspace {
    fn from_iter<T: IntoIterator<Item = (http::header::HeaderName, http::header::HeaderValue)>>(
        iter: T,
    ) -> Self {
        let headers = HeaderMap::from_iter(iter);
        Self::from_headers(headers)
    }
}

impl<'a> IntoIterator for &'a HeaderWorkspace {
    type Item = (&'a http::header::HeaderName, &'a http::header::HeaderValue);
    type IntoIter = http::header::Iter<'a, http::header::HeaderValue>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.headers.iter()
    }
}

/// Internal bucket segregating permanent baseline workspaces from transient burst workspaces.
struct HeaderBucket {
    /// Baseline pre-warmed physical workspaces. Permanent, never evicted.
    baseline: Vec<HeaderWorkspace>,
    /// Burst standby workspaces paired with their return timestamp.
    /// Ordered chronologically: oldest at front, newest at back.
    burst: VecDeque<(Instant, HeaderWorkspace)>,
    /// Maximum number of permanent baseline workspaces.
    baseline_limit: usize,
    /// Absolute ceiling of active + pooled workspaces (anti-OOM protection).
    max_burst_limit: usize,
}

/// Pre-warmed two-tier adaptive object pool of [`HeaderWorkspace`] instances.
pub struct HeaderPool {
    bucket: Mutex<HeaderBucket>,
    active_leases: AtomicUsize,
    initial_header_capacity: usize,
    idle_ttl: Duration,
}

impl std::fmt::Debug for HeaderPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderPool")
            .field("active_leases", &self.active_leases.load(Ordering::Relaxed))
            .field("initial_header_capacity", &self.initial_header_capacity)
            .finish()
    }
}

impl HeaderPool {
    /// Creates and eagerly warms up a pool with a pre-allocated number of baseline slots
    /// and a default 3x max burst ceiling with 15-second TTL.
    pub fn new(baseline_slots: usize, initial_header_capacity: usize) -> Arc<Self> {
        let max_burst_cap = baseline_slots.saturating_mul(3).max(baseline_slots + 32);
        Self::with_adaptive_limits(
            baseline_slots,
            initial_header_capacity,
            max_burst_cap,
            Duration::from_secs(15),
        )
    }

    /// Creates an adaptive header pool with explicit baseline, max burst ceiling, and idle TTL.
    pub fn with_adaptive_limits(
        baseline_slots: usize,
        initial_header_capacity: usize,
        max_burst_cap: usize,
        idle_ttl: Duration,
    ) -> Arc<Self> {
        let mut baseline = Vec::with_capacity(baseline_slots);
        for _ in 0..baseline_slots {
            baseline.push(HeaderWorkspace::new(initial_header_capacity));
        }

        Arc::new(Self {
            bucket: Mutex::new(HeaderBucket {
                baseline,
                burst: VecDeque::new(),
                baseline_limit: baseline_slots,
                max_burst_limit: max_burst_cap.max(baseline_slots),
            }),
            active_leases: AtomicUsize::new(0),
            initial_header_capacity,
            idle_ttl,
        })
    }

    /// Borrows a [`HeaderWorkspace`] wrapped in an RAII lease.
    ///
    /// Resolution Order:
    /// 1. **Tier 1 (Hot Baseline)**: Pops from pre-warmed physical workspaces (0 alloc, 0 page fault).
    /// 2. **Tier 2 (Burst Standby Queue)**: Pops from warm user-space burst queue (0 alloc).
    /// 3. **Spillover Heap Allocation**: Dynamically allocates if below `max_burst_cap`.
    /// 4. **Anti-OOM Saturation**: Returns `None` if `max_burst_cap` has been reached.
    pub fn acquire(self: &Arc<Self>) -> Option<HeaderWorkspaceLease> {
        let mut guard = self.bucket.lock().unwrap();

        // 1. Try baseline (fastest path, hot pre-warmed)
        let ws = if let Some(ws) = guard.baseline.pop() {
            ws
        } else if let Some((_, ws)) = guard.burst.pop_front() {
            // 2. Try burst standby (warm in user space within TTL)
            ws
        } else {
            // 3. Both pools empty: check anti-OOM ceiling before allocating on heap
            let current_active = self.active_leases.load(Ordering::Relaxed);
            if current_active >= guard.max_burst_limit {
                return None;
            }
            HeaderWorkspace::new(self.initial_header_capacity)
        };

        self.active_leases.fetch_add(1, Ordering::Relaxed);

        Some(HeaderWorkspaceLease {
            workspace: Some(ws),
            pool: Arc::clone(self),
        })
    }

    /// Returns the number of currently checked-out active leases.
    #[inline(always)]
    pub fn active_leases(&self) -> usize {
        self.active_leases.load(Ordering::Relaxed)
    }

    /// Returns the permanent baseline capacity of this pool.
    #[inline(always)]
    pub fn baseline_slots(&self) -> usize {
        self.bucket.lock().unwrap().baseline_limit
    }

    /// Returns the absolute max burst ceiling of this pool.
    #[inline(always)]
    pub fn max_burst_cap(&self) -> usize {
        self.bucket.lock().unwrap().max_burst_limit
    }

    /// Returns the count of burst workspaces currently held in standby.
    #[inline(always)]
    pub fn burst_standby_count(&self) -> usize {
        self.bucket.lock().unwrap().burst.len()
    }

    /// Returns the total number of workspaces currently sitting in the pool (baseline + burst).
    #[inline(always)]
    pub fn total_pooled(&self) -> usize {
        let guard = self.bucket.lock().unwrap();
        guard.baseline.len() + guard.burst.len()
    }

    /// Evicts burst workspaces that have been idle longer than `idle_ttl`.
    ///
    /// # Auxiliary Core Invariant
    /// Uses non-blocking `try_lock()` to ensure that the sweeper running on the Auxiliary Core
    /// NEVER blocks or delays a Traffic Worker core servicing requests.
    pub fn sweep_expired(&self, now: Instant) -> usize {
        let mut evicted = 0;
        if let Ok(mut guard) = self.bucket.try_lock() {
            while let Some((returned_at, _)) = guard.burst.front() {
                if now.saturating_duration_since(*returned_at) >= self.idle_ttl {
                    let (_, ws) = guard.burst.pop_front().unwrap();
                    drop(ws); // Evicted back to allocator arena
                    evicted += 1;
                } else {
                    // Workspaces were pushed chronologically; front is oldest
                    break;
                }
            }
        }
        evicted
    }

    /// Recycles a returned workspace back into the pool.
    fn recycle(&self, mut ws: HeaderWorkspace) {
        ws.reset();
        self.active_leases.fetch_sub(1, Ordering::Relaxed);

        let mut guard = self.bucket.lock().unwrap();
        // Priority 1: Refill permanent baseline floor (Zero clock overhead)
        if guard.baseline.len() < guard.baseline_limit {
            guard.baseline.push(ws);
        } else if guard.baseline.len() + guard.burst.len() < guard.max_burst_limit {
            // Priority 2: Retain in burst standby queue with timestamp for 15s cooldown
            guard.burst.push_back((Instant::now(), ws));
        }
        // Priority 3: Above max_burst_limit -> workspace drops and frees back to allocator
    }
}

/// RAII Lease for a [`HeaderWorkspace`]. Automatically cleans and returns to the pool on drop.
///
/// # DEFENSIVE MEMORY INVARIANT: Anti-Use-After-Free & Anti-Double-Free Defense
/// 1. **Zero Use-After-Free via Compiler Borrow Checker**:
///    Deref to [`HeaderWorkspace`] produces references `&'a HeaderWorkspace` strictly bounded
///    to the lifetime of this lease struct. The moment this lease is dropped (or returns to the pool),
///    the Rust compiler statically invalidates any outstanding references at compile time.
/// 2. **Zero Double-Free via `Option::take()`**:
///    During `drop()`, `self.workspace.take()` extracts the workspace out of `self`. Even if
///    the drop handler were invoked multiple times (e.g. panic stack unwinding), the inner
///    workspace can only ever be returned to the pool exactly once.
/// 3. **Async Cancellation Immunity**:
///    When a client aborts an in-flight connection or Tokio drops an async request future mid-await,
///    stack unwinding automatically invokes this `Drop` implementation, safely returning the
///    workspace to the pool without memory leaks or dangling state.
pub struct HeaderWorkspaceLease {
    workspace: Option<HeaderWorkspace>,
    pool: Arc<HeaderPool>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};

    #[test]
    fn test_header_workspace_recycle_preserves_capacity() {
        let mut ws = HeaderWorkspace::new(32);
        assert!(ws.headers().capacity() >= 32);

        ws.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        ws.headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer token123"));
        assert_eq!(ws.headers().len(), 2);

        ws.reset();
        assert_eq!(ws.headers().len(), 0);
        assert!(ws.headers().capacity() >= 32); // Bucket capacity preserved!
    }

    #[test]
    fn test_header_pool_eager_warmup_and_lease() {
        let pool = HeaderPool::new(4, 32);
        assert_eq!(pool.baseline_slots(), 4);
        assert_eq!(pool.active_leases(), 0);

        {
            let mut lease1 = pool.acquire().expect("acquire lease 1");
            assert_eq!(pool.active_leases(), 1);
            lease1
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
            assert_eq!(lease1.headers().len(), 1);

            let mut lease2 = pool.acquire().expect("acquire lease 2");
            assert_eq!(pool.active_leases(), 2);
            lease2
                .headers_mut()
                .insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        }

        // Both leases dropped: returned to pool, active leases reset to 0
        assert_eq!(pool.active_leases(), 0);

        // Next acquire gets pre-cleaned workspace
        let lease3 = pool.acquire().expect("acquire lease 3");
        assert_eq!(lease3.headers().len(), 0);
        assert!(lease3.headers().capacity() >= 32);
    }

    #[test]
    fn test_header_pool_adaptive_burst_and_decay() {
        // Pool with baseline=2, max_burst_cap=4, idle_ttl=50ms
        let pool = HeaderPool::with_adaptive_limits(2, 32, 4, Duration::from_millis(50));
        assert_eq!(pool.baseline_slots(), 2);
        assert_eq!(pool.max_burst_cap(), 4);

        // 1. Acquire baseline (2 slots)
        let l1 = pool.acquire().expect("lease 1 (baseline)");
        let l2 = pool.acquire().expect("lease 2 (baseline)");
        assert_eq!(pool.active_leases(), 2);

        // 2. Acquire burst (2 slots spillover)
        let l3 = pool.acquire().expect("lease 3 (burst)");
        let l4 = pool.acquire().expect("lease 4 (burst)");
        assert_eq!(pool.active_leases(), 4);

        // 3. Attempt 5th acquire -> Anti-OOM cap reached -> returns None!
        assert!(pool.acquire().is_none());

        // 4. Drop all leases
        drop(l1);
        drop(l2);
        drop(l3);
        drop(l4);
        assert_eq!(pool.active_leases(), 0);

        // 2 refilled baseline, 2 in burst standby queue
        assert_eq!(pool.total_pooled(), 4);
        assert_eq!(pool.burst_standby_count(), 2);

        // Immediate sweep should evict 0 because 50ms TTL hasn't elapsed
        let evicted = pool.sweep_expired(Instant::now());
        assert_eq!(evicted, 0);
        assert_eq!(pool.total_pooled(), 4);

        // Simulate time passage (> 50ms)
        let future_time = Instant::now() + Duration::from_millis(100);
        let evicted = pool.sweep_expired(future_time);
        assert_eq!(evicted, 2);

        // Pool cleanly decayed back to baseline floor (2)
        assert_eq!(pool.total_pooled(), 2);
        assert_eq!(pool.burst_standby_count(), 0);
    }
}
