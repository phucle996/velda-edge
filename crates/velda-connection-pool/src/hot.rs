//! Core-local Hot Pool backed by 4KB page-aligned contiguous SoA slabs.
//!
//! ```text
//! HotPool -> lanes[worker_id & mask] -> HotLane<K, R> (align=64B)
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │ lock_state: AtomicU8 (0=UNLOCKED, 1=LOCKED) -- 0 Mutex / 0 futex       │
//! │                                                                         │
//! │ HotTable (SoA, 4KB-aligned contiguous slabs):                           │
//! │   keys:  [ K_0 | K_1 | ... | K_n ]  <-- linear scan (1-2 cachelines)    │
//! │   pools: [ P_0 | P_1 | ... | P_n ]  <-- parallel LIFO SubPools          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::config::optimal_shard_count;
use crate::resource::PoolableResource;
use crate::slab::PageAlignedSlab;
use crate::subpool::SubPool;

thread_local! {
    static THREAD_LANE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

static LANE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Sets the explicit zero-based lane identifier for the calling thread.
#[inline]
pub fn set_current_thread_lane(lane: usize) {
    THREAD_LANE.with(|cell| cell.set(Some(lane)));
}

/// Returns the zero-based lane identifier for the current calling thread.
#[inline]
pub fn current_thread_lane() -> usize {
    THREAD_LANE.with(|cell| match cell.get() {
        Some(lane) => lane,
        None => {
            let lane = LANE_COUNTER.fetch_add(1, Ordering::Relaxed);
            cell.set(Some(lane));
            lane
        }
    })
}

/// Cache-line isolated atomic performance counters for a pool lane.
#[repr(align(64))]
pub struct LaneMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    releases: AtomicU64,
    evictions: AtomicU64,
    spills: AtomicU64,
    cold_pulls: AtomicU64,
}

impl Default for LaneMetrics {
    fn default() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            spills: AtomicU64::new(0),
            cold_pulls: AtomicU64::new(0),
        }
    }
}

/// Contiguous Structure-of-Arrays (SoA) table stored inside each [`HotLane`].
///
/// `keys` and `pools` reside in separate `PageAlignedSlab`s (`align = 4096, size = k * 4096`),
/// so scanning keys touches only compact key cachelines without loading `SubPool` metadata.
pub struct HotTable<K, R> {
    keys: PageAlignedSlab<K>,
    pools: PageAlignedSlab<SubPool<R>>,
}

impl<K: Eq, R: PoolableResource> HotTable<K, R> {
    /// Creates an empty `HotTable` backed by 1 contiguous 4KB page per array.
    pub fn new() -> Self {
        Self {
            keys: PageAlignedSlab::with_pages(1),
            pools: PageAlignedSlab::with_pages(1),
        }
    }

    /// Number of active key containers in this lane.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns `true` if no key containers exist in this lane.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns the number of 4KB pages currently backing keys + subpools.
    #[inline]
    pub fn total_pages(&self) -> usize {
        self.keys.page_count() + self.pools.page_count()
    }

    /// Linear scan over contiguous `keys` slice (1–16 endpoints fit in 1–2 cachelines).
    #[inline]
    fn find_index(&self, key: &K) -> Option<usize> {
        self.keys.as_slice().iter().position(|k| k == key)
    }

    /// Gets a mutable reference to an existing `SubPool<R>` for `key`.
    #[inline]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut SubPool<R>> {
        let idx = self.find_index(key)?;
        Some(&mut self.pools.as_mut_slice()[idx])
    }

    /// Gets an immutable reference to an existing `SubPool<R>` for `key`.
    #[inline]
    pub fn get(&self, key: &K) -> Option<&SubPool<R>> {
        let idx = self.find_index(key)?;
        Some(&self.pools.as_slice()[idx])
    }

    /// Gets an existing `SubPool<R>` or inserts a new one with `max_idle`.
    #[inline]
    pub fn get_or_insert_with(&mut self, key: K, max_idle: usize) -> &mut SubPool<R> {
        if let Some(idx) = self.find_index(&key) {
            return &mut self.pools.as_mut_slice()[idx];
        }
        let idx = self.keys.len();
        self.keys.push(key);
        self.pools.push(SubPool::new(max_idle));
        &mut self.pools.as_mut_slice()[idx]
    }

    /// Inserts or replaces the `SubPool<R>` for `key`.
    pub fn insert(&mut self, key: K, pool: SubPool<R>) {
        if let Some(idx) = self.find_index(&key) {
            self.pools.as_mut_slice()[idx] = pool;
            return;
        }
        self.keys.push(key);
        self.pools.push(pool);
    }

    /// Removes the container for `key` via O(1) swap-remove.
    #[inline]
    pub fn remove(&mut self, key: &K) -> Option<SubPool<R>> {
        let idx = self.find_index(key)?;
        self.keys.swap_remove(idx);
        self.pools.swap_remove(idx)
    }

    /// Retains entries matching `pred`, closing and swap-removing any dropped containers.
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: FnMut(&K, &mut SubPool<R>) -> bool,
    {
        let mut i = 0;
        while i < self.keys.len() {
            let keep = {
                let k = &self.keys.as_slice()[i];
                let p = &mut self.pools.as_mut_slice()[i];
                pred(k, p)
            };
            if keep {
                i += 1;
            } else {
                self.keys.swap_remove(i);
                if let Some(mut dropped) = self.pools.swap_remove(i) {
                    dropped.clear();
                }
            }
        }
    }

    /// Iterates over all `(key, subpool)` pairs in this lane.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&K, &SubPool<R>)> {
        self.keys
            .as_slice()
            .iter()
            .zip(self.pools.as_slice().iter())
    }

    /// Iterates mutably over all `(key, subpool)` pairs in this lane.
    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut SubPool<R>)> {
        self.keys
            .as_slice()
            .iter()
            .zip(self.pools.as_mut_slice().iter_mut())
    }

    /// Closes all idle connections and clears all containers.
    pub fn clear(&mut self) {
        for pool in self.pools.as_mut_slice() {
            pool.clear();
        }
        self.keys.clear();
        self.pools.clear();
    }
}

impl<K: Eq, R: PoolableResource> Default for HotTable<K, R> {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for atomic access to a [`HotLane`].
pub struct HotLaneGuard<'a, K, R> {
    lock: &'a AtomicU8,
    table: &'a mut HotTable<K, R>,
}

impl<K, R> Deref for HotLaneGuard<'_, K, R> {
    type Target = HotTable<K, R>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.table
    }
}

impl<K, R> DerefMut for HotLaneGuard<'_, K, R> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.table
    }
}

impl<K, R> Drop for HotLaneGuard<'_, K, R> {
    #[inline]
    fn drop(&mut self) {
        self.lock.store(0, Ordering::Release);
    }
}

/// Core-local cache-line aligned lane owning a contiguous [`HotTable`] and [`LaneMetrics`].
#[repr(align(64))]
pub struct HotLane<K, R> {
    lock_state: AtomicU8,
    table: UnsafeCell<HotTable<K, R>>,
    metrics: LaneMetrics,
}

unsafe impl<K: Send, R: Send> Send for HotLane<K, R> {}
unsafe impl<K: Send, R: Send> Sync for HotLane<K, R> {}

impl<K: Eq, R: PoolableResource> HotLane<K, R> {
    /// Creates a new empty core-local lane.
    pub fn new() -> Self {
        Self {
            lock_state: AtomicU8::new(0),
            table: UnsafeCell::new(HotTable::new()),
            metrics: LaneMetrics::default(),
        }
    }

    /// Acquires exclusive access to this lane's contiguous table without OS `Mutex` / futex.
    #[inline]
    pub fn lock(&self) -> HotLaneGuard<'_, K, R> {
        let mut spins = 0u32;
        while self
            .lock_state
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spins = spins.wrapping_add(1);
            if spins & 0x3F == 0 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
        HotLaneGuard {
            lock: &self.lock_state,
            table: unsafe { &mut *self.table.get() },
        }
    }

    /// Pops a candidate from this lane for `key` (pure contiguous RAM, no syscalls under lock).
    #[inline]
    pub fn pop_candidate(
        &self,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
        now: Instant,
    ) -> Option<R> {
        let mut table = self.lock();
        table
            .get_mut(key)
            .and_then(|subpool| subpool.pop_candidate(idle_timeout, max_lifetime, now))
    }

    #[inline]
    pub fn record_hit(&self) {
        self.metrics.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_miss(&self) {
        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_release(&self) {
        self.metrics.releases.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_evictions(&self, count: u64) {
        if count > 0 {
            self.metrics.evictions.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_spill(&self) {
        self.metrics.spills.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cold_pull(&self) {
        self.metrics.cold_pulls.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn hits(&self) -> u64 {
        self.metrics.hits.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn misses(&self) -> u64 {
        self.metrics.misses.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn releases(&self) -> u64 {
        self.metrics.releases.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn evictions(&self) -> u64 {
        self.metrics.evictions.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn spills(&self) -> u64 {
        self.metrics.spills.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn cold_pulls(&self) -> u64 {
        self.metrics.cold_pulls.load(Ordering::Relaxed)
    }

    pub fn reset_stats(&self) {
        self.metrics.hits.store(0, Ordering::Relaxed);
        self.metrics.misses.store(0, Ordering::Relaxed);
        self.metrics.releases.store(0, Ordering::Relaxed);
        self.metrics.evictions.store(0, Ordering::Relaxed);
        self.metrics.spills.store(0, Ordering::Relaxed);
        self.metrics.cold_pulls.store(0, Ordering::Relaxed);
    }
}

impl<K: Eq, R: PoolableResource> Default for HotLane<K, R> {
    fn default() -> Self {
        Self::new()
    }
}

/// Core-indexed collection of [`HotLane`]s (direct `lane & mask` indexing, zero hashing).
pub struct HotPool<K, R> {
    lanes: Vec<HotLane<K, R>>,
    mask: usize,
}

impl<K: Eq, R: PoolableResource> HotPool<K, R> {
    /// Creates a new `HotPool` sized to the nearest power-of-two >= `lane_count`.
    pub fn new(lane_count: usize) -> Self {
        let count = lane_count.max(1).next_power_of_two();
        let mut lanes = Vec::with_capacity(count);
        for _ in 0..count {
            lanes.push(HotLane::new());
        }
        Self {
            lanes,
            mask: count - 1,
        }
    }

    /// Creates a `HotPool` sized automatically for `workers`.
    pub fn with_workers(workers: usize) -> Self {
        Self::new(optimal_shard_count(workers))
    }

    /// Total number of core lanes.
    #[inline]
    pub fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// Direct O(1) lookup of the lane assigned to `lane` (`lane & mask`, zero hashing).
    #[inline]
    pub fn lane(&self, lane: usize) -> &HotLane<K, R> {
        &self.lanes[lane & self.mask]
    }

    /// Direct O(1) lookup of the calling thread's assigned lane.
    #[inline]
    pub fn current_lane(&self) -> &HotLane<K, R> {
        self.lane(current_thread_lane())
    }

    /// Returns all lanes for global sweeps, metrics, and draining.
    #[inline]
    pub fn all_lanes(&self) -> &[HotLane<K, R>] {
        &self.lanes
    }
}

impl<K: Eq, R: PoolableResource> Default for HotPool<K, R> {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        Self::with_workers(workers)
    }
}
