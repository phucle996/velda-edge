//! Sharded container architecture combining LIFO subpools and cache-aligned mutex partitions.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::resource::PoolableResource;

/// Minimum number of idle connections retained per key container.
pub const MIN_IDLE_PER_KEY: usize = 8;

/// Maximum number of idle connections retained per key container to protect from EMFILE.
pub const MAX_IDLE_PER_KEY: usize = 128;

/// Minimum shard count for concurrency distribution.
pub const MIN_SHARDS: usize = 8;

/// Maximum shard count for concurrency distribution on ultra-high-core hardware.
pub const MAX_SHARDS: usize = 1024;

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

/// Isolated LIFO container holding idle resources for a single key identity.
#[derive(Debug)]
pub struct SubPool<R> {
    idle_resources: Vec<R>,
    max_idle: usize,
    created_at: Instant,
    empty_since: Option<Instant>,
}

impl<R: PoolableResource> Default for SubPool<R> {
    fn default() -> Self {
        Self::new(probed_max_idle_per_key())
    }
}

impl<R: PoolableResource> SubPool<R> {
    /// Creates an empty container with the specified capacity limit.
    pub fn new(max_idle: usize) -> Self {
        Self {
            idle_resources: Vec::new(),
            max_idle: max_idle.max(1),
            created_at: Instant::now(),
            empty_since: Some(Instant::now()),
        }
    }

    /// Returns the timestamp when this container was created.
    #[inline]
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Returns the configured maximum idle capacity.
    #[inline]
    pub fn max_idle(&self) -> usize {
        self.max_idle
    }

    /// Returns the duration this container has continuously remained empty, or Duration::ZERO if not empty.
    #[inline]
    pub fn empty_duration(&self) -> Duration {
        self.empty_since.map_or(Duration::ZERO, |t| t.elapsed())
    }

    /// Sets the maximum idle capacity for this container and closes any excess idle resources.
    pub fn set_max_idle(&mut self, max_idle: usize) {
        self.max_idle = max_idle.max(1);
        if self.idle_resources.len() > self.max_idle {
            let excess = self.idle_resources.len() - self.max_idle;
            for mut excess_res in self.idle_resources.drain(0..excess) {
                excess_res.close();
            }
        }
    }

    /// Attempts to acquire an idle resource respecting health, idle expiration, and max lifetime.
    ///
    /// Uses LIFO (Last-In-First-Out) retrieval: the most recently used connection is
    /// popped first, maximizing hot TCP window reuse and avoiding remote server keep-alive timeouts.
    /// Samples system clock once to minimize critical section latency under lock.
    pub fn acquire(&mut self, idle_timeout: Duration, max_lifetime: Option<Duration>) -> Option<R> {
        let now = Instant::now();
        while let Some(mut resource) = self.idle_resources.pop() {
            let is_idle_valid =
                now.saturating_duration_since(resource.last_used_at()) <= idle_timeout;
            let is_lifetime_valid = match max_lifetime {
                Some(ttl) => now.saturating_duration_since(resource.created_at()) <= ttl,
                None => true,
            };

            if resource.is_healthy() && is_idle_valid && is_lifetime_valid {
                resource.touch_at(now);
                if self.idle_resources.is_empty() {
                    self.empty_since = Some(now);
                }
                return Some(resource);
            }
            resource.close();
        }

        if self.idle_resources.is_empty() && self.empty_since.is_none() {
            self.empty_since = Some(now);
        }
        None
    }

    /// Releases an active resource back into the idle container.
    ///
    /// If the container is full (`len >= max_idle`), the resource is closed immediately
    /// to prevent File Descriptor exhaustion (`EMFILE`).
    pub fn release(&mut self, mut resource: R) {
        if !resource.is_healthy() {
            resource.close();
            return;
        }

        if self.idle_resources.len() < self.max_idle {
            self.empty_since = None;
            self.idle_resources.push(resource);
        } else {
            resource.close();
        }
    }

    /// Purges all expired or unhealthy resources from this container.
    ///
    /// Returns the number of evicted resources.
    pub fn evict_expired(
        &mut self,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> usize {
        let now = Instant::now();
        let initial_len = self.idle_resources.len();
        self.idle_resources.retain_mut(|res| {
            let is_idle_valid = now.saturating_duration_since(res.last_used_at()) <= idle_timeout;
            let is_lifetime_valid = match max_lifetime {
                Some(ttl) => now.saturating_duration_since(res.created_at()) <= ttl,
                None => true,
            };

            if res.is_healthy() && is_idle_valid && is_lifetime_valid {
                true
            } else {
                res.close();
                false
            }
        });

        if self.idle_resources.is_empty() && self.empty_since.is_none() {
            self.empty_since = Some(now);
        }

        initial_len - self.idle_resources.len()
    }

    /// Closes and drains all connections in this container.
    ///
    /// Returns the number of drained resources.
    pub fn drain_all(&mut self) -> usize {
        let count = self.idle_resources.len();
        for mut res in self.idle_resources.drain(..) {
            res.close();
        }
        self.empty_since = Some(Instant::now());
        count
    }

    /// Closes all connections and clears the container.
    #[inline]
    pub fn clear(&mut self) {
        self.drain_all();
    }

    /// Returns the current number of idle resources in this container.
    #[inline]
    pub fn len(&self) -> usize {
        self.idle_resources.len()
    }

    /// Returns `true` if there are no idle resources.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.idle_resources.is_empty()
    }
}

/// A single concurrency shard holding an independent subpool table and striped metrics.
///
/// Cache-line aligned (64 bytes) to guarantee zero false sharing between CPU cores
/// when adjacent shards are locked simultaneously by different worker threads.
#[repr(align(64))]
pub struct PoolShard<K, R> {
    table: Mutex<HashMap<K, SubPool<R>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    releases: AtomicU64,
    evictions: AtomicU64,
}

impl<K, R> Default for PoolShard<K, R> {
    fn default() -> Self {
        Self {
            table: Mutex::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
}

impl<K: Eq + Hash, R: PoolableResource> PoolShard<K, R> {
    /// Creates a new empty pool shard with striped metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Accesses the inner locked table with poison error recovery.
    #[inline]
    pub fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<K, SubPool<R>>> {
        self.table.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Increments the local shard hit counter.
    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the local shard miss counter.
    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the local shard release counter.
    #[inline]
    pub fn record_release(&self) {
        self.releases.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the local shard eviction counter.
    #[inline]
    pub fn record_evictions(&self, count: u64) {
        if count > 0 {
            self.evictions.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Returns the cumulative hits recorded by this shard.
    #[inline]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Returns the cumulative misses recorded by this shard.
    #[inline]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Returns the cumulative releases recorded by this shard.
    #[inline]
    pub fn releases(&self) -> u64 {
        self.releases.load(Ordering::Relaxed)
    }

    /// Returns the cumulative evictions recorded by this shard.
    #[inline]
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Resets all counters on this shard to zero.
    #[inline]
    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.releases.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
}

/// Sharded partition manager distributing keys across independent mutex-protected tables.
pub struct ShardTable<K, R, S = std::collections::hash_map::RandomState> {
    shards: Vec<PoolShard<K, R>>,
    hash_builder: S,
    mask: usize,
}

impl<K: Eq + Hash, R: PoolableResource> Default for ShardTable<K, R> {
    fn default() -> Self {
        Self::with_workers(
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
        )
    }
}

impl<K: Eq + Hash, R: PoolableResource> ShardTable<K, R> {
    /// Creates a new shard table scaled dynamically to the number of worker threads.
    pub fn with_workers(workers: usize) -> Self {
        Self::new(optimal_shard_count(workers))
    }

    /// Creates a new shard table with the exact number of shards (clamped to power of 2, min 1).
    pub fn new(shard_count: usize) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(PoolShard::new());
        }

        Self {
            shards,
            hash_builder: std::collections::hash_map::RandomState::new(),
            mask: count - 1,
        }
    }

    /// Number of active shards in this table.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Computes the target shard index for the given key.
    #[inline]
    pub fn shard_index(&self, key: &K) -> usize {
        (self.hash_builder.hash_one(key) as usize) & self.mask
    }

    /// Returns a reference to the shard responsible for `key`.
    #[inline]
    pub fn shard_for(&self, key: &K) -> &PoolShard<K, R> {
        let idx = self.shard_index(key);
        &self.shards[idx]
    }

    /// Returns a slice of all shards (used for global sweeping, draining, and metrics).
    #[inline]
    pub fn all_shards(&self) -> &[PoolShard<K, R>] {
        &self.shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

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
        // Normal lock
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

        // 1 expired connection
        subpool.release(Dummy(1, past, AtomicBool::new(true)));
        // 1 active connection
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
