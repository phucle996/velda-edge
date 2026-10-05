//! Sharded partition architecture with cache-aligned mutex tables.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::container::hasher::FastBuildHasher;
use crate::container::optimal_shard_count;
use crate::container::subpool::SubPool;
use crate::resource::PoolableResource;

thread_local! {
    static THREAD_LANE: usize = {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    };
}

/// Returns the zero-based lane identifier for the current calling thread.
#[inline]
pub fn current_thread_lane() -> usize {
    THREAD_LANE.with(|&lane| lane)
}

/// Cache-line isolated atomic performance counters for a pool shard.
#[repr(align(64))]
pub struct ShardMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    releases: AtomicU64,
    evictions: AtomicU64,
}

impl Default for ShardMetrics {
    fn default() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
}

/// A single concurrency shard holding an independent subpool table and striped metrics.
///
/// Cache-line aligned (64 bytes) to guarantee zero false sharing between CPU cores
/// when adjacent shards are locked simultaneously by different worker threads.
#[repr(align(64))]
pub struct PoolShard<K, R> {
    table: Mutex<HashMap<K, SubPool<R>, FastBuildHasher>>,
    metrics: ShardMetrics,
}

impl<K, R> Default for PoolShard<K, R> {
    fn default() -> Self {
        Self {
            table: Mutex::new(HashMap::with_hasher(FastBuildHasher)),
            metrics: ShardMetrics::default(),
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
    pub fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<K, SubPool<R>, FastBuildHasher>> {
        self.table.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Increments the local shard hit counter.
    #[inline]
    pub fn record_hit(&self) {
        self.metrics.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the local shard miss counter.
    #[inline]
    pub fn record_miss(&self) {
        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the local shard release counter.
    #[inline]
    pub fn record_release(&self) {
        self.metrics.releases.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the local shard eviction counter.
    #[inline]
    pub fn record_evictions(&self, count: u64) {
        if count > 0 {
            self.metrics.evictions.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Returns the cumulative hits recorded by this shard.
    #[inline]
    pub fn hits(&self) -> u64 {
        self.metrics.hits.load(Ordering::Relaxed)
    }

    /// Returns the cumulative misses recorded by this shard.
    #[inline]
    pub fn misses(&self) -> u64 {
        self.metrics.misses.load(Ordering::Relaxed)
    }

    /// Returns the cumulative releases recorded by this shard.
    #[inline]
    pub fn releases(&self) -> u64 {
        self.metrics.releases.load(Ordering::Relaxed)
    }

    /// Returns the cumulative evictions recorded by this shard.
    #[inline]
    pub fn evictions(&self) -> u64 {
        self.metrics.evictions.load(Ordering::Relaxed)
    }

    /// Resets all counters on this shard to zero.
    #[inline]
    pub fn reset_stats(&self) {
        self.metrics.hits.store(0, Ordering::Relaxed);
        self.metrics.misses.store(0, Ordering::Relaxed);
        self.metrics.releases.store(0, Ordering::Relaxed);
        self.metrics.evictions.store(0, Ordering::Relaxed);
    }
}

/// Sharded partition manager distributing keys across independent mutex-protected tables.
pub struct ShardTable<K, R, S = FastBuildHasher> {
    shards: Vec<PoolShard<K, R>>,
    hash_builder: S,
    mask: usize,
}

impl<K: Eq + Hash, R: PoolableResource> Default for ShardTable<K, R, FastBuildHasher> {
    fn default() -> Self {
        Self::with_workers(
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
        )
    }
}

impl<K: Eq + Hash, R: PoolableResource> ShardTable<K, R, FastBuildHasher> {
    /// Creates a new shard table scaled dynamically to the number of worker threads.
    pub fn with_workers(workers: usize) -> Self {
        Self::new(optimal_shard_count(workers))
    }

    /// Creates a new shard table with the exact number of shards (clamped to power of 2, min 1).
    pub fn new(shard_count: usize) -> Self {
        Self::with_hasher(shard_count, FastBuildHasher)
    }
}

impl<K: Eq + Hash, R: PoolableResource, S: BuildHasher> ShardTable<K, R, S> {
    /// Creates a new shard table with custom shard count and custom hash builder.
    pub fn with_hasher(shard_count: usize, hash_builder: S) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(PoolShard::new());
        }

        Self {
            shards,
            hash_builder,
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

    /// Returns a reference to the shard responsible for `key` biased by a worker lane.
    ///
    /// Distributes hot-key access across independent shards when multiple worker threads
    /// concurrently access the same destination address.
    #[inline]
    pub fn shard_for_lane(&self, key: &K, lane: usize) -> &PoolShard<K, R> {
        let base = self.shard_index(key);
        let idx = (base ^ lane) & self.mask;
        &self.shards[idx]
    }

    /// Returns a slice of all shards (used for global sweeping, draining, and metrics).
    #[inline]
    pub fn all_shards(&self) -> &[PoolShard<K, R>] {
        &self.shards
    }
}
