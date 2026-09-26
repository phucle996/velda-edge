//! Core `PoolManager` struct definition, hardware scaling, and container inspection.

use std::hash::Hash;

use crate::connection::MultiplexedPool;
use crate::container::{ShardTable, SubPool, optimal_max_idle_per_key, optimal_shard_count};
use crate::key::ConnectionKey;
use crate::manager::config::{PoolConfig, PoolStats};
use crate::resource::PoolableResource;

/// Sharded connection pool manager generic over key `K` and resource `R`.
pub struct PoolManager<K = ConnectionKey, R = Box<dyn PoolableResource>>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    pub(crate) shards: ShardTable<K, R>,
    pub(crate) multiplexed: MultiplexedPool<K, R>,
    pub(crate) config: PoolConfig,
}

impl<K: Eq + Hash + Clone, R: PoolableResource> Default for PoolManager<K, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone, R: PoolableResource> PoolManager<K, R> {
    /// Creates a new pool manager dynamically scaled to the system's available CPU parallelism.
    pub fn new() -> Self {
        Self::with_config(PoolConfig::default())
    }

    /// Creates a pool manager with custom configuration dynamically scaled to the system's available CPU parallelism.
    pub fn with_config(config: PoolConfig) -> Self {
        let workers = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        Self::with_config_and_workers(config, workers)
    }

    /// Creates a pool manager dynamically scaled to the number of worker threads.
    pub fn with_workers(workers: usize) -> Self {
        let config = PoolConfig {
            max_idle_per_key: optimal_max_idle_per_key(workers),
            ..Default::default()
        };
        Self::with_config_and_workers(config, workers)
    }

    /// Creates a pool manager with custom configuration and dynamic hardware worker shards.
    pub fn with_config_and_workers(config: PoolConfig, workers: usize) -> Self {
        Self::with_config_and_shards(config, optimal_shard_count(workers))
    }

    /// Creates a pool manager with the specified configuration and exact number of shards.
    pub fn with_config_and_shards(config: PoolConfig, shard_count: usize) -> Self {
        Self {
            shards: ShardTable::new(shard_count),
            multiplexed: MultiplexedPool::with_shards(shard_count),
            config,
        }
    }

    /// Returns a reference to the inner multiplexed pool.
    #[inline]
    pub fn multiplexed(&self) -> &MultiplexedPool<K, R> {
        &self.multiplexed
    }

    /// Returns the active configuration of this pool manager.
    #[inline]
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Returns the active shard partition count.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.shard_count()
    }

    /// Checks whether a pool container has been created for `key`.
    pub fn has_pool(&self, key: &K) -> bool {
        let shard = self.shards.shard_for(key);
        let table = shard.lock();
        table.contains_key(key)
    }

    /// Pre-registers or ensures that a pool container exists for `key`.
    pub fn ensure_pool(&self, key: &K) -> bool {
        let shard = self.shards.shard_for(key);
        let mut table = shard.lock();
        if table.contains_key(key) {
            false
        } else {
            table.insert(key.clone(), SubPool::new(self.config.max_idle_per_key));
            true
        }
    }

    /// Returns the total number of currently idle connections across all pool containers.
    pub fn total_idle_conns(&self) -> usize {
        let mut count = 0;
        for shard in self.shards.all_shards() {
            let table = shard.lock();
            count += table.values().map(|s| s.len()).sum::<usize>();
        }
        count
    }

    /// Returns the total number of active pool containers across all shards.
    pub fn total_pool_containers(&self) -> usize {
        let mut count = 0;
        for shard in self.shards.all_shards() {
            let table = shard.lock();
            count += table.len();
        }
        count
    }

    /// Returns real-time metrics for pool operations aggregated across striped shards.
    pub fn stats(&self) -> PoolStats {
        let mut total = PoolStats::default();
        for shard in self.shards.all_shards() {
            total.hits += shard.hits();
            total.misses += shard.misses();
            total.releases += shard.releases();
            total.evictions += shard.evictions();
        }
        total
    }

    /// Resets all metrics counters across all shards to zero.
    pub fn reset_stats(&self) {
        for shard in self.shards.all_shards() {
            shard.reset_stats();
        }
    }
}
