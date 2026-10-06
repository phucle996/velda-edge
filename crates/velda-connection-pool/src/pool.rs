//! High-performance sharded connection pool manager and unified acquisition facade.
//!
//! # Architecture & Responsibilities (SRP)
//!
//! [`PoolManager`] is the single composition root and facade for connection reuse.
//! It does NOT own connection establishment (`connect()` is strictly external),
//! but coordinates:
//! - **LIFO Subpools**: For 1:1 transactional HTTP/1.1 and dedicated TCP streams.
//! - **Multiplexed Pools**: For 1:N concurrent stream-sharing HTTP/2 and HTTP/3 sessions.
//! - **Hardware Scaled Shards**: Cache-line aligned partitions (`#[repr(align(64))]`) to eliminate multicore lock contention.

use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::{PoolConfig, PoolStats};
use crate::container::{
    MultiplexedPool, ShardTable, SubPool, optimal_max_idle_per_key, optimal_shard_count,
};
use crate::key::ConnectionKey;
use crate::lease::{ConnectionLease, ExclusiveLease, SequentialLease, StreamLease};
use crate::profile::{ConnectionProfile, ReuseMode};
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
    // ------------------------------------------------------------------------
    // Constructors & Hardware Scaling
    // ------------------------------------------------------------------------

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

    // ------------------------------------------------------------------------
    // Inspection & State Queries
    // ------------------------------------------------------------------------

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
        for shard in self.shards.all_shards() {
            if shard.lock().contains_key(key) {
                return true;
            }
        }
        false
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

    // ------------------------------------------------------------------------
    // Low-Level Resource Acquisition & Return
    // ------------------------------------------------------------------------

    /// Attempts to acquire an idle reusable connection for `key` using default lifetime limit.
    pub fn acquire(&self, key: &K, idle_timeout: Duration) -> Option<R> {
        self.acquire_with_lifetime(key, idle_timeout, self.config.max_lifetime)
    }

    /// Attempts to acquire an idle connection with explicit max lifetime validation.
    ///
    /// # Bare-Metal Hardware Invariant
    /// - Candidate popping from LIFO subpool executes 100% in RAM under shard lock (< 10 ns).
    /// - Health verification (`resource.is_healthy()`) and closure of stale sockets execute
    ///   strictly OUTSIDE the mutex lock, eliminating kernel syscall serialization across CPU cores.
    /// - Multi-lane striping checks thread-local shard first to eliminate cross-core lock contention.
    pub fn acquire_with_lifetime(
        &self,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> Option<R> {
        let now = Instant::now();
        let lane = crate::container::shard::current_thread_lane();
        let base_shard = self.shards.shard_for(key);
        let local_shard = self.shards.shard_for_lane(key, lane);

        // Step 1: Pop candidate from local lane shard (pure RAM, < 10 ns, zero cross-core contention)
        loop {
            let candidate = {
                let mut table = local_shard.lock();
                if let Some(subpool) = table.get_mut(key) {
                    subpool.pop_candidate(idle_timeout, max_lifetime, now)
                } else {
                    None
                }
            };

            match candidate {
                Some(mut resource) => {
                    if resource.is_healthy() {
                        resource.touch_at(now);
                        local_shard.record_hit();
                        return Some(resource);
                    } else {
                        resource.close();
                    }
                }
                None => break,
            }
        }

        // Step 2: If local lane has no healthy candidate and local != base, borrow from base shard
        if !std::ptr::eq(local_shard, base_shard) {
            loop {
                let candidate = {
                    let mut table = base_shard.lock();
                    if let Some(subpool) = table.get_mut(key) {
                        subpool.pop_candidate(idle_timeout, max_lifetime, now)
                    } else {
                        None
                    }
                };

                match candidate {
                    Some(mut resource) => {
                        if resource.is_healthy() {
                            resource.touch_at(now);
                            base_shard.record_hit();
                            return Some(resource);
                        } else {
                            resource.close();
                        }
                    }
                    None => break,
                }
            }
        }

        local_shard.record_miss();
        None
    }

    /// Acquires an idle reusable connection wrapped in an RAII [`SequentialLease`].
    pub fn acquire_lease(
        self: &Arc<Self>,
        key: &K,
        idle_timeout: Duration,
    ) -> Option<SequentialLease<K, R>> {
        self.acquire_lease_with_lifetime(key, idle_timeout, self.config.max_lifetime, false)
    }

    /// Acquires an idle reusable connection wrapped in an RAII [`SequentialLease`] with explicit lifetime and draining mode.
    pub fn acquire_lease_with_lifetime(
        self: &Arc<Self>,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
        is_draining: bool,
    ) -> Option<SequentialLease<K, R>> {
        let res = self.acquire_with_lifetime(key, idle_timeout, max_lifetime)?;
        Some(SequentialLease::new(
            res,
            key.clone(),
            Arc::clone(self),
            is_draining,
        ))
    }

    /// Releases a resource back into its designated pool container.
    ///
    /// If `is_reusable` is false, `is_draining` is true, or the resource is unhealthy,
    /// it is immediately closed instead of returned to the pool.
    /// If the pool container is full (`len >= max_idle_per_key`), excess connections are closed.
    pub fn release(&self, key: &K, mut resource: R, is_reusable: bool, is_draining: bool) {
        if !is_reusable || is_draining || !resource.is_healthy() {
            resource.close();
            return;
        }

        let lane = crate::container::shard::current_thread_lane();
        let shard = self.shards.shard_for_lane(key, lane);
        let mut table = shard.lock();

        if let Some(subpool) = table.get_mut(key) {
            subpool.release(resource);
        } else {
            let mut subpool = SubPool::new(self.config.max_idle_per_key);
            subpool.release(resource);
            table.insert(key.clone(), subpool);
        }
        shard.record_release();
    }

    // ------------------------------------------------------------------------
    // Sweeping, Eviction, and Draining Operations
    // ------------------------------------------------------------------------

    /// Closes and drains all idle connections for `key`, and removes the pool container across all shards.
    pub fn drain_key(&self, key: &K) -> usize {
        let mut shards_drained = 0;
        for shard in self.shards.all_shards() {
            let mut table = shard.lock();
            if let Some(mut subpool) = table.remove(key) {
                shards_drained += subpool.drain_all();
            }
        }
        let mux_drained = self.multiplexed.drain_matching(|k| k == key);
        shards_drained + mux_drained
    }

    /// Drains all idle connections matching a predicate function and prunes their containers.
    ///
    /// Ensures zero memory leaks when an entire backend endpoint or service is decommissioned.
    pub fn drain_matching(&self, predicate: impl Fn(&K) -> bool) -> usize {
        let mut total_drained = 0;
        for shard in self.shards.all_shards() {
            let mut table = shard.lock();
            table.retain(|k, subpool| {
                if predicate(k) {
                    total_drained += subpool.drain_all();
                    false
                } else {
                    true
                }
            });
        }
        total_drained += self.multiplexed.drain_matching(&predicate);
        total_drained
    }

    /// Closes all idle connections across all shards and clears all containers.
    pub fn clear(&self) {
        for shard in self.shards.all_shards() {
            let mut table = shard.lock();
            for (_, mut subpool) in table.drain() {
                subpool.drain_all();
            }
        }
        self.multiplexed.drain_matching(|_| true);
    }

    /// Sweeps all pool containers across all shards, purging connections that exceeded
    /// `idle_timeout` or configured `max_lifetime`.
    pub fn evict_expired(&self, idle_timeout: Duration) -> usize {
        self.evict_expired_with_lifetime(idle_timeout, self.config.max_lifetime)
    }

    /// Sweeps all pool containers with explicit lifetime control.
    pub fn evict_expired_with_lifetime(
        &self,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> usize {
        let mut total_evicted = 0;
        for shard in self.shards.all_shards() {
            let mut table = shard.lock();
            let mut shard_evicted = 0;
            for subpool in table.values_mut() {
                shard_evicted += subpool.evict_expired(idle_timeout, max_lifetime);
            }
            shard.record_evictions(shard_evicted as u64);
            total_evicted += shard_evicted;
        }
        let mux_evicted = self.multiplexed.evict_idle(idle_timeout);
        total_evicted += mux_evicted;
        total_evicted
    }

    /// Prunes pool containers that have continuously remained empty for longer than `empty_ttl`.
    pub fn prune_empty_pools(&self, empty_ttl: Duration) -> usize {
        let mut pruned = 0;
        for shard in self.shards.all_shards() {
            let mut table = shard.lock();
            table.retain(|_, subpool| {
                if subpool.is_empty() && subpool.empty_duration() >= empty_ttl {
                    pruned += 1;
                    false
                } else {
                    true
                }
            });
        }
        pruned
    }
}

// ============================================================================
// High-Level ConnectionProfile Facade (ConnectionKey-specific)
// ============================================================================

impl<R: PoolableResource> PoolManager<ConnectionKey, R> {
    /// Attempts to acquire an idle connection or stream slot matching the given [`ConnectionProfile`].
    pub fn acquire_profile(
        self: &Arc<Self>,
        profile: &ConnectionProfile,
    ) -> Option<ConnectionLease<ConnectionKey, R>> {
        match profile.reuse_mode {
            ReuseMode::Multiplexed => {
                let shard = self.shards.shard_for(&profile.key);
                if let Some(stream_lease) = self
                    .multiplexed
                    .acquire_stream(&profile.key, profile.idle_timeout)
                {
                    shard.record_hit();
                    Some(ConnectionLease::Multiplexed(stream_lease))
                } else {
                    shard.record_miss();
                    None
                }
            }
            ReuseMode::Sequential => {
                let res = self.acquire_with_lifetime(
                    &profile.key,
                    profile.idle_timeout,
                    profile.max_lifetime,
                )?;
                Some(ConnectionLease::Sequential(SequentialLease::new(
                    res,
                    profile.key.clone(),
                    Arc::clone(self),
                    false,
                )))
            }
            ReuseMode::Exclusive => {
                let res = self.acquire_with_lifetime(
                    &profile.key,
                    profile.idle_timeout,
                    profile.max_lifetime,
                )?;
                let lease =
                    ExclusiveLease::with_pool(res, profile.key.clone(), Arc::clone(self), false);
                Some(ConnectionLease::Exclusive(lease))
            }
        }
    }

    /// Registers a newly established physical connection under the given [`ConnectionProfile`].
    pub fn register_profile(
        self: &Arc<Self>,
        profile: &ConnectionProfile,
        resource: R,
    ) -> ConnectionLease<ConnectionKey, R> {
        let shard = self.shards.shard_for(&profile.key);
        shard.record_release();

        match profile.reuse_mode {
            ReuseMode::Multiplexed => {
                let stream_lease = self.multiplexed.register(
                    profile.key.clone(),
                    resource,
                    profile.max_concurrent_streams,
                );
                ConnectionLease::Multiplexed(stream_lease)
            }
            ReuseMode::Sequential => ConnectionLease::Sequential(SequentialLease::new(
                resource,
                profile.key.clone(),
                Arc::clone(self),
                false,
            )),
            ReuseMode::Exclusive => {
                let lease = ExclusiveLease::with_pool(
                    resource,
                    profile.key.clone(),
                    Arc::clone(self),
                    false,
                );
                ConnectionLease::Exclusive(lease)
            }
        }
    }

    /// Directly acquires a stream slot on an existing multiplexed connection.
    pub fn acquire_stream(&self, profile: &ConnectionProfile) -> Option<StreamLease<R>> {
        let shard = self.shards.shard_for(&profile.key);
        if let Some(stream) = self
            .multiplexed
            .acquire_stream(&profile.key, profile.idle_timeout)
        {
            shard.record_hit();
            Some(stream)
        } else {
            shard.record_miss();
            None
        }
    }

    /// Registers a new physical connection for multiplexed stream sharing and reserves 1 stream slot.
    pub fn register_multiplexed(&self, profile: &ConnectionProfile, resource: R) -> StreamLease<R> {
        let shard = self.shards.shard_for(&profile.key);
        shard.record_release();
        self.multiplexed.register(
            profile.key.clone(),
            resource,
            profile.max_concurrent_streams,
        )
    }

    /// Acquires an idle connection by value for sequential or exclusive modes.
    pub fn acquire_by_value(&self, profile: &ConnectionProfile) -> Option<R> {
        assert!(
            profile.reuse_mode != ReuseMode::Multiplexed,
            "Multiplexed connections cannot be checked out by-value; use acquire_profile or acquire_stream"
        );
        self.acquire_with_lifetime(&profile.key, profile.idle_timeout, profile.max_lifetime)
    }

    /// Releases a connection by value according to the specified [`ConnectionProfile`].
    pub fn release_by_value(
        &self,
        profile: &ConnectionProfile,
        resource: R,
        is_reusable: bool,
        is_draining: bool,
    ) {
        match profile.reuse_mode {
            ReuseMode::Sequential | ReuseMode::Exclusive => {
                self.release(&profile.key, resource, is_reusable, is_draining);
            }
            ReuseMode::Multiplexed => {
                if !is_reusable || is_draining || !resource.is_healthy() {
                    let mut res = resource;
                    res.close();
                }
            }
        }
    }
}
