//! Two-tier page-aligned connection pool manager (`HotPool` + `ColdPool`).
//!
//! ```text
//!  acquire():
//!    [1] hot.current_lane().pop() ──► Hit? ──► return (0 Mutex, 0 epoll_ctl)
//!         │ Miss
//!         ▼
//!    [2] cold.pop() ──► Hit? ──► on_hot_restore() ──► return
//!         │ Miss
//!         ▼
//!       None (caller dials fresh connection via velda-sock)
//!
//!  release():
//!    push into hot.lane(lane) ──► overflow max_idle_per_key?
//!                                     └─► pop coldest -> on_cold_store() -> cold.push()
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cold::ColdPool;
use crate::config::{PoolConfig, PoolStats, optimal_max_idle_per_key, optimal_shard_count};
use crate::hot::{HotPool, current_thread_lane};
use crate::key::CompactPoolKey;
use crate::lease::{ConnectionLease, ExclusiveLease, SequentialLease, StreamLease};
use crate::multiplexed::MultiplexedPool;
use crate::profile::{ConnectionProfile, ReuseMode};
use crate::resource::PoolableResource;
use crate::subpool::SubPool;

/// Two-tier connection pool manager generic over key `K` and resource `R`.
pub struct PoolManager<K = CompactPoolKey, R = Box<dyn PoolableResource>>
where
    K: Eq + Clone,
    R: PoolableResource,
{
    pub(crate) hot: HotPool<K, R>,
    pub(crate) cold: ColdPool<K, R>,
    pub(crate) multiplexed: MultiplexedPool<K, R>,
    pub(crate) config: PoolConfig,
}

impl<K: Eq + Clone, R: PoolableResource> Default for PoolManager<K, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Clone, R: PoolableResource> PoolManager<K, R> {
    // ------------------------------------------------------------------------
    // Constructors & Hardware Scaling
    // ------------------------------------------------------------------------

    /// Creates a new pool manager dynamically scaled to the system's available CPU parallelism.
    pub fn new() -> Self {
        Self::with_config(PoolConfig::default())
    }

    /// Creates a pool manager with custom configuration dynamically scaled to CPU parallelism.
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

    /// Creates a pool manager with custom configuration and dynamic hardware worker lanes.
    pub fn with_config_and_workers(config: PoolConfig, workers: usize) -> Self {
        Self::with_config_and_shards(config, optimal_shard_count(workers))
    }

    /// Creates a pool manager with the specified configuration and exact number of lanes.
    pub fn with_config_and_shards(config: PoolConfig, lane_count: usize) -> Self {
        let lanes = lane_count.max(1).next_power_of_two();
        // Size shared ColdPool to absorb overflow across lanes cleanly in contiguous 4KB pages.
        let cold_cap = (config.max_idle_per_key.max(16) * 2).clamp(64, 4096);
        Self {
            hot: HotPool::new(lanes),
            cold: ColdPool::with_capacity(cold_cap),
            multiplexed: MultiplexedPool::with_shards(lanes),
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

    /// Returns a reference to the shared lock-free Cold Pool.
    #[inline]
    pub fn cold_pool(&self) -> &ColdPool<K, R> {
        &self.cold
    }

    /// Returns the active configuration of this pool manager.
    #[inline]
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Returns the active core lane count.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.hot.lane_count()
    }

    /// Checks whether a hot container has been created for `key`.
    pub fn has_pool(&self, key: &K) -> bool {
        for lane in self.hot.all_lanes() {
            if lane.lock().get(key).is_some() {
                return true;
            }
        }
        self.cold.count_for_key(key) > 0
    }

    /// Pre-registers or ensures that a hot container exists for `key` on the current lane.
    pub fn ensure_pool(&self, key: &K) -> bool {
        let lane = self.hot.current_lane();
        let mut table = lane.lock();
        if table.get(key).is_some() {
            false
        } else {
            table.insert(key.clone(), SubPool::new(self.config.max_idle_per_key));
            true
        }
    }

    /// Returns the total number of currently idle connections across Hot lanes + Cold Pool.
    pub fn total_idle_conns(&self) -> usize {
        let mut count = self.cold.len();
        for lane in self.hot.all_lanes() {
            let table = lane.lock();
            count += table.iter().map(|(_, s)| s.len()).sum::<usize>();
        }
        count
    }

    /// Returns the total number of active key containers across all Hot lanes.
    pub fn total_pool_containers(&self) -> usize {
        let mut count = 0;
        for lane in self.hot.all_lanes() {
            count += lane.lock().len();
        }
        count
    }

    /// Returns real-time metrics for pool operations aggregated across all lanes.
    pub fn stats(&self) -> PoolStats {
        let mut total = PoolStats::default();
        for lane in self.hot.all_lanes() {
            total.hits += lane.hits();
            total.misses += lane.misses();
            total.releases += lane.releases();
            total.evictions += lane.evictions();
        }
        total
    }

    /// Resets all metrics counters across all lanes to zero.
    pub fn reset_stats(&self) {
        for lane in self.hot.all_lanes() {
            lane.reset_stats();
        }
    }

    /// Returns the number of idle connections for `key` in the specified worker lane.
    #[inline]
    pub fn idle_conns_for_lane(&self, key: &K, lane: usize) -> usize {
        let table = self.hot.lane(lane).lock();
        table.get(key).map_or(0, |s| s.len())
    }

    /// Updates the maximum idle capacity per key across all active containers.
    pub fn set_max_idle_per_key(&mut self, max_idle: usize) {
        self.config.max_idle_per_key = max_idle.max(1);
        for lane in self.hot.all_lanes() {
            let mut table = lane.lock();
            for (_, subpool) in table.iter_mut() {
                subpool.set_max_idle(self.config.max_idle_per_key);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Low-Level Resource Acquisition & Return (Hot -> Cold 2-Tier)
    // ------------------------------------------------------------------------

    /// Attempts to acquire an idle reusable connection for `key` using default lifetime limit.
    pub fn acquire(&self, key: &K, idle_timeout: Duration) -> Option<R> {
        self.acquire_with_lifetime(key, idle_timeout, self.config.max_lifetime)
    }

    /// Attempts to acquire an idle connection:
    /// 1. **Tier 1 (Core-Local HotPool)**: Pure contiguous RAM lookup (`0 Mutex`, `0 HashMap`, `0 epoll_ctl`).
    /// 2. **Tier 2 (Shared Lock-Free ColdPool)**: O(1) lock-free pull when local HotPool is empty.
    pub fn acquire_with_lifetime(
        &self,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> Option<R> {
        let now = Instant::now();
        let local_lane = self.hot.current_lane();

        // Tier 1: Core-local HotPool
        loop {
            let candidate = local_lane.pop_candidate(key, idle_timeout, max_lifetime, now);
            match candidate {
                Some(mut resource) => {
                    if resource.is_healthy() {
                        resource.touch_at(now);
                        local_lane.record_hit();
                        return Some(resource);
                    }
                    resource.close();
                }
                None => break,
            }
        }

        // Tier 2: Shared lock-free ColdPool pull (re-binds resource onto acquiring core)
        if let Some(mut resource) = self.cold.pop(key, idle_timeout, max_lifetime, now) {
            resource.on_hot_restore();
            local_lane.record_hit();
            local_lane.record_cold_pull();
            return Some(resource);
        }

        local_lane.record_miss();
        None
    }

    /// Acquires a connection wrapped in a [`SequentialLease`] that auto-returns on drop.
    pub fn acquire_lease(
        self: &Arc<Self>,
        key: &K,
        idle_timeout: Duration,
    ) -> Option<SequentialLease<K, R>> {
        self.acquire_lease_with_lifetime(key, idle_timeout, self.config.max_lifetime)
    }

    /// Acquires a [`SequentialLease`] with explicit maximum lifetime validation.
    pub fn acquire_lease_with_lifetime(
        self: &Arc<Self>,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> Option<SequentialLease<K, R>> {
        let resource = self.acquire_with_lifetime(key, idle_timeout, max_lifetime)?;
        Some(SequentialLease::new(
            resource,
            key.clone(),
            Arc::clone(self),
            false,
        ))
    }

    /// Releases a resource into the calling core's HotPool, spilling the coldest connection
    /// to the shared ColdPool if the local HotPool reaches `max_idle_per_key`.
    pub fn release(&self, key: &K, mut resource: R, is_reusable: bool, is_draining: bool) {
        if !is_reusable || is_draining || !resource.is_healthy() {
            resource.close();
            return;
        }

        let lane = current_thread_lane();
        self.push_hot_or_spill(key, lane, resource);
    }

    /// Releases a pre-warmed resource into the designated core lane (spilling excess to ColdPool).
    pub fn release_to_lane(&self, key: &K, lane: usize, mut resource: R) {
        if !resource.is_healthy() {
            resource.close();
            return;
        }

        self.push_hot_or_spill(key, lane, resource);
    }

    #[inline]
    fn push_hot_or_spill(&self, key: &K, lane: usize, resource: R) {
        let hot_lane = self.hot.lane(lane);
        let spilled = {
            let mut table = hot_lane.lock();
            let subpool = table.get_or_insert_with(key.clone(), self.config.max_idle_per_key);
            subpool.push_or_spill(resource)
        };
        hot_lane.record_release();

        if let Some(mut coldest) = spilled {
            coldest.on_cold_store();
            hot_lane.record_spill();
            if let Err(mut rejected) = self.cold.push(key.clone(), coldest) {
                rejected.close();
            }
        }
    }

    /// Attempts to acquire an idle connection chosen via pseudo-random sampling (FastRange)
    /// from the specified worker lane.
    pub fn acquire_random_from_lane(
        &self,
        key: &K,
        lane: usize,
        idle_timeout: Duration,
        seed: u32,
    ) -> Option<R> {
        let now = Instant::now();
        let hot_lane = self.hot.lane(lane);
        loop {
            let candidate = {
                let mut table = hot_lane.lock();
                table.get_mut(key).and_then(|subpool| {
                    subpool.pop_random_candidate(idle_timeout, self.config.max_lifetime, now, seed)
                })
            };

            match candidate {
                Some(mut resource) => {
                    if resource.is_healthy() {
                        resource.touch_at(now);
                        hot_lane.record_hit();
                        return Some(resource);
                    }
                    resource.close();
                }
                None => break,
            }
        }
        None
    }

    /// Drains and closes all idle connections for `key` in the designated worker lane.
    pub fn drain_endpoint_from_lane(&self, key: &K, lane: usize) -> usize {
        let hot_lane = self.hot.lane(lane);
        let mut table = hot_lane.lock();
        if let Some(mut subpool) = table.remove(key) {
            subpool.drain_all()
        } else {
            0
        }
    }

    // ------------------------------------------------------------------------
    // Sweeping, Eviction, and Draining Operations
    // ------------------------------------------------------------------------

    /// Closes and drains all idle connections for `key` across Hot lanes, Cold Pool, and Multiplexed Pool.
    pub fn drain_key(&self, key: &K) -> usize {
        let mut drained = self.cold.drain_key(key);
        for lane in self.hot.all_lanes() {
            let mut table = lane.lock();
            if let Some(mut subpool) = table.remove(key) {
                drained += subpool.drain_all();
            }
        }
        drained += self.multiplexed.drain_matching(|k| k == key);
        drained
    }

    /// Drains all idle connections matching a predicate function and prunes their containers.
    pub fn drain_matching(&self, predicate: impl Fn(&K) -> bool) -> usize {
        let mut total_drained = 0;
        for lane in self.hot.all_lanes() {
            let mut table = lane.lock();
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

    /// Closes all idle connections across Hot lanes, Cold Pool, and Multiplexed Pool.
    pub fn clear(&self) {
        self.cold.clear();
        for lane in self.hot.all_lanes() {
            lane.lock().clear();
        }
        self.multiplexed.drain_matching(|_| true);
    }

    /// Sweeps all Hot lanes, Cold Pool, and Multiplexed Pool, purging expired or unhealthy connections.
    pub fn evict_expired(&self, idle_timeout: Duration) -> usize {
        self.evict_expired_with_lifetime(idle_timeout, self.config.max_lifetime)
    }

    /// Sweeps all containers with explicit lifetime control.
    pub fn evict_expired_with_lifetime(
        &self,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> usize {
        let mut total_evicted = self.cold.evict_expired(idle_timeout, max_lifetime);
        for lane in self.hot.all_lanes() {
            let mut table = lane.lock();
            let mut lane_evicted = 0;
            for (_, subpool) in table.iter_mut() {
                lane_evicted += subpool.evict_expired(idle_timeout, max_lifetime);
            }
            lane.record_evictions(lane_evicted as u64);
            total_evicted += lane_evicted;
        }
        total_evicted += self.multiplexed.evict_idle(idle_timeout);
        total_evicted
    }

    /// Prunes empty Hot containers that have continuously remained empty for longer than `empty_ttl`.
    pub fn prune_empty_pools(&self, empty_ttl: Duration) -> usize {
        let mut pruned = 0;
        for lane in self.hot.all_lanes() {
            let mut table = lane.lock();
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
// High-Level ConnectionProfile Facade (CompactPoolKey-specific)
// ============================================================================

impl<R: PoolableResource> PoolManager<CompactPoolKey, R> {
    /// Attempts to acquire an idle connection or stream slot matching the given [`ConnectionProfile`].
    pub fn acquire_profile(
        self: &Arc<Self>,
        profile: &ConnectionProfile,
    ) -> Option<ConnectionLease<CompactPoolKey, R>> {
        match profile.reuse_mode {
            ReuseMode::Multiplexed => {
                let lane = self.hot.current_lane();
                if let Some(stream_lease) = self
                    .multiplexed
                    .acquire_stream(&profile.key, profile.idle_timeout)
                {
                    lane.record_hit();
                    Some(ConnectionLease::Multiplexed(stream_lease))
                } else {
                    lane.record_miss();
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
                    profile.key,
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
                let lease = ExclusiveLease::with_pool(res, profile.key, Arc::clone(self), false);
                Some(ConnectionLease::Exclusive(lease))
            }
        }
    }

    /// Registers a newly established physical connection under the given [`ConnectionProfile`].
    pub fn register_profile(
        self: &Arc<Self>,
        profile: &ConnectionProfile,
        resource: R,
    ) -> ConnectionLease<CompactPoolKey, R> {
        let lane = self.hot.current_lane();
        lane.record_release();

        match profile.reuse_mode {
            ReuseMode::Multiplexed => {
                let stream_lease = self.multiplexed.register(
                    profile.key,
                    resource,
                    profile.max_concurrent_streams,
                );
                ConnectionLease::Multiplexed(stream_lease)
            }
            ReuseMode::Sequential => ConnectionLease::Sequential(SequentialLease::new(
                resource,
                profile.key,
                Arc::clone(self),
                false,
            )),
            ReuseMode::Exclusive => {
                let lease =
                    ExclusiveLease::with_pool(resource, profile.key, Arc::clone(self), false);
                ConnectionLease::Exclusive(lease)
            }
        }
    }

    /// Directly acquires a stream slot on an existing multiplexed connection.
    pub fn acquire_stream(&self, profile: &ConnectionProfile) -> Option<StreamLease<R>> {
        let lane = self.hot.current_lane();
        if let Some(stream) = self
            .multiplexed
            .acquire_stream(&profile.key, profile.idle_timeout)
        {
            lane.record_hit();
            Some(stream)
        } else {
            lane.record_miss();
            None
        }
    }

    /// Registers a new physical connection for multiplexed stream sharing and reserves 1 stream slot.
    pub fn register_multiplexed(&self, profile: &ConnectionProfile, resource: R) -> StreamLease<R> {
        let lane = self.hot.current_lane();
        lane.record_release();
        self.multiplexed
            .register(profile.key, resource, profile.max_concurrent_streams)
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
