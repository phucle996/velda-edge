//! Background idle expiration sweeping, container pruning, and key draining logic.

use std::hash::Hash;
use std::time::Duration;

use crate::manager::PoolManager;
use crate::resource::PoolableResource;

impl<K: Eq + Hash + Clone, R: PoolableResource> PoolManager<K, R> {
    /// Closes and drains all idle connections for `key`, and removes the pool container.
    ///
    /// Returns the number of drained connections across both shards and multiplexed pools.
    pub fn drain_key(&self, key: &K) -> usize {
        let shard = self.shards.shard_for(key);
        let mut table = shard.lock();

        let shards_drained = if let Some(mut subpool) = table.remove(key) {
            subpool.drain_all()
        } else {
            0
        };
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
    ///
    /// Returns the total number of connections evicted during this sweep.
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
    ///
    /// Uses continuous empty duration tracking to prevent mistakenly purging active containers
    /// that are only momentarily empty due to high in-flight request checkout.
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
