//! Low-level resource acquisition, RAII lease creation, and return operations.

use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::container::SubPool;
use crate::manager::PoolManager;
use crate::resource::{PoolLease, PoolableResource};

impl<K: Eq + Hash + Clone, R: PoolableResource> PoolManager<K, R> {
    /// Attempts to acquire an idle reusable connection for `key`.
    ///
    /// Follows the 3-state Container-First lifecycle:
    /// 1. **Pool does not exist**: Lazy-creates the `Pool[Key]` container, returns `None` (MISS).
    /// 2. **Pool exists but empty**: Returns `None` (MISS).
    /// 3. **Pool exists with usable connection**: Pops connection in $O(1)$ LIFO order, returns `Some(R)` (HIT).
    pub fn acquire(&self, key: &K, idle_timeout: Duration) -> Option<R> {
        self.acquire_with_lifetime(key, idle_timeout, self.config.max_lifetime)
    }

    /// Attempts to acquire an idle connection with explicit max lifetime validation.
    pub fn acquire_with_lifetime(
        &self,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
    ) -> Option<R> {
        let shard = self.shards.shard_for(key);
        let mut table = shard.lock();

        if let Some(subpool) = table.get_mut(key) {
            let resource = subpool.acquire(idle_timeout, max_lifetime);
            if let Some(res) = resource {
                shard.record_hit();
                Some(res)
            } else {
                shard.record_miss();
                None
            }
        } else {
            // State 1: Pool container does not exist -> lazy-create container
            table.insert(key.clone(), SubPool::new(self.config.max_idle_per_key));
            shard.record_miss();
            None
        }
    }

    /// Acquires an idle reusable connection wrapped in an RAII [`PoolLease`].
    ///
    /// Returns `None` if the pool container has no idle connection (MISS).
    pub fn acquire_lease(
        self: &Arc<Self>,
        key: &K,
        idle_timeout: Duration,
    ) -> Option<PoolLease<K, R>> {
        self.acquire_lease_with_lifetime(key, idle_timeout, self.config.max_lifetime, false)
    }

    /// Acquires an idle reusable connection wrapped in an RAII [`PoolLease`] with explicit lifetime and draining mode.
    pub fn acquire_lease_with_lifetime(
        self: &Arc<Self>,
        key: &K,
        idle_timeout: Duration,
        max_lifetime: Option<Duration>,
        is_draining: bool,
    ) -> Option<PoolLease<K, R>> {
        let res = self.acquire_with_lifetime(key, idle_timeout, max_lifetime)?;
        Some(PoolLease::new(
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

        let shard = self.shards.shard_for(key);
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
}
