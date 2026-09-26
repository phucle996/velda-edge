//! High-level connection profile dispatching and registration facade.

use std::sync::Arc;

use crate::connection::{
    ConnectionLease, ConnectionProfile, ExclusiveLease, ReuseMode, SequentialLease, StreamLease,
};
use crate::key::ConnectionKey;
use crate::manager::PoolManager;
use crate::resource::PoolableResource;

impl<R: PoolableResource> PoolManager<ConnectionKey, R> {
    /// Attempts to acquire an idle connection or stream slot matching the given [`ConnectionProfile`].
    ///
    /// The acquisition strategy depends on the profile's [`ReuseMode`]:
    /// - [`ReuseMode::Sequential`]: Checks out an idle connection from the LIFO subpool in $O(1)$ time,
    ///   wrapped in an RAII [`ConnectionLease::Sequential`]. Upon drop, returns connection to the queue.
    /// - [`ReuseMode::Exclusive`]: Checks out an idle connection from the LIFO subpool,
    ///   wrapped in an RAII [`ConnectionLease::Exclusive`]. Upon drop, closes connection unless recycled.
    /// - [`ReuseMode::Multiplexed`]: Atomically claims an in-flight stream slot from an existing
    ///   healthy connection in [`crate::connection::MultiplexedPool`], wrapped in [`ConnectionLease::Multiplexed`].
    ///
    /// Returns `None` (MISS) if no idle connection or spare stream capacity is available.
    pub fn acquire_profile(
        self: &Arc<Self>,
        profile: &ConnectionProfile,
    ) -> Option<ConnectionLease<ConnectionKey, R>> {
        match profile.reuse_mode {
            ReuseMode::Multiplexed => {
                let shard = self.shards.shard_for(&profile.key);
                if let Some(stream_lease) = self.multiplexed.acquire_stream(&profile.key) {
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

    /// Registers a newly established physical connection under the given [`ConnectionProfile`],
    /// immediately returning an active [`ConnectionLease`] for the caller's request.
    ///
    /// - [`ReuseMode::Multiplexed`]: Inserts the connection into [`crate::connection::MultiplexedPool`] with
    ///   `profile.max_concurrent_streams` and reserves the 1st stream slot.
    /// - [`ReuseMode::Sequential`]: Wraps the new connection in a [`SequentialLease`].
    /// - [`ReuseMode::Exclusive`]: Wraps the new connection in an [`ExclusiveLease`].
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
        if let Some(stream) = self.multiplexed.acquire_stream(&profile.key) {
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
    ///
    /// # Panics
    /// Panics if called on a [`ReuseMode::Multiplexed`] profile, because multiplexed
    /// connections cannot be checked out by value without violating stream concurrency.
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
