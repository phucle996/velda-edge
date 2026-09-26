//! Multiplexed connection profile and concurrent stream slot semantics.
//!
//! Designed for HTTP/2 and HTTP/3 where a physical connection stays inside the pool
//! and serves up to `max_concurrent_streams` in parallel across independent callers.

use std::collections::HashMap;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::resource::PoolableResource;

static PROCESS_EPOCH: OnceLock<Instant> = OnceLock::new();

#[inline]
fn process_epoch() -> Instant {
    *PROCESS_EPOCH.get_or_init(Instant::now)
}

#[inline]
fn current_epoch_ms() -> u64 {
    Instant::now()
        .saturating_duration_since(process_epoch())
        .as_millis() as u64
}

/// A physical connection managed under multiplexed stream-sharing semantics.
///
/// Unlike sequential or exclusive connections, this connection is NOT popped out
/// of the pool. Instead, callers acquire lightweight [`StreamLease`] handles.
pub struct MultiplexedConnection<R: PoolableResource> {
    resource: R,
    active_streams: AtomicU32,
    max_concurrent_streams: u32,
    is_goaway: AtomicBool,
    created_at: Instant,
    last_activity_ms: AtomicU64,
}

impl<R: PoolableResource> fmt::Debug for MultiplexedConnection<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiplexedConnection")
            .field("active_streams", &self.active_streams())
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .field("is_goaway", &self.is_goaway())
            .field("is_healthy", &self.resource.is_healthy())
            .finish()
    }
}

impl<R: PoolableResource> MultiplexedConnection<R> {
    /// Wraps a healthy physical connection into a multiplexed container.
    pub fn new(resource: R, max_concurrent_streams: u32) -> Self {
        Self {
            resource,
            active_streams: AtomicU32::new(0),
            max_concurrent_streams: max_concurrent_streams.max(1),
            is_goaway: AtomicBool::new(false),
            created_at: Instant::now(),
            last_activity_ms: AtomicU64::new(current_epoch_ms()),
        }
    }

    /// Tries to atomically acquire a stream slot on this connection.
    ///
    /// Returns `None` if the connection is at max capacity, has received GOAWAY,
    /// or is unhealthy.
    pub fn try_acquire_stream(self: &Arc<Self>) -> Option<StreamLease<R>> {
        let mut current = self.active_streams.load(Ordering::Relaxed);
        loop {
            if self.is_goaway.load(Ordering::Acquire) || !self.resource.is_healthy() {
                return None;
            }
            if current >= self.max_concurrent_streams {
                return None;
            }

            match self.active_streams.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(StreamLease {
                        connection: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Releases one active stream slot and updates the last activity timestamp (100% lock-free).
    #[inline]
    pub fn release_stream(&self) {
        self.active_streams.fetch_sub(1, Ordering::AcqRel);
        self.last_activity_ms
            .store(current_epoch_ms(), Ordering::Release);
    }

    /// Marks this connection with GOAWAY. No further streams can be acquired.
    #[inline]
    pub fn mark_goaway(&self) {
        self.is_goaway.store(true, Ordering::Release);
    }

    /// Returns `true` if this connection received GOAWAY or is shutting down.
    #[inline]
    pub fn is_goaway(&self) -> bool {
        self.is_goaway.load(Ordering::Acquire)
    }

    /// Returns the current number of in-flight active streams.
    #[inline]
    pub fn active_streams(&self) -> u32 {
        self.active_streams.load(Ordering::Relaxed)
    }

    /// Returns the remaining stream capacity before saturation.
    #[inline]
    pub fn available_capacity(&self) -> u32 {
        if self.is_goaway() || !self.resource.is_healthy() {
            0
        } else {
            self.max_concurrent_streams
                .saturating_sub(self.active_streams())
        }
    }

    /// Returns `true` if this connection cannot accept any more streams.
    #[inline]
    pub fn is_exhausted(&self) -> bool {
        self.available_capacity() == 0
    }

    /// Returns `true` if this connection is currently idle (0 active streams).
    #[inline]
    pub fn is_idle(&self) -> bool {
        self.active_streams() == 0
    }

    /// Returns `true` if the connection is healthy and not closed by peer.
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.resource.is_healthy() && !self.is_goaway()
    }

    /// Checks if this connection is idle and expired past `idle_timeout`.
    pub fn is_idle_expired(&self, idle_timeout: Duration, now: Instant) -> bool {
        if self.active_streams() > 0 {
            return false;
        }
        let now_ms = now.saturating_duration_since(process_epoch()).as_millis() as u64;
        let last_ms = self.last_activity_ms.load(Ordering::Acquire);
        now_ms.saturating_sub(last_ms) >= idle_timeout.as_millis() as u64
    }

    /// Timestamp when this multiplexed connection was established.
    #[inline]
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Direct reference to the underlying resource.
    #[inline]
    pub fn resource(&self) -> &R {
        &self.resource
    }
}

/// An active RAII lease representing one reserved concurrent stream slot on a multiplexed connection.
///
/// Multiple `StreamLease` handles may share the same underlying [`MultiplexedConnection`].
/// When dropped, decrements the active stream counter automatically.
pub struct StreamLease<R: PoolableResource> {
    connection: Arc<MultiplexedConnection<R>>,
}

impl<R: PoolableResource> fmt::Debug for StreamLease<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamLease")
            .field(
                "connection_active_streams",
                &self.connection.active_streams(),
            )
            .finish()
    }
}

impl<R: PoolableResource> StreamLease<R> {
    /// Accesses the underlying multiplexed connection wrapper.
    #[inline]
    pub fn multiplexed_connection(&self) -> &Arc<MultiplexedConnection<R>> {
        &self.connection
    }
}

impl<R: PoolableResource> Deref for StreamLease<R> {
    type Target = R;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.connection.resource()
    }
}

impl<R: PoolableResource> Drop for StreamLease<R> {
    fn drop(&mut self) {
        self.connection.release_stream();
    }
}

/// Type alias for an isolated mutex bucket holding multiplexed connections.
pub type MuxBucket<K, R> = Mutex<HashMap<K, Vec<Arc<MultiplexedConnection<R>>>>>;

/// A sharded generic pool for multiplexed connections.
///
/// Distributes keys across cache-friendly, independent mutex shards
/// with power-of-two bitmask indexing for minimal lock contention.
pub struct MultiplexedPool<K, R, S = std::collections::hash_map::RandomState>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    shards: Box<[MuxBucket<K, R>]>,
    hash_builder: S,
    mask: usize,
}

impl<K, R> Default for MultiplexedPool<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    fn default() -> Self {
        Self::with_shards(crate::container::probed_shard_count())
    }
}

impl<K, R> MultiplexedPool<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Creates a new empty multiplexed pool with probed hardware shard count.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new multiplexed pool with the specified shard count (clamped to power of 2, min 1).
    pub fn with_shards(shard_count: usize) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(Mutex::new(HashMap::new()));
        }

        Self {
            shards: shards.into_boxed_slice(),
            hash_builder: std::collections::hash_map::RandomState::new(),
            mask: count - 1,
        }
    }

    #[inline]
    fn shard_for(&self, key: &K) -> &MuxBucket<K, R> {
        let idx = (self.hash_builder.hash_one(key) as usize) & self.mask;
        &self.shards[idx]
    }

    /// Acquires a stream slot from an existing available multiplexed connection.
    ///
    /// Returns `Some(StreamLease)` if an existing connection has spare capacity.
    /// Returns `None` (MISS) if no connection exists or all are saturated.
    pub fn acquire_stream(&self, key: &K) -> Option<StreamLease<R>> {
        let shard = self.shard_for(key);
        let guard = shard.lock().unwrap_or_else(|e| e.into_inner());
        let conns = guard.get(key)?;

        for conn in conns.iter() {
            if let Some(lease) = conn.try_acquire_stream() {
                return Some(lease);
            }
        }
        None
    }

    /// Registers a newly established connection and immediately claims 1 stream lease.
    pub fn register(&self, key: K, resource: R, max_streams: u32) -> StreamLease<R> {
        let conn = Arc::new(MultiplexedConnection::new(resource, max_streams));
        // Reserve the first stream slot
        let lease = conn
            .try_acquire_stream()
            .expect("Fresh multiplexed connection must have capacity");

        let shard = self.shard_for(&key);
        let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
        guard.entry(key).or_default().push(conn);
        lease
    }

    /// Returns the total number of physical connections managed across all keys.
    pub fn total_connections(&self) -> usize {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            total += guard.values().map(|v| v.len()).sum::<usize>();
        }
        total
    }

    /// Returns the total number of in-flight active streams across all connections.
    pub fn total_active_streams(&self) -> u32 {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            total += guard
                .values()
                .flat_map(|v| v.iter())
                .map(|c| c.active_streams())
                .sum::<u32>();
        }
        total
    }

    /// Evicts idle connections that have 0 active streams and exceeded `idle_timeout`.
    pub fn evict_idle(&self, idle_timeout: Duration) -> usize {
        let now = Instant::now();
        let mut evicted = 0;
        for shard in self.shards.iter() {
            let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            for conns in guard.values_mut() {
                let before = conns.len();
                conns.retain(|c| !c.is_idle_expired(idle_timeout, now));
                evicted += before - conns.len();
            }
            // Clean up empty key entries
            guard.retain(|_, v| !v.is_empty());
        }
        evicted
    }

    /// Marks matching connections with GOAWAY and removes them from the pool.
    pub fn drain_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&K) -> bool,
    {
        let mut drained = 0;
        for shard in self.shards.iter() {
            let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            for (k, conns) in guard.iter_mut() {
                if predicate(k) {
                    for c in conns.drain(..) {
                        c.mark_goaway();
                        drained += 1;
                    }
                }
            }
            guard.retain(|_, v| !v.is_empty());
        }
        drained
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[derive(Debug)]
    struct MockMuxConn {
        healthy: AtomicBool,
        created_at: Instant,
        last_used_at: Instant,
    }

    impl MockMuxConn {
        fn new() -> Self {
            Self {
                healthy: AtomicBool::new(true),
                created_at: Instant::now(),
                last_used_at: Instant::now(),
            }
        }
    }

    impl PoolableResource for MockMuxConn {
        fn is_healthy(&self) -> bool {
            self.healthy.load(Ordering::Acquire)
        }
        fn created_at(&self) -> Instant {
            self.created_at
        }
        fn last_used_at(&self) -> Instant {
            self.last_used_at
        }
        fn touch(&mut self) {
            self.last_used_at = Instant::now();
        }
        fn close(&mut self) {
            self.healthy.store(false, Ordering::Release);
        }
    }

    #[test]
    fn test_multiplexed_concurrency_slots() {
        let conn = Arc::new(MultiplexedConnection::new(MockMuxConn::new(), 3));
        assert_eq!(conn.available_capacity(), 3);

        let l1 = conn.try_acquire_stream().unwrap();
        assert_eq!(conn.active_streams(), 1);
        assert_eq!(conn.available_capacity(), 2);

        let l2 = conn.try_acquire_stream().unwrap();
        let l3 = conn.try_acquire_stream().unwrap();
        assert_eq!(conn.active_streams(), 3);
        assert!(conn.is_exhausted());

        // 4th stream must fail (capacity saturated)
        assert!(conn.try_acquire_stream().is_none());

        // Drop 1 stream -> capacity freed up
        drop(l1);
        assert_eq!(conn.active_streams(), 2);
        assert_eq!(conn.available_capacity(), 1);

        let l4 = conn.try_acquire_stream().unwrap();
        assert_eq!(conn.active_streams(), 3);

        drop(l2);
        drop(l3);
        drop(l4);
        assert_eq!(conn.active_streams(), 0);
        assert!(conn.is_idle());
    }

    #[test]
    fn test_multiplexed_goaway_rejects_new_streams() {
        let conn = Arc::new(MultiplexedConnection::new(MockMuxConn::new(), 10));
        let l1 = conn.try_acquire_stream().unwrap();

        conn.mark_goaway();
        assert!(conn.is_goaway());

        // New streams rejected
        assert!(conn.try_acquire_stream().is_none());

        // Existing stream still valid until drop
        assert_eq!(conn.active_streams(), 1);
        drop(l1);
        assert_eq!(conn.active_streams(), 0);
    }

    #[test]
    fn test_multiplexed_pool_register_and_acquire() {
        let pool = MultiplexedPool::<String, MockMuxConn>::new();
        let key = "http2_target".to_string();

        // 1. Initial pool is empty -> Acquire is MISS
        assert!(pool.acquire_stream(&key).is_none());

        // 2. Register fresh connection with capacity 2
        let stream1 = pool.register(key.clone(), MockMuxConn::new(), 2);
        assert_eq!(pool.total_connections(), 1);
        assert_eq!(pool.total_active_streams(), 1);

        // 3. Acquire 2nd stream -> HIT on same physical connection
        let stream2 = pool.acquire_stream(&key).unwrap();
        assert_eq!(pool.total_active_streams(), 2);

        // 4. Connection is now saturated (2/2) -> Acquire is MISS
        assert!(pool.acquire_stream(&key).is_none());

        // 5. Drop stream1 -> Slot freed -> Acquire is HIT
        drop(stream1);
        assert_eq!(pool.total_active_streams(), 1);
        let stream3 = pool.acquire_stream(&key).unwrap();
        assert_eq!(pool.total_active_streams(), 2);

        drop(stream2);
        drop(stream3);
        assert_eq!(pool.total_active_streams(), 0);
    }

    #[test]
    fn test_multiplexed_pool_drain_matching() {
        let pool = MultiplexedPool::<String, MockMuxConn>::new();
        let k1 = "service_a".to_string();
        let k2 = "service_b".to_string();

        let _s1 = pool.register(k1.clone(), MockMuxConn::new(), 10);
        let _s2 = pool.register(k2.clone(), MockMuxConn::new(), 10);
        assert_eq!(pool.total_connections(), 2);

        let drained = pool.drain_matching(|k| k == "service_a");
        assert_eq!(drained, 1);
        assert_eq!(pool.total_connections(), 1);
        assert!(pool.acquire_stream(&k1).is_none());
    }
}
