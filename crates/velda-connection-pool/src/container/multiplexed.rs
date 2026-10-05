//! Sharded container and connection wrappers for concurrent multiplexed protocols (HTTP/2, HTTP/3).
//!
//! Unlike sequential connections where sockets are checked out by value and returned to a LIFO queue,
//! multiplexed connections stay resident in the pool and serve multiple independent requests
//! simultaneously up to `max_concurrent_streams`.

use std::collections::HashMap;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::container::hasher::FastBuildHasher;
use crate::container::probed_shard_count;
use crate::lease::StreamLease;
use crate::resource::PoolableResource;

// ============================================================================
// Lock-Free Millisecond Timestamp Infrastructure
// ============================================================================

/// Process startup epoch used as base for lock-free millisecond timestamps.
///
/// # Rationale
/// Rust `std::time::Instant` does not have an atomic equivalent (`AtomicInstant` does not exist
/// in `std::sync::atomic`). Storing the elapsed milliseconds since process start inside an
/// `AtomicU64` allows updating and reading the last activity timestamp completely lock-free
/// during high-frequency stream releases without taking a shard mutex.
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

// ============================================================================
// MultiplexedConnection
// ============================================================================

/// A physical connection managed under multiplexed stream-sharing semantics (HTTP/2, HTTP/3).
///
/// Manages atomic stream checkout, saturation detection, and GOAWAY handling.
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

    /// Returns `true` if the connection is healthy and not marked with GOAWAY.
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.resource.is_healthy() && !self.is_goaway()
    }

    /// Checks if this connection is idle and expired past `idle_timeout` or dead.
    ///
    /// Connections marked GOAWAY or broken with 0 active streams expire immediately
    /// without waiting for `idle_timeout`.
    pub fn is_idle_expired(&self, idle_timeout: Duration, now: Instant) -> bool {
        if self.active_streams() > 0 {
            return false;
        }
        // If connection is dead/goaway/unhealthy and has 0 active streams, evict immediately!
        if self.is_goaway() || !self.resource.is_healthy() {
            return true;
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

impl<R: PoolableResource> Drop for MultiplexedConnection<R> {
    fn drop(&mut self) {
        self.resource.close();
    }
}

// ============================================================================
// MultiplexedPool & MuxShard
// ============================================================================

/// An isolated mutex partition bucket holding multiplexed connections.
///
/// Cache-line aligned (64 bytes) to eliminate false sharing between CPU cores.
#[repr(align(64))]
#[allow(clippy::type_complexity)]
pub struct MuxShard<K, R: PoolableResource> {
    table: Mutex<HashMap<K, Vec<Arc<MultiplexedConnection<R>>>, FastBuildHasher>>,
}

impl<K, R: PoolableResource> Default for MuxShard<K, R> {
    fn default() -> Self {
        Self {
            table: Mutex::new(HashMap::with_hasher(FastBuildHasher)),
        }
    }
}

/// A sharded generic pool for multiplexed connections.
///
/// Distributes keys across cache-friendly, independent 64-byte aligned mutex shards
/// with power-of-two bitmask indexing for minimal lock contention.
pub struct MultiplexedPool<K, R, S = FastBuildHasher>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    shards: Box<[MuxShard<K, R>]>,
    hash_builder: S,
    mask: usize,
}

impl<K, R> Default for MultiplexedPool<K, R, FastBuildHasher>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    fn default() -> Self {
        Self::with_shards(probed_shard_count())
    }
}

impl<K, R> MultiplexedPool<K, R, FastBuildHasher>
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
        Self::with_hasher(shard_count, FastBuildHasher)
    }
}

impl<K, R, S> MultiplexedPool<K, R, S>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
    S: BuildHasher,
{
    /// Creates a new multiplexed pool with custom shard count and custom hash builder.
    pub fn with_hasher(shard_count: usize, hash_builder: S) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(MuxShard::default());
        }

        Self {
            shards: shards.into_boxed_slice(),
            hash_builder,
            mask: count - 1,
        }
    }

    #[inline]
    fn shard_for(&self, key: &K) -> &MuxShard<K, R> {
        let idx = (self.hash_builder.hash_one(key) as usize) & self.mask;
        &self.shards[idx]
    }

    /// Acquires a stream slot from an existing available multiplexed connection.
    ///
    /// Returns `Some(StreamLease)` if an existing connection has spare capacity.
    /// Returns `None` (MISS) if no connection exists or all are saturated.
    /// Fast-path checks connections with early break, pruning dead connections only on demand.
    pub fn acquire_stream(&self, key: &K) -> Option<StreamLease<R>> {
        let shard = self.shard_for(key);

        // Fast-path: quickly scan and clone candidate Arc under minimal lock hold time (< 10 ns)
        let candidate = {
            let guard = shard.table.lock().unwrap_or_else(|e| e.into_inner());
            let conns = guard.get(key)?;
            let mut found = None;
            for conn in conns.iter() {
                if !conn.is_goaway() && !conn.is_exhausted() {
                    found = Some(Arc::clone(conn));
                    break;
                }
            }
            found
        };

        // Try acquire stream lock-free OUTSIDE the shard mutex lock
        if let Some(conn) = candidate
            && let Some(lease) = conn.try_acquire_stream()
        {
            return Some(lease);
        }

        // Slow-path fallback: If candidate was saturated in race or dead connections exist
        let mut guard = shard.table.lock().unwrap_or_else(|e| e.into_inner());
        let conns = guard.get_mut(key)?;

        let mut acquired = None;
        let mut has_dead = false;

        for conn in conns.iter() {
            if !conn.is_goaway() && !conn.is_exhausted() {
                if let Some(lease) = conn.try_acquire_stream() {
                    acquired = Some(lease);
                    break;
                }
            } else if conn.is_idle() && (!conn.is_healthy() || conn.is_goaway()) {
                has_dead = true;
            }
        }

        // Only prune dead connections if one was encountered
        if has_dead {
            conns.retain(|conn| !(conn.is_idle() && (!conn.is_healthy() || conn.is_goaway())));
        }

        acquired
    }

    /// Registers a newly established connection and immediately claims 1 stream lease.
    pub fn register(&self, key: K, resource: R, max_streams: u32) -> StreamLease<R> {
        let conn = Arc::new(MultiplexedConnection::new(resource, max_streams));
        // Reserve the first stream slot
        let lease = conn
            .try_acquire_stream()
            .expect("Fresh multiplexed connection must have capacity");

        let shard = self.shard_for(&key);
        let mut guard = shard.table.lock().unwrap_or_else(|e| e.into_inner());
        guard.entry(key).or_default().push(conn);
        lease
    }

    /// Returns the total number of physical connections managed across all keys.
    pub fn total_connections(&self) -> usize {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.table.lock().unwrap_or_else(|e| e.into_inner());
            total += guard.values().map(|v| v.len()).sum::<usize>();
        }
        total
    }

    /// Returns the total number of in-flight active streams across all connections.
    pub fn total_active_streams(&self) -> u32 {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.table.lock().unwrap_or_else(|e| e.into_inner());
            total += guard
                .values()
                .flat_map(|v| v.iter())
                .map(|c| c.active_streams())
                .sum::<u32>();
        }
        total
    }

    /// Evicts idle connections that have 0 active streams and exceeded `idle_timeout` or are dead.
    pub fn evict_idle(&self, idle_timeout: Duration) -> usize {
        let now = Instant::now();
        let mut evicted = 0;
        for shard in self.shards.iter() {
            let mut guard = shard.table.lock().unwrap_or_else(|e| e.into_inner());
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
            let mut guard = shard.table.lock().unwrap_or_else(|e| e.into_inner());
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
