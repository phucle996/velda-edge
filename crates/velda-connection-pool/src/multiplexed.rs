//! Lane-sharded contiguous container for concurrent multiplexed protocols (HTTP/2, HTTP/3, gRPC).
//!
//! ```text
//! MultiplexedPool -> shards[lane & mask] -> MuxShard (align=64B, AtomicU8)
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │ MuxTable (SoA PageAlignedSlab, 0 HashMap):                              │
//! │   keys:  [ K_0 | K_1 | ... ]                                            │
//! │   conns: [ Vec<Arc<MultiplexedConnection<R>>> ]                         │
//! │                                                                         │
//! │ Stream Lease: lock-free AtomicU32 active_streams CAS outside shard lock │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::config::probed_shard_count;
use crate::hot::current_thread_lane;
use crate::lease::StreamLease;
use crate::resource::PoolableResource;
use crate::slab::PageAlignedSlab;

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

    /// Checks if this connection is idle and at risk of keep-alive race
    /// (0 active streams and idle duration >= 75% of `idle_timeout`).
    #[inline]
    pub fn is_idle_at_risk(&self, idle_timeout: Duration, now: Instant) -> bool {
        if self.active_streams() > 0 {
            return false;
        }
        if self.is_goaway() || !self.resource.is_healthy() {
            return true;
        }
        let safe_cutoff_ms = (idle_timeout.as_millis() as u64).saturating_mul(3) / 4;
        let now_ms = now.saturating_duration_since(process_epoch()).as_millis() as u64;
        let last_ms = self.last_activity_ms.load(Ordering::Acquire);
        now_ms.saturating_sub(last_ms) >= safe_cutoff_ms
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
// MultiplexedPool & MuxShard (4KB Page-Aligned Contiguous SoA, Zero HashMap)
// ============================================================================

/// Contiguous Structure-of-Arrays table inside each [`MuxShard`].
pub struct MuxTable<K, R: PoolableResource> {
    keys: PageAlignedSlab<K>,
    conns: PageAlignedSlab<Vec<Arc<MultiplexedConnection<R>>>>,
}

impl<K: Eq, R: PoolableResource> MuxTable<K, R> {
    fn new() -> Self {
        Self {
            keys: PageAlignedSlab::with_pages(1),
            conns: PageAlignedSlab::with_pages(1),
        }
    }

    #[inline]
    fn get(&self, key: &K) -> Option<&Vec<Arc<MultiplexedConnection<R>>>> {
        let idx = self.keys.as_slice().iter().position(|k| k == key)?;
        Some(&self.conns.as_slice()[idx])
    }

    #[inline]
    fn get_mut(&mut self, key: &K) -> Option<&mut Vec<Arc<MultiplexedConnection<R>>>> {
        let idx = self.keys.as_slice().iter().position(|k| k == key)?;
        Some(&mut self.conns.as_mut_slice()[idx])
    }

    #[inline]
    fn get_or_insert_default(&mut self, key: K) -> &mut Vec<Arc<MultiplexedConnection<R>>> {
        if let Some(idx) = self.keys.as_slice().iter().position(|k| k == &key) {
            return &mut self.conns.as_mut_slice()[idx];
        }
        let idx = self.keys.len();
        self.keys.push(key);
        self.conns.push(Vec::new());
        &mut self.conns.as_mut_slice()[idx]
    }

    fn retain_non_empty(&mut self) {
        let mut i = 0;
        while i < self.keys.len() {
            if self.conns.as_slice()[i].is_empty() {
                self.keys.swap_remove(i);
                self.conns.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
}

struct MuxShardGuard<'a, K, R: PoolableResource> {
    lock: &'a AtomicU8,
    table: &'a mut MuxTable<K, R>,
}

impl<K, R: PoolableResource> Deref for MuxShardGuard<'_, K, R> {
    type Target = MuxTable<K, R>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.table
    }
}

impl<K, R: PoolableResource> DerefMut for MuxShardGuard<'_, K, R> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.table
    }
}

impl<K, R: PoolableResource> Drop for MuxShardGuard<'_, K, R> {
    #[inline]
    fn drop(&mut self) {
        self.lock.store(0, Ordering::Release);
    }
}

/// An isolated cache-line aligned partition bucket holding multiplexed connections.
#[repr(align(64))]
pub struct MuxShard<K, R: PoolableResource> {
    lock_state: AtomicU8,
    table: UnsafeCell<MuxTable<K, R>>,
}

unsafe impl<K: Send, R: PoolableResource + Send> Send for MuxShard<K, R> {}
unsafe impl<K: Send, R: PoolableResource + Send> Sync for MuxShard<K, R> {}

impl<K: Eq, R: PoolableResource> Default for MuxShard<K, R> {
    fn default() -> Self {
        Self {
            lock_state: AtomicU8::new(0),
            table: UnsafeCell::new(MuxTable::new()),
        }
    }
}

impl<K: Eq, R: PoolableResource> MuxShard<K, R> {
    #[inline]
    fn lock(&self) -> MuxShardGuard<'_, K, R> {
        let mut spins = 0u32;
        while self
            .lock_state
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spins = spins.wrapping_add(1);
            if spins & 0x3F == 0 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
        MuxShardGuard {
            lock: &self.lock_state,
            table: unsafe { &mut *self.table.get() },
        }
    }
}

/// A lane-sharded generic pool for multiplexed connections backed by 4KB page-aligned slabs.
pub struct MultiplexedPool<K, R>
where
    K: Eq + Clone,
    R: PoolableResource,
{
    shards: Box<[MuxShard<K, R>]>,
    mask: usize,
    max_connections_per_key: usize,
    rr_cursor: AtomicUsize,
}

impl<K, R> Default for MultiplexedPool<K, R>
where
    K: Eq + Clone,
    R: PoolableResource,
{
    fn default() -> Self {
        Self::with_shards(probed_shard_count())
    }
}

impl<K, R> MultiplexedPool<K, R>
where
    K: Eq + Clone,
    R: PoolableResource,
{
    /// Creates a new empty multiplexed pool with probed hardware shard count.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new multiplexed pool with the specified shard count (clamped to power of 2, min 1).
    pub fn with_shards(shard_count: usize) -> Self {
        Self::with_options(shard_count, shard_count)
    }

    /// Sets the maximum concurrent connections permitted per individual key/endpoint.
    pub fn with_max_connections_per_key(mut self, max: usize) -> Self {
        self.max_connections_per_key = max.max(1);
        self
    }

    /// Creates a new multiplexed pool with custom shard count and custom max connections per key.
    pub fn with_options(shard_count: usize, max_connections_per_key: usize) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(MuxShard::default());
        }

        Self {
            shards: shards.into_boxed_slice(),
            mask: count - 1,
            max_connections_per_key: max_connections_per_key.max(1),
            rr_cursor: AtomicUsize::new(0),
        }
    }

    /// Returns a reference to the shard responsible for a worker lane (`lane & mask`, zero hashing).
    #[inline]
    pub fn shard_for_lane(&self, _key: &K, lane: usize) -> &MuxShard<K, R> {
        &self.shards[lane & self.mask]
    }

    /// Internal helper scanning and acquiring a stream lease from a specific shard.
    fn acquire_stream_from_shard(
        &self,
        shard: &MuxShard<K, R>,
        key: &K,
        idle_timeout: Duration,
        now: Instant,
    ) -> Option<StreamLease<R>> {
        let candidate = {
            let guard = shard.lock();
            let conns = guard.get(key)?;
            let total = conns.len();
            if total == 0 {
                return None;
            }

            let start_idx = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % total;
            let mut best: Option<Arc<MultiplexedConnection<R>>> = None;
            let mut min_load = u32::MAX;

            for i in 0..total {
                let idx = (start_idx + i) % total;
                let conn = &conns[idx];
                if !conn.is_goaway()
                    && !conn.is_exhausted()
                    && !conn.is_idle_at_risk(idle_timeout, now)
                {
                    let load = conn.active_streams();
                    if load < min_load {
                        min_load = load;
                        best = Some(Arc::clone(conn));
                        if load == 0 {
                            break;
                        }
                    }
                }
            }

            best
        };

        if let Some(conn) = candidate
            && let Some(lease) = conn.try_acquire_stream()
        {
            return Some(lease);
        }

        let mut guard = shard.lock();
        let conns = guard.get_mut(key)?;
        let total = conns.len();
        if total == 0 {
            return None;
        }

        let mut acquired = None;
        let mut has_dead = false;
        let start_idx = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % total;

        for i in 0..total {
            let idx = (start_idx + i) % total;
            let conn = &conns[idx];
            if !conn.is_goaway() && !conn.is_exhausted() && !conn.is_idle_at_risk(idle_timeout, now)
            {
                if let Some(lease) = conn.try_acquire_stream() {
                    acquired = Some(lease);
                    break;
                }
            } else if conn.is_idle()
                && (!conn.is_healthy()
                    || conn.is_goaway()
                    || conn.is_idle_at_risk(idle_timeout, now))
            {
                has_dead = true;
            }
        }

        if has_dead {
            conns.retain(|conn| {
                !(conn.is_idle()
                    && (!conn.is_healthy()
                        || conn.is_goaway()
                        || conn.is_idle_at_risk(idle_timeout, now)))
            });
        }

        acquired
    }

    /// Returns the number of physical connections currently stored for a given key.
    pub fn connection_count(&self, key: &K) -> usize {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.lock();
            if let Some(conns) = guard.get(key) {
                total += conns.len();
            }
        }
        total
    }

    /// Returns the number of connections for `key` in the designated worker lane shard.
    #[inline]
    pub fn connection_count_for_lane(&self, key: &K, lane: usize) -> usize {
        let shard = self.shard_for_lane(key, lane);
        let guard = shard.lock();
        guard.get(key).map_or(0, |c| c.len())
    }

    /// Checks whether the lane shard has an existing healthy connection with available capacity headroom (< 75% load).
    #[inline]
    pub fn has_headroom_for_lane(&self, key: &K, lane: usize, max_streams: u32) -> bool {
        let shard = self.shard_for_lane(key, lane);
        let guard = shard.lock();
        let Some(conns) = guard.get(key) else {
            return false;
        };
        let high_load_threshold = max_streams.saturating_mul(3) / 4;
        conns.iter().any(|c| {
            !c.is_goaway() && c.resource().is_healthy() && c.active_streams() < high_load_threshold
        })
    }

    /// Acquires a stream slot from an existing available multiplexed connection.
    pub fn acquire_stream(&self, key: &K, idle_timeout: Duration) -> Option<StreamLease<R>> {
        let now = Instant::now();
        let lane = current_thread_lane();
        let local_shard = self.shard_for_lane(key, lane);

        if let Some(lease) = self.acquire_stream_from_shard(local_shard, key, idle_timeout, now) {
            return Some(lease);
        }

        for shard in self.shards.iter() {
            if !std::ptr::eq(shard, local_shard)
                && let Some(lease) = self.acquire_stream_from_shard(shard, key, idle_timeout, now)
            {
                return Some(lease);
            }
        }

        None
    }

    /// Acquires a stream slot only if an existing connection is currently idle (0 active streams).
    pub fn acquire_idle_stream(&self, key: &K, idle_timeout: Duration) -> Option<StreamLease<R>> {
        let now = Instant::now();
        let lane = current_thread_lane();
        let local_shard = self.shard_for_lane(key, lane);

        let candidate = {
            let guard = local_shard.lock();
            guard.get(key).and_then(|conns| {
                let total = conns.len();
                if total == 0 {
                    return None;
                }
                let start_idx = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % total;
                for i in 0..total {
                    let idx = (start_idx + i) % total;
                    let conn = &conns[idx];
                    if conn.is_idle()
                        && !conn.is_goaway()
                        && !conn.is_exhausted()
                        && !conn.is_idle_at_risk(idle_timeout, now)
                    {
                        return Some(Arc::clone(conn));
                    }
                }
                None
            })
        };

        if let Some(conn) = candidate
            && let Some(lease) = conn.try_acquire_stream()
        {
            return Some(lease);
        }

        None
    }

    /// Registers a newly established connection and immediately claims 1 stream lease.
    pub fn register(&self, key: K, resource: R, max_streams: u32) -> StreamLease<R> {
        let conn = Arc::new(MultiplexedConnection::new(resource, max_streams));
        let lease = conn
            .try_acquire_stream()
            .expect("Fresh multiplexed connection must have capacity");

        let lane = current_thread_lane();
        let shard = self.shard_for_lane(&key, lane);
        let mut guard = shard.lock();
        guard.get_or_insert_default(key).push(conn);
        lease
    }

    /// Registers a newly established pre-warmed multiplexed connection into the designated lane shard.
    pub fn register_warmed_to_lane(&self, key: K, lane: usize, resource: R, max_streams: u32) {
        if !resource.is_healthy() {
            let mut dead = resource;
            dead.close();
            return;
        }
        let conn = Arc::new(MultiplexedConnection::new(resource, max_streams));
        let shard = self.shard_for_lane(&key, lane);
        let mut guard = shard.lock();
        guard.get_or_insert_default(key).push(conn);
    }

    /// Returns the total number of physical connections managed across all keys.
    pub fn total_connections(&self) -> usize {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.lock();
            total += guard
                .conns
                .as_slice()
                .iter()
                .map(|v| v.len())
                .sum::<usize>();
        }
        total
    }

    /// Returns the total number of in-flight active streams across all connections.
    pub fn total_active_streams(&self) -> u32 {
        let mut total = 0;
        for shard in self.shards.iter() {
            let guard = shard.lock();
            total += guard
                .conns
                .as_slice()
                .iter()
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
            let mut guard = shard.lock();
            for conns in guard.conns.as_mut_slice() {
                let before = conns.len();
                conns.retain(|c| !c.is_idle_expired(idle_timeout, now));
                evicted += before - conns.len();
            }
            guard.retain_non_empty();
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
            let mut guard = shard.lock();
            let keys = guard.keys.as_slice();
            let mut mark_drain = Vec::new();
            for (idx, k) in keys.iter().enumerate() {
                if predicate(k) {
                    mark_drain.push(idx);
                }
            }
            for idx in mark_drain {
                for c in guard.conns.as_mut_slice()[idx].drain(..) {
                    c.mark_goaway();
                    drained += 1;
                }
            }
            guard.retain_non_empty();
        }
        drained
    }
}
