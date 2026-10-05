//! RAII connection leases and stream slot guard contracts.
//!
//! # Protocol-Specific Lease Semantics
//!
//! A lease represents an active, borrow-guarded checkout from [`crate::PoolManager`].
//!
//! ```text
//!                      ConnectionLease (Enum)
//!                                │
//!        ┌───────────────────────┼───────────────────────┐
//!        ▼                       ▼                       ▼
//!   SequentialLease        ExclusiveLease           StreamLease
//!   (HTTP/1.1)             (Raw TCP / Tunnel)       (HTTP/2, HTTP/3)
//!   1:1 Transactional      1:1 Dedicated Socket     1:N Concurrent Slot
//!   Auto-returns to LIFO   Auto-closes on drop      Decrements stream slot
//! ```

use std::fmt;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::container::MultiplexedConnection;
use crate::pool::PoolManager;
use crate::resource::PoolableResource;

// ============================================================================
// 1. SequentialLease (HTTP/1.1 Keep-Alive)
// ============================================================================

/// An active RAII lease for sequential transactional connections (HTTP/1.1 Keep-Alive).
///
/// Implements automatic return to the pool upon drop:
/// - If still healthy and backend is not draining, returns to the LIFO queue.
/// - If unhealthy or draining, closes the connection immediately.
pub struct SequentialLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    pub resource: Option<R>,
    pub key: K,
    pub pool: Arc<PoolManager<K, R>>,
    pub is_draining: bool,
}

impl<K, R> fmt::Debug for SequentialLease<K, R>
where
    K: Eq + Hash + Clone + fmt::Debug,
    R: PoolableResource,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequentialLease")
            .field("key", &self.key)
            .field("is_draining", &self.is_draining)
            .field("has_resource", &self.resource.is_some())
            .finish()
    }
}

impl<K, R> SequentialLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Creates a new sequential lease wrapping an active connection.
    pub fn new(resource: R, key: K, pool: Arc<PoolManager<K, R>>, is_draining: bool) -> Self {
        Self {
            resource: Some(resource),
            key,
            pool,
            is_draining,
        }
    }

    /// Explicitly completes the transaction and returns or retires the connection.
    ///
    /// If `reusable` is true and backend is not draining, pushes connection into LIFO queue.
    /// Otherwise, closes connection immediately.
    pub fn release(mut self, reusable: bool) {
        if let Some(res) = self.resource.take() {
            self.pool
                .release(&self.key, res, reusable, self.is_draining);
        }
    }

    /// Consumes the lease and extracts the raw underlying resource without returning to pool.
    pub fn into_inner(mut self) -> Option<R> {
        self.resource.take()
    }
}

impl<K, R> Deref for SequentialLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    type Target = R;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.resource.as_ref().unwrap()
    }
}

impl<K, R> DerefMut for SequentialLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.resource.as_mut().unwrap()
    }
}

impl<K, R> Drop for SequentialLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    fn drop(&mut self) {
        if let Some(res) = self.resource.take() {
            // Note: self.pool.release performs the health check and closes if unhealthy.
            // Avoid duplicate kernel syscalls by passing reusable=true directly.
            self.pool.release(&self.key, res, true, self.is_draining);
        }
    }
}

/// Backwards-compatible alias for [`SequentialLease`].
pub type PoolLease<K, R> = SequentialLease<K, R>;

// ============================================================================
// 2. ExclusiveLease (Raw TCP / L4 Dedicated Tunnel)
// ============================================================================

/// Target destination when an exclusive lease is returned or recycled.
pub enum ExclusiveReturnTarget<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Closes underlying resource when lease terminates.
    None,
    /// Returns resource back to origin [`PoolManager`] with zero heap allocation.
    Pool(Arc<PoolManager<K, R>>),
}

impl<K, R> fmt::Debug for ExclusiveReturnTarget<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Pool(_) => write!(f, "Pool(..)"),
        }
    }
}

/// An exclusive RAII lease over a single physical connection.
///
/// Guarantees that only one borrower holds this connection.
/// When dropped or released:
/// - If released explicitly via [`Self::release`] with `reusable = true`, caller can recycle it to pool.
/// - Otherwise, closes the underlying connection immediately on drop to avoid FD leaks and dirty states.
pub struct ExclusiveLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    pub resource: Option<R>,
    pub key: K,
    pub is_draining: bool,
    pub auto_close: bool,
    pub return_target: ExclusiveReturnTarget<K, R>,
}

impl<K, R> fmt::Debug for ExclusiveLease<K, R>
where
    K: Eq + Hash + Clone + fmt::Debug,
    R: PoolableResource,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExclusiveLease")
            .field("key", &self.key)
            .field("is_draining", &self.is_draining)
            .field("has_resource", &self.resource.is_some())
            .field("return_target", &self.return_target)
            .finish()
    }
}

impl<K, R> ExclusiveLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Creates a new standalone exclusive lease with no return pool.
    pub fn new(resource: R, key: K, is_draining: bool) -> Self {
        Self {
            resource: Some(resource),
            key,
            is_draining,
            auto_close: true,
            return_target: ExclusiveReturnTarget::None,
        }
    }

    /// Creates an exclusive lease linked to a parent [`PoolManager`] with zero heap allocation.
    pub fn with_pool(resource: R, key: K, pool: Arc<PoolManager<K, R>>, is_draining: bool) -> Self {
        Self {
            resource: Some(resource),
            key,
            is_draining,
            auto_close: true,
            return_target: ExclusiveReturnTarget::Pool(pool),
        }
    }

    /// Sets whether this lease should close the underlying socket on drop.
    pub fn set_auto_close(&mut self, auto_close: bool) {
        self.auto_close = auto_close;
    }

    /// Explicitly releases the connection.
    ///
    /// If `reusable` is true, healthy, and not draining, triggers the return target.
    /// Otherwise, calls [`PoolableResource::close`] immediately.
    pub fn release(mut self, reusable: bool) {
        if let Some(mut res) = self.resource.take() {
            if reusable
                && !self.is_draining
                && res.is_healthy()
                && let ExclusiveReturnTarget::Pool(pool) =
                    std::mem::replace(&mut self.return_target, ExclusiveReturnTarget::None)
            {
                pool.release(&self.key, res, true, false);
                return;
            }
            res.close();
        }
    }

    /// Consumes the lease and extracts the raw underlying resource.
    ///
    /// Cancels any auto-close or return behavior.
    pub fn into_inner(mut self) -> Option<R> {
        self.resource.take()
    }
}

impl<K, R> Deref for ExclusiveLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    type Target = R;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.resource.as_ref().unwrap()
    }
}

impl<K, R> DerefMut for ExclusiveLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.resource.as_mut().unwrap()
    }
}

impl<K, R> Drop for ExclusiveLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    fn drop(&mut self) {
        if let Some(res) = self.resource.take() {
            if !self.auto_close
                && !self.is_draining
                && let ExclusiveReturnTarget::Pool(pool) =
                    std::mem::replace(&mut self.return_target, ExclusiveReturnTarget::None)
            {
                pool.release(&self.key, res, true, false);
                return;
            }
            let mut r = res;
            r.close();
        }
    }
}

// ============================================================================
// 3. StreamLease (HTTP/2, HTTP/3 Multiplexed Stream Slot)
// ============================================================================

/// An active RAII lease representing one reserved concurrent stream slot on a multiplexed connection.
///
/// Multiple `StreamLease` handles share the same underlying [`MultiplexedConnection`].
/// When dropped, decrements the active stream counter automatically in a lock-free manner.
pub struct StreamLease<R: PoolableResource> {
    pub(crate) connection: Arc<MultiplexedConnection<R>>,
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

    /// Marks the underlying connection as having received GOAWAY or being closed.
    #[inline]
    pub fn mark_goaway(&self) {
        self.connection.mark_goaway();
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

// ============================================================================
// 4. ConnectionLease (Unified Profile Lease Enum)
// ============================================================================

/// An active RAII lease returned by [`crate::PoolManager`] matching the profile's reuse mode.
///
/// Implements [`Deref`] to `R`, allowing callers to transparently access the underlying connection.
/// When dropped, executes the exact release semantics of the profile:
/// - [`ConnectionLease::Sequential`]: Returned to LIFO queue for the next sequential request.
/// - [`ConnectionLease::Exclusive`]: Closed or returned via pool to protect socket state.
/// - [`ConnectionLease::Multiplexed`]: Decrements the active stream counter on the shared connection.
pub enum ConnectionLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Sequential transactional lease (HTTP/1.1).
    Sequential(SequentialLease<K, R>),
    /// Exclusive single-borrower lease (Raw TCP / L4 Tunnel).
    Exclusive(ExclusiveLease<K, R>),
    /// Multiplexed stream slot lease (HTTP/2, HTTP/3).
    Multiplexed(StreamLease<R>),
}

impl<K, R> fmt::Debug for ConnectionLease<K, R>
where
    K: Eq + Hash + Clone + fmt::Debug,
    R: PoolableResource,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequential(l) => f
                .debug_tuple("ConnectionLease::Sequential")
                .field(l)
                .finish(),
            Self::Exclusive(l) => f
                .debug_tuple("ConnectionLease::Exclusive")
                .field(l)
                .finish(),
            Self::Multiplexed(l) => f
                .debug_tuple("ConnectionLease::Multiplexed")
                .field(l)
                .finish(),
        }
    }
}

impl<K, R> ConnectionLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    /// Explicitly releases the connection lease before it naturally falls out of scope.
    ///
    /// - For [`Self::Sequential`]: If `reusable` is true and backend is healthy, returns to LIFO pool.
    /// - For [`Self::Exclusive`]: If `reusable` is true, recycles to pool; otherwise closes.
    /// - For [`Self::Multiplexed`]: Releases stream slot; if `reusable` is false, marks connection with GOAWAY.
    pub fn release(self, reusable: bool) {
        match self {
            Self::Sequential(l) => l.release(reusable),
            Self::Exclusive(l) => l.release(reusable),
            Self::Multiplexed(l) => {
                if !reusable {
                    l.multiplexed_connection().mark_goaway();
                }
                // drop(l) naturally decrements active streams
            }
        }
    }
}

impl<K, R> Deref for ConnectionLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    type Target = R;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Sequential(l) => l.deref(),
            Self::Exclusive(l) => l.deref(),
            Self::Multiplexed(l) => l.deref(),
        }
    }
}
