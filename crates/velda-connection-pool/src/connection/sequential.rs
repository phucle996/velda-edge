//! Sequential connection profile and transactional lease semantics.
//!
//! Designed for HTTP/1.1 Keep-Alive or similar transactional protocols where
//! a connection is borrowed for exactly one request-response transaction at a time,
//! then safely returned to a LIFO pool for the next caller to reuse.

use std::fmt;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::manager::PoolManager;
use crate::resource::PoolableResource;

/// An active RAII lease for sequential transactional connections.
///
/// Implements automatic return to the pool upon drop:
/// - If still healthy and backend is not draining, returns to the LIFO queue.
/// - If unhealthy or draining, closes the connection immediately.
pub struct SequentialLease<K, R>
where
    K: Eq + Hash + Clone,
    R: PoolableResource,
{
    resource: Option<R>,
    key: K,
    pool: Arc<PoolManager<K, R>>,
    is_draining: bool,
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
    /// Creates a new sequential lease wrapping an active resource.
    pub fn new(resource: R, key: K, pool: Arc<PoolManager<K, R>>, is_draining: bool) -> Self {
        Self {
            resource: Some(resource),
            key,
            pool,
            is_draining,
        }
    }

    /// Accesses the underlying connection key.
    #[inline]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Returns `true` if the target backend is currently draining.
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.is_draining
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
            let is_healthy = res.is_healthy();
            self.pool
                .release(&self.key, res, is_healthy, self.is_draining);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    #[derive(Debug)]
    struct MockSequentialConn {
        healthy: AtomicBool,
        closed: AtomicBool,
        created_at: Instant,
        last_used_at: Instant,
    }

    impl MockSequentialConn {
        fn new() -> Self {
            Self {
                healthy: AtomicBool::new(true),
                closed: AtomicBool::new(false),
                created_at: Instant::now(),
                last_used_at: Instant::now(),
            }
        }
    }

    impl PoolableResource for MockSequentialConn {
        fn is_healthy(&self) -> bool {
            self.healthy.load(Ordering::Acquire) && !self.closed.load(Ordering::Acquire)
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
            self.closed.store(true, Ordering::Release);
        }
    }

    #[test]
    fn test_sequential_lease_auto_return_on_drop() {
        let pool = Arc::new(PoolManager::<String, MockSequentialConn>::new());
        let key = "http1_backend".to_string();

        {
            let conn = MockSequentialConn::new();
            let lease = SequentialLease::new(conn, key.clone(), Arc::clone(&pool), false);
            assert!(lease.is_healthy());
            // Drops here
        }

        assert_eq!(pool.total_idle_conns(), 1);
    }

    #[test]
    fn test_sequential_lease_unhealthy_not_returned() {
        let pool = Arc::new(PoolManager::<String, MockSequentialConn>::new());
        let key = "http1_broken".to_string();

        {
            let mut lease = SequentialLease::new(
                MockSequentialConn::new(),
                key.clone(),
                Arc::clone(&pool),
                false,
            );
            lease.close(); // marked closed/unhealthy
            // Drops here
        }

        assert_eq!(pool.total_idle_conns(), 0);
    }
}
