//! Exclusive connection profile and lease semantics.
//!
//! Designed for Raw TCP, L4 stream forwarding, database tunnels, or one-off
//! exclusive connections where concurrent or sequential sharing is forbidden.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::key::ConnectionKey;
use crate::manager::PoolManager;
use crate::resource::PoolableResource;

/// Target destination when an exclusive lease is returned or recycled.
pub enum ExclusiveReturnTarget<R: PoolableResource> {
    /// Closes underlying resource when lease terminates.
    None,
    /// Returns resource back to origin [`PoolManager`] with zero heap allocation.
    Pool(Arc<PoolManager<ConnectionKey, R>>),
    /// Custom closure hook executed on clean release.
    Hook(Box<dyn FnOnce(ConnectionKey, R) + Send>),
}

impl<R: PoolableResource> fmt::Debug for ExclusiveReturnTarget<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Pool(_) => write!(f, "Pool(..)"),
            Self::Hook(_) => write!(f, "Hook(..)"),
        }
    }
}

/// An exclusive RAII lease over a single physical connection.
///
/// Guarantees that only one borrower holds this connection.
/// When dropped or released:
/// - If released explicitly via [`Self::release`] with `reusable = true`, caller can recycle it.
/// - Otherwise, closes the underlying connection immediately to avoid FD leaks and dirty states.
pub struct ExclusiveLease<R: PoolableResource> {
    resource: Option<R>,
    key: ConnectionKey,
    is_draining: bool,
    auto_close: bool,
    return_target: ExclusiveReturnTarget<R>,
}

impl<R: PoolableResource> fmt::Debug for ExclusiveLease<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExclusiveLease")
            .field("key", &self.key)
            .field("is_draining", &self.is_draining)
            .field("has_resource", &self.resource.is_some())
            .field("return_target", &self.return_target)
            .finish()
    }
}

impl<R: PoolableResource> ExclusiveLease<R> {
    /// Creates a new standalone exclusive lease with no return pool.
    pub fn new(resource: R, key: ConnectionKey, is_draining: bool) -> Self {
        Self {
            resource: Some(resource),
            key,
            is_draining,
            auto_close: true,
            return_target: ExclusiveReturnTarget::None,
        }
    }

    /// Creates an exclusive lease linked to a parent [`PoolManager`] with zero heap allocation.
    pub fn with_pool(
        resource: R,
        key: ConnectionKey,
        pool: Arc<PoolManager<ConnectionKey, R>>,
        is_draining: bool,
    ) -> Self {
        Self {
            resource: Some(resource),
            key,
            is_draining,
            auto_close: true,
            return_target: ExclusiveReturnTarget::Pool(pool),
        }
    }

    /// Creates an exclusive lease with a custom return callback on clean release.
    pub fn with_return_hook<F>(
        resource: R,
        key: ConnectionKey,
        is_draining: bool,
        return_fn: F,
    ) -> Self
    where
        F: FnOnce(ConnectionKey, R) + Send + 'static,
    {
        Self {
            resource: Some(resource),
            key,
            is_draining,
            auto_close: true,
            return_target: ExclusiveReturnTarget::Hook(Box::new(return_fn)),
        }
    }

    /// Accesses the target connection key.
    #[inline]
    pub fn key(&self) -> &ConnectionKey {
        &self.key
    }

    /// Returns `true` if the target backend endpoint is currently draining.
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.is_draining
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
            if reusable && !self.is_draining && res.is_healthy() {
                match std::mem::replace(&mut self.return_target, ExclusiveReturnTarget::None) {
                    ExclusiveReturnTarget::Pool(pool) => {
                        pool.release(&self.key, res, true, false);
                        return;
                    }
                    ExclusiveReturnTarget::Hook(cb) => {
                        cb(self.key.clone(), res);
                        return;
                    }
                    ExclusiveReturnTarget::None => {}
                }
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

impl<R: PoolableResource> Deref for ExclusiveLease<R> {
    type Target = R;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.resource.as_ref().unwrap()
    }
}

impl<R: PoolableResource> DerefMut for ExclusiveLease<R> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.resource.as_mut().unwrap()
    }
}

impl<R: PoolableResource> Drop for ExclusiveLease<R> {
    fn drop(&mut self) {
        if let Some(mut res) = self.resource.take() {
            if !self.auto_close && !self.is_draining && res.is_healthy() {
                match std::mem::replace(&mut self.return_target, ExclusiveReturnTarget::None) {
                    ExclusiveReturnTarget::Pool(pool) => {
                        pool.release(&self.key, res, true, false);
                        return;
                    }
                    ExclusiveReturnTarget::Hook(cb) => {
                        cb(self.key.clone(), res);
                        return;
                    }
                    ExclusiveReturnTarget::None => {}
                }
            }
            res.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    #[derive(Debug)]
    struct MockConn {
        healthy: AtomicBool,
        closed: Arc<AtomicBool>,
        created_at: Instant,
        last_used_at: Instant,
    }

    impl MockConn {
        fn new(closed: Arc<AtomicBool>) -> Self {
            Self {
                healthy: AtomicBool::new(true),
                closed,
                created_at: Instant::now(),
                last_used_at: Instant::now(),
            }
        }
    }

    impl PoolableResource for MockConn {
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
    fn test_exclusive_lease_auto_close_on_drop() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        let closed = Arc::new(AtomicBool::new(false));

        {
            let conn = MockConn::new(Arc::clone(&closed));
            let lease = ExclusiveLease::new(conn, key, false);
            assert!(lease.is_healthy());
            // Drops here
        }

        assert!(closed.load(Ordering::Acquire));
    }

    #[test]
    fn test_exclusive_lease_return_hook() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        let closed = Arc::new(AtomicBool::new(false));
        let returned = Arc::new(AtomicBool::new(false));

        let ret_clone = Arc::clone(&returned);
        let conn = MockConn::new(Arc::clone(&closed));
        let lease = ExclusiveLease::with_return_hook(conn, key, false, move |_, _| {
            ret_clone.store(true, Ordering::Release);
        });

        lease.release(true);
        assert!(returned.load(Ordering::Acquire));
        assert!(!closed.load(Ordering::Acquire));
    }

    #[test]
    fn test_exclusive_lease_with_pool() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        let closed = Arc::new(AtomicBool::new(false));
        let pool = Arc::new(PoolManager::<ConnectionKey, MockConn>::new());

        let conn = MockConn::new(Arc::clone(&closed));
        let lease = ExclusiveLease::with_pool(conn, key.clone(), Arc::clone(&pool), false);

        lease.release(true);
        assert!(!closed.load(Ordering::Acquire));
        assert_eq!(pool.total_idle_conns(), 1);
    }
}
