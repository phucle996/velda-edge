//! Resource contracts and RAII lease guards for pooled connections.

use std::fmt;
use std::time::Instant;

/// Contract that any resource pooled by [`crate::PoolManager`] must implement.
///
/// Designed to be protocol-agnostic: raw TCP streams, TLS sessions, HTTP/1 connections,
/// or multiplexed HTTP/2 client sessions can all implement this contract.
pub trait PoolableResource: Send + Sync + fmt::Debug + 'static {
    /// Checks if the resource is healthy and open for traffic.
    fn is_healthy(&self) -> bool;

    /// Closes the resource and releases underlying OS handles immediately.
    fn close(&mut self);

    /// Timestamp when this resource was established (default: Instant::now()).
    #[inline]
    fn created_at(&self) -> Instant {
        Instant::now()
    }

    /// Timestamp when this resource was last utilized (default: Instant::now()).
    #[inline]
    fn last_used_at(&self) -> Instant {
        Instant::now()
    }

    /// Updates the last used timestamp to now.
    #[inline]
    fn touch(&mut self) {}

    /// Updates the last used timestamp with a pre-sampled timestamp.
    ///
    /// Avoids redundant system clock / vDSO calls on high-frequency hot paths.
    #[inline]
    fn touch_at(&mut self, _now: Instant) {}
}

// Blanket implementation for any boxed PoolableResource
impl<T: PoolableResource + ?Sized> PoolableResource for Box<T> {
    #[inline]
    fn is_healthy(&self) -> bool {
        (**self).is_healthy()
    }

    #[inline]
    fn created_at(&self) -> Instant {
        (**self).created_at()
    }

    #[inline]
    fn last_used_at(&self) -> Instant {
        (**self).last_used_at()
    }

    #[inline]
    fn touch(&mut self) {
        (**self).touch();
    }

    #[inline]
    fn touch_at(&mut self, now: Instant) {
        (**self).touch_at(now);
    }

    #[inline]
    fn close(&mut self) {
        (**self).close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use crate::lease::SequentialLease;
    use crate::pool::PoolManager;

    #[derive(Debug)]
    struct MockResource {
        healthy: AtomicBool,
        created_at: Instant,
        last_used_at: Instant,
        closed: AtomicBool,
    }

    impl MockResource {
        fn new() -> Self {
            Self {
                healthy: AtomicBool::new(true),
                created_at: Instant::now(),
                last_used_at: Instant::now(),
                closed: AtomicBool::new(false),
            }
        }
    }

    impl PoolableResource for MockResource {
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
            self.healthy.store(false, Ordering::Release);
            self.closed.store(true, Ordering::Release);
        }
    }

    #[test]
    fn test_boxed_poolable_resource() {
        let mut boxed: Box<dyn PoolableResource> = Box::new(MockResource::new());
        assert!(boxed.is_healthy());
        let created = boxed.created_at();
        assert!(created <= Instant::now());
        boxed.touch();
        boxed.close();
        assert!(!boxed.is_healthy());
    }

    #[test]
    fn test_pool_lease_deref_and_into_inner() {
        let pool = Arc::new(PoolManager::<String, MockResource>::new());
        let key = "test_backend".to_string();
        let res = MockResource::new();
        let mut lease = SequentialLease::new(res, key.clone(), Arc::clone(&pool), false);

        assert_eq!(&lease.key, &key);
        assert!(!lease.is_draining);
        assert!(lease.is_healthy());
        lease.touch();

        // into_inner extracts resource without releasing to pool
        let inner = lease.into_inner();
        assert!(inner.is_some());
        assert_eq!(pool.total_idle_conns(), 0);
    }

    #[test]
    fn test_pool_lease_explicit_release() {
        let pool = Arc::new(PoolManager::<String, MockResource>::new());
        let key = "backend_a".to_string();
        let res = MockResource::new();
        let lease = SequentialLease::new(res, key.clone(), Arc::clone(&pool), false);

        // explicit release as reusable returns connection to pool
        lease.release(true);
        assert_eq!(pool.total_idle_conns(), 1);

        // acquire it back
        let lease2 = pool.acquire_lease(&key, Duration::from_secs(30)).unwrap();
        // explicit release as non-reusable closes connection
        lease2.release(false);
        assert_eq!(pool.total_idle_conns(), 0);
    }

    #[test]
    fn test_pool_lease_deref_mut_and_touch() {
        let pool = Arc::new(PoolManager::<String, MockResource>::new());
        let key = "backend_b".to_string();
        let res = MockResource::new();
        let mut lease = SequentialLease::new(res, key.clone(), Arc::clone(&pool), false);

        let before = lease.last_used_at();
        std::thread::sleep(Duration::from_millis(1));
        lease.touch();
        assert!(lease.last_used_at() > before);

        // Mutate through DerefMut
        lease.close();
        assert!(!lease.is_healthy());
    }

    #[test]
    fn test_pool_lease_auto_drop_unhealthy_not_returned() {
        let pool = Arc::new(PoolManager::<String, MockResource>::new());
        let key = "backend_unhealthy".to_string();
        let res = MockResource::new();

        {
            let mut lease = SequentialLease::new(res, key.clone(), Arc::clone(&pool), false);
            lease.close(); // marked unhealthy
            // drops here
        }

        // Must NOT be returned to pool
        assert_eq!(pool.total_idle_conns(), 0);
    }

    #[test]
    fn test_pool_lease_auto_drop_draining_not_returned() {
        let pool = Arc::new(PoolManager::<String, MockResource>::new());
        let key = "backend_draining".to_string();
        let res = MockResource::new();

        {
            let _lease = SequentialLease::new(res, key.clone(), Arc::clone(&pool), true); // draining = true
            // drops here
        }

        // Must NOT be returned to pool because backend is draining
        assert_eq!(pool.total_idle_conns(), 0);
    }
}
