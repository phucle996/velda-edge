use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use velda_connection_pool::container::probed_max_idle_per_key;
use velda_connection_pool::{
    ConnectionKey, ConnectionProfile, PoolConfig, PoolManager, PoolStats, PoolableResource,
};

#[derive(Debug)]
struct MockConn(usize, Instant, AtomicBool);

impl PoolableResource for MockConn {
    fn is_healthy(&self) -> bool {
        self.2.load(Ordering::Acquire)
    }
    fn created_at(&self) -> Instant {
        self.1
    }
    fn last_used_at(&self) -> Instant {
        self.1
    }
    fn touch(&mut self) {
        self.1 = Instant::now();
    }
    fn touch_at(&mut self, now: Instant) {
        self.1 = now;
    }
    fn close(&mut self) {
        self.2.store(false, Ordering::Release);
    }
}

#[test]
fn test_pool_config_defaults() {
    let cfg = PoolConfig::default();
    assert_eq!(cfg.max_idle_per_key, probed_max_idle_per_key());
    assert!((8..=128).contains(&cfg.max_idle_per_key));
    assert_eq!(cfg.idle_timeout, Duration::from_secs(30));
    assert_eq!(cfg.max_lifetime, Some(Duration::from_secs(3600)));

    // with_workers sets capacity scaled to workers
    let p2 = PoolManager::<String, MockConn>::with_workers(2);
    assert_eq!(p2.config().max_idle_per_key, 8);
    let p64 = PoolManager::<String, MockConn>::with_workers(64);
    assert_eq!(p64.config().max_idle_per_key, 128);
}

#[test]
fn test_pool_stats_hit_ratio() {
    let mut stats = PoolStats::default();
    assert_eq!(stats.hit_ratio(), 0.0);

    stats.hits = 75;
    stats.misses = 25;
    assert!((stats.hit_ratio() - 0.75).abs() < f64::EPSILON);
}

#[test]
fn test_pool_manager_acquire_miss_then_hit() {
    let pool = PoolManager::<String, MockConn>::new();
    let key = "backend_1".to_string();

    // 1. Miss does not pollute table with empty containers
    let miss = pool.acquire(&key, Duration::from_secs(30));
    assert!(miss.is_none());
    assert!(!pool.has_pool(&key));
    assert_eq!(pool.stats().misses, 1);
    assert_eq!(pool.stats().hits, 0);

    // 2. Release a connection creates container and stores idle conn
    let conn = MockConn(1, Instant::now(), AtomicBool::new(true));
    pool.release(&key, conn, true, false);
    assert!(pool.has_pool(&key));
    assert_eq!(pool.total_idle_conns(), 1);
    assert_eq!(pool.stats().releases, 1);

    // 3. Hit reuses connection
    let hit = pool.acquire(&key, Duration::from_secs(30));
    assert!(hit.is_some());
    assert_eq!(hit.unwrap().0, 1);
    assert_eq!(pool.stats().hits, 1);
    assert_eq!(pool.total_idle_conns(), 0);
}

#[test]
fn test_pool_manager_drain_key_and_matching() {
    let pool = PoolManager::<String, MockConn>::new();
    let k1 = "svc_auth".to_string();
    let k2 = "svc_pay".to_string();

    pool.release(
        &k1,
        MockConn(1, Instant::now(), AtomicBool::new(true)),
        true,
        false,
    );
    pool.release(
        &k2,
        MockConn(2, Instant::now(), AtomicBool::new(true)),
        true,
        false,
    );
    assert_eq!(pool.total_idle_conns(), 2);
    assert_eq!(pool.total_pool_containers(), 2);

    // drain_key
    let drained = pool.drain_key(&k1);
    assert_eq!(drained, 1);
    assert_eq!(pool.total_idle_conns(), 1);
    assert_eq!(pool.total_pool_containers(), 1);

    // drain_matching
    let drained2 = pool.drain_matching(|k| k.starts_with("svc_"));
    assert_eq!(drained2, 1);
    assert_eq!(pool.total_idle_conns(), 0);
    assert_eq!(pool.total_pool_containers(), 0);
}

#[test]
fn test_pool_manager_ensure_pool_and_has_pool() {
    let pool = PoolManager::<String, MockConn>::new();
    let key = "service_x".to_string();

    assert!(!pool.has_pool(&key));
    assert!(pool.ensure_pool(&key));
    assert!(pool.has_pool(&key));
    assert_eq!(pool.total_pool_containers(), 1);
    assert_eq!(pool.total_idle_conns(), 0);
}

#[test]
fn test_pool_manager_acquire_lease_and_auto_return() {
    let pool = Arc::new(PoolManager::<String, MockConn>::new());
    let key = "service_lease".to_string();

    pool.release(
        &key,
        MockConn(42, Instant::now(), AtomicBool::new(true)),
        true,
        false,
    );
    assert_eq!(pool.total_idle_conns(), 1);

    {
        let lease = pool.acquire_lease(&key, Duration::from_secs(30));
        assert!(lease.is_some());
        let lease = lease.unwrap();
        assert_eq!(lease.0, 42);
        assert_eq!(pool.total_idle_conns(), 0);
        // lease drops here
    }

    // Auto returned
    assert_eq!(pool.total_idle_conns(), 1);
}

#[test]
fn test_pool_manager_prune_and_clear() {
    let pool = PoolManager::<String, MockConn>::new();
    let k1 = "k1".to_string();
    let k2 = "k2".to_string();

    assert!(pool.ensure_pool(&k1));
    assert!(pool.ensure_pool(&k2));
    assert_eq!(pool.total_pool_containers(), 2);

    // Pruning with 0 TTL cleans empty pools
    let pruned = pool.prune_empty_pools(Duration::ZERO);
    assert_eq!(pruned, 2);
    assert_eq!(pool.total_pool_containers(), 0);

    // Add connections
    pool.release(
        &k1,
        MockConn(1, Instant::now(), AtomicBool::new(true)),
        true,
        false,
    );
    pool.release(
        &k2,
        MockConn(2, Instant::now(), AtomicBool::new(true)),
        true,
        false,
    );
    assert_eq!(pool.total_idle_conns(), 2);

    // Clear all
    pool.clear();
    assert_eq!(pool.total_idle_conns(), 0);
    assert_eq!(pool.total_pool_containers(), 0);
}

#[test]
fn test_pool_manager_rejects_unhealthy_or_draining() {
    let pool = PoolManager::<String, MockConn>::new();
    let key = "target".to_string();

    // 1. Unhealthy connection rejected
    let unhealthy = MockConn(1, Instant::now(), AtomicBool::new(false));
    pool.release(&key, unhealthy, true, false);
    assert_eq!(pool.total_idle_conns(), 0);

    // 2. Reusable = false rejected
    let non_reusable = MockConn(2, Instant::now(), AtomicBool::new(true));
    pool.release(&key, non_reusable, false, false);
    assert_eq!(pool.total_idle_conns(), 0);

    // 3. Draining = true rejected
    let draining = MockConn(3, Instant::now(), AtomicBool::new(true));
    pool.release(&key, draining, true, true);
    assert_eq!(pool.total_idle_conns(), 0);
}

#[test]
fn test_pool_manager_acquire_profile_sequential() {
    let pool = Arc::new(PoolManager::<ConnectionKey, MockConn>::new());
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);
    let profile = ConnectionProfile::sequential(key.clone(), Duration::from_secs(30));

    // 1. Initial acquire misses
    assert!(pool.acquire_profile(&profile).is_none());
    assert_eq!(pool.stats().misses, 1);

    // 2. Caller dials backend and registers physical connection
    let conn = MockConn(100, Instant::now(), AtomicBool::new(true));
    let lease = pool.register_profile(&profile, conn);
    assert_eq!(lease.0, 100);

    // 3. Drop lease returns connection to sequential LIFO subpool
    drop(lease);
    assert_eq!(pool.total_idle_conns(), 1);

    // 4. Next acquire hits the pooled connection
    let hit_lease = pool.acquire_profile(&profile);
    assert!(hit_lease.is_some());
    let hit_lease = hit_lease.unwrap();
    assert_eq!(hit_lease.0, 100);
    assert_eq!(pool.stats().hits, 1);
    assert_eq!(pool.total_idle_conns(), 0);

    // 5. Explicit release non-reusable closes connection
    hit_lease.release(false);
    assert_eq!(pool.total_idle_conns(), 0);
}

#[test]
fn test_pool_manager_acquire_profile_exclusive() {
    let pool = Arc::new(PoolManager::<ConnectionKey, MockConn>::new());
    let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
    let key = ConnectionKey::tcp(addr);
    let profile = ConnectionProfile::exclusive(key.clone(), Duration::from_secs(30));

    // 1. Miss
    assert!(pool.acquire_profile(&profile).is_none());

    // 2. Register new connection
    let conn = MockConn(200, Instant::now(), AtomicBool::new(true));
    let lease = pool.register_profile(&profile, conn);
    assert_eq!(lease.0, 200);

    // 3. Explicit release with reusable=true recycles through attached hook
    lease.release(true);
    assert_eq!(pool.total_idle_conns(), 1);

    // 4. Re-acquire exclusive lease
    let hit = pool.acquire_profile(&profile).unwrap();
    assert_eq!(hit.0, 200);
    assert_eq!(pool.total_idle_conns(), 0);

    // 5. Drop without explicit release auto-closes to protect socket state
    drop(hit);
    assert_eq!(pool.total_idle_conns(), 0);
}

#[test]
fn test_pool_manager_acquire_profile_multiplexed() {
    let pool = Arc::new(PoolManager::<ConnectionKey, MockConn>::new());
    let addr: SocketAddr = "127.0.0.1:443".parse().unwrap();
    let key = ConnectionKey::http(addr, "h2", Some("api.velda.io".into()), Some("h2".into()));
    let profile = ConnectionProfile::multiplexed(key.clone(), Duration::from_secs(30), 2);

    // 1. Initial acquire misses
    assert!(pool.acquire_profile(&profile).is_none());
    assert_eq!(pool.stats().misses, 1);

    // 2. Register newly established multiplexed connection with max_concurrent_streams = 2
    let conn = MockConn(300, Instant::now(), AtomicBool::new(true));
    let stream1 = pool.register_profile(&profile, conn);
    assert_eq!(stream1.0, 300);

    // 3. Second stream borrows from the same active physical connection (HIT)
    let stream2 = pool.acquire_profile(&profile);
    assert!(stream2.is_some());
    let stream2 = stream2.unwrap();
    assert_eq!(stream2.0, 300);
    assert_eq!(pool.stats().hits, 1);

    // 4. Third stream attempt saturates capacity -> returns None (MISS)
    let stream3 = pool.acquire_profile(&profile);
    assert!(stream3.is_none());
    assert_eq!(pool.stats().misses, 2);

    // 5. Dropping stream2 frees up capacity
    drop(stream2);
    let stream3_retry = pool.acquire_profile(&profile);
    assert!(stream3_retry.is_some());
    assert_eq!(stream3_retry.unwrap().0, 300);
    assert_eq!(pool.stats().hits, 2);

    // 6. Non-reusable release marks GOAWAY
    stream1.release(false);
    let after_goaway = pool.acquire_profile(&profile);
    assert!(after_goaway.is_none());
}

#[test]
fn test_pool_manager_drain_and_evict_multiplexed() {
    let pool = Arc::new(PoolManager::<ConnectionKey, MockConn>::new());
    let addr: SocketAddr = "127.0.0.1:8443".parse().unwrap();
    let key = ConnectionKey::tcp(addr);
    let profile = ConnectionProfile::multiplexed(key.clone(), Duration::from_millis(10), 10);

    let conn = MockConn(400, Instant::now(), AtomicBool::new(true));
    let stream = pool.register_profile(&profile, conn);
    assert_eq!(pool.multiplexed().total_connections(), 1);

    // Drain key sweeps multiplexed as well
    drop(stream);
    let drained = pool.drain_key(&key);
    assert_eq!(drained, 1);
    assert_eq!(pool.multiplexed().total_connections(), 0);
}
