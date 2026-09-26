//! Integration test suite for the `velda-connection-pool` crate.
//!
//! Focuses exclusively on subsystem-level integration workflows:
//! 1. End-to-end checkout and RAII lease lifecycle.
//! 2. Protocol, SNI, and target address identity isolation.
//! 3. Capacity limit enforcement and File Descriptor (`EMFILE`) protection.
//! 4. Connection expiration (idle timeout + max lifetime) and background eviction sweeps.
//! 5. Accurate container pruning with `empty_duration` TTL protection.
//! 6. Service decommissioning via predicate-based `drain_matching`.
//! 7. High-concurrency multi-worker stress testing under Tokio runtime.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use velda_connection_pool::{
    ConnectionKey, ConnectionProfile, PoolConfig, PoolManager, PoolableResource,
};

#[derive(Debug)]
struct MockConnection {
    peer: SocketAddr,
    created_at: Instant,
    last_used: Instant,
    healthy: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}

impl MockConnection {
    fn new(peer: SocketAddr) -> Self {
        Self {
            peer,
            created_at: Instant::now(),
            last_used: Instant::now(),
            healthy: Arc::new(AtomicBool::new(true)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn with_created_at(peer: SocketAddr, created_at: Instant) -> Self {
        Self {
            peer,
            created_at,
            last_used: Instant::now(),
            healthy: Arc::new(AtomicBool::new(true)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl PoolableResource for MockConnection {
    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire) && !self.closed.load(Ordering::Acquire)
    }

    fn created_at(&self) -> Instant {
        self.created_at
    }

    fn last_used_at(&self) -> Instant {
        self.last_used
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    fn touch_at(&mut self, now: Instant) {
        self.last_used = now;
    }

    fn close(&mut self) {
        self.healthy.store(false, Ordering::Release);
        self.closed.store(true, Ordering::Release);
    }
}

#[test]
fn test_pool_end_to_end_acquire_and_lease_lifecycle() {
    let pool = Arc::new(PoolManager::<ConnectionKey, MockConnection>::new());
    let addr: SocketAddr = "10.0.0.10:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);

    // 1. Initial acquire misses and lazy-creates container
    assert!(pool.acquire(&key, Duration::from_secs(30)).is_none());
    assert_eq!(pool.stats().misses, 1);
    assert_eq!(pool.total_pool_containers(), 1);
    assert_eq!(pool.total_idle_conns(), 0);

    // 2. Upstream releases a newly established connection into the pool
    pool.release(&key, MockConnection::new(addr), true, false);
    assert_eq!(pool.total_idle_conns(), 1);
    assert_eq!(pool.stats().releases, 1);

    // 3. Next request acquires via RAII PoolLease (pool hit)
    {
        let lease = pool.acquire_lease(&key, Duration::from_secs(30));
        assert!(lease.is_some());
        let lease = lease.unwrap();
        assert_eq!(lease.peer, addr);
        assert_eq!(pool.stats().hits, 1);
        assert_eq!(pool.total_idle_conns(), 0); // Checked out
        // Auto drop returns connection back to pool
    }

    // 4. Connection is returned and available for subsequent callers
    assert_eq!(pool.total_idle_conns(), 1);

    // 5. Subsequent checkout explicitly released as non-reusable closes connection
    {
        let lease = pool.acquire_lease(&key, Duration::from_secs(30)).unwrap();
        lease.release(false);
    }
    assert_eq!(pool.total_idle_conns(), 0);
}

#[test]
fn test_pool_protocol_and_sni_isolation() {
    let pool = PoolManager::<ConnectionKey, MockConnection>::new();
    let addr: SocketAddr = "10.0.0.1:443".parse().unwrap();

    let key_h1 = ConnectionKey::http(addr, "http1", Some("api.velda.io".into()), None);
    let key_h2 = ConnectionKey::http(addr, "http2", Some("api.velda.io".into()), None);
    let key_other_sni = ConnectionKey::http(addr, "http2", Some("admin.velda.io".into()), None);
    let key_tcp = ConnectionKey::tcp(addr);

    // Release a connection only for HTTP/1.1 with api.velda.io
    pool.release(&key_h1, MockConnection::new(addr), true, false);
    assert_eq!(pool.total_idle_conns(), 1);

    // Verify complete isolation across protocol, SNI, and raw transport
    assert!(pool.acquire(&key_h2, Duration::from_secs(30)).is_none());
    assert!(
        pool.acquire(&key_other_sni, Duration::from_secs(30))
            .is_none()
    );
    assert!(pool.acquire(&key_tcp, Duration::from_secs(30)).is_none());

    // Matches only the exact key
    let acquired = pool.acquire(&key_h1, Duration::from_secs(30));
    assert!(acquired.is_some());
}

#[test]
fn test_pool_capacity_and_fd_protection() {
    let config = PoolConfig {
        max_idle_per_key: 2,
        idle_timeout: Duration::from_secs(30),
        max_lifetime: None,
    };
    let pool = PoolManager::<ConnectionKey, MockConnection>::with_config(config);
    let addr: SocketAddr = "10.0.0.2:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);

    let conn1 = MockConnection::new(addr);
    let conn2 = MockConnection::new(addr);
    let conn3 = MockConnection::new(addr);
    let conn3_closed = Arc::clone(&conn3.closed);

    pool.release(&key, conn1, true, false);
    pool.release(&key, conn2, true, false);
    assert_eq!(pool.total_idle_conns(), 2);

    // 3rd connection exceeds capacity: must be immediately closed to prevent FD exhaustion
    pool.release(&key, conn3, true, false);
    assert_eq!(pool.total_idle_conns(), 2);
    assert!(conn3_closed.load(Ordering::Acquire));
}

#[test]
fn test_pool_expiration_and_eviction() {
    let pool = PoolManager::<ConnectionKey, MockConnection>::new();
    let addr: SocketAddr = "10.0.0.3:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);

    // 1. Connection that exceeded max lifetime (created 2 hours ago)
    let expired_lifetime =
        MockConnection::with_created_at(addr, Instant::now() - Duration::from_secs(7200));
    let lifetime_closed = Arc::clone(&expired_lifetime.closed);
    pool.release(&key, expired_lifetime, true, false);

    // Acquire refuses expired connection based on max_lifetime
    let res = pool.acquire_with_lifetime(
        &key,
        Duration::from_secs(300),
        Some(Duration::from_secs(3600)),
    );
    assert!(res.is_none());
    assert!(lifetime_closed.load(Ordering::Acquire));
    assert_eq!(pool.total_idle_conns(), 0);

    // 2. Connection that exceeded idle timeout (last used 60s ago)
    let mut idle_conn = MockConnection::new(addr);
    idle_conn.last_used = Instant::now() - Duration::from_secs(60);
    pool.release(&key, idle_conn, true, false);
    assert_eq!(pool.total_idle_conns(), 1);

    // Sweep evicts connection
    let evicted = pool.evict_expired(Duration::from_secs(10));
    assert_eq!(evicted, 1);
    assert_eq!(pool.total_idle_conns(), 0);
    assert_eq!(pool.stats().evictions, 1);
}

#[test]
fn test_pool_empty_duration_pruning() {
    let pool = PoolManager::<ConnectionKey, MockConnection>::new();
    let addr: SocketAddr = "10.0.0.4:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);

    // 1. Ensure pool created; it has no idle connections
    assert!(pool.ensure_pool(&key));
    assert_eq!(pool.total_pool_containers(), 1);

    // Pruning with 0 delay purges permanently unused empty container
    let pruned = pool.prune_empty_pools(Duration::ZERO);
    assert_eq!(pruned, 1);
    assert_eq!(pool.total_pool_containers(), 0);

    // 2. Active pool with connection must not be pruned
    pool.release(&key, MockConnection::new(addr), true, false);
    assert_eq!(pool.total_idle_conns(), 1);
    let pruned2 = pool.prune_empty_pools(Duration::ZERO);
    assert_eq!(pruned2, 0);
    assert_eq!(pool.total_pool_containers(), 1);

    // 3. When checked out, pool is momentarily empty; TTL prevents accidental purge
    let conn = pool.acquire(&key, Duration::from_secs(30)).unwrap();
    assert_eq!(pool.total_idle_conns(), 0);
    let pruned3 = pool.prune_empty_pools(Duration::from_secs(10));
    assert_eq!(pruned3, 0);
    assert_eq!(pool.total_pool_containers(), 1);

    // Return connection
    pool.release(&key, conn, true, false);
    assert_eq!(pool.total_idle_conns(), 1);
}

#[test]
fn test_pool_drain_matching_service() {
    let pool = PoolManager::<ConnectionKey, MockConnection>::new();
    let svc_addr: SocketAddr = "10.0.1.1:8080".parse().unwrap();
    let other_addr: SocketAddr = "10.0.2.1:8080".parse().unwrap();

    let k1 = ConnectionKey::tcp(svc_addr);
    let k2 = ConnectionKey::http(svc_addr, "http1", None, None);
    let k_other = ConnectionKey::tcp(other_addr);

    pool.release(&k1, MockConnection::new(svc_addr), true, false);
    pool.release(&k2, MockConnection::new(svc_addr), true, false);
    pool.release(&k_other, MockConnection::new(other_addr), true, false);
    assert_eq!(pool.total_idle_conns(), 3);
    assert_eq!(pool.total_pool_containers(), 3);

    // Decommissioning svc_addr drains all its connections AND removes its containers
    let drained = pool.drain_matching(|k| k.target_addr == svc_addr);
    assert_eq!(drained, 2);
    assert_eq!(pool.total_idle_conns(), 1);
    assert_eq!(pool.total_pool_containers(), 1);

    // other_addr remains intact and functional
    assert!(pool.acquire(&k_other, Duration::from_secs(30)).is_some());
}

#[tokio::test]
async fn test_pool_sharding_concurrency() {
    let pool = Arc::new(PoolManager::<ConnectionKey, MockConnection>::new());
    let mut handles = Vec::new();

    for worker_id in 0..16 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let port = 8000 + (worker_id * 4 + (i % 4)) as u16;
                let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                let key = ConnectionKey::tcp(addr);

                let _ = pool_clone.acquire(&key, Duration::from_secs(30));
                pool_clone.release(&key, MockConnection::new(addr), true, false);

                let hit = pool_clone.acquire(&key, Duration::from_secs(30));
                assert!(hit.is_some());
                pool_clone.release(&key, hit.unwrap(), true, false);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let stats = pool.stats();
    assert_eq!(stats.hits + stats.misses, 3200);
    assert_eq!(stats.releases, 3200);
    assert!(stats.hits > 0);
    assert!(stats.misses > 0);
    assert!(stats.hit_ratio() > 0.0);
}

#[test]
fn test_pool_profiles_end_to_end_integration() {
    let pool = Arc::new(PoolManager::<ConnectionKey, MockConnection>::new());

    // 1. Sequential Profile (HTTP/1.1)
    let addr_h1: SocketAddr = "10.10.1.1:80".parse().unwrap();
    let key_h1 = ConnectionKey::http(addr_h1, "http/1.1", None, None);
    let prof_h1 = ConnectionProfile::sequential(key_h1, Duration::from_secs(30));

    assert!(pool.acquire_profile(&prof_h1).is_none());
    let lease_h1 = pool.register_profile(&prof_h1, MockConnection::new(addr_h1));
    assert_eq!(lease_h1.peer, addr_h1);
    drop(lease_h1); // returns to LIFO queue
    assert_eq!(pool.total_idle_conns(), 1);

    let reacquired_h1 = pool.acquire_profile(&prof_h1);
    assert!(reacquired_h1.is_some());
    assert_eq!(pool.total_idle_conns(), 0);
    drop(reacquired_h1);
    assert_eq!(pool.total_idle_conns(), 1);

    // 2. Exclusive Profile (Raw TCP)
    let addr_tcp: SocketAddr = "10.10.2.1:9000".parse().unwrap();
    let key_tcp = ConnectionKey::tcp(addr_tcp);
    let prof_tcp = ConnectionProfile::exclusive(key_tcp, Duration::from_secs(30));

    assert!(pool.acquire_profile(&prof_tcp).is_none());
    let lease_tcp = pool.register_profile(&prof_tcp, MockConnection::new(addr_tcp));
    lease_tcp.release(true); // explicit recycle
    assert_eq!(pool.total_idle_conns(), 2);

    let reacquired_tcp = pool.acquire_profile(&prof_tcp).unwrap();
    drop(reacquired_tcp); // drop without explicit release auto-closes!
    assert_eq!(pool.total_idle_conns(), 1);

    // 3. Multiplexed Profile (HTTP/2)
    let addr_h2: SocketAddr = "10.10.3.1:443".parse().unwrap();
    let key_h2 = ConnectionKey::http(
        addr_h2,
        "h2",
        Some("service.internal".into()),
        Some("h2".into()),
    );
    let prof_h2 = ConnectionProfile::multiplexed(key_h2, Duration::from_secs(30), 4);

    assert!(pool.acquire_profile(&prof_h2).is_none());
    let s1 = pool.register_profile(&prof_h2, MockConnection::new(addr_h2));
    assert_eq!(s1.peer, addr_h2);

    // Concurrently borrow streams 2, 3, 4
    let s2 = pool.acquire_profile(&prof_h2).expect("stream 2");
    let s3 = pool.acquire_profile(&prof_h2).expect("stream 3");
    let s4 = pool.acquire_profile(&prof_h2).expect("stream 4");

    // Saturated
    assert!(pool.acquire_profile(&prof_h2).is_none());

    // Release 2 streams
    drop(s3);
    drop(s4);

    // Can acquire again
    let s5 = pool.acquire_profile(&prof_h2).expect("stream 5");
    drop(s1);
    drop(s2);
    drop(s5);
}
