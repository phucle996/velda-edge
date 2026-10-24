//! Integration tests for the Shadow pre-warming and lane-synchronization mechanisms.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use velda_connection_pool::{
    ConnectionKey, MultiplexedPool, PoolManager, PoolableResource, current_thread_lane,
    set_current_thread_lane,
};

#[derive(Debug)]
struct MockConnection {
    _peer: SocketAddr,
    created_at: Instant,
    last_used: Instant,
    healthy: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}

impl MockConnection {
    fn new(peer: SocketAddr) -> Self {
        Self {
            _peer: peer,
            created_at: Instant::now(),
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
fn test_set_current_thread_lane_synchronizes_threads() {
    let lane_id = 7;
    set_current_thread_lane(lane_id);
    assert_eq!(current_thread_lane(), lane_id);

    // Another thread also binds to lane_id (simulating velda-shadow-7 for velda-worker-7)
    let handle = std::thread::spawn(move || {
        set_current_thread_lane(lane_id);
        current_thread_lane()
    });
    let shadow_lane = handle.join().unwrap();
    assert_eq!(shadow_lane, lane_id);
}

#[test]
fn test_pool_manager_shadow_logistics() {
    let pool = PoolManager::<ConnectionKey, MockConnection>::new();
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);
    let worker_lane = 3;

    // Shadow worker thread (lane 3) checks and deposits connections into lane 3
    assert_eq!(pool.idle_conns_for_lane(&key, worker_lane), 0);

    // Shadow pre-warms and deposits 2 connections into lane 3
    pool.release_to_lane(&key, worker_lane, MockConnection::new(addr));
    pool.release_to_lane(&key, worker_lane, MockConnection::new(addr));
    assert_eq!(pool.idle_conns_for_lane(&key, worker_lane), 2);

    // Traffic Worker (running on lane 3) acquires immediately without connect
    set_current_thread_lane(worker_lane);
    let acquired = pool.acquire(&key, Duration::from_secs(30));
    assert!(acquired.is_some());
    assert_eq!(pool.idle_conns_for_lane(&key, worker_lane), 1);
}

#[test]
fn test_multiplexed_pool_shadow_logistics() {
    let pool = MultiplexedPool::<SocketAddr, MockConnection>::new().with_max_connections_per_key(2);
    let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
    let worker_lane = 2;
    let max_streams = 100;

    // 1. Initial empty pool: lane has no connections and no headroom
    assert_eq!(pool.connection_count_for_lane(&addr, worker_lane), 0);
    assert!(!pool.has_headroom_for_lane(&addr, worker_lane, max_streams));

    // 2. Shadow worker pre-warms 1 connection and deposits into lane 2
    pool.register_warmed_to_lane(addr, worker_lane, MockConnection::new(addr), max_streams);
    assert_eq!(pool.connection_count_for_lane(&addr, worker_lane), 1);
    assert!(pool.has_headroom_for_lane(&addr, worker_lane, max_streams));

    // 3. Traffic worker on lane 2 acquires streams
    set_current_thread_lane(worker_lane);
    let mut leases = Vec::new();
    for _ in 0..75 {
        let lease = pool.acquire_stream(&addr, Duration::from_secs(30)).unwrap();
        leases.push(lease);
    }

    // 4. At 75% load, has_headroom_for_lane becomes false -> Shadow worker knows to pre-warm next connection!
    assert!(!pool.has_headroom_for_lane(&addr, worker_lane, max_streams));

    // 5. Shadow worker adds 2nd connection
    pool.register_warmed_to_lane(addr, worker_lane, MockConnection::new(addr), max_streams);
    assert_eq!(pool.connection_count_for_lane(&addr, worker_lane), 2);
    assert!(pool.has_headroom_for_lane(&addr, worker_lane, max_streams));

    drop(leases);
}
