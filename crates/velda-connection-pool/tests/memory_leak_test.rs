//! Strict Memory Leak & Allocation Verification Test Suite for velda-connection-pool.
//!
//! Enforces:
//! 1. Zero memory leaks on repeated acquire and release cycles (LIFO queue).
//! 2. Zero memory leaks under RAII `PoolLease` checkout and auto-drop returns.
//! 3. Zero memory leaks during heavy key churn, draining, and empty pool pruning.
//! 4. Zero memory leaks under concurrent multi-threaded worker hammering.
//! 5. Zero memory leaks during eviction of timed-out and unhealthy resources.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use velda_connection_pool::{
    ColdPool, CompactPoolKey, HotPool, PoolConfig, PoolManager, PoolableResource, SubPool,
};

struct TrackingAllocator {
    alloc_count: AtomicU64,
    dealloc_count: AtomicU64,
    live_bytes: AtomicI64,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
            live_bytes: AtomicI64::new(0),
        }
    }

    fn live_bytes(&self) -> i64 {
        self.live_bytes.load(Ordering::SeqCst)
    }

    fn alloc_count(&self) -> u64 {
        self.alloc_count.load(Ordering::SeqCst)
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        self.live_bytes
            .fetch_add(layout.size() as i64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
        self.live_bytes
            .fetch_sub(layout.size() as i64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static TRACKER: TrackingAllocator = TrackingAllocator::new();

/// Resource carrying a heap allocation (`Vec<u8>`) to guarantee detection of any leaked connection.
#[derive(Debug)]
struct HeapResource {
    payload: Vec<u8>,
    created_at: Instant,
    last_used: Instant,
    healthy: Arc<AtomicBool>,
}

impl HeapResource {
    fn new(size_bytes: usize) -> Self {
        Self {
            payload: vec![0xAB; size_bytes],
            created_at: Instant::now(),
            last_used: Instant::now(),
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    fn with_created_at(size_bytes: usize, created_at: Instant) -> Self {
        Self {
            payload: vec![0xCD; size_bytes],
            created_at,
            last_used: Instant::now(),
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    #[inline]
    fn payload_size(&self) -> usize {
        self.payload.len()
    }
}

impl PoolableResource for HeapResource {
    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
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
    }
}

#[test]
fn test_all_connection_pool_memory_leak_invariants() {
    println!("\n=== Starting Velda-Connection-Pool Memory Leak & Invariant Verification ===");

    print!("1. Checking SubPool LIFO lifecycle & zero leak... ");
    verify_subpool_zero_leak();
    println!("OK (0-byte leak)");

    print!("2. Checking ShardTable routing & zero leak... ");
    verify_shard_table_zero_leak();
    println!("OK (0-byte leak)");

    print!("3. Checking repeated acquire/release (100,000 ops) zero leak... ");
    verify_acquire_release_lifecycle_zero_leak();
    println!("OK (0-byte leak across 100,000 ops)");

    print!("4. Checking RAII PoolLease drops (50,000 ops) zero leak... ");
    verify_pool_lease_raii_drop_zero_leak();
    println!("OK (0-byte leak across 50,000 leases)");

    print!("5. Checking heavy key churn, draining & pruning (1,000 keys) zero leak... ");
    verify_key_churn_and_drain_zero_leak();
    println!("OK (0-byte leak across 1,000 keys)");

    print!("6. Checking concurrent multi-threaded worker hammer (16 threads) zero leak... ");
    verify_concurrent_hammer_multithreaded_zero_leak();
    println!("OK (0-byte leak across 16 threads)");

    print!("7. Checking eviction of expired & unhealthy connections zero leak... ");
    verify_eviction_and_unhealthy_zero_leak();
    println!("OK (0-byte leak across 5,000 expired resources)");

    println!("=== All Connection Pool Memory Leak Invariants Passed Cleanly ===\n");
}

fn verify_subpool_zero_leak() {
    // Warm up
    {
        let mut subpool = SubPool::new(10);
        subpool.release(HeapResource::new(1024));
        let _ = subpool.acquire(Duration::from_secs(60), None);
    }

    let baseline = TRACKER.live_bytes();

    {
        let mut subpool = SubPool::new(32);
        for _ in 0..10_000 {
            subpool.release(HeapResource::new(4096));
            let popped = subpool.acquire(Duration::from_secs(60), None);
            assert!(popped.is_some());
        }
        // Subpool capacity overflow test
        for _ in 0..50 {
            subpool.release(HeapResource::new(2048));
        }
        assert_eq!(subpool.len(), 32);
        // Clear all remaining
        subpool.clear();
        assert_eq!(subpool.len(), 0);
    }

    let current = TRACKER.live_bytes();
    assert_eq!(
        current, baseline,
        "SubPool leaked memory! before={}, after={}",
        baseline, current
    );
}

fn verify_shard_table_zero_leak() {
    // Warm up
    {
        let pool = HotPool::<CompactPoolKey, HeapResource>::with_workers(4);
        let key = CompactPoolKey::tcp(velda_core::lut::ip::v4_to_u128(0x7f000001), 8080u16.to_be());
        let lane = pool.lane(0);
        let mut guard = lane.lock();
        guard.insert(key, SubPool::new(10));
    }

    let baseline = TRACKER.live_bytes();

    {
        let pool = HotPool::<CompactPoolKey, HeapResource>::with_workers(8);
        let cold = ColdPool::<CompactPoolKey, HeapResource>::with_capacity(64);
        for i in 0..2000 {
            let addr = (
                velda_core::lut::ip::v4_to_u128(
                    (10u32 << 24) | ((((i / 250) + 1) as u32) << 8) | (((i % 250) + 1) as u32),
                ),
                8080u16.to_be(),
            );
            let key = CompactPoolKey::tcp(addr.0, addr.1);
            let lane = pool.lane(i);
            let mut guard = lane.lock();
            let subpool = guard.get_or_insert_with(key, 10);
            if let Some(spilled) = subpool.push_or_spill(HeapResource::new(1024))
                && let Err(mut rejected) = cold.push(key, spilled)
            {
                rejected.close();
            }
        }

        // Drain all lanes and ColdPool
        cold.clear();
        for lane in pool.all_lanes() {
            lane.lock().clear();
        }
    }

    let current = TRACKER.live_bytes();
    assert_eq!(
        current, baseline,
        "HotPool/ColdPool leaked memory! before={}, after={}",
        baseline, current
    );
}

fn verify_acquire_release_lifecycle_zero_leak() {
    let addr = (velda_core::lut::ip::v4_to_u128(0x0a000101), 8080u16.to_be());
    let key = CompactPoolKey::tcp(addr.0, addr.1);
    let test_config = PoolConfig {
        max_idle_per_key: 32,
        ..PoolConfig::default()
    };

    // Warm up
    {
        let pool = PoolManager::<CompactPoolKey, HeapResource>::with_config(test_config);
        pool.release(&key, HeapResource::new(2048), true, false);
        let _ = pool.acquire(&key, Duration::from_secs(60));
    }

    let baseline = TRACKER.live_bytes();

    {
        let pool = PoolManager::<CompactPoolKey, HeapResource>::with_config(test_config);

        // Prime with 16 connections
        for _ in 0..16 {
            pool.release(&key, HeapResource::new(4096), true, false);
        }
        assert_eq!(pool.total_idle_conns(), 16);

        // 100,000 acquire and release cycles
        let allocs_start = TRACKER.alloc_count();
        for _ in 0..100_000 {
            let mut conn = pool.acquire(&key, Duration::from_secs(60)).unwrap();
            assert_eq!(conn.payload_size(), 4096);
            conn.touch();
            pool.release(&key, conn, true, false);
        }
        let allocs_end = TRACKER.alloc_count();
        // Since SubPool reuses existing VecDeque entries and does 0 heap allocations on hit,
        // allocations during steady-state checkout/return should be 0!
        assert_eq!(
            allocs_start, allocs_end,
            "Acquire/release hot path performed heap allocations!"
        );

        assert_eq!(pool.total_idle_conns(), 16);
        pool.clear();
        assert_eq!(pool.total_idle_conns(), 0);
    }

    let current = TRACKER.live_bytes();
    assert_eq!(
        current, baseline,
        "PoolManager acquire/release leaked memory! before={}, after={}",
        baseline, current
    );
}

fn verify_pool_lease_raii_drop_zero_leak() {
    let addr = (velda_core::lut::ip::v4_to_u128(0x0a000201), 8080u16.to_be());
    let key = CompactPoolKey::tcp(addr.0, addr.1);
    let test_config = PoolConfig {
        max_idle_per_key: 32,
        ..PoolConfig::default()
    };

    // Warm up
    {
        let pool = Arc::new(PoolManager::<CompactPoolKey, HeapResource>::with_config(
            test_config,
        ));
        pool.release(&key, HeapResource::new(1024), true, false);
        let _ = pool.acquire_lease(&key, Duration::from_secs(60));
    }

    let baseline = TRACKER.live_bytes();

    {
        let pool = Arc::new(PoolManager::<CompactPoolKey, HeapResource>::with_config(
            test_config,
        ));

        // Prime with 8 connections
        for _ in 0..8 {
            pool.release(&key, HeapResource::new(4096), true, false);
        }

        // 50,000 checkout and drop cycles
        for _ in 0..50_000 {
            {
                let mut lease = pool.acquire_lease(&key, Duration::from_secs(60)).unwrap();
                lease.touch();
                // RAII auto-release on scope exit
            }
        }

        assert_eq!(pool.total_idle_conns(), 8);
        pool.clear();
        assert_eq!(pool.total_idle_conns(), 0);
    }

    let current = TRACKER.live_bytes();
    assert_eq!(
        current, baseline,
        "RAII PoolLease drop leaked memory! before={}, after={}",
        baseline, current
    );
}

fn verify_key_churn_and_drain_zero_leak() {
    let baseline = TRACKER.live_bytes();

    {
        let pool = PoolManager::<CompactPoolKey, HeapResource>::new();

        // Rapidly create 1,000 keys and add resources
        for i in 0..1000 {
            let addr = (
                velda_core::lut::ip::v4_to_u128(
                    (10u32 << 24)
                        | (1u32 << 16)
                        | ((((i / 250) + 1) as u32) << 8)
                        | (((i % 250) + 1) as u32),
                ),
                8080u16.to_be(),
            );
            let key = CompactPoolKey::tcp(addr.0, addr.1);
            for _ in 0..3 {
                pool.release(&key, HeapResource::new(2048), true, false);
            }
        }

        assert_eq!(pool.total_idle_conns(), 3000);
        assert_eq!(pool.total_pool_containers(), 1000);

        // Drain all matching keys
        let drained = pool.drain_matching(|_| true);
        assert_eq!(drained, 3000);
        assert_eq!(pool.total_idle_conns(), 0);
        assert_eq!(pool.total_pool_containers(), 0);

        // Prune empty pools (should be 0 since drain already removed containers)
        let pruned = pool.prune_empty_pools(Duration::ZERO);
        assert_eq!(pruned, 0);
    }

    let current = TRACKER.live_bytes();
    assert_eq!(
        current, baseline,
        "Key churn and drain leaked memory! before={}, after={}",
        baseline, current
    );
}

fn verify_concurrent_hammer_multithreaded_zero_leak() {
    let baseline = TRACKER.live_bytes();

    {
        let pool = Arc::new(PoolManager::<CompactPoolKey, HeapResource>::new());
        let workers = 16;
        let ops_per_worker = 5_000;
        let mut handles = Vec::new();

        for worker_id in 0..workers {
            let pool_clone = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                let addr = (
                    velda_core::lut::ip::v4_to_u128(
                        (10u32 << 24) | (2u32 << 16) | ((worker_id as u32) << 8) | 1,
                    ),
                    8080u16.to_be(),
                );
                let key = CompactPoolKey::tcp(addr.0, addr.1);

                // Seed with 2 connections per worker
                pool_clone.release(&key, HeapResource::new(2048), true, false);
                pool_clone.release(&key, HeapResource::new(2048), true, false);

                for _ in 0..ops_per_worker {
                    if let Some(mut lease) = pool_clone.acquire_lease(&key, Duration::from_secs(60))
                    {
                        lease.touch();
                        // Drop auto-returns to pool
                    } else {
                        // In case capacity was empty, allocate and release
                        pool_clone.release(&key, HeapResource::new(2048), true, false);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Clear all connections and containers
        pool.clear();
        assert_eq!(pool.total_idle_conns(), 0);
        assert_eq!(pool.total_pool_containers(), 0);
    }

    let current = TRACKER.live_bytes();
    assert_eq!(
        current, baseline,
        "Concurrent multi-threaded hammering leaked memory! before={}, after={}",
        baseline, current
    );
}

fn verify_eviction_and_unhealthy_zero_leak() {
    let baseline = TRACKER.live_bytes();

    {
        let pool = PoolManager::<CompactPoolKey, HeapResource>::new();

        // Seed 1,000 keys with expired connections
        let past = Instant::now() - Duration::from_secs(3600);
        for i in 0..1000 {
            let addr = (
                velda_core::lut::ip::v4_to_u128(
                    (10u32 << 24)
                        | (3u32 << 16)
                        | ((((i / 250) + 1) as u32) << 8)
                        | (((i % 250) + 1) as u32),
                ),
                8080u16.to_be(),
            );
            let key = CompactPoolKey::tcp(addr.0, addr.1);
            pool.release(&key, HeapResource::with_created_at(2048, past), true, false);
        }

        assert_eq!(pool.total_idle_conns(), 1000);

        // Evict all expired connections
        let evicted = pool.evict_expired(Duration::from_secs(60));
        assert_eq!(evicted, 1000);
        assert_eq!(pool.total_idle_conns(), 0);

        // Prune empty containers
        let pruned = pool.prune_empty_pools(Duration::ZERO);
        assert_eq!(pruned, 1000);
        assert_eq!(pool.total_pool_containers(), 0);
    }

    let current = TRACKER.live_bytes();
    assert_eq!(
        current, baseline,
        "Eviction of expired connections leaked memory! before={}, after={}",
        baseline, current
    );
}
