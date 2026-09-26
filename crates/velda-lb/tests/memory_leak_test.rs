//! Strict Memory Leak & Heap Allocation Verification Test Suite for velda-lb.
//!
//! Enforces:
//! 1. Sharded RoundRobin zero-heap allocation invariant.
//! 2. Sharded SWRR RAII cleanup and zero-leak verification under concurrent thread execution.
//! 3. Maglev / RingHash table rebuild churn and ArcSwap reference count deallocation.
//! 4. Thread-local storage (TLS) clean destruction across thread spawn/join cycles.
//! 5. Sharded EndpointMetrics lock-free concurrency and zero-leak lifecycle.

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use velda_lb::{
    Endpoint, EndpointMetrics, LoadBalancer, Maglev, RingHash, RoundRobin, SelectionContext,
    WeightedRoundRobin,
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

fn make_test_endpoints(count: usize) -> Vec<Endpoint> {
    (0..count)
        .map(|i| {
            let addr: SocketAddr = format!("10.0.0.{}:8080", i + 1).parse().unwrap();
            Endpoint::new(format!("ep-{}", i), addr, ((i % 5) + 1) as u32)
        })
        .collect()
}

#[test]
fn test_all_memory_leak_and_zero_alloc_invariants() {
    println!("\n=== Starting Velda-LB Memory Leak & Zero-Alloc Invariant Verification ===");

    print!("Checking Sharded RoundRobin zero heap allocation... ");
    verify_sharded_round_robin_zero_heap_allocation();
    println!("OK (0 heap allocations)");

    print!("Checking Sharded WeightedRoundRobin lifecycle & cleanup... ");
    verify_sharded_weighted_round_robin_lifecycle_no_leak();
    println!("OK (0-byte leak across 16 threads)");

    print!("Checking Maglev & RingHash rebuild churn... ");
    verify_maglev_and_ring_hash_rebuild_churn_no_leak();
    println!("OK (0-byte leak across 64,000 churn requests)");

    print!("Checking Worker threads concurrent zero-alloc hot path... ");
    verify_worker_threads_zero_alloc_and_no_leak();
    println!("OK (0 heap allocations across 800,000 requests)");

    print!("Checking Sharded EndpointMetrics concurrency & zero-leak... ");
    verify_sharded_metrics_zero_leak_and_concurrency();
    println!("OK (0-byte leak across 16 threads)");

    println!("=== All Memory Leak & Allocation Invariants Passed Cleanly ===\n");
}

fn verify_sharded_round_robin_zero_heap_allocation() {
    let endpoints = make_test_endpoints(5);
    let balancer = RoundRobin::new();
    let ctx = SelectionContext::NONE;

    // Warm up thread-local storage initialization on this thread
    let _ = balancer.select(&endpoints, &ctx);

    // Record baseline allocations
    let allocs_before = TRACKER.alloc_count();

    // Execute 100,000 selection rounds on the hot path
    for _ in 0..100_000 {
        let ep = balancer.select(&endpoints, &ctx);
        let _ = std::hint::black_box(ep);
    }

    let allocs_after = TRACKER.alloc_count();

    // Invariant: Sharded RoundRobin must execute with ZERO heap allocations
    assert_eq!(
        allocs_before, allocs_after,
        "Sharded RoundRobin allocated on heap! Hot path must be 100% zero-alloc"
    );
}

fn verify_sharded_weighted_round_robin_lifecycle_no_leak() {
    let endpoints = make_test_endpoints(5);
    let baseline_bytes = TRACKER.live_bytes();

    {
        let balancer = Arc::new(WeightedRoundRobin::new());
        let mut handles = Vec::new();

        // 16 concurrent worker threads hammering the 16 shards
        for w in 0..16 {
            let b_clone = Arc::clone(&balancer);
            let eps_clone = endpoints.clone();
            handles.push(std::thread::spawn(move || {
                let ctx = SelectionContext::NONE;
                for _ in 0..10_000 {
                    let ep = b_clone.select(&eps_clone, &ctx);
                    let _ = std::hint::black_box(ep);
                }
                let _ = w;
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    } // Balancer dropped here! All 16 Mutex shards and inner vectors must be reclaimed.

    let final_bytes = TRACKER.live_bytes();

    assert_eq!(
        final_bytes, baseline_bytes,
        "Memory leak detected in Sharded SWRR! Live bytes: before={}, after={}",
        baseline_bytes, final_bytes
    );
}

fn verify_maglev_and_ring_hash_rebuild_churn_no_leak() {
    let endpoints_v1 = make_test_endpoints(5);
    let endpoints_v2 = make_test_endpoints(10);

    // Warm up thread runtime & arc-swap internal debt pool
    {
        let maglev = Arc::new(Maglev::new(257));
        let ring = Arc::new(RingHash::new());
        let _ = maglev.select(&endpoints_v1, &SelectionContext::with_hash(1));
        let _ = ring.select(&endpoints_v1, &SelectionContext::with_hash(1));
    }

    // Function to run a heavy churn cycle
    let run_churn_cycle = || {
        let maglev = Arc::new(Maglev::new(257));
        let ring = Arc::new(RingHash::new());
        let mut handles = Vec::new();

        for w in 0..8 {
            let m_clone = Arc::clone(&maglev);
            let r_clone = Arc::clone(&ring);
            let eps1 = endpoints_v1.clone();
            let eps2 = endpoints_v2.clone();

            handles.push(std::thread::spawn(move || {
                let ctx = SelectionContext::with_hash(w as u64);
                for i in 0..2_000 {
                    let eps = if i % 2 == 0 { &eps1 } else { &eps2 };
                    let _ = m_clone.select(eps, &ctx);
                    let _ = r_clone.select(eps, &ctx);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    };

    // Stabilize lazy allocator structures (arc_swap debt pool & thread cache)
    run_churn_cycle();
    run_churn_cycle();
    let baseline_bytes = TRACKER.live_bytes();

    // Execute 32,000 requests under concurrent rebuild churn
    run_churn_cycle();
    let cycle2_bytes = TRACKER.live_bytes();

    // Execute another 32,000 requests under concurrent rebuild churn
    run_churn_cycle();
    let cycle3_bytes = TRACKER.live_bytes();

    assert_eq!(
        cycle2_bytes, baseline_bytes,
        "Memory leak in Maglev/RingHash! Cycle 1: {}, Cycle 2: {}",
        baseline_bytes, cycle2_bytes
    );
    assert_eq!(
        cycle3_bytes, baseline_bytes,
        "Memory leak in Maglev/RingHash! Cycle 2: {}, Cycle 3: {}",
        cycle2_bytes, cycle3_bytes
    );
}

fn verify_worker_threads_zero_alloc_and_no_leak() {
    let endpoints = make_test_endpoints(5);
    let balancer = Arc::new(RoundRobin::new());
    let swrr = Arc::new(WeightedRoundRobin::new());

    // Pre-initialize and warm up all 16 shards once so current_weights is sized
    for _ in 0..32 {
        let _ = balancer.select(&endpoints, &SelectionContext::NONE);
        let _ = swrr.select(&endpoints, &SelectionContext::NONE);
    }

    let workers = 8;
    let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
    let mut handles = Vec::new();

    // 8 concurrent worker threads running 100,000 requests per thread
    for _ in 0..workers {
        let b1 = Arc::clone(&balancer);
        let b2 = Arc::clone(&swrr);
        let eps = endpoints.clone();
        let b = Arc::clone(&barrier);

        handles.push(std::thread::spawn(move || {
            let ctx = SelectionContext::NONE;

            // Warm up this specific thread's shard
            let _ = b1.select(&eps, &ctx);
            let _ = b2.select(&eps, &ctx);

            // Wait for main thread to start measuring
            b.wait();

            // Execute 100,000 requests on worker thread
            for _ in 0..100_000 {
                let e1 = b1.select(&eps, &ctx);
                let e2 = b2.select(&eps, &ctx);
                let _ = std::hint::black_box((e1, e2));
            }

            // Signal main thread that workload is finished
            b.wait();
        }));
    }

    // Wait for all workers to complete initialization
    barrier.wait();
    let allocs_start = TRACKER.alloc_count();

    // Wait for all workers to complete their 100,000 requests loop
    barrier.wait();
    let allocs_end = TRACKER.alloc_count();

    for h in handles {
        h.join().unwrap();
    }

    // INVARIANT: Exactly 0 heap allocations across all 800,000 worker selections
    assert_eq!(
        allocs_start, allocs_end,
        "Worker threads allocated on heap during selection loop! start={}, end={}",
        allocs_start, allocs_end
    );
}

fn verify_sharded_metrics_zero_leak_and_concurrency() {
    let baseline_bytes = TRACKER.live_bytes();

    {
        let metrics = Arc::new(EndpointMetrics::new());
        let workers = 16;
        let mut handles = Vec::new();

        for _ in 0..workers {
            let m = Arc::clone(&metrics);
            handles.push(std::thread::spawn(move || {
                for i in 0..10_000 {
                    m.inc_inflight();
                    m.inc_active();
                    if i % 10 == 0 {
                        m.record_latency_nanos(1_000_000);
                    }
                    m.dec_active();
                    m.dec_inflight();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Metrics must balance back to exactly 0 active and 0 in-flight
        assert_eq!(metrics.active_connections(), 0);
        assert_eq!(metrics.inflight_requests(), 0);
    } // Metrics dropped here! All 16 shards and latency tracker dropped.

    let final_bytes = TRACKER.live_bytes();
    assert_eq!(
        final_bytes, baseline_bytes,
        "Memory leak in Sharded EndpointMetrics! before={}, after={}",
        baseline_bytes, final_bytes
    );
}
