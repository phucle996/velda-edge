//! Memory Leak, Allocation & Heap Regression Audit Benchmark for `velda-observability`.
//!
//! Validates zero-leak and steady-state invariants under sustained and long-running workloads:
//! 1. Sustained Request Telemetry Zero-Allocation Invariant (10,000,000 ops)
//! 2. Sustained Connection Pool Leasing Zero-Allocation Invariant (10,000,000 ops)
//! 3. High-Frequency Snapshot Generation Zero-Retention Audit (50,000 snapshots)
//! 4. Multi-Threaded Concurrent Zero-Leak Audit (16 Threads, 16,000,000 ops)

mod common;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use common::{CountingAllocator, FastRng, format_bytes, format_duration};
use velda_observability::MetricsStore;
use velda_observability::pool::EvictionReason;
use velda_observability::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: Sustained Request Telemetry Zero-Allocation Invariant
// ============================================================================

fn bench_sustained_request_serving_zero_leak() {
    println!("### 1. Sustained Request Telemetry Zero-Allocation Invariant (10,000,000 Ops)\n");
    println!("> Evaluating 10,000,000 sustained stream completions on `MetricsStore`...\n");

    let iters = 10_000_000u64;
    let store = MetricsStore::new();

    let completion = StreamCompletion::new(
        200,
        128,
        1024,
        Duration::from_millis(3),
        Some(Duration::from_millis(1)),
    );

    // Warm-up
    for _ in 0..10_000 {
        store.record_stream_completion(
            DownstreamProto::H1,
            UpstreamProto::H1,
            PipeStrategy::Buffered,
            completion,
        );
    }

    ALLOCATOR.reset();
    let before = ALLOCATOR.detailed_snapshot();
    let start = Instant::now();

    for _ in 0..iters {
        store.record_stream_completion(
            DownstreamProto::H2,
            UpstreamProto::H1,
            PipeStrategy::ServerStream,
            completion,
        );
    }

    let elapsed = start.elapsed();
    let after = ALLOCATOR.detailed_snapshot();

    let net_bytes = after.net_bytes() - before.net_bytes();
    let net_allocs = after.net_allocs() - before.net_allocs();

    println!("| Metric | Initial | Final | Net Delta | Invariant Target | Status |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| **Operations** | 0 | {} | **{} ops** | 10,000,000 ops | **PASS** |",
        iters, iters
    );
    println!(
        "| **Active Heap Delta** | {} | {} | **{} B** | **0 B (Zero Leak)** | **PASS** |",
        format_bytes(before.net_bytes() as usize),
        format_bytes(after.net_bytes() as usize),
        net_bytes
    );
    println!(
        "| **Allocation Calls** | {} | {} | **{} allocs** | **0 (Zero-Alloc)** | **PASS** |",
        before.alloc_count, after.alloc_count, net_allocs
    );
    println!(
        "| **Execution Time** | - | - | **{}** | - | **{:.2} ns/op** |",
        format_duration(elapsed),
        elapsed.as_nanos() as f64 / iters as f64
    );
    println!();

    assert_eq!(
        net_bytes, 0,
        "Heap bytes MUST NOT grow during hot-path telemetry!"
    );
    assert_eq!(
        net_allocs, 0,
        "Zero allocations allowed on hot-path telemetry!"
    );
}

// ============================================================================
// Stage 2: Sustained Connection Pool Leasing Zero-Allocation Invariant
// ============================================================================

fn bench_sustained_pool_leasing_zero_leak() {
    println!(
        "### 2. Sustained Connection Pool Leasing Zero-Allocation Invariant (10,000,000 Ops)\n"
    );
    println!("> Evaluating 10,000,000 pool lease, release, and eviction cycles...\n");

    let iters = 10_000_000u64;
    let store = MetricsStore::new();
    let pool = store.pool();

    // Warm-up
    for _ in 0..10_000 {
        pool.record_hit();
        pool.record_release();
    }

    ALLOCATOR.reset();
    let before = ALLOCATOR.detailed_snapshot();
    let start = Instant::now();

    for i in 0..iters {
        if i % 4 == 0 {
            pool.record_hit();
            pool.record_release();
        } else if i % 4 == 1 {
            pool.record_miss();
            pool.record_release();
        } else if i % 4 == 2 {
            pool.record_hit();
            pool.record_lease_dropped();
            pool.record_eviction(EvictionReason::IdleTimeout, Duration::from_secs(5));
        } else {
            pool.record_timeout();
        }
    }

    let elapsed = start.elapsed();
    let after = ALLOCATOR.detailed_snapshot();

    let net_bytes = after.net_bytes() - before.net_bytes();
    let net_allocs = after.net_allocs() - before.net_allocs();

    println!("| Metric | Initial | Final | Net Delta | Invariant Target | Status |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| **Operations** | 0 | {} | **{} ops** | 10,000,000 ops | **PASS** |",
        iters, iters
    );
    println!(
        "| **Active Heap Delta** | {} | {} | **{} B** | **0 B (Zero Leak)** | **PASS** |",
        format_bytes(before.net_bytes() as usize),
        format_bytes(after.net_bytes() as usize),
        net_bytes
    );
    println!(
        "| **Allocation Calls** | {} | {} | **{} allocs** | **0 (Zero-Alloc)** | **PASS** |",
        before.alloc_count, after.alloc_count, net_allocs
    );
    println!(
        "| **Execution Time** | - | - | **{}** | - | **{:.2} ns/op** |",
        format_duration(elapsed),
        elapsed.as_nanos() as f64 / iters as f64
    );
    println!();

    assert_eq!(net_bytes, 0, "Pool operations MUST NOT allocate on heap!");
    assert_eq!(net_allocs, 0, "Zero allocations allowed in pool telemetry!");
}

// ============================================================================
// Stage 3: High-Frequency Snapshot Generation Zero-Retention Audit
// ============================================================================

fn bench_snapshot_generation_zero_retention() {
    println!("### 3. High-Frequency Snapshot Generation Zero-Retention Audit (50,000 Snapshots)\n");
    println!("> Repeatedly generating and dropping 50,000 point-in-time snapshots...\n");

    let iters = 50_000u64;
    let store = MetricsStore::new();

    // Populate initial state
    store.record_stream_completion(
        DownstreamProto::H1,
        UpstreamProto::H1,
        PipeStrategy::Buffered,
        StreamCompletion::new(200, 64, 512, Duration::from_millis(1), None),
    );

    // Warm-up
    for _ in 0..100 {
        let _ = store.snapshot();
    }

    ALLOCATOR.reset();
    let before = ALLOCATOR.detailed_snapshot();
    let start = Instant::now();

    for _ in 0..iters {
        let snap = store.snapshot();
        std::hint::black_box(&snap);
        drop(snap);
    }

    let elapsed = start.elapsed();
    let after = ALLOCATOR.detailed_snapshot();

    let net_bytes = after.net_bytes() - before.net_bytes();
    let net_allocs = after.net_allocs() - before.net_allocs();

    println!("| Metric | Initial | Final | Net Delta | Invariant Target | Status |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| **Snapshots Captured** | 0 | {} | **{} snapshots** | 50,000 | **PASS** |",
        iters, iters
    );
    println!(
        "| **Residual Heap Delta** | {} | {} | **{} B** | **0 B (Zero Leak)** | **PASS** |",
        format_bytes(before.net_bytes() as usize),
        format_bytes(after.net_bytes() as usize),
        net_bytes
    );
    println!(
        "| **Unfreed Allocations** | {} | {} | **{} residual** | **0 (Clean Drop)** | **PASS** |",
        before.alloc_count, after.alloc_count, net_allocs
    );
    println!(
        "| **Execution Time** | - | - | **{}** | - | **{:.2} µs/snap** |",
        format_duration(elapsed),
        elapsed.as_micros() as f64 / iters as f64
    );
    println!();

    assert_eq!(
        net_bytes, 0,
        "All snapshot memory must be perfectly freed upon drop!"
    );
    assert_eq!(net_allocs, 0, "No lingering heap allocations allowed!");
}

// ============================================================================
// Stage 4: Multi-Threaded Concurrent Zero-Leak Audit
// ============================================================================

fn bench_multi_threaded_concurrent_zero_leak() {
    println!("### 4. Multi-Threaded Concurrent Zero-Leak Audit (16 Threads, 16,000,000 Ops)\n");
    println!(
        "> 16 worker threads concurrently mutating metrics, verifying zero memory retention...\n"
    );

    let num_threads = 16;
    let ops_per_thread = 1_000_000u64;
    let total_ops = num_threads as u64 * ops_per_thread;
    let store = Arc::new(MetricsStore::new());

    ALLOCATOR.reset();
    let before = ALLOCATOR.detailed_snapshot();
    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_threads);
    for tid in 0..num_threads {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let mut rng = FastRng::new(tid as u64 + 1);
            let pool = store_clone.pool();

            for _ in 0..ops_per_thread {
                let (d, u, p) = rng.next_pipeline_coords();
                let comp = rng.next_completion();
                store_clone.record_stream_completion(d, u, p, comp);

                pool.record_hit();
                pool.record_release();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let after = ALLOCATOR.detailed_snapshot();

    let net_bytes = after.net_bytes() - before.net_bytes();
    let net_allocs = after.net_allocs() - before.net_allocs();

    println!("| Metric | Initial | Final | Net Delta | Invariant Target | Status |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| **Total Ops** | 0 | {} | **{} ops** | 16,000,000 ops | **PASS** |",
        total_ops, total_ops
    );
    println!(
        "| **Active Heap Delta** | {} | {} | **{} B** | **0 B (Zero Leak)** | **PASS** |",
        format_bytes(before.net_bytes() as usize),
        format_bytes(after.net_bytes() as usize),
        net_bytes
    );
    println!(
        "| **Allocation Calls** | {} | {} | **{} allocs** | **0 (Zero-Alloc)** | **PASS** |",
        before.alloc_count, after.alloc_count, net_allocs
    );
    println!(
        "| **Execution Time** | - | - | **{}** | - | **{:.2} ns/op** |",
        format_duration(elapsed),
        elapsed.as_nanos() as f64 / total_ops as f64
    );
    println!();

    assert_eq!(
        net_bytes, 0,
        "Concurrent mutations must not leak heap memory!"
    );
    assert_eq!(
        net_allocs, 0,
        "No heap allocations in concurrent mutations!"
    );
}

fn main() {
    println!("================================================================================");
    println!("Velda Observability — Memory Leak & Allocation Audit Suite");
    println!("================================================================================\n");

    bench_sustained_request_serving_zero_leak();
    bench_sustained_pool_leasing_zero_leak();
    bench_snapshot_generation_zero_retention();
    bench_multi_threaded_concurrent_zero_leak();

    println!("Memory leak benchmark suite completed successfully.");
}
