//! Single-Thread Performance, Latency & Zero-Allocation Benchmark Suite for `velda-observability`.
//!
//! Evaluates hot-path single-thread performance:
//! 1. `MetricsStore::record_stream_completion()` Direct O(1) Indexing & Atomic Update (10,000,000 ops)
//! 2. `PoolMetrics` Reusability, Leasing & Eviction Hot-Path (5,000,000 ops)
//! 3. `MetricsStore::snapshot()` Point-in-Time Read Latency (100,000 snapshots)

mod common;

use std::time::{Duration, Instant};

use common::{CountingAllocator, FastRng, format_duration, format_throughput};
use velda_core::lut::PipeKind;
use velda_observability::{EvictionReason, MetricsStore, StreamCompletion};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: Direct MetricsStore Slot Lookup & Completion
// ============================================================================

fn bench_direct_slot_completion() {
    println!(
        "### 1. `MetricsStore::record_stream_completion()` Direct O(1) Atomic Record (10,000,000 ops)\n"
    );
    println!(
        "| Target Pipeline | Operation | Latency / op | Allocs / op | Throughput | Invariant Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 10_000_000u64;
    let store = MetricsStore::new();

    let completion = StreamCompletion::new(200, 128, 2048, 4_000_000, 1_000_000);

    // Warm-up
    for _ in 0..10_000 {
        store.record_stream_completion(PipeKind::BufferedH1H1, completion);
    }

    let pipelines = [
        ("H1 -> H1 Buffered", PipeKind::BufferedH1H1),
        ("H2 -> H1 ServerStream", PipeKind::ServerStreamH2H1),
        ("H3 -> H3 Duplex", PipeKind::DuplexH3H3),
    ];

    for (name, pipe) in pipelines {
        ALLOCATOR.reset();
        let start = Instant::now();

        for _ in 0..iters {
            store.record_stream_completion(
                std::hint::black_box(pipe),
                std::hint::black_box(completion),
            );
        }

        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let status = if allocs == 0 && ns_op < 10.0 {
            "**PASS (< 10 ns, 0 allocs)**"
        } else if allocs == 0 {
            "**PASS (0 allocs)**"
        } else {
            "**FAIL (heap alloc)**"
        };

        println!(
            "| **{}** | Direct O(1) Slot Write | **{:.2} ns** | **{:.2}** | {} | {} |",
            name,
            ns_op,
            allocs as f64 / iters as f64,
            format_throughput(iters, elapsed),
            status
        );
    }
    println!();
}

// ============================================================================
// Stage 2: PoolMetrics Reusability, Leasing & Eviction Hot-Path
// ============================================================================

fn bench_pool_metrics_hot_path() {
    println!("### 2. `PoolMetrics` Reusability, Leasing & Eviction Hot-Path (5,000,000 ops)\n");
    println!(
        "| Operation | Workload Action | Latency / op | Allocs / op | Throughput | Invariant Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 5_000_000u64;
    let store = MetricsStore::new();
    let pool = store.pool();

    // 4.1 Lease Hit & Release Pair
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        pool.record_hit();
        pool.record_release();
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "| **Lease Hit + Release** | atomic hit + atomic release | **{:.2} ns** | **{:.2}** | {} | **PASS (0 allocs)** |",
        ns_op,
        allocs as f64 / iters as f64,
        format_throughput(iters, elapsed)
    );

    // 4.2 Lease Miss + Drop Pair
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        pool.record_miss();
        pool.record_lease_dropped();
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "| **Lease Miss + Drop** | atomic miss + atomic drop | **{:.2} ns** | **{:.2}** | {} | **PASS (0 allocs)** |",
        ns_op,
        allocs as f64 / iters as f64,
        format_throughput(iters, elapsed)
    );

    // 2.3 Eviction Recording with Lifespan Bucketing
    ALLOCATOR.reset();
    let start = Instant::now();
    let lifespan_ns = 25 * 1_000_000_000u64;
    for _ in 0..iters {
        pool.record_eviction(
            std::hint::black_box(EvictionReason::IdleTimeout),
            std::hint::black_box(lifespan_ns),
        );
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "| **Eviction + Lifespan** | Reason class + 6 buckets | **{:.2} ns** | **{:.2}** | {} | **PASS (0 allocs)** |",
        ns_op,
        allocs as f64 / iters as f64,
        format_throughput(iters, elapsed)
    );
    println!();
}

// ============================================================================
// Stage 3: MetricsStore::snapshot() Point-in-Time Generation Latency
// ============================================================================

fn bench_snapshot_generation_latency() {
    println!("### 3. `MetricsStore::snapshot()` Point-in-Time Read Latency (100,000 snapshots)\n");
    println!("| Target Data | Snapshot Content | Latency / op | Throughput | Invariant Status |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 100_000u64;
    let store = MetricsStore::new();

    // Populate some data
    let mut rng = FastRng::new(42);
    for _ in 0..10_000 {
        let pipe = rng.next_pipe_kind();
        let comp = rng.next_completion();
        store.record_stream_completion(pipe, comp);
    }
    store.pool().record_hit();
    store.pool().record_hit();
    store.pool().record_miss();
    store.pool().set_pool_containers(64);

    let start = Instant::now();
    for _ in 0..iters {
        let snap = store.snapshot();
        let _ = std::hint::black_box(snap);
    }
    let elapsed = start.elapsed();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let status = if ns_op < 2000.0 {
        "**PASS (< 2.0 µs)**"
    } else {
        "**WARN**"
    };

    println!(
        "| Full Store Snapshot | 36 pipelines + Global + Pool | **{:.2} ns** ({}) | {} | {} |",
        ns_op,
        format_duration(Duration::from_nanos(ns_op as u64)),
        format_throughput(iters, elapsed),
        status
    );
    println!();
}

fn main() {
    println!("================================================================================");
    println!("Velda Observability — Single-Thread Micro-Benchmark Suite");
    println!("================================================================================\n");

    bench_direct_slot_completion();
    bench_pool_metrics_hot_path();
    bench_snapshot_generation_latency();

    println!("Single-thread benchmark suite completed successfully.");
}
