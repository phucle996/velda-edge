//! Multi-Thread Concurrency, Contention & False Sharing Scaling Benchmark for `velda-observability`.
//!
//! Evaluates multi-threaded scaling across core counts (1, 2, 4, 8, 16, 32 threads):
//! 1. Disjoint Pipeline Slots (Zero False Sharing via 64-Byte Cacheline Alignment)
//! 2. Hot-Spot Contention on a Single Pipeline Slot (Atomic Cacheline Saturation Limit)
//! 3. Concurrent Connection Pool Operations (`record_hit`, `record_release`, `record_eviction`)

mod common;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use common::{FastRng, format_throughput};
use velda_observability::pool::EvictionReason;
use velda_observability::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};
use velda_observability::{MetricsStore, TOTAL_PIPELINE_SLOTS};

const OPS_PER_THREAD: u64 = 1_000_000;
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16, 32];

// ============================================================================
// Stage 1: Disjoint Pipeline Slots (L1 Cacheline Isolation / Zero False Sharing)
// ============================================================================

fn bench_disjoint_slots_scaling() {
    println!("### 1. Disjoint Pipeline Slots (Zero False Sharing via 64-Byte Alignment)\n");
    println!("> Each worker thread targets its own isolated `PipelineSlot` modulo 36...\n");
    println!(
        "| Threads | Total Operations | Elapsed Time | Latency / op | Aggregate Throughput | Scaling vs 1T |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let mut baseline_ops_sec = 0.0;

    for &num_threads in THREAD_COUNTS {
        let store = Arc::new(MetricsStore::new());
        let total_ops = num_threads as u64 * OPS_PER_THREAD;
        let mut handles = Vec::with_capacity(num_threads);

        let start = Instant::now();

        for tid in 0..num_threads {
            let store_clone = Arc::clone(&store);
            let (down, up, pipe) = MetricsStore::decode_slot_index(tid % TOTAL_PIPELINE_SLOTS);

            handles.push(thread::spawn(move || {
                let comp = StreamCompletion::new(
                    200,
                    64,
                    512,
                    Duration::from_millis(2),
                    Some(Duration::from_millis(1)),
                );

                for _ in 0..OPS_PER_THREAD {
                    store_clone.record_stream_completion(down, up, pipe, comp);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let ops_sec = total_ops as f64 / elapsed.as_secs_f64();
        let ns_op = elapsed.as_nanos() as f64 / total_ops as f64;

        if num_threads == 1 {
            baseline_ops_sec = ops_sec;
        }

        let scaling = ops_sec / baseline_ops_sec;

        println!(
            "| **{}T** | {} ops | {:.3} s | **{:.2} ns** | **{}** | **{:.2}x** |",
            num_threads,
            total_ops,
            elapsed.as_secs_f64(),
            ns_op,
            format_throughput(total_ops, elapsed),
            scaling
        );
    }
    println!();
}

// ============================================================================
// Stage 2: Hot-Spot Contention on a Single Slot (Atomic Ping-Pong Limit)
// ============================================================================

fn bench_contended_single_slot() {
    println!("### 2. Hot-Spot Contention on a Single Slot (Hardware Atomic Limit)\n");
    println!("> All threads simultaneously hammer the exact same `(H1, H1, Buffered)` slot...\n");
    println!(
        "| Threads | Total Operations | Elapsed Time | Latency / op | Aggregate Throughput | Efficiency |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    for &num_threads in THREAD_COUNTS {
        let store = Arc::new(MetricsStore::new());
        let total_ops = num_threads as u64 * OPS_PER_THREAD;
        let mut handles = Vec::with_capacity(num_threads);

        let start = Instant::now();

        for _ in 0..num_threads {
            let store_clone = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let comp = StreamCompletion::new(
                    200,
                    64,
                    512,
                    Duration::from_millis(2),
                    Some(Duration::from_millis(1)),
                );

                for _ in 0..OPS_PER_THREAD {
                    store_clone.record_stream_completion(
                        DownstreamProto::H1,
                        UpstreamProto::H1,
                        PipeStrategy::Buffered,
                        comp,
                    );
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let ns_op = elapsed.as_nanos() as f64 / total_ops as f64;

        println!(
            "| **{}T** | {} ops | {:.3} s | **{:.2} ns** | **{}** | High Contention |",
            num_threads,
            total_ops,
            elapsed.as_secs_f64(),
            ns_op,
            format_throughput(total_ops, elapsed)
        );
    }
    println!();
}

// ============================================================================
// Stage 3: Concurrent Connection Pool Contention
// ============================================================================

fn bench_concurrent_pool_operations() {
    println!("### 3. Concurrent Connection Pool Operations (Multi-Threaded Leases)\n");
    println!("> Multi-threaded lease hit/miss/release cycles on shared `PoolMetrics`...\n");
    println!(
        "| Threads | Total Operations | Elapsed Time | Latency / op | Aggregate Throughput | Final Leased Gauge |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    for &num_threads in THREAD_COUNTS {
        let store = Arc::new(MetricsStore::new());
        let total_ops = num_threads as u64 * OPS_PER_THREAD;
        let mut handles = Vec::with_capacity(num_threads);

        let start = Instant::now();

        for tid in 0..num_threads {
            let store_clone = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let pool = store_clone.pool();
                let mut rng = FastRng::new(tid as u64 + 1);

                for _ in 0..OPS_PER_THREAD {
                    if rng.next_usize(10) < 8 {
                        // 80% warm hit + release
                        pool.record_hit();
                        pool.record_release();
                    } else {
                        // 20% miss + drop + eviction
                        pool.record_miss();
                        pool.record_lease_dropped();
                        pool.record_eviction(EvictionReason::IdleTimeout, Duration::from_secs(10));
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let ns_op = elapsed.as_nanos() as f64 / total_ops as f64;
        let snap = store.snapshot().pool;

        assert_eq!(
            snap.connections_leased, 0,
            "Leased gauge must return to zero after all paired releases/drops!"
        );

        println!(
            "| **{}T** | {} ops | {:.3} s | **{:.2} ns** | **{}** | **{}** (Balanced) |",
            num_threads,
            total_ops,
            elapsed.as_secs_f64(),
            ns_op,
            format_throughput(total_ops, elapsed),
            snap.connections_leased
        );
    }
    println!();
}

fn main() {
    println!("================================================================================");
    println!("Velda Observability — Multi-Thread Concurrency Benchmark Suite");
    println!("================================================================================\n");

    bench_disjoint_slots_scaling();
    bench_contended_single_slot();
    bench_concurrent_pool_operations();

    println!("Multi-thread benchmark suite completed successfully.");
}
