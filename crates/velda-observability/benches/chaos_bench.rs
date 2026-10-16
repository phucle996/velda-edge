//! Chaos, Concurrency Interleaving & Invariant Integrity Benchmark for `velda-observability`.
//!
//! Evaluates resilience under chaotic concurrent reader-writer collision and erratic states:
//! 1. Heavy Concurrent Reader-Writer Chaos (16 Writers colliding with 4 Non-stop Snapshot Readers)
//! 2. Erratic Connection Pool Lifecycle Chaos (Random Drops, Timeouts & Eviction Flapping)
//! 3. All-36 Pipelines High-Permutation Flurry (32 Threads Randomly Permuting Coords)

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use common::{FastRng, format_throughput};
use velda_observability::MetricsStore;
use velda_observability::pool::EvictionReason;

// ============================================================================
// Stage 1: Heavy Concurrent Reader-Writer Chaos
// ============================================================================

fn bench_concurrent_reader_writer_chaos() {
    println!("### 1. Heavy Concurrent Reader-Writer Chaos (16 Writers vs 4 Readers)\n");
    println!("> 16 writers hammering hot-path while 4 readers continuously capture snapshots...\n");
    println!(
        "| Writers | Readers | Total Mutations | Snapshots Captured | Elapsed Time | Writer Latency | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    let num_writers = 16;
    let num_readers = 4;
    let writes_per_thread = 500_000u64;
    let total_expected_writes = num_writers as u64 * writes_per_thread;

    let store = Arc::new(MetricsStore::new());
    let running = Arc::new(AtomicBool::new(true));
    let snapshot_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    // Spawn 4 Readers
    let mut reader_handles = Vec::with_capacity(num_readers);
    for _ in 0..num_readers {
        let store_clone = Arc::clone(&store);
        let running_clone = Arc::clone(&running);
        let counter = Arc::clone(&snapshot_count);

        reader_handles.push(thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                let snap = store_clone.snapshot();
                std::hint::black_box(snap);
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Spawn 16 Writers
    let mut writer_handles = Vec::with_capacity(num_writers);
    for tid in 0..num_writers {
        let store_clone = Arc::clone(&store);
        writer_handles.push(thread::spawn(move || {
            let mut rng = FastRng::new((tid as u64 + 1) * 7919);
            for _ in 0..writes_per_thread {
                let (d, u, p) = rng.next_pipeline_coords();
                let comp = rng.next_completion();
                store_clone.record_stream_completion(d, u, p, comp);
            }
        }));
    }

    // Wait for all writers to complete
    for h in writer_handles {
        h.join().unwrap();
    }

    // Stop readers
    running.store(false, Ordering::Relaxed);
    for h in reader_handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_snaps = snapshot_count.load(Ordering::Relaxed);
    let ns_write = elapsed.as_nanos() as f64 / total_expected_writes as f64;

    let final_snap = store.snapshot();
    assert_eq!(
        final_snap.total_requests(),
        total_expected_writes,
        "All writes must be reflected in final snapshot!"
    );

    println!(
        "| **16** | **4** | {} writes | {} snapshots | {:.3} s | **{:.2} ns/op** | **PASS (Zero Lock, Zero Teardown)** |",
        total_expected_writes,
        total_snaps,
        elapsed.as_secs_f64(),
        ns_write
    );
    println!();
}

// ============================================================================
// Stage 2: Erratic Connection Pool Lifecycle Chaos
// ============================================================================

fn bench_erratic_pool_lifecycle_chaos() {
    println!(
        "### 2. Erratic Connection Pool Lifecycle Chaos (Random Drops, Timeouts, Evictions)\n"
    );
    println!(
        "> 8 threads executing unpredictable pool checkouts, unexpected socket drops, and evictions...\n"
    );
    println!(
        "| Workers | Total Operations | Elapsed Time | Latency / op | Final Leased Gauge | Consistency Target | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    let num_threads = 8;
    let ops_per_thread = 500_000u64;
    let total_ops = num_threads as u64 * ops_per_thread;

    let store = Arc::new(MetricsStore::new());
    let mut handles = Vec::with_capacity(num_threads);

    let start = Instant::now();

    for tid in 0..num_threads {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let pool = store_clone.pool();
            let mut rng = FastRng::new((tid as u64 + 1) * 31337);

            for _ in 0..ops_per_thread {
                match rng.next_usize(100) {
                    0..=69 => {
                        // 70% normal warm lease hit & return
                        pool.record_hit();
                        pool.record_release();
                    }
                    70..=84 => {
                        // 15% lease miss + connection created + return
                        pool.record_miss();
                        pool.record_connection_created();
                        pool.record_release();
                    }
                    85..=94 => {
                        // 10% lease hit or miss + socket breaks mid-flight -> drop
                        if rng.next_usize(2) == 0 {
                            pool.record_hit();
                        } else {
                            pool.record_miss();
                            pool.record_connection_created();
                        }
                        pool.record_lease_dropped();
                        pool.record_eviction(
                            EvictionReason::Unhealthy,
                            Duration::from_millis(rng.next_usize(1000) as u64),
                        );
                    }
                    _ => {
                        // 5% queue timeout
                        pool.record_timeout();
                    }
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

    // Mathematical integrity invariant: leased connections MUST be exactly 0
    assert_eq!(
        snap.connections_leased, 0,
        "connections_leased must perfectly reconcile to 0 after all operations complete!"
    );

    println!(
        "| **8** | {} ops | {:.3} s | **{:.2} ns** | **{}** | Strict 0 (No Drift) | **PASS** |",
        total_ops,
        elapsed.as_secs_f64(),
        ns_op,
        snap.connections_leased
    );
    println!();
}

// ============================================================================
// Stage 3: All-36 Pipelines High-Permutation Flurry
// ============================================================================

fn bench_all_36_pipelines_permutation_flurry() {
    println!("### 3. All-36 Pipelines High-Permutation Flurry (32 Threads Permuting Coords)\n");
    println!(
        "> 32 threads continuously scattering stream starts and completions across all 36 combinations...\n"
    );
    println!(
        "| Worker Threads | Total Dispatched | Elapsed Time | Latency / op | Aggregate Throughput | Invariant Check | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    let num_threads = 32;
    let ops_per_thread = 250_000u64;
    let total_dispatched = num_threads as u64 * ops_per_thread;

    let store = Arc::new(MetricsStore::new());
    let mut handles = Vec::with_capacity(num_threads);

    let start = Instant::now();

    for tid in 0..num_threads {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let mut rng = FastRng::new((tid as u64 + 1) * 104729);

            for _ in 0..ops_per_thread {
                let (d, u, p) = rng.next_pipeline_coords();
                let comp = rng.next_completion();

                store_clone.record_stream_start(d, u, p);
                store_clone.record_stream_completion(d, u, p, comp);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let ns_op = elapsed.as_nanos() as f64 / total_dispatched as f64;
    let snap = store.snapshot();

    assert_eq!(
        snap.total_requests(),
        total_dispatched,
        "Sum of all requests across 36 slots must equal total dispatched!"
    );
    assert_eq!(
        snap.total_active_streams(),
        0,
        "Active streams gauge across all 36 slots must be 0!"
    );

    // Verify all 36 pipelines received traffic
    for (idx, p) in snap.pipelines.iter().enumerate() {
        assert!(
            p.total_requests() > 0,
            "Slot {idx} should have received traffic!"
        );
    }

    println!(
        "| **32** | {} ops | {:.3} s | **{:.2} ns** | **{}** | All 36 slots matched & 0 active | **PASS** |",
        total_dispatched,
        elapsed.as_secs_f64(),
        ns_op,
        format_throughput(total_dispatched, elapsed)
    );
    println!();
}

fn main() {
    println!("================================================================================");
    println!("Velda Observability — Chaos & Concurrency Interleaving Benchmark Suite");
    println!("================================================================================\n");

    bench_concurrent_reader_writer_chaos();
    bench_erratic_pool_lifecycle_chaos();
    bench_all_36_pipelines_permutation_flurry();

    println!("Chaos benchmark suite completed successfully.");
}
