//! Multi-Thread Performance, Concurrency Scaling & Contention Benchmark Suite for velda-sync.
//!
//! Evaluates concurrency invariants under multi-threaded worker pressure:
//! 1. Parallel Domain Compilation (Routes, Upstreams, Listeners concurrently across worker threads)
//! 2. Concurrent SHA-256 Payload Integrity Verification under multi-worker fan-out
//! 3. Concurrent SyncMetrics In-Memory Atomic Contention across escalating thread counts

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use common::{
    CountingAllocator, format_duration, generate_listeners_workload, generate_routes_workload,
    generate_upstreams_workload,
};
use sha2::{Digest, Sha256};
use velda_sync::SyncOutcome;
use velda_sync::post_sync::listener::{compile_listeners_to_binary, parse_listeners};
use velda_sync::post_sync::route::{compile_routes_to_binary, parse_routes};
use velda_sync::post_sync::upstream::{compile_upstreams_to_binary, parse_upstreams};
use velda_sync::provider::metrics::SyncMetrics;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// 1. Parallel Domain Compilation Concurrency Scaling
// ============================================================================

fn bench_parallel_domain_compilation() {
    println!("### 1. Parallel Domain Compilation Concurrency Scaling\n");
    println!("> Evaluating concurrent compilation of Routes, Upstreams, and Listeners...\n");

    let (_routes, routes_json, _) = generate_routes_workload(500);
    let (_upstreams, upstreams_json, _) = generate_upstreams_workload(200);
    let (_listeners, listeners_json, _) = generate_listeners_workload(100);

    let routes_json = Arc::new(routes_json);
    let upstreams_json = Arc::new(upstreams_json);
    let listeners_json = Arc::new(listeners_json);

    let thread_counts = [1, 2, 4, 8, 16];
    let total_compilations = 1600;

    println!(
        "| {:<14} | {:<18} | {:<16} | {:<16} | {:<14} |",
        "Worker Threads", "Total Compilations", "Elapsed Time", "Throughput", "Speedup"
    );
    println!(
        "|{:-<16}|{:-<20}|{:-<18}|{:-<18}|{:-<16}|",
        "", "", "", "", ""
    );

    let mut baseline_secs = 1.0;

    for &num_threads in &thread_counts {
        let per_thread = total_compilations / num_threads;

        let start = Instant::now();
        let mut handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let r_json = Arc::clone(&routes_json);
            let u_json = Arc::clone(&upstreams_json);
            let l_json = Arc::clone(&listeners_json);

            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    match i % 3 {
                        0 => {
                            let parsed = parse_routes(&r_json).unwrap();
                            let bin = compile_routes_to_binary(&parsed, 1, [0u8; 32]).unwrap();
                            std::hint::black_box(bin);
                        }
                        1 => {
                            let parsed = parse_upstreams(&u_json).unwrap();
                            let bin = compile_upstreams_to_binary(&parsed, 1, [0u8; 32]).unwrap();
                            std::hint::black_box(bin);
                        }
                        _ => {
                            let parsed = parse_listeners(&l_json).unwrap();
                            let bin = compile_listeners_to_binary(&parsed, 1, [0u8; 32]).unwrap();
                            std::hint::black_box(bin);
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        let throughput = (total_compilations as f64 / secs) as u64;

        if num_threads == 1 {
            baseline_secs = secs;
        }
        let speedup = baseline_secs / secs;

        println!(
            "| {:<14} | {:<18} | {:<16} | {:<16} | {:<13.2}x |",
            num_threads,
            total_compilations,
            format_duration(elapsed),
            format!("{throughput} ops/s"),
            speedup,
        );
    }
    println!();
}

// ============================================================================
// 2. Concurrent SHA-256 Payload Verification Fan-out
// ============================================================================

fn bench_concurrent_sha256_fanout() {
    println!("### 2. Concurrent SHA-256 Payload Integrity Fan-out\n");

    let payload_size = 512 * 1024; // 512 KB per payload
    let payload = Arc::new(vec![0x7au8; payload_size]);
    let thread_counts = [1, 2, 4, 8, 16];
    let total_hashes = 2000;

    println!(
        "| {:<14} | {:<16} | {:<16} | {:<16} | {:<14} |",
        "Worker Threads", "Total Payloads", "Elapsed Time", "Throughput", "Data Rate"
    );
    println!(
        "|{:-<16}|{:-<18}|{:-<18}|{:-<18}|{:-<16}|",
        "", "", "", "", ""
    );

    for &num_threads in &thread_counts {
        let per_thread = total_hashes / num_threads;
        let start = Instant::now();
        let mut handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let p = Arc::clone(&payload);
            handles.push(thread::spawn(move || {
                for _ in 0..per_thread {
                    let mut hasher = Sha256::new();
                    hasher.update(&*p);
                    let res = hasher.finalize();
                    std::hint::black_box(res);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        let throughput = (total_hashes as f64 / secs) as u64;
        let total_mb = (total_hashes * payload_size) as f64 / (1024.0 * 1024.0);
        let mb_per_sec = total_mb / secs;

        println!(
            "| {:<14} | {:<16} | {:<16} | {:<16} | {:<14} |",
            num_threads,
            total_hashes,
            format_duration(elapsed),
            format!("{throughput} hashes/s"),
            format!("{mb_per_sec:.2} MB/s"),
        );
    }
    println!();
}

// ============================================================================
// 3. Concurrent Metrics Atomic Contention Stress
// ============================================================================

fn bench_concurrent_metrics_contention() {
    println!("### 3. Concurrent SyncMetrics In-Memory Contention Stress\n");

    let metrics = Arc::new(SyncMetrics::new());
    let outcome = SyncOutcome::Updated {
        changed_domains: vec!["routes".into(), "listeners".into()],
    };
    let cycle_dur = Duration::from_micros(800);
    let total_updates = 2_000_000;
    let thread_counts = [1, 2, 4, 8, 16];

    println!(
        "| {:<14} | {:<16} | {:<16} | {:<18} |",
        "Worker Threads", "Total Updates", "Elapsed Time", "Update Throughput"
    );
    println!("|{:-<16}|{:-<18}|{:-<18}|{:-<20}|", "", "", "", "");

    for &num_threads in &thread_counts {
        let per_thread = total_updates / num_threads;
        let start = Instant::now();
        let mut handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let m = Arc::clone(&metrics);
            let out = outcome.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..per_thread {
                    m.record_cycle_success(&out, cycle_dur);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let throughput = (total_updates as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| {:<14} | {:<16} | {:<16} | {:<18} |",
            num_threads,
            total_updates,
            format_duration(elapsed),
            format!("{throughput} ops/s"),
        );
    }
    println!();
}

fn main() {
    println!("\n================================================================================");
    println!("  VELDA-SYNC: MULTI-THREAD PERFORMANCE & CONCURRENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_parallel_domain_compilation();
    bench_concurrent_sha256_fanout();
    bench_concurrent_metrics_contention();

    println!("================================================================================\n");
}
