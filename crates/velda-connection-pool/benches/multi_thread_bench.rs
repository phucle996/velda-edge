//! Multi-Thread & Multi-Core Concurrency Benchmark Suite for velda-connection-pool.
//!
//! Stages:
//! 1. Concurrency Scaling across Worker Threads (1 .. 256 Workers)
//! 2. Shard Lock Contention Stress (Single-Key Hotspot vs Distributed Multi-Key)
//! 3. Realistic Edge Gateway Workload (Concurrent High-Throughput Checkouts + Periodic Sweeps)

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{BenchResource, format_duration};
use velda_connection_pool::{ConnectionKey, ConnectionProfile, PoolManager};

// ============================================================================
// Stage 1: Worker Thread Concurrency Scaling (1 .. 256 Workers)
// ============================================================================

fn bench_worker_concurrency_scaling() {
    println!("### 1. Multi-Core Concurrency Scaling (1 .. 256 Worker Threads)\n");
    println!(
        "| {:<8} | {:<14} | {:<14} | {:<16} | {:<14} | {:<14} | {:<12} | {:<18} |",
        "Workers",
        "Total Requests",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Latency",
        "Scaling Factor",
        "Reuse Ratio",
        "Handshakes Saved"
    );
    println!(
        "|{:-<10}|{:-<16}|{:-<16}|{:-<18}|{:-<16}|{:-<16}|{:-<14}|{:-<20}|",
        "", "", "", "", "", "", "", ""
    );

    let worker_counts = [1, 2, 4, 8, 12, 16, 32, 64, 128, 256];
    let iters_per_worker = 100_000;
    let mut baseline_throughput = 1.0;

    for (idx, &workers) in worker_counts.iter().enumerate() {
        let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::with_workers(
            workers,
        ));
        let total_requests = workers * iters_per_worker;

        // Prepopulate connections across 128 distinct endpoints distributed across all shards
        for ep_idx in 0..128 {
            let port = 8000 + ep_idx as u16;
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let key = ConnectionKey::tcp(addr);
            pool.release(&key, BenchResource::new(addr), true, false);
        }

        let start = Instant::now();
        let mut handles = Vec::with_capacity(workers);

        for worker_id in 0..workers {
            let p = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                for i in 0..iters_per_worker {
                    // Distribute across keys to exercise sharded locks
                    let ep_idx = (worker_id * 7 + (i % 32)) % 128;
                    let port = 8000 + ep_idx as u16;
                    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                    let key = ConnectionKey::tcp(addr);

                    if let Some(conn) = p.acquire(&key, Duration::from_secs(60)) {
                        p.release(&key, conn, true, false);
                    } else {
                        p.release(&key, BenchResource::new(addr), true, false);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let throughput = total_requests as f64 / elapsed.as_secs_f64();
        let avg_latency = elapsed / (total_requests as u32);
        let stats = pool.stats();
        let reuse_ratio = stats.hit_ratio() * 100.0;
        let handshakes_saved = stats.hits;

        if idx == 0 {
            baseline_throughput = throughput;
        }
        let scaling_factor = throughput / baseline_throughput;

        println!(
            "| {:<8} | {:<14} | {:<14} | {:<16} | {:<14} | {:<14.2}x | {:<12} | {:<18} |",
            workers,
            total_requests,
            format_duration(elapsed),
            format!("{:.0} ops/s", throughput),
            format_duration(avg_latency),
            scaling_factor,
            format!("{:.1}%", reuse_ratio),
            format!("{handshakes_saved}")
        );
    }
    println!();
}

// ============================================================================
// Stage 2: Shard Lock Contention Stress (Single-Key Hotspot vs Multi-Key)
// ============================================================================

fn bench_lock_contention_hotspot() {
    println!("### 2. Shard Lock Contention: Single-Key Hotspot vs Distributed Keys\n");

    let workers = 64;
    let iters_per_worker = 50_000;
    let total_requests = workers * iters_per_worker;

    println!(
        "| {:<28} | {:<14} | {:<14} | {:<16} | {:<14} | {:<12} | {:<18} |",
        "Workload Pattern",
        "Total Requests",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Latency",
        "Reuse Ratio",
        "Handshakes Saved"
    );
    println!(
        "|{:-<30}|{:-<16}|{:-<16}|{:-<18}|{:-<16}|{:-<14}|{:-<20}|",
        "", "", "", "", "", "", ""
    );

    // Scenario A: Distributed across 128 keys (contention distributed across shards)
    {
        let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::with_workers(
            workers,
        ));
        for ep_idx in 0..128 {
            let port = 8000 + ep_idx as u16;
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let key = ConnectionKey::tcp(addr);
            pool.release(&key, BenchResource::new(addr), true, false);
        }

        let start = Instant::now();
        let mut handles = Vec::with_capacity(workers);

        for worker_id in 0..workers {
            let p = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                for i in 0..iters_per_worker {
                    let ep_idx = (worker_id * 7 + (i % 32)) % 128;
                    let port = 8000 + ep_idx as u16;
                    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                    let key = ConnectionKey::tcp(addr);

                    if let Some(conn) = p.acquire(&key, Duration::from_secs(60)) {
                        p.release(&key, conn, true, false);
                    } else {
                        p.release(&key, BenchResource::new(addr), true, false);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let throughput = total_requests as f64 / elapsed.as_secs_f64();
        let avg_lat = elapsed / (total_requests as u32);
        let stats = pool.stats();

        println!(
            "| {:<28} | {:<14} | {:<14} | {:<16} | {:<14} | {:<12} | {:<18} |",
            "Distributed (128 Keys)",
            total_requests,
            format_duration(elapsed),
            format!("{:.0} ops/s", throughput),
            format_duration(avg_lat),
            format!("{:.1}%", stats.hit_ratio() * 100.0),
            format!("{}", stats.hits)
        );
    }

    // Scenario B: Hotspot on 1 single key (all 64 threads contend on 1 single Mutex shard)
    {
        let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::with_workers(
            workers,
        ));
        let hot_addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let hot_key = ConnectionKey::tcp(hot_addr);

        for _ in 0..64 {
            pool.release(&hot_key, BenchResource::new(hot_addr), true, false);
        }

        let start = Instant::now();
        let mut handles = Vec::with_capacity(workers);

        for _ in 0..workers {
            let p = Arc::clone(&pool);
            let k = hot_key.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..iters_per_worker {
                    if let Some(conn) = p.acquire(&k, Duration::from_secs(60)) {
                        p.release(&k, conn, true, false);
                    } else {
                        p.release(&k, BenchResource::new(hot_addr), true, false);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let throughput = total_requests as f64 / elapsed.as_secs_f64();
        let avg_lat = elapsed / (total_requests as u32);
        let stats = pool.stats();

        println!(
            "| {:<28} | {:<14} | {:<14} | {:<16} | {:<14} | {:<12} | {:<18} |",
            "Hotspot Contention (1 Key)",
            total_requests,
            format_duration(elapsed),
            format!("{:.0} ops/s", throughput),
            format_duration(avg_lat),
            format!("{:.1}%", stats.hit_ratio() * 100.0),
            format!("{}", stats.hits)
        );
    }
    println!();
}

// ============================================================================
// Stage 3: Realistic Workload: Checkouts + Simultaneous Sweeping
// ============================================================================

fn bench_concurrent_traffic_with_background_sweeps() {
    println!("### 3. Realistic Workload: Concurrent Traffic + Periodic Background Sweeping\n");

    let workers = 32;
    let duration = Duration::from_secs(3);
    let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::with_workers(
        workers,
    ));
    let stop_signal = Arc::new(AtomicBool::new(false));

    // Seed pool with 256 endpoints
    for i in 0..256 {
        let port = 8000 + (i as u16);
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        pool.release(&key, BenchResource::new(addr), true, false);
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers + 1);

    // 1. Spawn 32 worker threads performing high-speed checkout & returns
    let ops_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    for worker_id in 0..workers {
        let p = Arc::clone(&pool);
        let stop = Arc::clone(&stop_signal);
        let ops = Arc::clone(&ops_counter);

        handles.push(std::thread::spawn(move || {
            let mut local_ops = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let ep_idx = (worker_id * 13 + (local_ops as usize % 64)) % 256;
                let port = 8000 + (ep_idx as u16);
                let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                let key = ConnectionKey::tcp(addr);

                if let Some(conn) = p.acquire(&key, Duration::from_secs(60)) {
                    p.release(&key, conn, true, false);
                } else {
                    p.release(&key, BenchResource::new(addr), true, false);
                }
                local_ops += 1;
            }
            ops.fetch_add(local_ops, Ordering::Relaxed);
        }));
    }

    // 2. Spawn 1 background maintenance thread sweeping evictions and pruning empty pools
    let p_maint = Arc::clone(&pool);
    let stop_maint = Arc::clone(&stop_signal);
    let sweep_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sweep_count_clone = Arc::clone(&sweep_count);

    let maint_handle = std::thread::spawn(move || {
        let mut count = 0;
        while !stop_maint.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(10));
            let _ = p_maint.evict_expired(Duration::from_secs(30));
            let _ = p_maint.prune_empty_pools(Duration::from_secs(60));
            count += 1;
        }
        sweep_count_clone.store(count, Ordering::Relaxed);
    });

    // Let benchmark run for specified duration
    std::thread::sleep(duration);
    stop_signal.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }
    maint_handle.join().unwrap();

    let elapsed = start.elapsed();
    let total_ops = ops_counter.load(Ordering::Relaxed);
    let throughput = total_ops as f64 / elapsed.as_secs_f64();
    let total_sweeps = sweep_count.load(Ordering::Relaxed);
    let stats = pool.stats();
    let reuse_ratio = stats.hit_ratio() * 100.0;
    let handshakes_saved = stats.hits;

    println!(
        "| Workers | Duration | Total Operations | Throughput | Reuse Ratio | Handshakes Saved | Background Sweeps | Lock Contention |"
    );
    println!("|:---|:---|:---|:---|:---|:---|:---|:---|");
    println!(
        "| **{}** | {:.2} s | {} | **{:.0} ops/s** | **{:.2}%** | **{} handshakes** | {} sweeps | **Zero Deadlocks** |",
        workers,
        elapsed.as_secs_f64(),
        total_ops,
        throughput,
        reuse_ratio,
        handshakes_saved,
        total_sweeps
    );
    println!();
}

// ============================================================================
// Stage 4: Multiplexed Concurrent Stream Sharing (HTTP/2 1:N Sharing)
// ============================================================================

fn bench_multiplexed_stream_concurrency() {
    println!("### 4. Multiplexed Stream Sharing Concurrency (HTTP/2 1:N Sharing)\n");

    let workers = 64;
    let iters_per_worker = 100_000;
    let total_requests = workers * iters_per_worker;

    let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::with_workers(
        workers,
    ));
    let addr: SocketAddr = "10.0.0.1:443".parse().unwrap();
    let key = ConnectionKey::http(addr, "h2", Some("api.velda.io".into()), Some("h2".into()));
    let profile = ConnectionProfile::multiplexed(key.clone(), Duration::from_secs(60), 10_000);

    // Register 1 physical connection for 64 concurrent threads
    let _conn = pool.register_profile(&profile, BenchResource::new(addr));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let p = Arc::clone(&pool);
        let prof = profile.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..iters_per_worker {
                if let Some(stream) = p.acquire_profile(&prof) {
                    std::hint::black_box(&*stream);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();
    let avg_lat = elapsed / (total_requests as u32);

    println!(
        "| {:<36} | {:<14} | {:<14} | {:<16} | {:<14} | {:<20} |",
        "Architecture",
        "Total Streams",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Latency",
        "Physical Sockets"
    );
    println!(
        "|{:-<38}|{:-<16}|{:-<16}|{:-<18}|{:-<16}|{:-<22}|",
        "", "", "", "", "", ""
    );

    println!(
        "| {:<36} | {:<14} | {:<14} | {:<16} | {:<14} | {:<20} |",
        "HTTP/2 Multiplexed (Atomic Streams)",
        total_requests,
        format_duration(elapsed),
        format!("{:.0} ops/s", throughput),
        format_duration(avg_lat),
        "1 connection (Shared)"
    );
    println!();
}

fn main() {
    println!("# ===================================================================");
    println!("# Velda Connection Pool: Multi-Thread & Concurrency Benchmark Suite");
    println!("# ===================================================================\n");

    bench_worker_concurrency_scaling();
    bench_lock_contention_hotspot();
    bench_concurrent_traffic_with_background_sweeps();
    bench_multiplexed_stream_concurrency();

    println!("# Multi-thread connection pool benchmarks completed successfully.\n");
}
