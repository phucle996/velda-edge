//! Protocol Profile & Reuse Mode Benchmark Suite for velda-connection-pool.
//!
//! Stages:
//! 1. Protocol Profile Direct Comparison (Exclusive vs Sequential vs Multiplexed)
//! 2. Concurrency Scalability: 1:1 Sequential Checkout vs 1:N Stream Multiplexing
//! 3. Stream Slot Contention & Rapid Saturation Recovery
//! 4. GOAWAY Graceful Invalidation & Failover Latency

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{BenchResource, CountingAllocator, format_duration};
use velda_connection_pool::{ConnectionKey, ConnectionProfile, PoolManager};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: Profile Direct Comparison (Exclusive vs Sequential vs Multiplexed)
// ============================================================================

fn bench_profile_direct_comparison() {
    println!("### 1. ConnectionProfile Comparison: Exclusive vs Sequential vs Multiplexed\n");

    let iters = 500_000;
    let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::new());

    // Setup 3 profiles
    let addr_seq: SocketAddr = "10.0.1.1:80".parse().unwrap();
    let key_seq = ConnectionKey::http(addr_seq, "http/1.1", None, None);
    let prof_seq = ConnectionProfile::sequential(key_seq.clone(), Duration::from_secs(60));
    pool.register_profile(&prof_seq, BenchResource::new(addr_seq))
        .release(true);

    let addr_excl: SocketAddr = "10.0.2.1:9000".parse().unwrap();
    let key_excl = ConnectionKey::tcp(addr_excl);
    let prof_excl = ConnectionProfile::exclusive(key_excl.clone(), Duration::from_secs(60));
    pool.register_profile(&prof_excl, BenchResource::new(addr_excl))
        .release(true);

    let addr_mux: SocketAddr = "10.0.3.1:443".parse().unwrap();
    let key_mux = ConnectionKey::http(
        addr_mux,
        "h2",
        Some("api.velda.io".into()),
        Some("h2".into()),
    );
    let prof_mux = ConnectionProfile::multiplexed(key_mux.clone(), Duration::from_secs(60), 10_000);
    let _conn_mux = pool.register_profile(&prof_mux, BenchResource::new(addr_mux));

    // A. Sequential (HTTP/1.1 Keep-Alive)
    ALLOCATOR.reset();
    let start_seq = Instant::now();
    for _ in 0..iters {
        if let Some(lease) = pool.acquire_profile(&prof_seq) {
            std::hint::black_box(&*lease);
        }
    }
    let elapsed_seq = start_seq.elapsed();
    let (allocs_seq, _) = ALLOCATOR.snapshot();

    // B. Exclusive (Raw TCP / L4 Tunnel)
    ALLOCATOR.reset();
    let start_excl = Instant::now();
    for _ in 0..iters {
        if let Some(lease) = pool.acquire_profile(&prof_excl) {
            std::hint::black_box(&*lease);
            lease.release(true);
        }
    }
    let elapsed_excl = start_excl.elapsed();
    let (allocs_excl, _) = ALLOCATOR.snapshot();

    // C. Multiplexed (HTTP/2 Stream Slot)
    ALLOCATOR.reset();
    let start_mux = Instant::now();
    for _ in 0..iters {
        if let Some(stream) = pool.acquire_profile(&prof_mux) {
            std::hint::black_box(&*stream);
        }
    }
    let elapsed_mux = start_mux.elapsed();
    let (allocs_mux, _) = ALLOCATOR.snapshot();

    println!(
        "| {:<28} | {:<12} | {:<14} | {:<16} | {:<12} |",
        "Profile / Reuse Mode", "Iterations", "Latency / Op", "Throughput", "Heap Allocs"
    );
    println!(
        "|{:-<30}|{:-<14}|{:-<16}|{:-<18}|{:-<14}|",
        "", "", "", "", ""
    );

    let print_row = |name: &str, elapsed: Duration, allocs: u64| {
        let lat = elapsed / (iters as u32);
        let ops = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| {:<28} | {:<12} | {:<14} | {:<16} | {:<12} |",
            name,
            iters,
            format_duration(lat),
            format!("{ops} ops/s"),
            format!("{allocs} B")
        );
    };

    print_row("Sequential (HTTP/1.1)", elapsed_seq, allocs_seq);
    print_row("Exclusive (Raw TCP)", elapsed_excl, allocs_excl);
    print_row("Multiplexed (HTTP/2)", elapsed_mux, allocs_mux);
    println!();
}

// ============================================================================
// Stage 2: Concurrency Scalability: 1:1 vs 1:N Multiplexing
// ============================================================================

fn bench_concurrency_sequential_vs_multiplexed() {
    println!("### 2. High-Concurrency Stress: 1:1 Sequential Checkout vs 1:N Multiplexing\n");

    let thread_counts = [4, 16, 64, 128];
    let iters_per_thread = 50_000;

    println!(
        "| {:<8} | {:<24} | {:<14} | {:<16} | {:<14} | {:<16} |",
        "Threads", "Architecture", "Total Requests", "Throughput", "Avg Latency", "Sockets In Use"
    );
    println!(
        "|{:-<10}|{:-<26}|{:-<16}|{:-<18}|{:-<16}|{:-<18}|",
        "", "", "", "", "", ""
    );

    for &threads in &thread_counts {
        let total_requests = threads * iters_per_thread;

        // --- Scenario 1: Sequential HTTP/1.1 ---
        {
            let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::with_workers(
                threads,
            ));
            let addr: SocketAddr = "10.0.1.1:80".parse().unwrap();
            let key = ConnectionKey::tcp(addr);
            let profile = ConnectionProfile::sequential(key, Duration::from_secs(60));

            // Pre-seed connections for concurrent threads
            for _ in 0..threads {
                pool.register_profile(&profile, BenchResource::new(addr))
                    .release(true);
            }

            let start = Instant::now();
            let mut handles = Vec::with_capacity(threads);

            for _ in 0..threads {
                let p = Arc::clone(&pool);
                let prof = profile.clone();
                handles.push(std::thread::spawn(move || {
                    for _ in 0..iters_per_thread {
                        if let Some(lease) = p.acquire_profile(&prof) {
                            std::hint::black_box(&*lease);
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
                "| {:<8} | {:<24} | {:<14} | {:<16} | {:<14} | {:<16} |",
                threads,
                "Sequential (HTTP/1.1)",
                total_requests,
                format!("{:.0} ops/s", throughput),
                format_duration(avg_lat),
                format!("{threads} sockets")
            );
        }

        // --- Scenario 2: Multiplexed HTTP/2 ---
        {
            let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::with_workers(
                threads,
            ));
            let addr: SocketAddr = "10.0.2.1:443".parse().unwrap();
            let key =
                ConnectionKey::http(addr, "h2", Some("api.velda.io".into()), Some("h2".into()));
            let profile = ConnectionProfile::multiplexed(key, Duration::from_secs(60), 10_000);

            // Exactly 1 physical connection shared across all threads
            let _conn = pool.register_profile(&profile, BenchResource::new(addr));

            let start = Instant::now();
            let mut handles = Vec::with_capacity(threads);

            for _ in 0..threads {
                let p = Arc::clone(&pool);
                let prof = profile.clone();
                handles.push(std::thread::spawn(move || {
                    for _ in 0..iters_per_thread {
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
                "| {:<8} | {:<24} | {:<14} | {:<16} | {:<14} | {:<16} |",
                threads,
                "Multiplexed (HTTP/2)",
                total_requests,
                format!("{:.0} ops/s", throughput),
                format_duration(avg_lat),
                "1 socket (Shared)"
            );
        }
    }
    println!();
}

// ============================================================================
// Stage 3: Multiplexed Stream Saturation & Recovery
// ============================================================================

fn bench_stream_saturation_recovery() {
    println!("### 3. Multiplexed Stream Saturation & Recovery\n");

    let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::new());
    let addr: SocketAddr = "10.0.3.1:443".parse().unwrap();
    let key = ConnectionKey::http(addr, "h2", Some("api.velda.io".into()), Some("h2".into()));
    let max_streams = 128;
    let profile = ConnectionProfile::multiplexed(key.clone(), Duration::from_secs(60), max_streams);

    let _conn = pool.register_profile(&profile, BenchResource::new(addr));

    let cycles = 10_000;
    let start = Instant::now();

    for _ in 0..cycles {
        // 1. Borrow up to capacity
        let mut active_leases = Vec::with_capacity(max_streams as usize);
        for _ in 0..max_streams {
            if let Some(stream) = pool.acquire_profile(&profile) {
                active_leases.push(stream);
            }
        }

        // 2. Next acquire should be saturated (None)
        let saturated = pool.acquire_profile(&profile);
        assert!(saturated.is_none());

        // 3. Drop all leases to recover capacity
        drop(active_leases);

        // 4. Verify immediately acquirable again
        let recovered = pool.acquire_profile(&profile);
        assert!(recovered.is_some());
    }

    let elapsed = start.elapsed();
    let total_operations = cycles * (max_streams as u64 + 2);
    let throughput = total_operations as f64 / elapsed.as_secs_f64();
    let avg_cycle_time = elapsed / cycles as u32;

    println!(
        "| {:<20} | {:<16} | {:<16} | {:<16} | {:<18} |",
        "Stream Capacity", "Saturation Cycles", "Total Operations", "Avg Cycle Time", "Throughput"
    );
    println!(
        "|{:-<22}|{:-<18}|{:-<18}|{:-<18}|{:-<20}|",
        "", "", "", "", ""
    );
    println!(
        "| {:<20} | {:<16} | {:<16} | {:<16} | {:<18} |",
        format!("{max_streams} streams"),
        cycles,
        total_operations,
        format_duration(avg_cycle_time),
        format!("{:.0} ops/s", throughput)
    );
    println!();
}

fn main() {
    println!("# ===================================================================");
    println!("# Velda Connection Pool: Protocol Profile & Reuse Benchmark Suite");
    println!("# ===================================================================\n");

    bench_profile_direct_comparison();
    bench_concurrency_sequential_vs_multiplexed();
    bench_stream_saturation_recovery();

    println!("# Profile benchmarks completed successfully.\n");
}
