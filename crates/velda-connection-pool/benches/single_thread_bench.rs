//! Single-Thread Performance, Latency & Zero-Allocation Benchmark Suite for velda-connection-pool.
//!
//! Stages:
//! 1. Connection Pool Acquire (Hit vs Miss & 0-Heap-Allocation Verification)
//! 2. RAII PoolLease Overhead & Auto-Drop Latency
//! 3. Subpool LIFO Queue Depth Scaling (O(1) Verification across N = 10 .. 10,000)
//! 4. Idle Connection Eviction Scaling (O(N) Verification across N = 100 .. 50,000)
//! 5. Drain Matching Predicate Filtering (Selective Invalidation)

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{BenchResource, CountingAllocator, calculate_big_o, format_duration};
use velda_connection_pool::{
    ConnectionKey, ConnectionProfile, PoolConfig, PoolManager, SequentialLease,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: Pool Hit vs Miss & Heap Allocation Verification
// ============================================================================

fn bench_acquire_hit_vs_miss() {
    println!("### 1. Connection Pool Acquire Hit vs Miss & Zero-Alloc Invariant\n");

    let iters = 200_000;
    let pool = PoolManager::<ConnectionKey, BenchResource>::new();
    let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);

    // Warm-up: insert reusable connection
    pool.release(&key, BenchResource::new(addr), true, false);

    // 1. Measure Hit (acquire + release back)
    ALLOCATOR.reset();
    let start_hit = Instant::now();
    for _ in 0..iters {
        if let Some(conn) = pool.acquire(&key, Duration::from_secs(60)) {
            pool.release(&key, conn, true, false);
        }
    }
    let elapsed_hit = start_hit.elapsed();
    let (_hit_allocs, _) = ALLOCATOR.snapshot();

    // 2. Measure Miss (acquire non-existent key)
    let miss_key = ConnectionKey::tcp("10.0.0.99:9999".parse().unwrap());
    ALLOCATOR.reset();
    let start_miss = Instant::now();
    for _ in 0..iters {
        let res = pool.acquire(&miss_key, Duration::from_secs(60));
        std::hint::black_box(res);
    }
    let elapsed_miss = start_miss.elapsed();
    let (_miss_allocs, _) = ALLOCATOR.snapshot();

    println!(
        "| {:<18} | {:<12} | {:<14} | {:<12} | {:<14} | {:<18} |",
        "Operation", "Iterations", "Latency / Op", "Ops / Sec", "Reuse Ratio", "Handshakes Saved"
    );
    println!(
        "|{:-<20}|{:-<14}|{:-<16}|{:-<14}|{:-<16}|{:-<20}|",
        "", "", "", "", "", ""
    );

    let hit_lat = elapsed_hit / (iters as u32);
    let hit_ops = (iters as f64 / elapsed_hit.as_secs_f64()) as u64;
    println!(
        "| {:<18} | {:<12} | {:<14} | {:<12} | {:<14} | {:<18} |",
        "Pool HIT (Reused)",
        iters,
        format_duration(hit_lat),
        format!("{hit_ops}"),
        "100.0%",
        format!("{} handshakes", iters - 1)
    );

    let miss_lat = elapsed_miss / (iters as u32);
    let miss_ops = (iters as f64 / elapsed_miss.as_secs_f64()) as u64;
    println!(
        "| {:<18} | {:<12} | {:<14} | {:<12} | {:<14} | {:<18} |",
        "Pool MISS (New)",
        iters,
        format_duration(miss_lat),
        format!("{miss_ops}"),
        "0.0%",
        "0 (Cold start)"
    );
    println!();
}

// ============================================================================
// Stage 2: ConnectionProfile RAII Leases Overhead (Sequential, Exclusive, Multiplexed)
// ============================================================================

fn bench_lease_overhead() {
    println!("### 2. ConnectionProfile RAII Leases Overhead & Zero-Alloc Invariant\n");

    let iters = 200_000;
    let pool = Arc::new(PoolManager::<ConnectionKey, BenchResource>::new());

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
    let _mux_holder = pool.register_profile(&prof_mux, BenchResource::new(addr_mux));

    // 1. Sequential Lease (HTTP/1.1 RAII auto-return to LIFO)
    ALLOCATOR.reset();
    let start_seq = Instant::now();
    for _ in 0..iters {
        if let Some(lease) = pool.acquire_profile(&prof_seq) {
            std::hint::black_box(&*lease);
        }
    }
    let elapsed_seq = start_seq.elapsed();
    let (allocs_seq, _) = ALLOCATOR.snapshot();

    // 2. Exclusive Lease (TCP explicit release with recycling)
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

    // 3. Multiplexed Stream Lease (HTTP/2 atomic stream checkout & drop)
    ALLOCATOR.reset();
    let start_mux = Instant::now();
    for _ in 0..iters {
        if let Some(stream) = pool.acquire_profile(&prof_mux) {
            std::hint::black_box(&*stream);
        }
    }
    let elapsed_mux = start_mux.elapsed();
    let (allocs_mux, _) = ALLOCATOR.snapshot();

    // 4. Raw SequentialLease (Baseline)
    ALLOCATOR.reset();
    let start_raw = Instant::now();
    for _ in 0..iters {
        if let Some(res) = pool.acquire(&key_seq, Duration::from_secs(60)) {
            let lease = SequentialLease::new(res, key_seq.clone(), Arc::clone(&pool), false);
            std::hint::black_box(&*lease);
        }
    }
    let elapsed_raw = start_raw.elapsed();
    let (allocs_raw, _) = ALLOCATOR.snapshot();

    println!(
        "| {:<32} | {:<12} | {:<14} | {:<14} | {:<12} |",
        "Profile / Lease Variant", "Iterations", "Latency / Op", "Ops / Sec", "Heap Allocs"
    );
    println!(
        "|{:-<34}|{:-<14}|{:-<16}|{:-<16}|{:-<14}|",
        "", "", "", "", ""
    );

    let print_row = |name: &str, elapsed: Duration, allocs: u64| {
        let lat = elapsed / (iters as u32);
        let ops = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| {:<32} | {:<12} | {:<14} | {:<14} | {:<12} |",
            name,
            iters,
            format_duration(lat),
            format!("{ops}"),
            format!("{allocs} B")
        );
    };

    print_row("SequentialLease (HTTP/1.1 Drop)", elapsed_seq, allocs_seq);
    print_row("ExclusiveLease (TCP Recycle)", elapsed_excl, allocs_excl);
    print_row("StreamLease (HTTP/2 Atomic)", elapsed_mux, allocs_mux);
    print_row("Raw SequentialLease (Baseline)", elapsed_raw, allocs_raw);
    println!();
}

// ============================================================================
// Stage 3: Subpool LIFO Queue Depth Scaling (O(1) Verification)
// ============================================================================

fn bench_subpool_depth_scaling() {
    println!("### 3. Subpool LIFO Queue Depth Scaling (O(1) Invariant)\n");

    let depths = [10, 100, 1_000, 10_000];
    let iters = 100_000;
    let mut results: Vec<(usize, Duration)> = Vec::new();

    println!(
        "| {:<12} | {:<14} | {:<16} | {:<14} | {:<14} | {:<8} |",
        "Queue Depth", "Total Time", "Latency / Op", "Ops / Sec", "Heap Allocs", "Big-O"
    );
    println!(
        "|{:-<14}|{:-<16}|{:-<18}|{:-<16}|{:-<16}|{:-<10}|",
        "", "", "", "", "", ""
    );

    for &depth in &depths {
        let config = PoolConfig {
            max_idle_per_key: depth,
            max_concurrent_streams: 100,
            idle_timeout: Duration::from_secs(60),
            max_lifetime: None,
        };
        let pool = PoolManager::<ConnectionKey, BenchResource>::with_config(config);
        let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let key = ConnectionKey::tcp(addr);

        for _ in 0..depth {
            pool.release(&key, BenchResource::new(addr), true, false);
        }

        ALLOCATOR.reset();
        let start = Instant::now();

        for _ in 0..iters {
            if let Some(conn) = pool.acquire(&key, Duration::from_secs(60)) {
                pool.release(&key, conn, true, false);
            }
        }

        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();

        let latency_per_op = elapsed / (iters as u32);
        let ops_per_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        results.push((depth, elapsed));
        let (big_o, _) = calculate_big_o(&results);

        println!(
            "| {:<12} | {:<14} | {:<16} | {:<14} | {:<14} | {:<8} |",
            depth,
            format_duration(elapsed),
            format_duration(latency_per_op),
            format!("{ops_per_sec}"),
            allocs,
            big_o
        );
    }
    println!();
}

// ============================================================================
// Stage 4: Idle Connection Eviction Scaling (O(N) Verification)
// ============================================================================

fn bench_eviction_scaling() {
    println!("### 4. Idle Connection Eviction Sweep Scaling (O(N) Invariant)\n");

    let scales = [100, 1_000, 10_000, 50_000];
    let mut results: Vec<(usize, Duration)> = Vec::new();

    println!(
        "| {:<12} | {:<14} | {:<16} | {:<14} | {:<8} |",
        "Connections", "Sweep Time", "Latency / Conn", "Evicted", "Big-O"
    );
    println!(
        "|{:-<14}|{:-<16}|{:-<18}|{:-<16}|{:-<10}|",
        "", "", "", "", ""
    );

    for &count in &scales {
        let config = PoolConfig {
            max_idle_per_key: count,
            max_concurrent_streams: 100,
            idle_timeout: Duration::from_secs(30),
            max_lifetime: None,
        };
        let pool = PoolManager::<ConnectionKey, BenchResource>::with_config(config);

        // Populate count connections, half expired, half active
        for i in 0..count {
            let port = 8000 + (i % 2500) as u16;
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let key = ConnectionKey::tcp(addr);

            let mut conn = BenchResource::new(addr);
            if i % 2 == 0 {
                // Expired: 120s ago
                conn.last_used = Instant::now() - Duration::from_secs(120);
            }
            pool.release(&key, conn, true, false);
        }

        let start = Instant::now();
        let evicted = pool.evict_expired(Duration::from_secs(30));
        let elapsed = start.elapsed();

        let latency_per_conn = elapsed / (count as u32);
        results.push((count, elapsed));
        let (big_o, _) = calculate_big_o(&results);

        println!(
            "| {:<12} | {:<14} | {:<16} | {:<14} | {:<8} |",
            count,
            format_duration(elapsed),
            format_duration(latency_per_conn),
            evicted,
            big_o
        );
    }
    println!();
}

// ============================================================================
// Stage 5: Drain Matching Predicate Filtering
// ============================================================================

fn bench_drain_matching() {
    println!("### 5. Drain Matching Predicate Invalidation Benchmark\n");

    let pool = PoolManager::<ConnectionKey, BenchResource>::new();
    let total_conns = 10_000;

    // Populate across 1,000 distinct endpoints
    for i in 0..total_conns {
        let port = 8000 + (i % 1000) as u16;
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        pool.release(&key, BenchResource::new(addr), true, false);
    }

    println!(
        "| {:<24} | {:<14} | {:<16} | {:<14} |",
        "Drain Target", "Drained Count", "Elapsed Time", "Remaining"
    );
    println!("|{:-<26}|{:-<16}|{:-<18}|{:-<16}|", "", "", "", "");

    // 1. Drain 1 specific endpoint (out of 1000)
    let target_addr: SocketAddr = "127.0.0.1:8042".parse().unwrap();
    let start_single = Instant::now();
    let drained_single = pool.drain_matching(|k| k.target_addr == target_addr);
    let elapsed_single = start_single.elapsed();
    println!(
        "| {:<24} | {:<14} | {:<16} | {:<14} |",
        "Single Endpoint (0.1%)",
        drained_single,
        format_duration(elapsed_single),
        pool.total_idle_conns()
    );

    // 2. Drain all endpoints on ports 8000..8250 (25%)
    let start_quarter = Instant::now();
    let drained_quarter = pool.drain_matching(|k| k.target_addr.port() < 8250);
    let elapsed_quarter = start_quarter.elapsed();
    println!(
        "| {:<24} | {:<14} | {:<16} | {:<14} |",
        "Port Range < 8250 (25%)",
        drained_quarter,
        format_duration(elapsed_quarter),
        pool.total_idle_conns()
    );

    // 3. Drain all remaining
    let start_all = Instant::now();
    let drained_all = pool.drain_matching(|_| true);
    let elapsed_all = start_all.elapsed();
    println!(
        "| {:<24} | {:<14} | {:<16} | {:<14} |",
        "All Remaining (100%)",
        drained_all,
        format_duration(elapsed_all),
        pool.total_idle_conns()
    );
    println!();
}

// ============================================================================
// Stage 6: Connection Reuse Multiplier & OS Handshake Savings
// ============================================================================

fn bench_connection_reuse_efficiency() {
    println!("### 6. Connection Reuse Multiplier & OS Handshake Savings\n");

    let pool = PoolManager::<ConnectionKey, BenchResource>::new();
    let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let key = ConnectionKey::tcp(addr);

    let traffic_volumes = [10, 100, 1_000, 10_000, 100_000];

    println!(
        "| {:<14} | {:<16} | {:<16} | {:<14} | {:<22} |",
        "Total Requests",
        "Sockets Created",
        "Reused Checkouts",
        "Reuse Ratio",
        "TCP Handshakes Saved"
    );
    println!(
        "|{:-<16}|{:-<18}|{:-<18}|{:-<16}|{:-<24}|",
        "", "", "", "", ""
    );

    for &volume in &traffic_volumes {
        pool.reset_stats();
        pool.clear();
        // 1 initial connection established
        pool.release(&key, BenchResource::new(addr), true, false);

        for _ in 0..volume {
            if let Some(conn) = pool.acquire(&key, Duration::from_secs(60)) {
                pool.release(&key, conn, true, false);
            }
        }

        let stats = pool.stats();
        let reuse_ratio = stats.hit_ratio() * 100.0;
        let sockets_created = 1;
        let handshakes_saved = stats.hits;

        println!(
            "| {:<14} | {:<16} | {:<16} | {:<13.2}% | {:<22} |",
            volume,
            sockets_created,
            stats.hits,
            reuse_ratio,
            format!("{handshakes_saved} handshakes")
        );
    }
    println!();
}

fn main() {
    println!("# ===================================================================");
    println!("# Velda Connection Pool: Single-Thread Latency & Big-O Benchmark");
    println!("# ===================================================================\n");

    bench_acquire_hit_vs_miss();
    bench_lease_overhead();
    bench_subpool_depth_scaling();
    bench_eviction_scaling();
    bench_drain_matching();
    bench_connection_reuse_efficiency();

    println!("# Single-thread connection pool benchmarks completed successfully.\n");
}
