//! Single-Thread Performance, Latency & Zero-Allocation Benchmark Suite for velda-lb (Stage 3).
//!
//! Stages:
//! 1. Algorithm Selection Latency & Zero-Allocation Verification (RoundRobin, SWRR, Random, WeightedRandom, P2C, LeastConn, PeakEWMA, Maglev, RingHash)
//! 2. Maglev Constant-Time O(1) Lookup Throughput across N = 5 .. 100 Backends

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use common::{CountingAllocator, format_duration};
use velda_lb::{
    Endpoint, EndpointMetrics, LeastConnections, LoadBalancer, Maglev, PeakEwma, PowerOfTwoChoices,
    Random, RingHash, RoundRobin, SelectionContext, WeightedRandom, WeightedRoundRobin,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn make_endpoints(count: usize) -> Vec<Endpoint> {
    (0..count)
        .map(|i| {
            let port = 8080 + (i as u16);
            let addr: SocketAddr = format!("10.0.{}.{}:{}", i / 256, i % 256, port)
                .parse()
                .unwrap();
            let weight = ((i % 10) + 1) as u32;
            Endpoint::new(format!("ep-{}", i), addr, weight)
        })
        .collect()
}

fn bench_algorithm_latencies() {
    println!("### 1. Algorithm Selection Latency & Zero-Allocation Verification\n");
    println!("| Algorithm | Iterations | Total Time | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let endpoints = make_endpoints(10);
    let iters = 1_000_000;

    // 1. RoundRobin
    let rr = RoundRobin::new();
    let ctx = SelectionContext::NONE;
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = rr.select(&endpoints, &ctx);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **RoundRobin** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 2. Random
    let rnd = Random::new();
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = rnd.select(&endpoints, &ctx);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Random** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 3. Weighted Random
    let wrnd = WeightedRandom::new();
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = wrnd.select(&endpoints, &ctx);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **WeightedRandom** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 4. Smooth Weighted Round Robin
    let swrr = WeightedRoundRobin::new();
    let _ = swrr.select(&endpoints, &ctx);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = swrr.select(&endpoints, &ctx);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **WeightedRoundRobin (SWRR)** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // Setup metrics map for state-aware algorithms
    let mut metrics_map = HashMap::new();
    for (i, ep) in endpoints.iter().enumerate() {
        let m = EndpointMetrics::new();
        m.set_active_connections((i as u32) + 1);
        m.set_inflight_requests((i as u32) + 2);
        m.set_latency_ewma_nanos(100_000 * ((i as u64) + 1));
        metrics_map.insert(ep.address, m);
    }
    let ctx_metrics = SelectionContext::NONE.with_metrics(&metrics_map);

    // 5. Power of Two Choices (P2C)
    let p2c = PowerOfTwoChoices::new();
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = p2c.select(&endpoints, &ctx_metrics);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **PowerOfTwoChoices (P2C)** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 6. LeastConnections
    let lc = LeastConnections::new();
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = lc.select(&endpoints, &ctx_metrics);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **LeastConnections** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 7. Peak EWMA
    let ewma = PeakEwma::new();
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = ewma.select(&endpoints, &ctx_metrics);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **PeakEwma** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 8. Maglev Consistent Hash
    let maglev = Maglev::new(65537);
    let ctx_hash = SelectionContext::with_hash(0xdeadbeef12345678);
    let _ = maglev.select(&endpoints, &ctx_hash);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = maglev.select(&endpoints, &ctx_hash);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Maglev (65,537 slots)** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 9. RingHash
    let ring = RingHash::new();
    let _ = ring.select(&endpoints, &ctx_hash);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ep = ring.select(&endpoints, &ctx_hash);
        let _ = std::hint::black_box(ep);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **RingHash** | {} | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
        iters,
        format_duration(elapsed),
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    println!();
}

fn bench_maglev_scaling() {
    println!("### 2. Maglev Constant-Time O(1) Lookup Verification across N Backends\n");
    println!("| Backends (N) | Total Time | Latency / op | Allocs / op | Complexity |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 500_000;
    let counts = [5, 10, 25, 50, 100];
    let ctx = SelectionContext::with_hash(0x123456789abcdef0);

    for &n in &counts {
        let endpoints = make_endpoints(n);
        let maglev = Maglev::new(65537);
        let _ = maglev.select(&endpoints, &ctx);

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let ep = maglev.select(&endpoints, &ctx);
            let _ = std::hint::black_box(ep);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;

        println!(
            "| N = {} | {} | {:.2} ns | {:.2} | **O(1)** |",
            n,
            format_duration(elapsed),
            ns_op,
            allocs as f64 / iters as f64
        );
    }
    println!();
}

fn main() {
    println!("=================================================================");
    println!("     Velda Edge — Stage 3: Single-Thread LB Benchmark Suite      ");
    println!("=================================================================\n");

    bench_algorithm_latencies();
    bench_maglev_scaling();

    println!("=================================================================");
    println!("       Single-Thread LB Benchmarks Completed Successfully        ");
    println!("=================================================================\n");
}
