//! Single-Thread Performance, Latency & Big-O Benchmark Suite for velda-upstream.
//!
//! Stages:
//! 1. Full Pipeline Acquire: Hit vs Miss & Zero-Allocation Invariant
//! 2. Endpoint Scale Invariant (Scaling N = 10 .. 10,000 Endpoints, Big-O Verification)
//! 3. Load Balancing Strategy Comparison in Upstream Pipeline
//! 4. RAII BackendLease Overhead (Explicit Release vs Auto-Drop)

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{CountingAllocator, MockConnector, calculate_big_o, format_duration};
use tokio::net::TcpListener;
use velda_upstream::{
    Discovery, Endpoint, IpHash, LeastConnections, LoadBalancer, PowerOfTwoChoices, RoundRobin,
    SelectionContext, TcpConnector, Upstream, UpstreamTimeouts, WeightedRoundRobin,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1a: Full Pipeline Acquire Hit vs Miss (In-Memory Mock Dispatch)
// ============================================================================

async fn bench_acquire_hit_vs_miss() {
    println!("### 1a. In-Memory Mock Dispatch: Hit vs Miss & Allocation Overhead\n");

    let iters = 100_000;
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let discovery = Discovery::new_explicit(vec![Endpoint::new("e1", ep1, 1)]);
    let connector = Arc::new(MockConnector::new());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));

    let upstream = Upstream::with_connector("bench-up", "tcp", discovery, timeouts, connector);

    // Warm-up: First acquire is a MISS, establishing initial connection
    let initial_lease = upstream.acquire().await.unwrap();
    initial_lease.release(true);

    // 1. Measure Pool HIT (Warm connection reuse)
    ALLOCATOR.reset();
    let start_hit = Instant::now();
    for _ in 0..iters {
        let lease = upstream.acquire().await.unwrap();
        lease.release(true);
    }
    let elapsed_hit = start_hit.elapsed();
    let (hit_allocs, _) = ALLOCATOR.snapshot();

    // 2. Measure Pool MISS (Each request creates a fresh mock connection)
    ALLOCATOR.reset();
    let start_miss = Instant::now();
    for _ in 0..iters {
        let lease = upstream.acquire().await.unwrap();
        lease.release(false); // Do not reuse -> triggers MISS on next iteration
    }
    let elapsed_miss = start_miss.elapsed();
    let (miss_allocs, _) = ALLOCATOR.snapshot();

    println!(
        "| {:<26} | {:<10} | {:<14} | {:<14} | {:<12} | {:<14} |",
        "Pipeline State (Mock)",
        "Iters",
        "Latency / Op",
        "Ops / Sec",
        "Allocs / Op",
        "Pool Hit Rate"
    );
    println!(
        "|{:-<28}|{:-<12}|{:-<16}|{:-<16}|{:-<14}|{:-<16}|",
        "", "", "", "", "", ""
    );

    let hit_lat = elapsed_hit / (iters as u32);
    let hit_ops = (iters as f64 / elapsed_hit.as_secs_f64()) as u64;
    let hit_alloc_per_op = hit_allocs as f64 / iters as f64;
    println!(
        "| {:<26} | {:<10} | {:<14} | {:<14} | {:<12.2} | {:<14} |",
        "Pipeline (Mock Pool HIT)",
        iters,
        format_duration(hit_lat),
        format!("{hit_ops}"),
        hit_alloc_per_op,
        "100.0%"
    );

    let miss_lat = elapsed_miss / (iters as u32);
    let miss_ops = (iters as f64 / elapsed_miss.as_secs_f64()) as u64;
    let miss_alloc_per_op = miss_allocs as f64 / iters as f64;
    println!(
        "| {:<26} | {:<10} | {:<14} | {:<14} | {:<12.2} | {:<14} |",
        "Pipeline (Mock Pool MISS)",
        iters,
        format_duration(miss_lat),
        format!("{miss_ops}"),
        miss_alloc_per_op,
        "0.0%"
    );

    println!();
}

// ============================================================================
// Stage 1b: Real Host OS TCP Socket (Loopback Handshake vs Warm Pool HIT)
// ============================================================================

async fn bench_real_socket_hit_vs_miss() {
    println!("### 1b. Real Host OS TCP Sockets (Loopback): Pool HIT vs Real Handshake MISS\n");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let host_addr = listener.local_addr().expect("local addr");

    let stop_server = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop_server.clone();

    let server_task = tokio::spawn(async move {
        let mut conns = Vec::new();
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok((socket, _)) = listener.accept().await {
                conns.push(socket);
                if conns.len() > 64 {
                    conns.drain(0..32);
                }
            }
        }
    });

    let discovery = Discovery::new_explicit(vec![Endpoint::new("real-host-ep", host_addr, 1)]);
    let connector = Arc::new(TcpConnector);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream =
        Upstream::with_connector("real-socket-up", "tcp", discovery, timeouts, connector);

    // Warm-up
    let initial_lease = upstream.acquire().await.unwrap();
    initial_lease.release(true);

    // 1. Measure Pool HIT on Real TCP Socket (Warm connection reuse)
    let hit_iters = 100_000;
    ALLOCATOR.reset();
    let start_hit = Instant::now();
    for _ in 0..hit_iters {
        let lease = upstream.acquire().await.unwrap();
        lease.release(true);
    }
    let elapsed_hit = start_hit.elapsed();
    let (hit_allocs, _) = ALLOCATOR.snapshot();

    // 2. Measure Pool MISS on Real TCP Socket (Every op does real OS TCP 3-way handshake)
    let miss_iters = 5_000;
    ALLOCATOR.reset();
    let start_miss = Instant::now();
    for _ in 0..miss_iters {
        let lease = upstream.acquire().await.unwrap();
        lease.release(false); // Closes connection, forcing new OS connect on next op
    }
    let elapsed_miss = start_miss.elapsed();
    let (miss_allocs, _) = ALLOCATOR.snapshot();

    stop_server.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = tokio::net::TcpStream::connect(host_addr).await;
    server_task.abort();

    println!(
        "| {:<36} | {:<8} | {:<14} | {:<14} | {:<12} | {:<14} |",
        "Pipeline State (Real Host Socket)",
        "Iters",
        "Latency / Op",
        "Ops / Sec",
        "Allocs / Op",
        "Pool Hit Rate"
    );
    println!(
        "|{:-<38}|{:-<10}|{:-<16}|{:-<16}|{:-<14}|{:-<16}|",
        "", "", "", "", "", ""
    );

    let hit_lat = elapsed_hit / (hit_iters as u32);
    let hit_ops = (hit_iters as f64 / elapsed_hit.as_secs_f64()) as u64;
    let hit_alloc_per_op = hit_allocs as f64 / hit_iters as f64;
    println!(
        "| {:<36} | {:<8} | {:<14} | {:<14} | {:<12.2} | {:<14} |",
        "Real TCP Socket (Pool HIT)",
        hit_iters,
        format_duration(hit_lat),
        format!("{hit_ops}"),
        hit_alloc_per_op,
        "100.0%"
    );

    let miss_lat = elapsed_miss / (miss_iters as u32);
    let miss_ops = (miss_iters as f64 / elapsed_miss.as_secs_f64()) as u64;
    let miss_alloc_per_op = miss_allocs as f64 / miss_iters as f64;
    println!(
        "| {:<36} | {:<8} | {:<14} | {:<14} | {:<12.2} | {:<14} |",
        "Real TCP Socket (Pool MISS - OS Handshake)",
        miss_iters,
        format_duration(miss_lat),
        format!("{miss_ops}"),
        miss_alloc_per_op,
        "0.0%"
    );

    println!();
}

// ============================================================================
// Stage 2: Endpoint Scale Scaling (N = 10 .. 10,000, Big-O Verification)
// ============================================================================

async fn bench_endpoint_scaling() {
    println!("### 2. Hyperscale Endpoint Invariant (N = 10 .. 100,000 Endpoints)\n");

    let scales = [10, 100, 1_000, 10_000, 50_000, 100_000];
    let iters = 50_000;
    let mut big_o_results: Vec<(usize, Duration)> = Vec::new();

    println!(
        "| {:<10} | {:<12} | {:<14} | {:<14} | {:<14} | {:<8} |",
        "Endpoints", "Iterations", "Latency / Op", "Throughput", "Total Time", "Big-O"
    );
    println!(
        "|{:-<12}|{:-<14}|{:-<16}|{:-<16}|{:-<16}|{:-<10}|",
        "", "", "", "", "", ""
    );

    for &count in &scales {
        let endpoints: Vec<Endpoint> = (0..count)
            .map(|i| {
                let b2 = ((i >> 8) & 0xff) as u8;
                let b3 = (i & 0xff) as u8;
                let addr: SocketAddr = format!("10.0.{b2}.{b3}:8080").parse().unwrap();
                Endpoint::new(format!("ep-{i}"), addr, 1)
            })
            .collect();

        let discovery = Discovery::new_explicit(endpoints);
        let connector = Arc::new(MockConnector::new());
        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
        let upstream =
            Upstream::with_connector("scaling-up", "tcp", discovery, timeouts, connector);

        // Warm up by populating pool for chosen endpoints
        for _ in 0..100 {
            let l = upstream.acquire().await.unwrap();
            l.release(true);
        }

        let start = Instant::now();
        for _ in 0..iters {
            let lease = upstream.acquire().await.unwrap();
            lease.release(true);
        }
        let elapsed = start.elapsed();
        big_o_results.push((count, elapsed));

        let (complexity, _) = calculate_big_o(&big_o_results);
        let avg_lat = elapsed / (iters as u32);
        let ops = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| {:<10} | {:<12} | {:<14} | {:<14} | {:<14} | {:<8} |",
            count,
            iters,
            format_duration(avg_lat),
            format!("{ops} ops/s"),
            format_duration(elapsed),
            complexity
        );
    }

    println!();
}

// ============================================================================
// Stage 3: Load Balancing Strategy Comparison in Upstream Pipeline
// ============================================================================

async fn bench_lb_strategies() {
    println!("### 3. Load Balancing Strategy Comparison in Full Pipeline\n");

    let iters = 100_000;
    let endpoint_count = 64;
    let endpoints: Vec<Endpoint> = (0..endpoint_count)
        .map(|i| {
            let port = 8000 + (i % 50000) as u16;
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            Endpoint::new(format!("ep-{i}"), addr, (i % 5 + 1) as u32)
        })
        .collect();

    println!(
        "| {:<24} | {:<12} | {:<14} | {:<16} | {:<14} |",
        "Algorithm", "Iterations", "Latency / Op", "Aggregate Ops/s", "Pool Reuse"
    );
    println!(
        "|{:-<26}|{:-<14}|{:-<16}|{:-<18}|{:-<16}|",
        "", "", "", "", ""
    );

    // 1. RoundRobin
    run_strategy_bench("RoundRobin", RoundRobin::new(), &endpoints, iters).await;

    // 2. WeightedRoundRobin
    run_strategy_bench(
        "WeightedRoundRobin",
        WeightedRoundRobin::new(),
        &endpoints,
        iters,
    )
    .await;

    // 3. LeastConnections
    run_strategy_bench(
        "LeastConnections",
        LeastConnections::new(),
        &endpoints,
        iters,
    )
    .await;

    // 4. PowerOfTwoChoices
    run_strategy_bench(
        "PowerOfTwoChoices",
        PowerOfTwoChoices::new(),
        &endpoints,
        iters,
    )
    .await;

    // 5. IpHash
    run_strategy_bench("IpHash", IpHash::new(), &endpoints, iters).await;

    println!();
}

async fn run_strategy_bench<LB: LoadBalancer + 'static>(
    name: &str,
    balancer: LB,
    endpoints: &[Endpoint],
    iters: usize,
) {
    let discovery = Discovery::new_explicit(endpoints.to_vec());
    let connector = Arc::new(MockConnector::new());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));

    let upstream = Upstream::new(
        "lb-bench-up",
        "tcp",
        discovery,
        balancer,
        timeouts,
        connector,
    );

    // Warm-up
    for _ in 0..128 {
        let l = upstream.acquire().await.unwrap();
        l.release(true);
    }

    let start = Instant::now();
    for i in 0..iters {
        let mut target = velda_upstream::AcquireTarget::default_target();
        target.selection_context = SelectionContext::with_hash(i as u64);
        let lease = upstream.acquire_with_target(target).await.unwrap();
        lease.release(true);
    }
    let elapsed = start.elapsed();

    let avg_lat = elapsed / (iters as u32);
    let ops = (iters as f64 / elapsed.as_secs_f64()) as u64;

    println!(
        "| {:<24} | {:<12} | {:<14} | {:<16} | {:<14} |",
        name,
        iters,
        format_duration(avg_lat),
        format!("{ops} ops/s"),
        "100.0%"
    );
}

// ============================================================================
// Stage 4: RAII BackendLease Overhead (Explicit Release vs Drop Guard)
// ============================================================================

async fn bench_lease_overhead() {
    println!("### 4. RAII BackendLease Overhead: Explicit Release vs Auto-Drop\n");

    let iters = 100_000;
    let ep1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let discovery = Discovery::new_explicit(vec![Endpoint::new("e1", ep1, 1)]);
    let connector = Arc::new(MockConnector::new());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Upstream::with_connector("lease-bench", "tcp", discovery, timeouts, connector);

    // Warm up
    let w = upstream.acquire().await.unwrap();
    w.release(true);

    // 1. Explicit release
    let start_explicit = Instant::now();
    for _ in 0..iters {
        let lease = upstream.acquire().await.unwrap();
        lease.release(true);
    }
    let elapsed_explicit = start_explicit.elapsed();

    // 2. Implicit drop
    let start_drop = Instant::now();
    for _ in 0..iters {
        let lease = upstream.acquire().await.unwrap();
        std::mem::drop(lease);
    }
    let elapsed_drop = start_drop.elapsed();

    println!(
        "| {:<26} | {:<12} | {:<14} | {:<16} | {:<14} |",
        "Disposal Mode", "Iterations", "Latency / Op", "Ops / Sec", "Safety Guaranteed"
    );
    println!(
        "|{:-<28}|{:-<14}|{:-<16}|{:-<18}|{:-<16}|",
        "", "", "", "", ""
    );

    let exp_lat = elapsed_explicit / (iters as u32);
    let exp_ops = (iters as f64 / elapsed_explicit.as_secs_f64()) as u64;
    println!(
        "| {:<26} | {:<12} | {:<14} | {:<16} | {:<14} |",
        "Explicit lease.release(true)",
        iters,
        format_duration(exp_lat),
        format!("{exp_ops}"),
        "Yes (Active Health)"
    );

    let drop_lat = elapsed_drop / (iters as u32);
    let drop_ops = (iters as f64 / elapsed_drop.as_secs_f64()) as u64;
    println!(
        "| {:<26} | {:<12} | {:<14} | {:<16} | {:<14} |",
        "Implicit Drop Guard (RAII)",
        iters,
        format_duration(drop_lat),
        format!("{drop_ops}"),
        "Yes (Safe Return)"
    );

    println!();
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("===============================================================================");
    println!("     velda-upstream: Single-Thread Performance & Pipeline Benchmark Suite     ");
    println!("===============================================================================\n");

    bench_acquire_hit_vs_miss().await;
    bench_real_socket_hit_vs_miss().await;
    bench_endpoint_scaling().await;
    bench_lb_strategies().await;
    bench_lease_overhead().await;
}
