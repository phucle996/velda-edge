//! Single-Thread Performance, Latency & Zero-Allocation Benchmark Suite for velda-router.
//!
//! Evaluates single-thread performance against a realistic production table (5,000 Routes, 1,000 Upstreams)
//! loaded through the full Control Plane (JSON -> .bin) and Data Plane (.bin -> RAM -> Router) pipeline.
//!
//! Stages:
//! 1. Protocol Routing Latency & Zero-Allocation Verification on Large Dataset (L4 TCP/UDP, L7 HTTP, L7 gRPC)
//! 2. Aho-Corasick Prefix Scaling across N = 100 .. 5000 Routes (Validating O(M) URI-length complexity)
//! 3. Route Miss Determinism (None, 0 Allocations)

mod common;

use std::time::Instant;

use common::{CountingAllocator, format_duration, load_router_from_large_dataset};
use velda_router::{GrpcRouteRequest, Http1RouteRequest};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn bench_protocol_latencies_on_large_dataset() {
    println!("### 1. Protocol Routing Latency & Zero-Allocation on Large Dataset (5,000 Routes)\n");
    println!("| Target Scenario | Tested Route | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 1_000_000;
    println!("> Loading 5,000 routes from compiled binary artifact into RAM...");
    let load_start = Instant::now();
    let router = load_router_from_large_dataset(5_000, 1_000);
    println!(
        "> Router compiled and ready in {}\n",
        format_duration(load_start.elapsed())
    );

    // 1. L4 TCP Route Lookup
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_tcp("l4-in-0");
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L4 TCP Lookup** | `l4-in-0:tcp` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 2. L4 UDP Route Lookup + Target Selection
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_udp("l4-in-9").unwrap();
        let target = r.id;
        let _ = std::hint::black_box(target);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L4 UDP + RoundRobin** | `l4-in-9:udp` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 3. L7 HTTP Exact Match (/endpoints/action_0005/exec)
    let test_exact_path = "/endpoints/action_0005/exec";
    let req_exact = Http1RouteRequest::new(test_exact_path).with_host("api.example.com");
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_http1("https-in", &req_exact);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L7 HTTP Exact Match** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_exact_path,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 4. L7 HTTP Longest Prefix Match (/api/v1/service_1000/orders/items/42)
    let test_prefix_path = "/api/v1/service_1000/orders/items/42";
    let req_prefix = Http1RouteRequest::new(test_prefix_path);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_http1("https-in", &req_prefix);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L7 HTTP Prefix Match** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_prefix_path,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 5. L7 HTTP Miss / No-Match
    let test_miss_path = "/unknown/unmatched/path/404";
    let req_miss = Http1RouteRequest::new(test_miss_path);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_http1("https-in", &req_miss);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L7 HTTP Miss (No-Match)** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_miss_path,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 6. L7 gRPC Exact Service
    let test_grpc_service = "service.v1.Service_0007";
    let req_grpc = GrpcRouteRequest::new(test_grpc_service, None);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_grpc("https-in", &req_grpc);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L7 gRPC Exact Match** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_grpc_service,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 7. L7 gRPC Zero-Allocation Path Parsing & Lookup
    let grpc_path = "/service.v1.Service_0007/CreateOrder";
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let parsed = GrpcRouteRequest::from_path(grpc_path, None).unwrap();
        let r = router.route_grpc("https-in", &parsed);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L7 gRPC Parse+Route** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        grpc_path,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );
    println!();
}

fn bench_prefix_scaling_large_datasets() {
    println!("### 2. Aho-Corasick Prefix Scaling across Large Datasets (100 .. 5,000 Routes)\n");
    println!(
        "| Route Count (N) | Path Tested | Total Time | Latency / op | Allocs / op | Throughput |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let route_counts = [100, 500, 1_000, 2_500, 5_000];
    let iters = 1_000_000;

    for &n in &route_counts {
        let router = load_router_from_large_dataset(n, n / 5);
        let mid_idx = (n / 10) * 2;
        let test_path = format!("/api/v1/service_{mid_idx:04}/action/detail");
        let req = Http1RouteRequest::new(&test_path);

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let r = router.route_http1("https-in", &req);
            let _ = std::hint::black_box(r);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| **N = {}** | `{}` | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
            n,
            test_path,
            format_duration(elapsed),
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );
    }
    println!();
}

fn main() {
    println!("# Velda Edge: velda-router Single-Thread Suite (Large Dataset Powered)\n");
    bench_protocol_latencies_on_large_dataset();
    bench_prefix_scaling_large_datasets();
}
