//! Multi-Thread & Multi-Core Concurrency Benchmark Suite for velda-router.
//!
//! Evaluates multicore concurrency and lock-free scaling against a realistic production table
//! (5,000 Routes, 1,000 Upstreams) loaded through the binary ingest pipeline.
//!
//! Stages:
//! 1. Concurrency Scaling across Worker Threads (1 .. 256 Workers) under Mixed Traffic
//! 2. HTTP Route Lookup Multi-Core Contention Comparison across 12, 64, and 128 Workers

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::{format_duration, load_router_from_large_dataset};
use velda_core::TransportProtocol;
use velda_router::{GrpcRouteRequest, HttpRouteRequest, Router};

fn bench_mixed_traffic_worker_scaling(router: Arc<Router>) {
    println!(
        "### 1. Concurrency Scaling by Worker Threads (1 .. 256 Workers) on 5,000-Route Table\n"
    );
    println!(
        "| Workers | Total Operations | Total Time | Aggregate Throughput | Avg Latency / op | Scaling Factor |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let worker_counts = [1, 2, 4, 8, 12, 16, 32, 64, 128, 256];
    let ops_per_worker = 100_000;
    let mut baseline_throughput = 1.0;

    for (idx, &workers) in worker_counts.iter().enumerate() {
        let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
        let mut handles = Vec::with_capacity(workers);

        for w in 0..workers {
            let r = Arc::clone(&router);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let prefix_paths = [
                    "/api/v1/service_0100/orders/items/42",
                    "/api/v1/service_0500/orders/items/42",
                    "/api/v1/service_1000/orders/items/42",
                    "/api/v1/service_1500/orders/items/42",
                ];
                let exact_path = "/endpoints/action_0005/exec";
                let grpc_service = "service.v1.Service_0007";
                let host = "api.example.com";

                b.wait();
                for i in 0..ops_per_worker {
                    match (i + w) % 4 {
                        0 => {
                            // L4 TCP route lookup
                            let route = r.route_l4("l4-in-0", TransportProtocol::Tcp);
                            let _ = std::hint::black_box(route);
                        }
                        1 => {
                            // L4 UDP route lookup + round-robin target select
                            if let Some(route) = r.route_l4("l4-in-9", TransportProtocol::Udp) {
                                let target = route.select_target();
                                let _ = std::hint::black_box(target);
                            }
                        }
                        2 => {
                            // L7 HTTP prefix / exact lookup
                            let path = if i % 2 == 0 {
                                prefix_paths[i % prefix_paths.len()]
                            } else {
                                exact_path
                            };
                            let req = HttpRouteRequest::new(path).with_host(host);
                            let route = r.route_http("https-in", &req);
                            let _ = std::hint::black_box(route);
                        }
                        _ => {
                            // L7 gRPC lookup
                            let req = GrpcRouteRequest::new(grpc_service, None);
                            let route = r.route_grpc("https-in", &req);
                            let _ = std::hint::black_box(route);
                        }
                    }
                }
            }));
        }

        barrier.wait();
        let start = Instant::now();

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let total_ops = (workers as u64) * (ops_per_worker as u64);
        let agg_throughput = (total_ops as f64 / elapsed.as_secs_f64()) as u64;
        let avg_latency = (elapsed.as_nanos() as f64) / (total_ops as f64);

        if idx == 0 {
            baseline_throughput = agg_throughput as f64;
        }
        let scaling_factor = (agg_throughput as f64) / baseline_throughput;

        println!(
            "| **{}** | {} | {} | **{:.2} M ops/s** | **{:.2} ns** | **{:.2}x** |",
            workers,
            total_ops,
            format_duration(elapsed),
            agg_throughput as f64 / 1_000_000.0,
            avg_latency,
            scaling_factor
        );
    }
    println!();
}

fn bench_pure_http_contention(router: Arc<Router>) {
    println!(
        "### 2. Pure L7 HTTP Lookup Multi-Core Contention on 5,000-Route Table (Aho-Corasick)\n"
    );
    println!(
        "| Workers | Total Operations | Total Time | Aggregate Throughput | Avg Latency / op |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let worker_counts = [12, 64, 128];
    let ops_per_worker = 200_000;

    for &workers in &worker_counts {
        let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
        let mut handles = Vec::with_capacity(workers);

        for w in 0..workers {
            let r = Arc::clone(&router);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let service_id = (w * 17) % 2000;
                let path = format!("/api/v1/service_{service_id:04}/orders/items/42");
                let req = HttpRouteRequest::new(&path);

                b.wait();
                for _ in 0..ops_per_worker {
                    let route = r.route_http("https-in", &req);
                    let _ = std::hint::black_box(route);
                }
            }));
        }

        barrier.wait();
        let start = Instant::now();

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let total_ops = (workers as u64) * (ops_per_worker as u64);
        let agg_throughput = (total_ops as f64 / elapsed.as_secs_f64()) as u64;
        let avg_latency = (elapsed.as_nanos() as f64) / (total_ops as f64);

        println!(
            "| **{} Workers** | {} | {} | **{:.2} M ops/s** | **{:.2} ns** |",
            workers,
            total_ops,
            format_duration(elapsed),
            agg_throughput as f64 / 1_000_000.0,
            avg_latency
        );
    }
    println!();
}

fn main() {
    println!("# Velda Edge: velda-router Multi-Thread Suite (Large Dataset Powered)\n");
    println!("> Loading 5,000 routes from binary artifact into RAM...");
    let start = Instant::now();
    let router = Arc::new(load_router_from_large_dataset(5_000, 1_000));
    println!("> Ready in {}\n", format_duration(start.elapsed()));

    bench_mixed_traffic_worker_scaling(Arc::clone(&router));
    bench_pure_http_contention(router);
}
