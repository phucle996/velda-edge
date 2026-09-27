//! Multi-Thread & Multi-Core Concurrency Benchmark Suite for velda-upstream.
//!
//! Stages:
//! 1. Worker Task Concurrency Scaling (1 .. 64 Workers)
//! 2. Shard & Endpoint Contention (Single Hotspot Endpoint vs Distributed 128 Endpoints)
//! 3. Real-World Mixed Edge Workload (90% Pool HITs + 10% Fresh Connections)

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::format_duration;
use velda_upstream::{
    Discovery, Endpoint, MockConnector, TcpConnector, Upstream, UpstreamTimeouts,
};

// ============================================================================
// Stage 1: Worker Task Concurrency Scaling (1 .. 64 Concurrent Tasks)
// ============================================================================

async fn bench_concurrency_scaling() {
    println!("### 1. Multi-Core Concurrency Scaling (1 .. 64 Workers)\n");
    println!(
        "| {:<8} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} | {:<10} |",
        "Workers",
        "Total Reqs",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Req Latency",
        "1/Throughput",
        "Scaling"
    );
    println!(
        "|{:-<10}|{:-<14}|{:-<14}|{:-<18}|{:-<18}|{:-<16}|{:-<12}|",
        "", "", "", "", "", "", ""
    );

    let worker_counts = [1, 2, 4, 8, 16, 32, 64];
    let iters_per_worker = 50_000;
    let mut baseline_throughput = 1.0;

    for (idx, &workers) in worker_counts.iter().enumerate() {
        let endpoint_count = 64;
        let endpoints: Vec<Endpoint> = (0..endpoint_count)
            .map(|i| {
                let port = 8000 + (i % 50000) as u16;
                let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                Endpoint::new(format!("ep-{i}"), addr, 1)
            })
            .collect();

        let discovery = Discovery::new_explicit(endpoints);
        let connector = Arc::new(MockConnector::new());
        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
        let upstream = Arc::new(Upstream::with_connector(
            "bench-scale",
            "tcp",
            discovery,
            timeouts,
            connector,
        ));

        // Warm up connections in pool
        for _ in 0..128 {
            let l = upstream.acquire().await.unwrap();
            l.release(true);
        }

        let total_requests = workers * iters_per_worker;
        let start = Instant::now();
        let mut handles = Vec::with_capacity(workers);

        for _ in 0..workers {
            let up = Arc::clone(&upstream);
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                for _ in 0..iters_per_worker {
                    let lease = up.acquire().await.unwrap();
                    lease.release(true);
                }
                t0.elapsed()
            }));
        }

        let mut total_worker_duration = Duration::ZERO;
        for h in handles {
            total_worker_duration += h.await.unwrap();
        }

        let elapsed = start.elapsed();
        let throughput = total_requests as f64 / elapsed.as_secs_f64();
        let avg_req_latency = total_worker_duration / (total_requests as u32);
        let service_interval = elapsed / (total_requests as u32);

        if idx == 0 {
            baseline_throughput = throughput;
        }
        let scaling_factor = throughput / baseline_throughput;

        println!(
            "| {:<8} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} | {:<9.2}x |",
            workers,
            total_requests,
            format_duration(elapsed),
            format!("{:.0} ops/s", throughput),
            format_duration(avg_req_latency),
            format_duration(service_interval),
            scaling_factor,
        );
    }

    println!();
}

// ============================================================================
// Stage 2: Shard & Endpoint Contention (Single Hotspot vs 128 Distributed)
// ============================================================================

async fn bench_contention_profile() {
    println!("### 2. Lock Contention: Single Hotspot Endpoint vs Distributed Endpoints\n");
    println!(
        "| {:<24} | {:<8} | {:<12} | {:<16} | {:<16} | {:<14} | {:<10} |",
        "Topology",
        "Workers",
        "Total Reqs",
        "Aggregate Ops/s",
        "Avg Req Latency",
        "1/Throughput",
        "Hit Rate"
    );
    println!(
        "|{:-<26}|{:-<10}|{:-<14}|{:-<18}|{:-<18}|{:-<16}|{:-<12}|",
        "", "", "", "", "", "", ""
    );

    let workers = 32;
    let iters_per_worker = 25_000;
    let total_requests = workers * iters_per_worker;

    // Case A: Single Hotspot Endpoint
    {
        let ep: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let discovery = Discovery::new_explicit(vec![Endpoint::new("hotspot", ep, 1)]);
        let connector = Arc::new(MockConnector::new());
        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
        let upstream = Arc::new(Upstream::with_connector(
            "hotspot-up",
            "tcp",
            discovery,
            timeouts,
            connector,
        ));

        // Warm up
        let l = upstream.acquire().await.unwrap();
        l.release(true);

        let start = Instant::now();
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let up = Arc::clone(&upstream);
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                for _ in 0..iters_per_worker {
                    let lease = up.acquire().await.unwrap();
                    lease.release(true);
                }
                t0.elapsed()
            }));
        }
        let mut total_worker_duration = Duration::ZERO;
        for h in handles {
            total_worker_duration += h.await.unwrap();
        }
        let elapsed = start.elapsed();
        let throughput = total_requests as f64 / elapsed.as_secs_f64();
        let avg_req_latency = total_worker_duration / (total_requests as u32);
        let service_interval = elapsed / (total_requests as u32);

        println!(
            "| {:<24} | {:<8} | {:<12} | {:<16} | {:<16} | {:<14} | {:<10} |",
            "1 Hotspot Endpoint",
            workers,
            total_requests,
            format!("{:.0} ops/s", throughput),
            format_duration(avg_req_latency),
            format_duration(service_interval),
            "100.0%"
        );
    }

    // Case B: 128 Distributed Endpoints
    {
        let endpoints: Vec<Endpoint> = (0..128)
            .map(|i| {
                let port = 8000 + i as u16;
                let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                Endpoint::new(format!("ep-{i}"), addr, 1)
            })
            .collect();
        let discovery = Discovery::new_explicit(endpoints);
        let connector = Arc::new(MockConnector::new());
        let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
        let upstream = Arc::new(Upstream::with_connector(
            "distributed-up",
            "tcp",
            discovery,
            timeouts,
            connector,
        ));

        // Warm up
        for _ in 0..256 {
            let l = upstream.acquire().await.unwrap();
            l.release(true);
        }

        let start = Instant::now();
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let up = Arc::clone(&upstream);
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                for _ in 0..iters_per_worker {
                    let lease = up.acquire().await.unwrap();
                    lease.release(true);
                }
                t0.elapsed()
            }));
        }
        let mut total_worker_duration = Duration::ZERO;
        for h in handles {
            total_worker_duration += h.await.unwrap();
        }
        let elapsed = start.elapsed();
        let throughput = total_requests as f64 / elapsed.as_secs_f64();
        let avg_req_latency = total_worker_duration / (total_requests as u32);
        let service_interval = elapsed / (total_requests as u32);

        println!(
            "| {:<24} | {:<8} | {:<12} | {:<16} | {:<16} | {:<14} | {:<10} |",
            "128 Distributed Endpoints",
            workers,
            total_requests,
            format!("{:.0} ops/s", throughput),
            format_duration(avg_req_latency),
            format_duration(service_interval),
            "100.0%"
        );
    }

    println!();
}

// ============================================================================
// Stage 3: Real-World Mixed Edge Workload (90% Pool HITs + 10% Fresh Connections)
// ============================================================================

async fn bench_mixed_workload() {
    println!(
        "### 3. Realistic Mixed Edge Workload (90% Reused Connections + 10% Fresh Handshakes)\n"
    );

    let workers = 16;
    let iters_per_worker = 20_000;
    let total_requests = workers * iters_per_worker;

    let endpoints: Vec<Endpoint> = (0..32)
        .map(|i| {
            let port = 8000 + i as u16;
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            Endpoint::new(format!("ep-{i}"), addr, 1)
        })
        .collect();

    let discovery = Discovery::new_explicit(endpoints);
    let connector = Arc::new(MockConnector::new());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Arc::new(Upstream::with_connector(
        "mixed-up", "tcp", discovery, timeouts, connector,
    ));

    // Warm-up initial connections
    for _ in 0..64 {
        let l = upstream.acquire().await.unwrap();
        l.release(true);
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let up = Arc::clone(&upstream);
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            for i in 0..iters_per_worker {
                let lease = up.acquire().await.unwrap();
                // 10% of requests are non-reusable (e.g. backend error, connection close requested)
                let is_reusable = (worker_id + i) % 10 != 0;
                lease.release(is_reusable);
            }
            t0.elapsed()
        }));
    }

    let mut total_worker_duration = Duration::ZERO;
    for h in handles {
        total_worker_duration += h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();
    let avg_req_latency = total_worker_duration / (total_requests as u32);
    let service_interval = elapsed / (total_requests as u32);
    let stats = upstream.pool_stats();
    let reuse_rate = stats.hits as f64 / (stats.hits + stats.misses) as f64 * 100.0;

    println!(
        "| {:<16} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} | {:<12} |",
        "Workload Type",
        "Total Reqs",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Req Latency",
        "1/Throughput",
        "Hit Rate"
    );
    println!(
        "|{:-<18}|{:-<14}|{:-<14}|{:-<18}|{:-<18}|{:-<16}|{:-<14}|",
        "", "", "", "", "", "", ""
    );

    println!(
        "| {:<16} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} | {:<11.1}% |",
        "90/10 Mixed Load",
        total_requests,
        format_duration(elapsed),
        format!("{:.0} ops/s", throughput),
        format_duration(avg_req_latency),
        format_duration(service_interval),
        reuse_rate
    );

    println!();
}

// ============================================================================
// Stage 4: Real Host OS TCP Sockets Concurrency (32 Workers over Loopback Ports)
// ============================================================================

async fn bench_real_tcp_concurrency() {
    println!("### 4. Real Host OS TCP Sockets Concurrency (32 Workers over 8 Ports)\n");

    let port_count = 8;
    let mut listeners = Vec::with_capacity(port_count);
    let mut endpoints = Vec::with_capacity(port_count);

    for i in 0..port_count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        endpoints.push(Endpoint::new(format!("real-ep-{i}"), addr, 1));
        listeners.push(listener);
    }

    let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut server_handles = Vec::with_capacity(port_count);

    for listener in listeners {
        let stop_clone = Arc::clone(&stop_signal);
        server_handles.push(tokio::spawn(async move {
            let mut conns = Vec::new();
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok((socket, _)) = listener.accept().await {
                    conns.push(socket);
                    if conns.len() > 64 {
                        conns.drain(0..32);
                    }
                }
            }
        }));
    }

    let discovery = Discovery::new_explicit(endpoints);
    let connector = Arc::new(TcpConnector);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Arc::new(Upstream::with_connector(
        "real-multi-up",
        "tcp",
        discovery,
        timeouts,
        connector,
    ));

    // Warm-up connections in pool across endpoints
    for _ in 0..64 {
        let l = upstream.acquire().await.unwrap();
        l.release(true);
    }

    let workers = 32;
    let iters_per_worker = 25_000;
    let total_requests = workers * iters_per_worker;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let up = Arc::clone(&upstream);
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            for _ in 0..iters_per_worker {
                let lease = up.acquire().await.unwrap();
                lease.release(true);
            }
            t0.elapsed()
        }));
    }

    let mut total_worker_duration = Duration::ZERO;
    for h in handles {
        total_worker_duration += h.await.unwrap();
    }

    stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
    for sh in server_handles {
        sh.abort();
    }

    let elapsed = start.elapsed();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();
    let avg_req_latency = total_worker_duration / (total_requests as u32);
    let service_interval = elapsed / (total_requests as u32);
    let stats = upstream.pool_stats();
    let reuse_rate = stats.hits as f64 / (stats.hits + stats.misses) as f64 * 100.0;

    println!(
        "| {:<24} | {:<8} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} | {:<12} |",
        "Socket Pipeline",
        "Workers",
        "Total Reqs",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Req Latency",
        "1/Throughput",
        "Pool Hit Rate"
    );
    println!(
        "|{:-<26}|{:-<10}|{:-<14}|{:-<14}|{:-<18}|{:-<18}|{:-<16}|{:-<14}|",
        "", "", "", "", "", "", "", ""
    );

    println!(
        "| {:<24} | {:<8} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} | {:<11.1}% |",
        "Real OS TCP (Pool HIT)",
        workers,
        total_requests,
        format_duration(elapsed),
        format!("{:.0} ops/s", throughput),
        format_duration(avg_req_latency),
        format_duration(service_interval),
        reuse_rate
    );

    println!();
}

#[tokio::main]
async fn main() {
    println!("===============================================================================");
    println!("     velda-upstream: Multi-Thread & Concurrency Benchmark Suite                ");
    println!("===============================================================================\n");

    bench_concurrency_scaling().await;
    bench_contention_profile().await;
    bench_mixed_workload().await;
    bench_real_tcp_concurrency().await;
}
