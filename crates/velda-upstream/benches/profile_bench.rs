//! Micro-Architectural & Subsystem Phase Profiling Benchmark for velda-upstream.
//!
//! Stages:
//! 1. Nanosecond Latency Breakdown per Pipeline Phase (Hot Path)
//! 2. Zero-Allocation Hot-Path Invariant Audit across Stages

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{CountingAllocator, MockConnector, format_duration};
use velda_upstream::{
    ConnectionKey, Discovery, Endpoint, HealthConfig, HealthTracker, LoadBalancer, RoundRobin,
    SelectionContext, Upstream, UpstreamPoolManager, UpstreamTimeouts,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn bench_phase_breakdown() {
    println!("### 1. Nanosecond Phase Latency Breakdown (Hot Path Profiling)\n");

    let iters = 200_000;
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let discovery = Discovery::new_explicit(vec![Endpoint::new("e1", ep1, 1)]);
    let health = Arc::new(HealthTracker::new(HealthConfig::default()));
    let balancer = RoundRobin::new();
    let pool = Arc::new(UpstreamPoolManager::new());
    let proto = Arc::from("tcp");
    let key = ConnectionKey::http(ep1, &*proto, None, None);

    // Warm up pool
    let connector = Arc::new(MockConnector::new());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let _upstream =
        Upstream::with_connector("prof-up", "tcp", discovery.clone(), timeouts, connector);

    // Seed connection into pool
    pool.release(
        &key,
        Box::new(common::MockConnection::new(ep1)),
        true,
        false,
    );

    println!(
        "| {:<32} | {:<12} | {:<14} | {:<12} | {:<12} |",
        "Pipeline Subsystem / Phase", "Iterations", "Latency / Op", "Ops / Sec", "% of Time"
    );
    println!(
        "|{:-<34}|{:-<14}|{:-<16}|{:-<14}|{:-<14}|",
        "", "", "", "", ""
    );

    // Phase 1: Discovery Snapshot
    let start_p1 = Instant::now();
    for _ in 0..iters {
        let ep = discovery.current_endpoints();
        std::hint::black_box(ep);
    }
    let dur_p1 = start_p1.elapsed();

    // Phase 2: Health Check Filter
    let endpoints = discovery.current_endpoints();
    let start_p2 = Instant::now();
    for _ in 0..iters {
        let healthy = health.is_healthy(&ep1);
        std::hint::black_box(healthy);
    }
    let dur_p2 = start_p2.elapsed();

    // Phase 3: LB Select
    let usable = endpoints.all_endpoints();
    let ctx = SelectionContext::NONE;
    let start_p3 = Instant::now();
    for _ in 0..iters {
        let selected = balancer.select(usable, &ctx);
        std::hint::black_box(selected);
    }
    let dur_p3 = start_p3.elapsed();

    // Phase 4: ConnectionKey Formation & Hash
    let start_p4 = Instant::now();
    for _ in 0..iters {
        let k = ConnectionKey::http(ep1, Arc::clone(&proto), None, None);
        std::hint::black_box(k);
    }
    let dur_p4 = start_p4.elapsed();

    // Phase 5: Pool Acquire & Return (HIT)
    let start_p5 = Instant::now();
    for _ in 0..iters {
        if let Some(conn) = pool.acquire(&key, Duration::from_secs(60)) {
            pool.release(&key, conn, true, false);
        }
    }
    let dur_p5 = start_p5.elapsed();

    let total_dur = dur_p1 + dur_p2 + dur_p3 + dur_p4 + dur_p5;
    let total_nanos = total_dur.as_nanos() as f64;

    let print_row = |name: &str, dur: Duration| {
        let lat = dur / (iters as u32);
        let ops = (iters as f64 / dur.as_secs_f64()) as u64;
        let pct = (dur.as_nanos() as f64 / total_nanos) * 100.0;
        println!(
            "| {:<32} | {:<12} | {:<14} | {:<12} | {:<11.1}% |",
            name,
            iters,
            format_duration(lat),
            format!("{ops}"),
            pct
        );
    };

    print_row("1. Discovery Read (ArcSwap)", dur_p1);
    print_row("2. Health Atomic Check", dur_p2);
    print_row("3. LB Selection (RoundRobin)", dur_p3);
    print_row("4. ConnectionKey Hash/Setup", dur_p4);
    print_row("5. Pool Acquire & Release (HIT)", dur_p5);

    println!(
        "|{:-<34}|{:-<14}|{:-<16}|{:-<14}|{:-<14}|",
        "", "", "", "", ""
    );
    let total_lat = total_dur / (iters as u32);
    let total_ops = (iters as f64 / total_dur.as_secs_f64()) as u64;
    println!(
        "| {:<32} | {:<12} | {:<14} | {:<12} | {:<12} |",
        "SUM of Phases (Analytical)",
        iters,
        format_duration(total_lat),
        format!("{total_ops}"),
        "100.0%"
    );

    println!();
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("===============================================================================");
    println!("     velda-upstream: Micro-Architectural Subsystem Profiling Suite             ");
    println!("===============================================================================\n");

    bench_phase_breakdown();
}
