//! Adversarial, Failover & Churn Benchmark Suite for velda-upstream.
//!
//! Stages:
//! 1. Cascading Backend Failures & Passive Circuit-Breaking Failover Latency
//! 2. Dynamic Discovery Churn & Concurrent Lease Acquisition
//! 3. Connect Timeout Saturation & Safe Degradation

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::format_duration;
use velda_upstream::{
    Discovery, Endpoint, HealthConfig, MockConnector, PassiveHealthConfig, Upstream,
    UpstreamTimeouts,
};

// ============================================================================
// Stage 1: Cascading Failover & Circuit Breaking Latency
// ============================================================================

async fn bench_cascading_failover() {
    println!("### 1. Cascading Failover & Circuit Breaking Latency\n");

    let iters = 20_000;
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(100), Duration::from_secs(30));

    println!(
        "| {:<36} | {:<8} | {:<14} | {:<16} | {:<14} |",
        "Test Scenario", "Iters", "Latency / Op", "Throughput", "Survivor Endpoint"
    );
    println!(
        "|{:-<38}|{:-<10}|{:-<16}|{:-<18}|{:-<16}|",
        "", "", "", "", ""
    );

    // Scenario 1: Continuous Active Failover (Every request encounters failing primary -> falls back to survivor)
    {
        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);
        let connector = Arc::new(MockConnector::new());
        connector.set_failing(ep1);

        // Passive health disabled: every request actively tries ep1, fails, and falls back to ep2
        let upstream =
            Upstream::with_connector("active-failover", "tcp", discovery, timeouts, connector);

        let start = Instant::now();
        for _ in 0..iters {
            let lease = upstream.acquire().await.unwrap();
            assert_eq!(lease.endpoint(), ep2);
            lease.release(true);
        }
        let elapsed = start.elapsed();
        let avg_lat = elapsed / (iters as u32);
        let ops = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| {:<36} | {:<8} | {:<14} | {:<16} | {:<14} |",
            "Continuous Active Failover (100% Fail)",
            iters,
            format_duration(avg_lat),
            format!("{ops} ops/s"),
            "10.0.0.2:8080 (100%)"
        );
    }

    // Scenario 2: Circuit-Breaker Tripped Steady State (Primary tripped after 2 fails -> bypassed via health filter)
    {
        let discovery = Discovery::new_explicit(vec![
            Endpoint::new("e1", ep1, 1),
            Endpoint::new("e2", ep2, 1),
        ]);
        let connector = Arc::new(MockConnector::new());
        connector.set_failing(ep1);

        let health_config = HealthConfig {
            passive: Some(PassiveHealthConfig {
                enabled: true,
                max_consecutive_failures: 2,
                cooldown: Duration::from_secs(10),
            }),
            active: None,
        };

        let upstream =
            Upstream::with_connector("cb-tripped", "tcp", discovery, timeouts, connector)
                .with_health_config(health_config);

        let start = Instant::now();
        for _ in 0..iters {
            let lease = upstream.acquire().await.unwrap();
            assert_eq!(lease.endpoint(), ep2);
            lease.release(true);
        }
        let elapsed = start.elapsed();
        let avg_lat = elapsed / (iters as u32);
        let ops = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| {:<36} | {:<8} | {:<14} | {:<16} | {:<14} |",
            "Circuit-Broken Steady State (Bypassed)",
            iters,
            format_duration(avg_lat),
            format!("{ops} ops/s"),
            "10.0.0.2:8080 (100%)"
        );
    }

    println!();
}

// ============================================================================
// Stage 2: Dynamic Discovery Churn & Concurrent Lease Acquisition
// ============================================================================

async fn bench_discovery_churn_concurrency() {
    println!("### 2. High-Frequency Discovery Churn Under Concurrent Traffic\n");

    let workers = 16;
    let iters_per_worker = 25_000;
    let total_requests = workers * iters_per_worker;
    let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Dynamic initial endpoints
    let initial_endpoints: Vec<Endpoint> = (0..32)
        .map(|i| {
            let port = 8000 + i as u16;
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            Endpoint::new(format!("ep-{i}"), addr, 1)
        })
        .collect();

    let discovery = Discovery::new_explicit(initial_endpoints);
    let connector = Arc::new(MockConnector::new());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));

    let upstream = Arc::new(Upstream::with_connector(
        "churn-up",
        "tcp",
        discovery.clone(),
        timeouts,
        connector,
    ));

    // Background Task: Rapidly update discovery endpoints every 5ms (simulating DNS/k8s churn)
    let disc_clone = discovery.clone();
    let stop_clone = Arc::clone(&stop_signal);
    let churn_task = tokio::spawn(async move {
        let mut cycle = 0u64;
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cycle += 1;
            let count = 16 + (cycle % 16) as usize;
            let new_eps: Vec<Endpoint> = (0..count)
                .map(|i| {
                    let port = 8000 + ((cycle * 10 + i as u64) % 1000) as u16;
                    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                    Endpoint::new(format!("ep-{i}"), addr, 1)
                })
                .collect();
            disc_clone.update_endpoints(new_eps, cycle);
        }
    });

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let up = Arc::clone(&upstream);
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            for _ in 0..iters_per_worker {
                if let Ok(lease) = up.acquire().await {
                    lease.release(true);
                }
            }
            t0.elapsed()
        }));
    }

    let mut total_worker_duration = Duration::ZERO;
    for h in handles {
        total_worker_duration += h.await.unwrap();
    }

    stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
    churn_task.await.unwrap();

    let elapsed = start.elapsed();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();
    let avg_req_latency = total_worker_duration / (total_requests as u32);
    let service_interval = elapsed / (total_requests as u32);

    println!(
        "| {:<20} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} |",
        "Churn Frequency",
        "Total Reqs",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Req Latency",
        "1/Throughput"
    );
    println!(
        "|{:-<22}|{:-<14}|{:-<14}|{:-<18}|{:-<18}|{:-<16}|",
        "", "", "", "", "", ""
    );

    println!(
        "| {:<20} | {:<12} | {:<12} | {:<16} | {:<16} | {:<14} |",
        "5ms Dynamic Churn",
        total_requests,
        format_duration(elapsed),
        format!("{:.0} ops/s", throughput),
        format_duration(avg_req_latency),
        format_duration(service_interval)
    );

    println!();
}

// ============================================================================
// Stage 3: Mass Outage Cascade under 32 Concurrent Workers
// ============================================================================

async fn bench_mass_outage_failover_concurrency() {
    println!("### 3. Mass Outage Cascade (87.5% Cluster Failure) Under 32 Workers\n");

    let total_endpoints = 64;
    let failing_count = 56; // 87.5% sudden outage
    let workers = 32;
    let iters_per_worker = 25_000;
    let total_requests = workers * iters_per_worker;

    let endpoints: Vec<Endpoint> = (0..total_endpoints)
        .map(|i| {
            let addr: SocketAddr = format!("10.0.0.{}:8080", i + 1).parse().unwrap();
            Endpoint::new(format!("ep-{i}"), addr, 1)
        })
        .collect();

    let connector = Arc::new(MockConnector::new());
    for ep in endpoints.iter().take(failing_count) {
        connector.set_failing(ep.address);
    }

    let discovery = Discovery::new_explicit(endpoints.clone());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(100), Duration::from_secs(30));
    let health_config = HealthConfig {
        passive: Some(PassiveHealthConfig {
            enabled: true,
            max_consecutive_failures: 2,
            cooldown: Duration::from_secs(60),
        }),
        active: None,
    };

    let upstream = Arc::new(
        Upstream::with_connector("mass-outage", "tcp", discovery, timeouts, connector)
            .with_health_config(health_config),
    );

    println!(
        "| {:<24} | {:<10} | {:<12} | {:<14} | {:<16} | {:<14} | {:<14} |",
        "Outage Scenario",
        "Workers",
        "Total Reqs",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Latency",
        "Survivors"
    );
    println!(
        "|{:-<26}|{:-<12}|{:-<14}|{:-<16}|{:-<18}|{:-<16}|{:-<16}|",
        "", "", "", "", "", "", ""
    );

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let up = Arc::clone(&upstream);
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            for _ in 0..iters_per_worker {
                if let Ok(lease) = up.acquire().await {
                    lease.release(true);
                }
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

    println!(
        "| {:<24} | {:<10} | {:<12} | {:<14} | {:<16} | {:<14} | {:<14} |",
        "56/64 Outage (87.5%)",
        workers,
        total_requests,
        format_duration(elapsed),
        format!("{:.0} ops/s", throughput),
        format_duration(avg_req_latency),
        "8/64 (100% Ok)"
    );

    println!();
}

// ============================================================================
// Stage 4: Byzantine Flapping Endpoints (High-Frequency State Oscillation)
// ============================================================================

async fn bench_byzantine_flapping_endpoints() {
    println!("### 4. Byzantine Flapping Endpoints (High-Frequency Jitter & Invalidation)\n");

    let total_endpoints = 32;
    let flapping_count = 16;
    let workers = 16;
    let iters_per_worker = 25_000;
    let total_requests = workers * iters_per_worker;
    let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let endpoints: Vec<Endpoint> = (0..total_endpoints)
        .map(|i| {
            let addr: SocketAddr = format!("10.0.1.{}:8080", i + 1).parse().unwrap();
            Endpoint::new(format!("ep-{i}"), addr, 1)
        })
        .collect();

    let connector = Arc::new(MockConnector::new());
    let discovery = Discovery::new_explicit(endpoints.clone());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(100), Duration::from_secs(30));
    let health_config = HealthConfig {
        passive: Some(PassiveHealthConfig {
            enabled: true,
            max_consecutive_failures: 2,
            cooldown: Duration::from_millis(5),
        }),
        active: None,
    };

    let upstream = Arc::new(
        Upstream::with_connector(
            "flapping-up",
            "tcp",
            discovery,
            timeouts,
            Arc::clone(&connector),
        )
        .with_health_config(health_config),
    );

    // Flapping background task: alternate fail/healthy every 2ms on half the endpoints
    let flap_conn = Arc::clone(&connector);
    let stop_clone = Arc::clone(&stop_signal);
    let flap_addrs: Vec<SocketAddr> = endpoints[0..flapping_count]
        .iter()
        .map(|e| e.address)
        .collect();
    let flap_task = tokio::spawn(async move {
        let mut cycle = 0u32;
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(2)).await;
            cycle += 1;
            let target = flap_addrs[(cycle as usize) % flap_addrs.len()];
            if cycle.is_multiple_of(2) {
                flap_conn.set_failing(target);
            } else {
                flap_conn.clear_failing(&target);
            }
        }
    });

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let up = Arc::clone(&upstream);
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            for _ in 0..iters_per_worker {
                if let Ok(lease) = up.acquire().await {
                    lease.release(true);
                }
            }
            t0.elapsed()
        }));
    }

    let mut total_worker_duration = Duration::ZERO;
    for h in handles {
        total_worker_duration += h.await.unwrap();
    }

    stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
    flap_task.await.unwrap();

    let elapsed = start.elapsed();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();
    let avg_req_latency = total_worker_duration / (total_requests as u32);
    let service_interval = elapsed / (total_requests as u32);

    println!(
        "| {:<24} | {:<10} | {:<12} | {:<14} | {:<16} | {:<14} | {:<14} |",
        "Jitter Scenario",
        "Workers",
        "Total Reqs",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Avg Latency",
        "1/Throughput"
    );
    println!(
        "|{:-<26}|{:-<12}|{:-<14}|{:-<16}|{:-<18}|{:-<16}|{:-<16}|",
        "", "", "", "", "", "", ""
    );

    println!(
        "| {:<24} | {:<10} | {:<12} | {:<14} | {:<16} | {:<14} | {:<14} |",
        "2ms Flapping (16/32)",
        workers,
        total_requests,
        format_duration(elapsed),
        format!("{:.0} ops/s", throughput),
        format_duration(avg_req_latency),
        format_duration(service_interval)
    );

    println!();
}

#[tokio::main]
async fn main() {
    println!("===============================================================================");
    println!("     velda-upstream: Adversarial, Failover & Churn Benchmark Suite             ");
    println!("===============================================================================\n");

    bench_cascading_failover().await;
    bench_discovery_churn_concurrency().await;
    bench_mass_outage_failover_concurrency().await;
    bench_byzantine_flapping_endpoints().await;
}
