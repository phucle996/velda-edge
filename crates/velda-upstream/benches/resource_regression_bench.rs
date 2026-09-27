//! Resource Regression & Leak Audit Benchmark Suite for velda-upstream.
//!
//! Validates zero-leak invariants under sustained and adversarial workloads:
//! 1. Steady-State Zero-Allocation Invariant (500,000 requests on Real Host TCP sockets)
//! 2. Client Abort / Cancellation Storm (50,000 tasks, 50% aborted mid-flight)
//! 3. High-Frequency Ephemeral Pod Churn (Health record pruning & zero RAM growth)
//! 4. Idle Connection Eviction & Sweeper (Kernel File Descriptor reclamation)

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{CountingAllocator, count_open_fds, format_bytes, format_duration};
use velda_upstream::{
    Discovery, Endpoint, HealthConfig, MockConnector, TcpConnector, Upstream, UpstreamTimeouts,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: Steady-State Zero-Allocation & Zero-Socket-Leak Invariant
// ============================================================================

async fn bench_steady_state_resource_regression() {
    println!("### 1. Steady-State Zero-Allocation Invariant (500,000 Requests on Real OS TCP)\n");

    let baseline_fds = count_open_fds();
    let port_count = 8;
    let mut listeners = Vec::with_capacity(port_count);
    let mut endpoints = Vec::with_capacity(port_count);

    for i in 0..port_count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        endpoints.push(Endpoint::new(format!("steady-ep-{i}"), addr, 1));
        listeners.push(listener);
    }

    let stop_server = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut server_handles = Vec::with_capacity(port_count);

    for listener in listeners {
        let stop_clone = Arc::clone(&stop_server);
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
        "steady-up",
        "tcp",
        discovery,
        timeouts,
        connector,
    ));

    // Warm-up to populate warm pool sockets
    for _ in 0..64 {
        let l = upstream.acquire().await.unwrap();
        l.release(true);
    }

    ALLOCATOR.reset();

    let workers = 32;
    let iters_per_worker = 15_625; // 32 * 15,625 = 500,000 requests
    let total_requests = workers * iters_per_worker;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let up = Arc::clone(&upstream);
        handles.push(tokio::spawn(async move {
            for _ in 0..iters_per_worker {
                let lease = up.acquire().await.unwrap();
                lease.release(true);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let snapshot = ALLOCATOR.detailed_snapshot();
    let allocs_per_op = snapshot.alloc_count as f64 / total_requests as f64;
    let net_bytes = snapshot.net_bytes();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();

    // Reclaim all resources to verify zero socket descriptor leaks
    upstream.clear_pool();
    stop_server.store(true, std::sync::atomic::Ordering::Relaxed);
    for sh in server_handles {
        sh.abort();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let post_fds = count_open_fds();

    println!(
        "| {:<24} | {:<10} | {:<14} | {:<16} | {:<14} | {:<12} | {:<10} |",
        "Workload Phase",
        "Requests",
        "Elapsed Time",
        "Aggregate Ops/s",
        "Allocs / Op",
        "Net Heap Leak",
        "FD Diff"
    );
    println!(
        "|{:-<26}|{:-<12}|{:-<16}|{:-<18}|{:-<16}|{:-<14}|{:-<12}|",
        "", "", "", "", "", "", ""
    );

    println!(
        "| {:<24} | {:<10} | {:<14} | {:<16} | {:<14.4} | {:<12} | {:<10} |",
        "500k Steady HIT",
        total_requests,
        format_duration(elapsed),
        format!("{:.0} ops/s", throughput),
        allocs_per_op,
        format_bytes(net_bytes.max(0) as usize),
        format!("{:+}", post_fds as i64 - baseline_fds as i64)
    );

    // REGRESSION GATES
    assert!(
        allocs_per_op < 0.005,
        "FATAL: Steady-state acquire/release must be virtually zero-alloc (got {allocs_per_op})!"
    );
    assert_eq!(
        post_fds, baseline_fds,
        "FATAL: File descriptor leak detected in steady state ({post_fds} vs {baseline_fds})!"
    );
    println!("\n  ==> [PASS] Zero-allocation and zero-socket leak invariant verified.\n");
}

// ============================================================================
// Stage 2: Client Abort / Cancellation Storm (RAII Drop Leak Audit)
// ============================================================================

async fn bench_abort_storm_regression() {
    println!("### 2. Client Cancellation Storm (50,000 Tasks, 50% Aborted Mid-Flight)\n");

    let baseline_fds = count_open_fds();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let stop_server = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop_server);
    let server_handle = tokio::spawn(async move {
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok((mut socket, _)) = listener.accept().await {
                let stop_inner = Arc::clone(&stop_clone);
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    let mut buf = [0u8; 1];
                    while !stop_inner.load(std::sync::atomic::Ordering::Relaxed) {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                });
            }
        }
    });

    let discovery = Discovery::new_explicit(vec![Endpoint::new("abort-ep", addr, 1)]);
    let connector = Arc::new(TcpConnector);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Arc::new(Upstream::with_connector(
        "abort-up", "tcp", discovery, timeouts, connector,
    ));

    // Warm-up connection
    let l = upstream.acquire().await.unwrap();
    l.release(true);

    let total_tasks = 50_000;
    let batch_size = 250;
    let batches = total_tasks / batch_size;
    let start = Instant::now();

    let mut aborted_count = 0;
    let mut completed_count = 0;

    for _ in 0..batches {
        let mut handles = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let up = Arc::clone(&upstream);
            let handle = tokio::spawn(async move {
                if let Ok(lease) = up.acquire().await {
                    tokio::task::yield_now().await;
                    lease.release(true);
                }
            });

            if i % 2 == 0 {
                handle.abort();
            }
            handles.push(handle);
        }

        for h in handles {
            match h.await {
                Ok(_) => completed_count += 1,
                Err(e) if e.is_cancelled() => aborted_count += 1,
                Err(e) => panic!("Unexpected task panic: {e:?}"),
            }
        }
    }

    // Let any asynchronous drops settle
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Clean up all resources
    upstream.clear_pool();
    stop_server.store(true, std::sync::atomic::Ordering::Relaxed);
    server_handle.abort();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let post_fds = count_open_fds();
    let elapsed = start.elapsed();

    println!(
        "| {:<20} | {:<12} | {:<12} | {:<14} | {:<14} | {:<12} |",
        "Scenario", "Total Tasks", "Aborted (50%)", "Completed", "Elapsed Time", "FD Leak"
    );
    println!(
        "|{:-<22}|{:-<14}|{:-<14}|{:-<16}|{:-<16}|{:-<14}|",
        "", "", "", "", "", ""
    );

    println!(
        "| {:<20} | {:<12} | {:<12} | {:<14} | {:<14} | {:<12} |",
        "Task Abort Storm",
        total_tasks,
        aborted_count,
        completed_count,
        format_duration(elapsed),
        format!("{:+}", post_fds as i64 - baseline_fds as i64)
    );

    // REGRESSION GATE: Aborting tasks must not leak socket descriptors on the host OS
    assert_eq!(
        post_fds, baseline_fds,
        "FATAL: Socket descriptor leak detected after abort storm ({post_fds} vs {baseline_fds})!"
    );
    println!("\n  ==> [PASS] RAII BackendLease drop guard reclaimed 100% of aborted sockets.\n");
}

// ============================================================================
// Stage 3: Dynamic Pod Churn (Health Tracker Pruning & Zero RAM Growth)
// ============================================================================

async fn bench_pod_churn_pruning_regression() {
    println!("### 3. Ephemeral Pod Churn (Health State Pruning & RAM Bound Audit)\n");

    let initial_count = 32;
    let endpoints: Vec<Endpoint> = (0..initial_count)
        .map(|i| {
            let addr: SocketAddr = format!("10.1.0.{}:8080", i + 1).parse().unwrap();
            Endpoint::new(format!("pod-{i}"), addr, 1)
        })
        .collect();

    let discovery = Discovery::new_explicit(endpoints);
    let connector = Arc::new(MockConnector::new());
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(100), Duration::from_secs(30));
    let health_config = HealthConfig::passive_only(1, Duration::from_secs(60));

    let upstream = Arc::new(
        Upstream::with_connector("churn-audit", "tcp", discovery.clone(), timeouts, connector)
            .with_health_config(health_config),
    );

    let churn_cycles = 1_000;
    let start = Instant::now();

    for cycle in 1..=churn_cycles {
        let new_eps: Vec<Endpoint> = (0..initial_count)
            .map(|i| {
                let octet2 = ((cycle >> 8) & 0xff) as u8;
                let octet3 = (cycle & 0xff) as u8;
                let addr: SocketAddr = format!("10.{octet2}.{octet3}.{}:8080", i + 1)
                    .parse()
                    .unwrap();
                Endpoint::new(format!("pod-{cycle}-{i}"), addr, 1)
            })
            .collect();

        // Simulate failures on retiring pods to create health records
        for ep in new_eps.iter().take(8) {
            upstream.health().record_failure(&ep.address);
        }

        // Discovery rolls out new pods
        discovery.update_endpoints(new_eps, cycle as u64);

        // Maintenance task prunes dead endpoints
        upstream.prune_retired_endpoints();
    }

    let elapsed = start.elapsed();
    let tracked_records_after = upstream.health().records_count();

    println!(
        "| {:<20} | {:<16} | {:<16} | {:<16} | {:<14} |",
        "Churn Cycles", "Total Pods Cycled", "Active Pods", "Retained Records", "Elapsed Time"
    );
    println!(
        "|{:-<22}|{:-<18}|{:-<18}|{:-<18}|{:-<16}|",
        "", "", "", "", ""
    );

    println!(
        "| {:<20} | {:<16} | {:<16} | {:<16} | {:<14} |",
        churn_cycles,
        churn_cycles * initial_count,
        initial_count,
        tracked_records_after,
        format_duration(elapsed)
    );

    // REGRESSION GATE: Retained health records must NEVER exceed the active endpoint count!
    assert!(
        tracked_records_after <= initial_count,
        "FATAL: Memory leak in HealthTracker! {tracked_records_after} records retained for {initial_count} active pods"
    );
    println!("\n  ==> [PASS] Ephemeral endpoint pruning guarantees bounded memory.\n");
}

// ============================================================================
// Stage 4: Idle Connection Eviction & Sweeper (File Descriptor Reclamation)
// ============================================================================

async fn bench_idle_eviction_regression() {
    println!("### 4. Idle Connection Eviction & Sweeper (Kernel Socket Reclamation)\n");

    let port_count = 16;
    let mut listeners = Vec::with_capacity(port_count);
    let mut endpoints = Vec::with_capacity(port_count);

    for i in 0..port_count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        endpoints.push(Endpoint::new(format!("evict-ep-{i}"), addr, 1));
        listeners.push(listener);
    }

    let stop_server = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut server_handles = Vec::with_capacity(port_count);

    for listener in listeners {
        let stop_clone = Arc::clone(&stop_server);
        server_handles.push(tokio::spawn(async move {
            let mut conns = Vec::new();
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok((socket, _)) = listener.accept().await {
                    conns.push(socket);
                }
            }
        }));
    }

    let discovery = Discovery::new_explicit(endpoints.clone());
    let connector = Arc::new(TcpConnector);
    let idle_timeout = Duration::from_millis(20);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), idle_timeout);
    let upstream = Arc::new(Upstream::with_connector(
        "evict-up", "tcp", discovery, timeouts, connector,
    ));

    // Open and pool 16 warm connections across all 16 endpoints
    for ep in &endpoints {
        let target =
            velda_upstream::AcquireTarget::default_target().with_hash_key(ep.address.port() as u64);
        if let Ok(lease) = upstream.acquire_with_target(target).await {
            lease.release(true);
        }
    }

    let fds_with_open_pool = count_open_fds();

    // Sleep past the 20ms idle timeout
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Trigger active sweep
    let (evicted, pruned) = upstream.sweep_idle();

    // Allow kernel to finish closing sockets
    tokio::time::sleep(Duration::from_millis(20)).await;
    let fds_after_sweep = count_open_fds();

    stop_server.store(true, std::sync::atomic::Ordering::Relaxed);
    for sh in server_handles {
        sh.abort();
    }

    println!(
        "| {:<18} | {:<16} | {:<16} | {:<14} | {:<16} |",
        "Active Endpoints",
        "Conns Evicted",
        "Subpools Pruned",
        "FDs Before Sweep",
        "FDs After Sweep"
    );
    println!(
        "|{:-<20}|{:-<18}|{:-<18}|{:-<16}|{:-<18}|",
        "", "", "", "", ""
    );

    println!(
        "| {:<18} | {:<16} | {:<16} | {:<14} | {:<16} |",
        port_count, evicted, pruned, fds_with_open_pool, fds_after_sweep
    );

    // REGRESSION GATES
    assert_eq!(
        evicted, port_count,
        "FATAL: All idle connections must be evicted!"
    );
    assert!(
        fds_after_sweep < fds_with_open_pool,
        "FATAL: Kernel socket descriptors must decrease after idle sweep!"
    );
    println!(
        "\n  ==> [PASS] Idle connection sweep released {evicted} kernel socket descriptors.\n"
    );
}

#[tokio::main]
async fn main() {
    println!("===============================================================================");
    println!("     velda-upstream: Resource Regression & Leak Audit Benchmark Suite          ");
    println!("===============================================================================\n");

    bench_steady_state_resource_regression().await;
    bench_abort_storm_regression().await;
    bench_pod_churn_pruning_regression().await;
    bench_idle_eviction_regression().await;

    println!("===============================================================================");
    println!("     [ALL 4 RESOURCE REGRESSION AUDIT GATES PASSED CLEANLY]                    ");
    println!("===============================================================================");
}
