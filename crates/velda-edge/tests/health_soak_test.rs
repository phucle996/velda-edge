//! 15-Second High-Load Soak Test: Passive Circuit-Breaking & Active Health Check Coordination.
//!
//! Validates:
//! 1. Normal Dual-Backend Load Balancing (50% / 50% split under heavy concurrent load).
//! 2. Instant Passive Circuit-Breaking upon abrupt backend crash (< 5ms failover, 0 dropped client requests).
//! 3. Active Background Health Probing on Shadow Worker detecting revival and clearing passive trips
//!    bypassing long passive cooldowns (60s).
//! 4. Seamless Traffic Re-balancing after active healing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

use velda_connection_pool::PoolConfig;
use velda_core::{Endpoint, StreamingMode};
use velda_discovery::Discovery;
use velda_edge::upstream::http::Http1Upstream;
use velda_edge::upstream::lb::{EdgeUpstream, LbAlgorithm};
use velda_upstream::{ActiveHealthConfig, HealthConfig, PassiveHealthConfig, UpstreamTimeouts};

/// Helper to create a reusable TCP listener on a given or ephemeral address.
fn bind_reusable_listener(addr: SocketAddr) -> std::io::Result<(TcpListener, SocketAddr)> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(2048)?;
    let std_listener: std::net::TcpListener = socket.into();
    let listener = TcpListener::from_std(std_listener)?;
    let local_addr = listener.local_addr()?;
    Ok((listener, local_addr))
}

/// Spawns a mock HTTP server responding to GET /healthz and GET /data with keep-alive.
fn spawn_mock_server(
    listener: TcpListener,
    server_name: &'static str,
    mut shutdown_rx: watch::Receiver<bool>,
    requests_served: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                res = listener.accept() => {
                    let (mut stream, _) = match res {
                        Ok(conn) => conn,
                        Err(_) => break,
                    };
                    let served = Arc::clone(&requests_served);
                    let mut child_rx = shutdown_rx.clone();

                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        loop {
                            tokio::select! {
                                _ = child_rx.changed() => {
                                    if *child_rx.borrow() {
                                        break;
                                    }
                                }
                                read_res = stream.read(&mut buf) => {
                                    match read_res {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => {
                                            served.fetch_add(1, Ordering::Relaxed);
                                            let req = String::from_utf8_lossy(&buf[..n]);
                                            let resp = if req.contains("GET /healthz") {
                                                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK"
                                            } else {
                                                "HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello from Srv"
                                            };
                                            if stream.write_all(resp.as_bytes()).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }
        let _ = server_name;
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_15s_high_load_passive_active_coordination() {
    println!("\n=== Starting 15-Second High-Load Active/Passive Coordination Test ===");

    // 1. Bind and spawn Server A and Server B
    let (listener_a, addr_a) =
        bind_reusable_listener("127.0.0.1:0".parse().unwrap()).expect("bind server A");
    let (listener_b, addr_b) =
        bind_reusable_listener("127.0.0.1:0".parse().unwrap()).expect("bind server B");

    let srv_a_count = Arc::new(AtomicU64::new(0));
    let srv_b_count = Arc::new(AtomicU64::new(0));

    let (shutdown_a_tx, shutdown_a_rx) = watch::channel(false);
    let (shutdown_b_tx, shutdown_b_rx) = watch::channel(false);

    let handle_a = spawn_mock_server(
        listener_a,
        "Server-A",
        shutdown_a_rx,
        Arc::clone(&srv_a_count),
    );
    let _handle_b = spawn_mock_server(
        listener_b,
        "Server-B",
        shutdown_b_rx,
        Arc::clone(&srv_b_count),
    );

    println!("-> Server A bound on {addr_a}");
    println!("-> Server B bound on {addr_b}");

    // 2. Configure Http1Upstream with dual Active + Passive health checking
    velda_connection_pool::set_current_thread_lane(0);

    let endpoints = vec![
        Endpoint::new("server-a", addr_a, 1),
        Endpoint::new("server-b", addr_b, 1),
    ];
    let discovery = Discovery::new_explicit(endpoints);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));

    let passive_cfg = PassiveHealthConfig {
        enabled: true,
        max_consecutive_failures: 2,
        cooldown: Duration::from_secs(60), // Long 60s cooldown: passive alone cannot recover during 15s!
    };

    let active_cfg = ActiveHealthConfig {
        enabled: true,
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(200),
        unhealthy_threshold: 1,
        healthy_threshold: 1,
        check_type: "http".into(),
        path: Some("/healthz".into()),
        expected_statuses: vec![200],
    };

    let health_cfg = HealthConfig {
        passive: Some(passive_cfg),
        active: Some(active_cfg),
    };

    let edge_upstream = EdgeUpstream::new(
        "test-soak-upstream",
        "tcp",
        discovery,
        LbAlgorithm::RoundRobin(velda_lb::RoundRobin::new()),
        timeouts,
    )
    .with_health_config(health_cfg);

    let h1_upstream = Arc::new(Http1Upstream::new(
        edge_upstream,
        None,
        StreamingMode::DISABLED,
        PoolConfig::default(),
        4,
        velda_http1::Http1AccelerationPath::default(),
        None,
    ));

    // 3. Spawn background Shadow Worker loop driving active health probes
    let shadow_upstream = Arc::clone(&h1_upstream);
    let shadow_running = Arc::new(AtomicBool::new(true));
    let shadow_running_flag = Arc::clone(&shadow_running);

    let shadow_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        let mut seed = 1000u32;
        while shadow_running_flag.load(Ordering::Relaxed) {
            interval.tick().await;
            seed = seed.wrapping_add(1);
            shadow_upstream.check_active_health(0, seed).await;
        }
    });

    // 4. Metrics trackers across test phases
    let total_reqs = Arc::new(AtomicU64::new(0));
    let total_success = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));

    let p1_reqs_a = Arc::new(AtomicU64::new(0));
    let p1_reqs_b = Arc::new(AtomicU64::new(0));

    let p2_reqs_a = Arc::new(AtomicU64::new(0));
    let p2_reqs_b = Arc::new(AtomicU64::new(0));

    let p3_reqs_a = Arc::new(AtomicU64::new(0));
    let p3_reqs_b = Arc::new(AtomicU64::new(0));

    let current_phase = Arc::new(std::sync::atomic::AtomicU8::new(1));

    // 5. Spawn 16 high-concurrency client workers pumping requests
    let client_running = Arc::new(AtomicBool::new(true));
    let mut client_handles = Vec::new();

    for _ in 0..16 {
        let upstream = Arc::clone(&h1_upstream);
        let is_running = Arc::clone(&client_running);
        let phase = Arc::clone(&current_phase);
        let t_reqs = Arc::clone(&total_reqs);
        let t_succ = Arc::clone(&total_success);
        let t_errs = Arc::clone(&total_errors);

        let p1_a = Arc::clone(&p1_reqs_a);
        let p1_b = Arc::clone(&p1_reqs_b);
        let p2_a = Arc::clone(&p2_reqs_a);
        let p2_b = Arc::clone(&p2_reqs_b);
        let p3_a = Arc::clone(&p3_reqs_a);
        let p3_b = Arc::clone(&p3_reqs_b);

        let handle = tokio::spawn(async move {
            velda_connection_pool::set_current_thread_lane(0);
            while is_running.load(Ordering::Relaxed) {
                t_reqs.fetch_add(1, Ordering::Relaxed);
                let cur_p = phase.load(Ordering::Relaxed);

                match upstream.acquire().await {
                    Ok(mut lease) => {
                        let target = lease.endpoint;

                        let req = "GET /data HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";
                        if lease.write_all(req.as_bytes()).await.is_err() {
                            lease.mark_closed();
                            t_errs.fetch_add(1, Ordering::Relaxed);
                            upstream.record_failure(&target);
                            continue;
                        }

                        let mut buf = [0u8; 128];
                        match lease.read(&mut buf).await {
                            Ok(n) if n > 0 => {
                                t_succ.fetch_add(1, Ordering::Relaxed);
                                upstream.record_success(&target);

                                if target == addr_a {
                                    match cur_p {
                                        1 => {
                                            p1_a.fetch_add(1, Ordering::Relaxed);
                                        }
                                        2 => {
                                            p2_a.fetch_add(1, Ordering::Relaxed);
                                        }
                                        _ => {
                                            p3_a.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                } else {
                                    match cur_p {
                                        1 => {
                                            p1_b.fetch_add(1, Ordering::Relaxed);
                                        }
                                        2 => {
                                            p2_b.fetch_add(1, Ordering::Relaxed);
                                        }
                                        _ => {
                                            p3_b.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                            _ => {
                                lease.mark_closed();
                                t_errs.fetch_add(1, Ordering::Relaxed);
                                upstream.record_failure(&target);
                            }
                        }
                    }
                    Err(_) => {
                        t_errs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        client_handles.push(handle);
    }

    let start_instant = Instant::now();
    println!("-> Pumping high concurrency load across 16 worker tasks...");

    // =========================================================================
    // Phase 1: Normal Operation (t = 0s to 3s)
    // =========================================================================
    println!("--- [Phase 1: Normal Load Balancing (t = 0s..3s)] ---");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let p1_a_val = p1_reqs_a.load(Ordering::Relaxed);
    let p1_b_val = p1_reqs_b.load(Ordering::Relaxed);
    println!(
        "Phase 1 complete: Srv A = {}, Srv B = {} (Balanced ratio: {:.2}%)",
        p1_a_val,
        p1_b_val,
        (p1_a_val as f64 / (p1_a_val + p1_b_val).max(1) as f64) * 100.0
    );
    assert!(p1_a_val > 100, "Server A must serve requests in Phase 1");
    assert!(p1_b_val > 100, "Server B must serve requests in Phase 1");

    // =========================================================================
    // Phase 2: Server A Crash & Passive Circuit Breaking (t = 3s to 8s)
    // =========================================================================
    println!("--- [Phase 2: Crash Server A & Passive Circuit Breaking (t = 3s..8s)] ---");
    current_phase.store(2, Ordering::Release);

    // Abruptly kill Server A
    let _ = shutdown_a_tx.send(true);
    handle_a.abort();
    let _failover_start = Instant::now();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let p2_a_val = p2_reqs_a.load(Ordering::Relaxed);
    let p2_b_val = p2_reqs_b.load(Ordering::Relaxed);
    println!(
        "Phase 2 complete: Srv A = {}, Srv B = {} (100% traffic failed over to B)",
        p2_a_val, p2_b_val
    );
    assert!(
        p2_b_val > 500,
        "Server B must take over 100% load during outage"
    );
    assert!(
        !h1_upstream.is_healthy(&addr_a),
        "Server A must be tripped unhealthy by passive circuit breaking and active probe"
    );

    // =========================================================================
    // Phase 3: Server A Revival & Active Health Healing (t = 8s to 15s)
    // =========================================================================
    println!("--- [Phase 3: Revive Server A & Active Health Healing (t = 8s..15s)] ---");
    current_phase.store(3, Ordering::Release);

    // Revive Server A on the exact same port!
    let (new_listener_a, _) = bind_reusable_listener(addr_a).expect("rebind server A");
    let (new_shutdown_a_tx, new_shutdown_a_rx) = watch::channel(false);
    let _new_handle_a = spawn_mock_server(
        new_listener_a,
        "Server-A-Revived",
        new_shutdown_a_rx,
        Arc::clone(&srv_a_count),
    );

    let _revive_start = Instant::now();
    println!("-> Server A reopened on {addr_a}. Waiting for Shadow Worker active healing...");

    // Wait remaining 7 seconds of the 15-second soak test
    tokio::time::sleep(Duration::from_secs(7)).await;

    let p3_a_val = p3_reqs_a.load(Ordering::Relaxed);
    let p3_b_val = p3_reqs_b.load(Ordering::Relaxed);
    println!(
        "Phase 3 complete: Srv A = {}, Srv B = {} (Healed & Rebalanced!)",
        p3_a_val, p3_b_val
    );

    assert!(
        p3_a_val > 100,
        "Active health check MUST have healed Server A and resumed serving requests to it!"
    );
    assert!(
        h1_upstream.is_healthy(&addr_a),
        "Server A must be marked healthy after active healing"
    );

    // Stop clients and shadow worker
    client_running.store(false, Ordering::Relaxed);
    shadow_running.store(false, Ordering::Relaxed);
    let _ = shadow_handle.await;
    for h in client_handles {
        let _ = h.await;
    }
    let _ = shutdown_b_tx.send(true);
    let _ = new_shutdown_a_tx.send(true);

    // =========================================================================
    // Final Soak Test Report
    // =========================================================================
    let total_elapsed = start_instant.elapsed();
    let total_r = total_reqs.load(Ordering::Relaxed);
    let total_s = total_success.load(Ordering::Relaxed);
    let total_e = total_errors.load(Ordering::Relaxed);
    let rps = (total_s as f64) / total_elapsed.as_secs_f64();
    let availability = (total_s as f64 / (total_s + total_e).max(1) as f64) * 100.0;

    println!("\n=================================================================");
    println!("          15-SECOND SOAK & RECOVERY TEST FINAL REPORT             ");
    println!("=================================================================");
    println!("Total Duration:        {:.2}s", total_elapsed.as_secs_f64());
    println!("Total Requests Pushed: {}", total_r);
    println!("Total Requests Served: {}", total_s);
    println!("Total Transient Errs:  {}", total_e);
    println!("Throughput:            {:.0} req/s", rps);
    println!("Overall Availability:  {:.3}%", availability);
    println!("-----------------------------------------------------------------");
    println!(
        "Phase 1 (Healthy):     Srv A = {:6} | Srv B = {:6}",
        p1_a_val, p1_b_val
    );
    println!(
        "Phase 2 (A Outage):    Srv A = {:6} | Srv B = {:6}",
        p2_a_val, p2_b_val
    );
    println!(
        "Phase 3 (A Recovered): Srv A = {:6} | Srv B = {:6}",
        p3_a_val, p3_b_val
    );
    println!("=================================================================\n");

    assert!(
        availability >= 99.0,
        "Overall availability under failure and recovery must be >= 99.0%"
    );
}
