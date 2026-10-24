//! Integration test verifying Shadow Worker connection pre-warming across HTTP/1.1 and HTTP/2 upstreams.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use velda_connection_pool::PoolConfig;
use velda_core::{Endpoint, StreamingMode};
use velda_discovery::Discovery;
use velda_edge::upstream::http::{Http1Upstream, Http2Upstream};
use velda_edge::upstream::lb::{EdgeUpstream, LbAlgorithm};
use velda_upstream::UpstreamTimeouts;

#[tokio::test]
async fn test_http1_shadow_worker_warmup_end_to_end() {
    // 1. Start a simple mock HTTP/1.1 backend server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        for _ in 0..10 {
            if let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                let req = String::from_utf8_lossy(&buf[..n]);
                                if req.contains("GET") {
                                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK";
                                    let _ = stream.write_all(resp.as_bytes()).await;
                                }
                            }
                        }
                    }
                });
            }
        }
    });

    // 2. Set current thread lane to Worker 0
    velda_connection_pool::set_current_thread_lane(0);

    let endpoint = Endpoint::new("mock-ep", backend_addr, 1);
    let discovery = Discovery::new_explicit(vec![endpoint]);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_secs(2), Duration::from_secs(30));

    let edge_upstream = EdgeUpstream::new(
        "test-h1-upstream",
        "tcp",
        discovery,
        LbAlgorithm::RoundRobin(velda_lb::RoundRobin::new()),
        timeouts,
    );

    let h1_upstream = Http1Upstream::new(
        edge_upstream,
        None,
        StreamingMode::DISABLED,
        PoolConfig::default(),
        4,
        velda_http1::Http1AccelerationPath::default(),
        None,
    );

    // Initial state: lane 0 has 0 idle connections
    assert_eq!(h1_upstream.idle_conns_for_lane(&backend_addr, 0), 0);

    // 3. Shadow Worker (Logistics) pre-warms 2 connections for Lane 0
    h1_upstream.prewarm_lane(0, 2).await;
    assert_eq!(h1_upstream.idle_conns_for_lane(&backend_addr, 0), 2);

    // 4. Traffic Worker (Frontline) acquires connection on the hot path -> instant reuse!
    let lease1 = h1_upstream
        .acquire()
        .await
        .expect("First acquire must succeed");
    assert!(
        lease1.is_reused,
        "Frontline acquire must hit pre-warmed connection placed by Shadow Worker!"
    );

    // One connection remains in pool
    assert_eq!(h1_upstream.idle_conns_for_lane(&backend_addr, 0), 1);

    // Second acquire: also an instant hit!
    let lease2 = h1_upstream
        .acquire()
        .await
        .expect("Second acquire must succeed");
    assert!(
        lease2.is_reused,
        "Second acquire must hit second pre-warmed connection!"
    );

    // Pool for lane 0 is now depleted
    assert_eq!(h1_upstream.idle_conns_for_lane(&backend_addr, 0), 0);

    // Third acquire: cold fallback (connects fresh on demand)
    let lease3 = h1_upstream
        .acquire()
        .await
        .expect("Third acquire must succeed");
    assert!(
        !lease3.is_reused,
        "Third acquire connects on demand when pre-warmed buffer is depleted!"
    );

    drop(lease1);
    drop(lease2);
    drop(lease3);
    server_task.abort();
}

#[tokio::test]
async fn test_http2_shadow_worker_warmup_end_to_end() {
    // 1. Start a simple cleartext h2c mock backend server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                if let Ok(mut h2) = h2::server::handshake(stream).await {
                    while let Some(Ok((req, mut respond))) = h2.accept().await {
                        let _ = req;
                        let response = http::Response::builder().status(200).body(()).unwrap();
                        if let Ok(mut send_stream) = respond.send_response(response, false) {
                            let _ = send_stream.send_data(bytes::Bytes::from_static(b"OK"), true);
                        }
                    }
                }
            });
        }
    });

    // 2. Set current thread lane to Worker 0
    velda_connection_pool::set_current_thread_lane(0);

    let endpoint = Endpoint::new("mock-h2-ep", backend_addr, 1);
    let discovery = Discovery::new_explicit(vec![endpoint]);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_secs(2), Duration::from_secs(30));

    let edge_upstream = EdgeUpstream::new(
        "test-h2-upstream",
        "tcp",
        discovery,
        LbAlgorithm::RoundRobin(velda_lb::RoundRobin::new()),
        timeouts,
    );

    let h2_upstream = Http2Upstream::new(
        edge_upstream,
        None,
        StreamingMode::DISABLED,
        4,
        4,
        100,
        velda_http2::Http2AccelerationPath::default(),
    );

    let config = velda_http2::Http2Config::auto();

    // Initial state: lane 0 has 0 multiplexed connections
    assert_eq!(h2_upstream.connection_count_for_lane(&backend_addr, 0), 0);

    // 3. Shadow Worker (Logistics) pre-warms multiplexed connection for Lane 0
    h2_upstream.prewarm_lane(0, &config).await;
    assert_eq!(h2_upstream.connection_count_for_lane(&backend_addr, 0), 1);

    // 4. Traffic Worker (Frontline) acquires multiplexed stream slot on the hot path
    let (ready_client, lease) = h2_upstream
        .acquire(&config)
        .await
        .expect("Acquire must succeed");
    assert_eq!(
        lease.active_streams(),
        1,
        "Acquire should reuse pre-warmed multiplexed connection"
    );

    // Still exactly 1 physical connection in lane 0 (stream multiplexed on existing conn)
    assert_eq!(h2_upstream.connection_count_for_lane(&backend_addr, 0), 1);

    drop(ready_client);
    drop(lease);
    server_task.abort();
}
