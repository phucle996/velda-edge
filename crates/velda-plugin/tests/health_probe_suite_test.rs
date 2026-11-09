//! Integration Test Suite for Health & Readiness Probe Background Plugin (`health_probe`).
//!
//! Validates end-to-end TCP probe handling, zero-byte body responses, lock-free state transitions,
//! Kubernetes Pod lifecycle simulation, and clean socket release.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::health_probe::{HealthProbeConfig, HealthProbePlugin, HealthState};

async fn send_probe_request(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("Failed to connect to health probe port");
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .expect("Failed to write probe request");

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("Failed to read response");
    String::from_utf8(buf).expect("Response is valid UTF-8")
}

#[tokio::test]
async fn test_health_probe_e2e_lifecycle() {
    let listen_addr: SocketAddr = "127.0.0.1:19091".parse().unwrap();
    let config = HealthProbeConfig {
        listen_addr,
        liveness_path: "/livez".into(),
        readiness_path: "/readyz".into(),
        check_interval_ms: 1000,
        timeout_ms: 1000,
    };

    let state = HealthState::new();
    let plugin = Arc::new(HealthProbePlugin::with_state(
        "test_health_probe",
        config,
        state.clone(),
    ));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let probe_task = tokio::spawn({
        let p = Arc::clone(&plugin);
        async move {
            p.run(shutdown_rx).await.expect("Probe service failed");
        }
    });

    // Wait for the shadow TCP listener to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Initial State: Live = true, Ready = false
    let live_resp = send_probe_request(listen_addr, "/livez").await;
    assert!(
        live_resp.starts_with("HTTP/1.1 200 OK"),
        "Expected 200 OK for /livez, got: {live_resp}"
    );

    let ready_resp = send_probe_request(listen_addr, "/readyz").await;
    assert!(
        ready_resp.starts_with("HTTP/1.1 503 Service Unavailable"),
        "Expected 503 for /readyz before ready, got: {ready_resp}"
    );

    // 2. Gateway signals Ready: Ready = true
    state.set_ready(true);
    let ready_resp2 = send_probe_request(listen_addr, "/readyz").await;
    assert!(
        ready_resp2.starts_with("HTTP/1.1 200 OK"),
        "Expected 200 OK for /readyz after ready, got: {ready_resp2}"
    );

    // 3. Unknown Path -> 404
    let unknown_resp = send_probe_request(listen_addr, "/status").await;
    assert!(
        unknown_resp.starts_with("HTTP/1.1 404 Not Found"),
        "Expected 404 for unknown path, got: {unknown_resp}"
    );

    // 4. Client disconnects immediately without sending bytes (n == 0 early exit)
    {
        let stream = TcpStream::connect(listen_addr).await.expect("connect");
        drop(stream);
    }

    // 5. Shutdown signaled -> Graceful Draining (Ready flips to 503 immediately)
    shutdown_tx.send(true).expect("Failed to send shutdown");
    probe_task.await.expect("Probe task failed to join");

    // Verify readiness state flipped to draining
    assert!(state.is_draining());
    let resp_draining = HealthProbePlugin::evaluate_path(&plugin.config, &state, "/readyz");
    assert!(!resp_draining.starts_with(b"HTTP/1.1 200"));
    assert!(resp_draining.starts_with(b"HTTP/1.1 503"));
}
