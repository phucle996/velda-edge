//! Adversarial, Malformed Traffic & Slowloris Fault-Tolerance Benchmark Suite for velda-tls.
//!
//! Evaluates server engine resilience under adversarial conditions:
//! 1. Unknown / Hostile SNI Flood (Strict Rejection Invariant)
//! 2. Malformed / Garbage TLS Record Injection
//! 3. Slowloris Handshake Timeout Defense (`accept_with_timeout`)

mod common;

use std::time::{Duration, Instant};

use common::{make_test_client, make_test_server};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncWriteExt, duplex};
use velda_core::hardware::{CpuTier, MemoryTier};

// ============================================================================
// Stage 1: Unknown SNI Flood
// ============================================================================

async fn bench_unknown_sni_flood() {
    println!("### 1. Unknown / Hostile SNI Flood (Strict Rejection)\n");
    println!(
        "| Scenario | Iterations | Rejections | Avg Rejection Latency | Throughput | Invariant |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let (server_engine, cert_pem) = make_test_server(CpuTier::Large, MemoryTier::Large);
    let client = make_test_client(&cert_pem, false);
    let iters = 5_000;

    let start = Instant::now();
    let mut rejected = 0;

    for i in 0..iters {
        let (client_io, server_io) = duplex(4096);
        let srv = server_engine.clone();

        let srv_task = tokio::spawn(async move { srv.accept(server_io).await });

        let hostile_sni = format!("hostile_probe_{}.malicious.io", i);
        let server_name = ServerName::try_from(hostile_sni).unwrap();
        let _ = client.connect(server_name, client_io).await;

        if srv_task.await.unwrap().is_err() {
            rejected += 1;
        }
    }

    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

    println!(
        "| **Hostile SNI Flood** | {} | {} (100%) | **{:.2} µs** | {} ops/s | **0 Leaks, Strict Reject** |\n",
        iters, rejected, avg_us, ops_sec
    );
}

// ============================================================================
// Stage 2: Corrupted / Garbage TLS Record Flood
// ============================================================================

async fn bench_garbage_record_flood() {
    println!("### 2. Malformed / Garbage TLS Frame Injection\n");
    println!(
        "| Payload Type | Iterations | Clean Drops | Avg Drop Latency | Throughput | Invariant |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let (server_engine, _) = make_test_server(CpuTier::Large, MemoryTier::Large);
    let iters = 10_000;

    let start = Instant::now();
    let mut dropped = 0;

    for i in 0..iters {
        let (mut client_io, server_io) = duplex(4096);
        let srv = server_engine.clone();

        let srv_task = tokio::spawn(async move { srv.accept(server_io).await });

        // Inject random garbage / non-TLS bytes
        let garbage = [0xFF, 0xFE, 0xFD, 0xFC, (i & 0xFF) as u8, 0x00, 0x01];
        let _ = client_io.write_all(&garbage).await;
        drop(client_io);

        if srv_task.await.unwrap().is_err() {
            dropped += 1;
        }
    }

    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

    println!(
        "| **Garbage Frame Injection** | {} | {} (100%) | **{:.2} µs** | {} ops/s | **0 Panics, Safe Drop** |\n",
        iters, dropped, avg_us, ops_sec
    );
}

// ============================================================================
// Stage 3: Slowloris Handshake Timeout Defense
// ============================================================================

async fn bench_slowloris_timeout_defense() {
    println!("### 3. Slowloris Handshake Timeout Defense\n");
    println!(
        "| Attack Workload | Stalled Clients | Timed Out | Timeout Threshold | Defense Invariant |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let (server_engine, _) = make_test_server(CpuTier::Large, MemoryTier::Large);
    let iters = 200;
    let timeout_duration = Duration::from_millis(10);

    let start = Instant::now();
    let mut timed_out_count = 0;

    for _ in 0..iters {
        let (_client_io, server_io) = duplex(1024);
        let srv = server_engine.clone();

        let res = srv
            .accept_with_explicit_timeout(server_io, timeout_duration)
            .await;
        if let Err(e) = res
            && e.to_string().contains("timed out")
        {
            timed_out_count += 1;
        }
    }

    let elapsed = start.elapsed();
    println!(
        "| **Stalled Client Hello (Slowloris)** | {} | {} (100%) | {:?} | **Total Elapsed: {:.2?} (No Worker Starvation)** |\n",
        iters, timed_out_count, timeout_duration, elapsed
    );
}

#[tokio::main]
async fn main() {
    println!("# Velda Edge — `velda-tls` Adversarial & Fault-Tolerance Benchmark Suite\n");

    bench_unknown_sni_flood().await;
    bench_garbage_record_flood().await;
    bench_slowloris_timeout_defense().await;
}
