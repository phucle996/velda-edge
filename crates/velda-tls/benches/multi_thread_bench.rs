//! Multi-Thread Concurrency & Scalability Benchmark Suite for velda-tls.
//!
//! Evaluates downstream TLS termination scalability under concurrent multi-core load:
//! 1. Concurrent TLS 1.3 Handshake Scaling across 1, 2, 4, 8, 16, 32 Threads
//! 2. Concurrent Session Resumption Cache Contention under Parallel Resumption

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::{make_test_client, make_test_server};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_core::hardware::{CpuTier, MemoryTier};

async fn run_concurrent_handshakes(
    threads: usize,
    iters_per_thread: usize,
    resumed: bool,
) -> (f64, u64) {
    let (server_engine, cert_pem) = make_test_server(CpuTier::Ultra, MemoryTier::Ultra);
    let srv_arc = Arc::new(server_engine);
    let client = Arc::new(make_test_client(&cert_pem, resumed));

    // Warm up session cache if testing resumption
    if resumed {
        let (client_io, server_io) = duplex(65536);
        let srv = srv_arc.clone();
        let srv_task = tokio::spawn(async move {
            let mut s = srv.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(b"pong").await.unwrap();
        });
        let server_name = ServerName::try_from("api.example.com".to_string()).unwrap();
        let mut cli = client.connect(server_name, client_io).await.unwrap();
        cli.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        cli.read_exact(&mut resp).await.unwrap();
        srv_task.await.unwrap();
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(threads);

    for _ in 0..threads {
        let srv = srv_arc.clone();
        let cli_connector = client.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..iters_per_thread {
                let (client_io, server_io) = duplex(65536);
                let srv_clone = srv.clone();
                let srv_task = tokio::spawn(async move {
                    let mut s = srv_clone.accept(server_io).await.unwrap();
                    let mut buf = [0u8; 4];
                    s.read_exact(&mut buf).await.unwrap();
                    s.write_all(b"pong").await.unwrap();
                });

                let server_name = ServerName::try_from("api.example.com".to_string()).unwrap();
                let mut cli = cli_connector.connect(server_name, client_io).await.unwrap();
                cli.write_all(b"ping").await.unwrap();
                let mut resp = [0u8; 4];
                cli.read_exact(&mut resp).await.unwrap();
                srv_task.await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_iters = (threads * iters_per_thread) as f64;
    let avg_us = elapsed.as_micros() as f64 / total_iters;
    let ops_sec = (total_iters / elapsed.as_secs_f64()) as u64;
    (avg_us, ops_sec)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("# Velda Edge — `velda-tls` Multi-Thread Scalability Benchmark\n");

    let thread_counts = [1, 2, 4, 8, 16];
    let iters_per_thread = 500;

    println!("### 1. Concurrent Full TLS 1.3 Handshake Scaling\n");
    println!(
        "| Threads | Total Handshakes | Avg Latency / Handshake | Throughput | Scaling Efficiency |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let mut baseline_ops = 0;
    for &threads in &thread_counts {
        let (avg_us, ops_sec) = run_concurrent_handshakes(threads, iters_per_thread, false).await;
        if threads == 1 {
            baseline_ops = ops_sec;
        }
        let speedup = if baseline_ops > 0 {
            ops_sec as f64 / baseline_ops as f64
        } else {
            1.0
        };
        println!(
            "| **{} threads** | {} | **{:.2} µs** | **{} hsk/s** | **{:.2}x** |",
            threads,
            threads * iters_per_thread,
            avg_us,
            ops_sec,
            speedup
        );
    }

    println!("\n### 2. Concurrent TLS 1.3 Session Resumption Scaling\n");
    println!(
        "| Threads | Total Handshakes | Avg Latency / Handshake | Throughput | Scaling Efficiency |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let mut baseline_resumed_ops = 0;
    for &threads in &thread_counts {
        let (avg_us, ops_sec) = run_concurrent_handshakes(threads, iters_per_thread, true).await;
        if threads == 1 {
            baseline_resumed_ops = ops_sec;
        }
        let speedup = if baseline_resumed_ops > 0 {
            ops_sec as f64 / baseline_resumed_ops as f64
        } else {
            1.0
        };
        println!(
            "| **{} threads** | {} | **{:.2} µs** | **{} hsk/s** | **{:.2}x** |",
            threads,
            threads * iters_per_thread,
            avg_us,
            ops_sec,
            speedup
        );
    }
}
