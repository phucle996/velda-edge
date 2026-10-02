//! Single-Thread Performance, Latency & In-Memory Benchmark Suite for velda-tls.
//!
//! Evaluates hot-path single-thread performance:
//! 1. In-Memory SNI Resolver Lookup Latency across 10, 100, 1,000, and 5,000 Certificates
//! 2. Handshake Metadata Extraction Latency (`extract_handshake_info`)
//! 3. Downstream TLS 1.3 Handshake Latency & Throughput (In-Memory Duplex)
//! 4. TLS 1.3 Session Resumption (Tickets) vs Full Handshake
//! 5. QUIC ServerConfig Bridge Compilation (`build_quic_config`)

mod common;

use std::time::Instant;

use common::{CountingAllocator, build_test_sni_resolver, make_test_client, make_test_server};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_core::hardware::{CpuTier, MemoryTier};
use velda_tls::TlsServerEngine;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: SNI Resolver Lookup Scaling
// ============================================================================

fn bench_sni_resolver_scaling() {
    println!("### 1. In-Memory SNI Resolver Lookup Latency & Scaling\n");
    println!("| Table Size | Match Kind | Target SNI | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 500_000;
    let sizes = [10, 100, 1_000, 5_000];

    for &size in &sizes {
        let resolver = build_test_sni_resolver(size);

        // 1. Exact match hit
        let exact_sni = format!("service_{:04}.domain.com", size / 2);
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let res = resolver.lookup(&exact_sni);
            let _ = std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| **N = {}** | **Exact Hit** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            size,
            exact_sni,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );

        // 2. Wildcard match hit
        let wildcard_sni = format!("api.cluster_{:04}.internal", size / 2);
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let res = resolver.lookup(&wildcard_sni);
            let _ = std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| **N = {}** | **Wildcard Hit** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            size,
            wildcard_sni,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );

        // 3. Unknown SNI Miss (Strict Reject)
        let miss_sni = "unknown.attacker.invalid";
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let res = resolver.lookup(miss_sni);
            let _ = std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| **N = {}** | **Miss (Reject)** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            size,
            miss_sni,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );
    }
    println!();
}

// ============================================================================
// Stage 2: Handshake Metadata Extraction Latency
// ============================================================================

async fn bench_metadata_extraction() {
    println!("### 2. Handshake Metadata Extraction Latency\n");
    println!("| Operation | Iterations | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let (server_engine, cert_pem) = make_test_server(CpuTier::Medium, MemoryTier::Medium);
    let client = make_test_client(&cert_pem, false);

    let (client_io, server_io) = duplex(65536);
    let srv = server_engine.clone();

    let srv_task = tokio::spawn(async move { srv.accept(server_io).await.unwrap() });

    let server_name = ServerName::try_from("api.example.com".to_string()).unwrap();
    let _client_stream = client.connect(server_name, client_io).await.unwrap();
    let tls_stream = srv_task.await.unwrap();

    let iters = 1_000_000;
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let info = TlsServerEngine::extract_handshake_info(&tls_stream);
        let _ = std::hint::black_box(info);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

    println!(
        "| `extract_handshake_info` | {} | **{:.2} ns** | **{:.2}** | {} ops/s |\n",
        iters,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );
}

// ============================================================================
// Stage 3 & 4: TLS 1.3 Full Handshake vs Session Resumption
// ============================================================================

async fn bench_handshake_and_resumption() {
    println!("### 3. Downstream TLS 1.3 Handshake: Full Handshake vs Resumption\n");
    println!(
        "| Handshake Type | Iterations | Avg Latency / Handshake | Throughput | Resumption Speedup |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let (server_engine, cert_pem) = make_test_server(CpuTier::Large, MemoryTier::Large);
    let iters = 2_000;

    // 1. Full Handshake (no session cache)
    let client_no_cache = make_test_client(&cert_pem, false);
    let start_full = Instant::now();
    for _ in 0..iters {
        let (client_io, server_io) = duplex(65536);
        let srv = server_engine.clone();
        let srv_task = tokio::spawn(async move {
            let mut s = srv.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(b"pong").await.unwrap();
        });

        let server_name = ServerName::try_from("api.example.com".to_string()).unwrap();
        let mut cli = client_no_cache
            .connect(server_name, client_io)
            .await
            .unwrap();
        cli.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        cli.read_exact(&mut resp).await.unwrap();
        srv_task.await.unwrap();
    }
    let elapsed_full = start_full.elapsed();
    let full_us_op = elapsed_full.as_micros() as f64 / iters as f64;
    let full_ops_sec = (iters as f64 / elapsed_full.as_secs_f64()) as u64;

    println!(
        "| **Full Handshake (TLS 1.3)** | {} | **{:.2} µs** | {} hsk/s | 1.00x (Baseline) |",
        iters, full_us_op, full_ops_sec
    );

    // 2. Resumed Handshake (with session cache)
    let client_with_cache = make_test_client(&cert_pem, true);

    // Initial handshake to populate session ticket
    {
        let (client_io, server_io) = duplex(65536);
        let srv = server_engine.clone();
        let srv_task = tokio::spawn(async move {
            let mut s = srv.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(b"pong").await.unwrap();
        });
        let server_name = ServerName::try_from("api.example.com".to_string()).unwrap();
        let mut cli = client_with_cache
            .connect(server_name, client_io)
            .await
            .unwrap();
        cli.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        cli.read_exact(&mut resp).await.unwrap();
        srv_task.await.unwrap();
    }

    let start_resumed = Instant::now();
    for _ in 0..iters {
        let (client_io, server_io) = duplex(65536);
        let srv = server_engine.clone();
        let srv_task = tokio::spawn(async move {
            let mut s = srv.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(b"pong").await.unwrap();
        });

        let server_name = ServerName::try_from("api.example.com".to_string()).unwrap();
        let mut cli = client_with_cache
            .connect(server_name, client_io)
            .await
            .unwrap();
        cli.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        cli.read_exact(&mut resp).await.unwrap();
        srv_task.await.unwrap();
    }
    let elapsed_resumed = start_resumed.elapsed();
    let resumed_us_op = elapsed_resumed.as_micros() as f64 / iters as f64;
    let resumed_ops_sec = (iters as f64 / elapsed_resumed.as_secs_f64()) as u64;
    let speedup = full_us_op / resumed_us_op;

    println!(
        "| **Resumed Handshake (TLS 1.3)** | {} | **{:.2} µs** | {} hsk/s | **{:.2}x faster** |",
        iters, resumed_us_op, resumed_ops_sec, speedup
    );
    println!();
}

// ============================================================================
// Stage 5: QUIC Server Config Compilation
// ============================================================================

fn bench_quic_config_compilation() {
    println!("### 4. QUIC ServerConfig Bridge Compilation Latency\n");
    println!("| Operation | Iterations | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let (server_engine, _) = make_test_server(CpuTier::Medium, MemoryTier::Medium);
    let iters = 50_000;

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let quic_cfg = server_engine.build_quic_config();
        let _ = std::hint::black_box(quic_cfg);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

    println!(
        "| `build_quic_config` | {} | **{:.2} ns** | **{:.2}** | {} ops/s |\n",
        iters,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );
}

// ============================================================================
// Main Benchmark Runner
// ============================================================================

#[tokio::main]
async fn main() {
    println!("# Velda Edge — `velda-tls` Single-Thread Benchmark Suite\n");

    bench_sni_resolver_scaling();
    bench_metadata_extraction().await;
    bench_handshake_and_resumption().await;
    bench_quic_config_compilation();
}
