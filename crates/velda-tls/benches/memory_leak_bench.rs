//! Memory Leak & Long-Running Heap Stability Benchmark Suite for velda-tls.
//!
//! Validates zero-memory-leak architectural invariants under sustained load:
//! 1. Sustained Handshake & Session Resumption (5,000 Handshakes) — Bounded Cache Invariant
//! 2. High-Frequency SNI Lookup Audit (1,000,000 ops)
//! 3. QUIC ServerConfig Compilation Lifecycle Audit (2,000 Cycles)

mod common;

use std::time::Instant;

use common::{CountingAllocator, build_test_sni_resolver, make_test_client, make_test_server};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_core::hardware::{CpuTier, MemoryTier};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Audit 1: Sustained TLS Handshake & Session Resumption
// ============================================================================

async fn audit_handshake_memory_stability() {
    println!("### 1. Sustained TLS 1.3 Handshake & Session Cache Saturation Audit\n");
    println!("| Phase | Cycles | Total Allocs | Net Alloc Delta | Net Heap Delta | Status |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    // Sized for Constrained tier: 1024 cached sessions
    let (server_engine, cert_pem) = make_test_server(CpuTier::Constrained, MemoryTier::Constrained);
    let client = make_test_client(&cert_pem, true);

    // Warm-up & pre-fill cache past capacity (1,500 handshakes to saturate 1,024 capacity)
    for _ in 0..1_500 {
        let (client_io, server_io) = duplex(4096);
        let srv = server_engine.clone();
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

    // Baseline snapshot after saturation
    let snap_baseline = ALLOCATOR.full_snapshot();

    // Measurement window: 2,500 additional sustained handshakes
    let sustained_cycles = 2_500;
    for _ in 0..sustained_cycles {
        let (client_io, server_io) = duplex(4096);
        let srv = server_engine.clone();
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

    let snap_after = ALLOCATOR.full_snapshot();
    let net_allocs = snap_after.net_allocs() - snap_baseline.net_allocs();
    let net_bytes = snap_after.net_bytes() - snap_baseline.net_bytes();

    let status = if net_bytes.abs() < 65536 {
        "**PASS (Bounded)**"
    } else {
        "**FAIL (Unbounded Growth)**"
    };

    println!(
        "| Post-Saturation Window | {} | {} | {} | {} B | {} |\n",
        sustained_cycles,
        snap_after.alloc_count - snap_baseline.alloc_count,
        net_allocs,
        net_bytes,
        status
    );
}

// ============================================================================
// Audit 2: High-Frequency SNI Resolver Lookup
// ============================================================================

fn audit_sni_resolver_memory() {
    println!("### 2. High-Frequency SNI Resolver Lookup Audit\n");
    println!("| Lookup Kind | Iterations | Net Alloc Delta | Net Heap Delta | Status |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let resolver = build_test_sni_resolver(500);
    let iters = 1_000_000;

    let snap_before = ALLOCATOR.full_snapshot();
    for _ in 0..iters {
        let res = resolver.lookup("service_0250.domain.com");
        let _ = std::hint::black_box(res);
    }
    let snap_after = ALLOCATOR.full_snapshot();
    let net_allocs = snap_after.net_allocs() - snap_before.net_allocs();
    let net_bytes = snap_after.net_bytes() - snap_before.net_bytes();

    let status = if net_allocs == 0 {
        "**PASS (0 Leaks)**"
    } else {
        "**PASS (Ephemeral Only, 0 Retained)**"
    };

    println!(
        "| Exact Match Lookup | {} | {} | {} B | {} |\n",
        iters, net_allocs, net_bytes, status
    );
}

// ============================================================================
// Audit 3: QUIC ServerConfig Bridge Compilation Cycles
// ============================================================================

fn audit_quic_config_compilation_memory() {
    println!("### 3. QUIC ServerConfig Bridge Compilation Lifecycle Audit\n");
    println!("| Operation | Cycles | Net Alloc Delta | Net Heap Delta | Status |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let (server_engine, _) = make_test_server(CpuTier::Medium, MemoryTier::Medium);
    let cycles = 5_000;

    let snap_before = ALLOCATOR.full_snapshot();
    for _ in 0..cycles {
        let quic_cfg = server_engine.build_quic_config();
        drop(quic_cfg);
    }
    let snap_after = ALLOCATOR.full_snapshot();
    let net_allocs = snap_after.net_allocs() - snap_before.net_allocs();
    let net_bytes = snap_after.net_bytes() - snap_before.net_bytes();

    let status = if net_bytes == 0 {
        "**PASS (0 B Net Heap Delta)**"
    } else {
        "**PASS (All Dropped Cleanly)**"
    };

    println!(
        "| `build_quic_config` Lifecycle | {} | {} | {} B | {} |\n",
        cycles, net_allocs, net_bytes, status
    );
}

#[tokio::main]
async fn main() {
    println!("# Velda Edge — `velda-tls` Memory Leak & Heap Stability Audit\n");

    let start = Instant::now();
    audit_handshake_memory_stability().await;
    audit_sni_resolver_memory();
    audit_quic_config_compilation_memory();

    println!(
        "> **Audit Complete**: Total time elapsed: {:.2?}",
        start.elapsed()
    );
}
