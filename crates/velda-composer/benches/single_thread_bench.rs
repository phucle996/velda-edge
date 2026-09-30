//! Single-Thread Performance, Latency & Zero-Allocation Benchmark Suite for velda-composer.
//!
//! Evaluates hot-path single-thread performance:
//! 1. Listener Table Lookup & Zero-Allocation Invariant across 100, 1,000, and 10,000 Listeners
//! 2. Fast Protocol Parsing (`ApplicationProtocol::from_str_proto`)
//! 3. Connection Context Construction & TLS/ALPN Enrichment Latency
//! 4. End-to-End UDP L7 Handoff Composition (`compose_udp_handoff`)

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use common::{CountingAllocator, build_test_composer, create_test_udp_handoff};
use velda_composer::{ApplicationProtocol, ComposerContext, TlsMetadata};
use velda_core::ConnectionId;
use velda_transport::{UdpSocket, UdpSocketConfig};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: Listener Lookup Latency across Table Scaling
// ============================================================================

fn bench_listener_lookup_scaling() {
    println!("### 1. Listener Table Lookup Latency & Zero-Allocation Invariant\n");
    println!("| Table Size | Scenario | Target ID | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 2_000_000;
    let sizes = [100, 1_000, 10_000];

    for &size in &sizes {
        let composer = build_test_composer(size);

        // 1. Hit: HTTP/1.1
        let hit_h1 = "listener_http1_0000";
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let res = composer.get_listener(hit_h1);
            let _ = std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| **N = {}** | **Hit (HTTP/1)** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            size,
            hit_h1,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );

        // 2. Hit: HTTP/2
        let hit_h2 = format!("listener_http2_{:04}", (size / 2).min(size - 1));
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let res = composer.get_listener(&hit_h2);
            let _ = std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| **N = {}** | **Hit (HTTP/2)** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            size,
            hit_h2,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );

        // 3. Hit: HTTP/3
        let hit_h3 = "listener_http3_0007";
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let res = composer.get_listener(hit_h3);
            let _ = std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| **N = {}** | **Hit (HTTP/3)** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            size,
            hit_h3,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );

        // 4. Miss: Non-existent listener
        let miss_id = "nonexistent_listener_99999";
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let res = composer.get_listener(miss_id);
            let _ = std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        println!(
            "| **N = {}** | **Miss (Deterministic)** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            size,
            miss_id,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );
    }
    println!();
}

// ============================================================================
// Stage 2: Application Protocol Parsing Latency
// ============================================================================

fn bench_protocol_parsing() {
    println!("### 2. Protocol Token Parsing (`ApplicationProtocol::from_str_proto`)\n");
    println!("| Protocol Token | Case Variant | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 5_000_000;
    let tokens = [
        ("http1", "exact lower"),
        ("HTTP2", "uppercase"),
        ("hTtP3", "mixed case"),
        ("grpc", "exact lower"),
        ("invalid_proto", "miss / rejection"),
    ];

    for (token, desc) in tokens {
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let proto = ApplicationProtocol::from_str_proto(token);
            let _ = std::hint::black_box(proto);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| `{}` | {} | **{:.2} ns** | **{:.2}** | {} ops/s |",
            token,
            desc,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );
    }
    println!();
}

// ============================================================================
// Stage 3: Connection Context Creation & TLS/ALPN Enrichment
// ============================================================================

fn bench_context_lifecycle_and_tls_enrichment() {
    println!("### 3. Connection Context Creation & TLS/ALPN Enrichment\n");
    println!("| Operation | Details | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 2_000_000;
    let peer: SocketAddr = "192.168.1.100:54321".parse().unwrap();
    let local: SocketAddr = "10.0.0.1:443".parse().unwrap();
    let limits = velda_core::IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000);

    // 1. TCP Context Creation
    ALLOCATOR.reset();
    let start = Instant::now();
    for i in 0..iters {
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(i as u64),
            "listener_https_0001",
            peer,
            local,
            ApplicationProtocol::Http2,
            limits,
        );
        let _ = std::hint::black_box(ctx);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Context::new_tcp** | Stack init + ID generator | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 2. UDP Context Creation
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let ctx = ComposerContext::new_udp(
            "listener_h3_0001",
            peer,
            local,
            ApplicationProtocol::Http3,
            limits,
        );
        let _ = std::hint::black_box(ctx);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Context::new_udp** | Datagram peer identifier | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 3. TLS Metadata Enrichment (ALPN Matching - Invariant Verified)
    ALLOCATOR.reset();
    let start = Instant::now();
    for i in 0..iters {
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(i as u64),
            "https-h2",
            peer,
            local,
            ApplicationProtocol::Http2,
            limits,
        );
        let metadata = TlsMetadata::new(Some("api.example.com".into()), Some("h2".into()));
        let enriched = ctx.with_tls_metadata(metadata);
        let _ = std::hint::black_box(enriched);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **with_tls_metadata (ALPN Match)** | Validate h2, preserve protocol | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 4. TLS Metadata Enrichment (ALPN Mismatch - Non-mutating Invariant)
    ALLOCATOR.reset();
    let start = Instant::now();
    for i in 0..iters {
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(i as u64),
            "https-h1",
            peer,
            local,
            ApplicationProtocol::Http1,
            limits,
        );
        let metadata = TlsMetadata::new(Some("api.example.com".into()), Some("h2".into()));
        let enriched = ctx.with_tls_metadata(metadata);
        let _ = std::hint::black_box(enriched);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **with_tls_metadata (ALPN Mismatch)** | Warning logged, protocol preserved | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );
    println!();
}

// ============================================================================
// Stage 4: End-to-End L7 Handoff Composition Latency
// ============================================================================

#[tokio::main(flavor = "current_thread")]
async fn bench_handoff_composition() {
    println!("### 4. End-to-End L7 Handoff Composition (`compose_udp_handoff`)\n");
    println!("| Scenario | Protocol | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 1_000_000;
    let composer = build_test_composer(1_000);
    let socket = Arc::new(
        UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap(),
    );

    // 1. Registered UDP HTTP/3 Handoff (TLS required)
    let registered_h3_id = "listener_http3_0007";
    ALLOCATOR.reset();
    let start = Instant::now();
    for i in 0..iters {
        let handoff = create_test_udp_handoff(
            registered_h3_id,
            Arc::clone(&socket),
            (10000 + (i % 50000)) as u16,
        );
        let composed = composer.compose_udp_handoff(handoff).unwrap();
        let _ = std::hint::black_box(composed);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Registered H3 Listener** | HTTP/3 (TlsRequired) | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 2. Unregistered / Default UDP Handoff
    let unknown_udp_id = "unknown_udp_listener";
    ALLOCATOR.reset();
    let start = Instant::now();
    for i in 0..iters {
        let handoff = create_test_udp_handoff(
            unknown_udp_id,
            Arc::clone(&socket),
            (10000 + (i % 50000)) as u16,
        );
        let composed = composer.compose_udp_handoff(handoff).unwrap();
        let _ = std::hint::black_box(composed);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Unregistered Listener (Default)** | HTTP/3 (Safe fallback) | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );
    println!();
}

fn main() {
    println!("\n================================================================================");
    println!("  VELDA-COMPOSER: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_listener_lookup_scaling();
    bench_protocol_parsing();
    bench_context_lifecycle_and_tls_enrichment();
    bench_handoff_composition();

    println!("================================================================================\n");
}
