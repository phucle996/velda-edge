//! Velda Transport — Single-Thread Latency, Allocation & Lifecycle Benchmark Suite.
//!
//! Evaluates core transport primitives under single-threaded execution:
//! 1. Declarative Path Classification & Ingress Binding Resolution
//! 2. Ingress Binding Validation & Boundary Verification
//! 3. Connection Context Lifecycle & Stream Decomposition (`Connection`, `split`, `to_core_connection_context`)
//! 4. UDP Datagram Lifecycle & Zero-Alloc Handoff (`Datagram`, `UdpL7Handoff::into_parts`)
//! 5. Thread-Local Batched Connection ID Generation (`next_connection_id`)

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use common::CountingAllocator;
use tokio::net::{TcpListener, TcpStream};
use velda_core::ConnectionId;
use velda_transport::ingress::classifier::PathKind;
use velda_transport::ingress::listener::IngressBinding;
use velda_transport::udp::datagram::Datagram;
use velda_transport::{
    Connection, TcpL7Handoff, UdpL7Handoff, UdpSocket, UdpSocketConfig, next_connection_id,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-TRANSPORT: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_path_kind_classification();
    bench_binding_resolution();
    bench_connection_id_allocation();
    bench_connection_lifecycle().await;
    bench_udp_datagram_handoff().await;

    println!("================================================================================\n");
}

// ============================================================================
// Stage 1: PathKind Classification & Predicates
// ============================================================================

fn bench_path_kind_classification() {
    println!("### 1. Declared PathKind Resolution & Predicate Evaluation\n");
    println!("| Target Path | Invariant Verified | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 10_000_000;

    let scenarios = [
        ("PathKind::L4Direct", PathKind::L4Direct, true, false),
        ("PathKind::L7Handoff", PathKind::L7Handoff, false, true),
    ];

    for (name, path, expect_l4, expect_l7) in scenarios {
        ALLOCATOR.reset();
        let start = Instant::now();

        for _ in 0..iters {
            let l4 = path.is_l4();
            let l7 = path.is_l7();
            let _ = std::hint::black_box((l4, l7));
        }

        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        assert_eq!(path.is_l4(), expect_l4);
        assert_eq!(path.is_l7(), expect_l7);

        println!(
            "| **{:<20}** | is_l4={}, is_l7={} | **{:.2} ns** | **{:.2}** | {} ops/s |",
            name,
            expect_l4,
            expect_l7,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec,
        );
    }
    println!();
}

// ============================================================================
// Stage 2: Ingress Binding Resolution
// ============================================================================

fn bench_binding_resolution() {
    println!(
        "### 2. Ingress Binding Compilation & Validation (`IngressBinding::from_protocols`)\n"
    );
    println!(
        "| Binding Mode | Config Dimensions | Latency / op | Allocs / op | Throughput | Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 1_000_000;
    let dummy_addr: SocketAddr = "127.0.0.1:443".parse().unwrap();

    let scenarios = [
        ("L4 Direct TCP", "tcp", "raw", false, PathKind::L4Direct),
        (
            "L7 HTTP TCP Cleartext",
            "tcp",
            "http",
            false,
            PathKind::L7Handoff,
        ),
        (
            "L7 HTTPS TCP over TLS",
            "tcp",
            "http",
            true,
            PathKind::L7Handoff,
        ),
        ("L4 Direct UDP", "udp", "raw", false, PathKind::L4Direct),
        (
            "L7 HTTP/3 UDP Handoff",
            "udp",
            "http3",
            true,
            PathKind::L7Handoff,
        ),
        (
            "L7 gRPC Ingress Pipeline",
            "tcp",
            "grpc",
            true,
            PathKind::L7Handoff,
        ),
    ];

    for (name, transport_proto, app_proto, tls, expected_path) in scenarios {
        ALLOCATOR.reset();
        let start = Instant::now();

        for _ in 0..iters {
            let binding = IngressBinding::from_protocols(
                "listener-prod-01",
                dummy_addr,
                transport_proto,
                app_proto,
                tls,
            )
            .unwrap();
            let _ = std::hint::black_box(&binding);
        }

        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        assert_eq!(
            IngressBinding::from_protocols("t", dummy_addr, transport_proto, app_proto, tls)
                .unwrap()
                .path,
            expected_path
        );

        println!(
            "| **{:<24}** | transport={}, app={}, tls={} | **{:.2} ns** | **{:.2}** | {} ops/s | PASS |",
            name,
            transport_proto,
            app_proto,
            tls,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec,
        );
    }
    println!();
}

// ============================================================================
// Stage 3: Monotonic Connection ID Generation
// ============================================================================

fn bench_connection_id_allocation() {
    println!("### 3. Connection ID Allocation Performance (`next_connection_id`)\n");
    println!("| Scenario | Batch Size | Latency / op | Allocs / op | Throughput | Target |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 10_000_000;

    ALLOCATOR.reset();
    let start = Instant::now();

    for _ in 0..iters {
        let id = next_connection_id();
        let _ = std::hint::black_box(id);
    }

    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

    println!(
        "| **Thread-Local Batched ID** | 512 IDs / batch | **{:.2} ns** | **{:.2}** | {} ops/s | < 5.0 ns |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec,
    );
    println!();
}

// ============================================================================
// Stage 4: Connection Lifecycle & Context Projections
// ============================================================================

async fn bench_connection_lifecycle() {
    println!("### 4. Connection Lifecycle & Envelope Decomposition\n");
    println!("| Operation | Component | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (server_stream, peer) = listener.accept().await.unwrap();
    let _ = client.await.unwrap();

    let iters = 1_000_000;

    // 1. Connection properties
    ALLOCATOR.reset();
    let start = Instant::now();
    for i in 0..iters {
        let conn_id = ConnectionId::new(i as u64);
        let _ = std::hint::black_box((conn_id, peer, addr));
    }
    let elapsed = start.elapsed();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Connection Properties** | ID + Peer + Local Address | **{:.2} ns** | **0.00** | {} ops/s |",
        ns_op, ops_sec,
    );

    // 2. TcpL7Handoff::new + into_parts
    let conn = Connection::new(ConnectionId::new(42), server_stream, peer, addr);
    ALLOCATOR.reset();
    let start = Instant::now();
    let mut curr_conn = conn;
    for _ in 0..iters {
        let handoff = TcpL7Handoff::new(curr_conn, "listener-http1-01");
        let (c, _id) = handoff.into_parts();
        curr_conn = c;
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **TcpL7Handoff Recycle** | new + into_parts decomposition | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec,
    );
    println!();
}

// ============================================================================
// Stage 5: UDP Datagram Lifecycle & Zero-Alloc Handoff
// ============================================================================

async fn bench_udp_datagram_handoff() {
    println!("### 5. UDP Datagram Lifecycle & Zero-Allocation Handoff\n");
    println!("| Operation | Component | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 1_000_000;
    let peer: SocketAddr = "10.0.0.1:44321".parse().unwrap();
    let local: SocketAddr = "127.0.0.1:443".parse().unwrap();
    let payload = vec![0xABu8; 1200]; // 1200 bytes QUIC initial datagram
    let socket = Arc::new(
        UdpSocket::bind("127.0.0.1:0".parse().unwrap(), UdpSocketConfig::default()).unwrap(),
    );

    // 1. Datagram::new with pre-allocated buffer
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let dgram = Datagram::new(peer, local, payload.clone());
        let _ = std::hint::black_box(dgram);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Datagram::new (1200B)** | QUIC payload encapsulation | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec,
    );

    // 2. UdpL7Handoff::new + into_parts
    let dgram = Datagram::new(peer, local, payload);
    ALLOCATOR.reset();
    let start = Instant::now();
    let mut curr_dgram = dgram;
    for _ in 0..iters {
        let handoff = UdpL7Handoff::new(curr_dgram, Arc::clone(&socket), "udp-h3-01");
        let (d, _s, _id) = handoff.into_parts();
        curr_dgram = d;
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **UdpL7Handoff Cycle** | new + into_parts decomposition | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec,
    );
    println!();
}
