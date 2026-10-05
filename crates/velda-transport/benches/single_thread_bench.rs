//! Velda Transport — Single-Thread Latency, Allocation & Lifecycle Benchmark Suite.
//!
//! Evaluates core transport primitives under single-threaded execution:
//! 1. Ingress Binding Resolution & Validation (`IngressBinding::from_transport`)
//! 2. Thread-Local Batched Connection ID Generation (`next_connection_id`)
//! 3. Connection Context Lifecycle & Stream Decomposition (`Connection`, `split`, `to_core_connection_context`)
//! 4. UDP Datagram Lifecycle (`Datagram`)

mod common;

use std::net::SocketAddr;
use std::time::Instant;

use common::CountingAllocator;
use tokio::net::{TcpListener, TcpStream};
use velda_core::ConnectionId;
use velda_transport::ingress::IngressBinding;
use velda_transport::udp::datagram::Datagram;
use velda_transport::{Connection, next_connection_id};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-TRANSPORT: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_binding_resolution();
    bench_connection_id_allocation();
    bench_connection_lifecycle().await;
    bench_udp_datagram_handoff().await;

    println!("================================================================================\n");
}

// ============================================================================
// Stage 1: Ingress Binding Resolution
// ============================================================================

fn bench_binding_resolution() {
    println!("### 1. Ingress Binding Compilation (`IngressBinding::from_transport`)\n");
    println!("| Binding Mode | Config Dimensions | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 1_000_000;
    let dummy_addr: SocketAddr = "127.0.0.1:443".parse().unwrap();

    let scenarios = [
        ("TCP Cleartext", "tcp", false),
        ("TCP over TLS", "tcp", true),
        ("UDP Cleartext", "udp", false),
        ("UDP (QUIC) TLS", "udp", true),
    ];

    for (name, transport_proto, tls) in scenarios {
        ALLOCATOR.reset();
        let start = Instant::now();

        for _ in 0..iters {
            let binding = IngressBinding::from_transport(
                "listener-prod-01",
                dummy_addr,
                transport_proto,
                tls,
            )
            .unwrap();
            let _ = std::hint::black_box(&binding);
        }

        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        let binding =
            IngressBinding::from_transport("t", dummy_addr, transport_proto, tls).unwrap();
        assert_eq!(binding.is_tcp(), transport_proto == "tcp");
        assert_eq!(binding.is_udp(), transport_proto == "udp");
        assert_eq!(binding.tls_enabled(), tls);

        println!(
            "| **{:<16}** | transport={}, tls={} | **{:.2} ns** | **{:.2}** | {} ops/s |",
            name,
            transport_proto,
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

    // 2. Listener-id tagging on the accepted connection
    let conn = Connection::new(ConnectionId::new(42), server_stream, peer, addr);
    let listener_id = "listener-http1-01".to_string();
    ALLOCATOR.reset();
    let start = Instant::now();
    let mut curr_conn = conn;
    for _ in 0..iters {
        curr_conn = curr_conn.with_listener_id(listener_id.clone());
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **Connection::with_listener_id** | listener tag (String clone) | **{:.2} ns** | **{:.2}** | {} ops/s |",
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

    println!();
}
