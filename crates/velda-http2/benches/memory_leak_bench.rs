//! Velda HTTP/2 — Zero-Leak & Memory Compaction Benchmark Suite.

mod common;

use std::time::Instant;

use common::{CountingAllocator, format_duration, format_throughput};
use http::StatusCode;
use tokio::io::duplex;
use velda_core::{L7Response, MemoryTier};
use velda_http2::config::Http2Config;
use velda_http2::server::Http2ServerConnection;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

const TEST_CONFIG: Http2Config = Http2Config::for_tier(MemoryTier::Medium);

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-HTTP2: ZERO-LEAK & HEAP STABILITY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_heap_leak_detection(5_000).await;
    bench_heap_leak_detection(20_000).await;

    println!("================================================================================\n");
}

async fn bench_heap_leak_detection(iterations: u64) {
    let (client_io, server_io) = duplex(256 * 1024);

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // 1. Warmup phase (50 requests) to stabilize initial H2 connection allocations
        for _ in 0..50 {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/warmup")
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, _) = client.send_request(req, true).unwrap();
            let (parts, mut body) = resp_fut.await.unwrap().into_parts();
            assert_eq!(parts.status, StatusCode::OK);
            while let Some(chunk) = body.data().await {
                let _ = chunk.unwrap();
            }
        }

        let start = Instant::now();
        for _ in 0..iterations {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/leak-check")
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, _) = client.send_request(req, true).unwrap();
            let (parts, mut body) = resp_fut.await.unwrap().into_parts();
            assert_eq!(parts.status, StatusCode::OK);
            while let Some(chunk) = body.data().await {
                let _ = chunk.unwrap();
            }
        }
        start.elapsed()
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();

    // Consume warmup requests on server
    for _ in 0..50 {
        if let Ok(Some((_req, responder))) = server_conn.accept_request().await {
            let resp = L7Response::from_bytes(StatusCode::OK, vec![]);
            responder.send_response(&resp).unwrap();
        }
    }

    // Baseline snapshot strictly after warmup, capturing only the steady-state loop
    ALLOCATOR.reset();
    let initial_snapshot = ALLOCATOR.snapshot();

    while let Ok(Some((_req, responder))) = server_conn.accept_request().await {
        let resp = L7Response::from_bytes(StatusCode::OK, vec![]);
        responder.send_response(&resp).unwrap();
    }

    let elapsed = client_task.await.unwrap();
    let (final_allocs, final_bytes) = ALLOCATOR.snapshot();
    let net_bytes = ALLOCATOR.net_bytes();

    println!(
        "| Iterations: {:6} | Duration: {:>8} | Throughput: {:>12} | Net Heap Growth: {:>4} B | Total Allocated: {} B ({final_allocs} allocs) |",
        iterations,
        format_duration(elapsed),
        format_throughput(iterations, elapsed),
        net_bytes,
        final_bytes,
    );
    let _ = initial_snapshot;
}
