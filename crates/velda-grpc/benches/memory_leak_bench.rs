//! Velda gRPC — Zero-Leak & Memory Compaction Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use common::{CountingAllocator, format_duration, format_throughput};
use http::Method;
use http::header::CONTENT_TYPE;
use tokio::io::duplex;
use velda_core::MemoryTier;
use velda_grpc::config::GrpcConfig;
use velda_grpc::frame::encode_grpc_frame;
use velda_grpc::status::GrpcStatus;
use velda_grpc::tcp::server::GrpcServerConnection;
use velda_grpc::wire::GrpcWire;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

const TEST_CONFIG: GrpcConfig = GrpcConfig::for_tier(MemoryTier::Medium);

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-GRPC: ZERO-LEAK & HEAP STABILITY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_heap_leak_detection(5_000).await;
    bench_heap_leak_detection(20_000).await;

    println!("================================================================================\n");
}

async fn bench_heap_leak_detection(iterations: u64) {
    let (client_io, server_io) = duplex(256 * 1024);

    let server_task = tokio::spawn(async move {
        let mut server = GrpcServerConnection::handshake(server_io, &TEST_CONFIG)
            .await
            .unwrap();
        while let Some(mut stream) = server.accept().await.unwrap() {
            let _ = stream.read_unary_message(1024 * 1024).await.unwrap();
            let _ =
                stream
                    .respond
                    .send_unary_response(GrpcStatus::Ok, Some(b"leak_check_ack"), None);
        }
    });

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // 1. Warmup phase (50 requests) to stabilize initial H2 connection allocations
        let mut warmup_body = BytesMut::new();
        encode_grpc_frame(b"warmup", false, &mut warmup_body);
        let frozen_warmup = warmup_body.freeze();

        for _ in 0..50 {
            let req = http::Request::builder()
                .method(Method::POST)
                .uri("http://localhost/test.Bench/Warmup")
                .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, mut send) = client.send_request(req, false).unwrap();
            send.send_data(frozen_warmup.clone(), true).unwrap();
            let (_, mut body) = resp_fut.await.unwrap().into_parts();
            while let Some(chunk) = body.data().await {
                let _ = chunk.unwrap();
            }
            let _ = body.trailers().await.unwrap();
        }

        // 2. Measure phase
        let mut payload_buf = BytesMut::new();
        encode_grpc_frame(b"memory_leak_test_payload", false, &mut payload_buf);
        let frozen_payload = payload_buf.freeze();

        ALLOCATOR.reset();
        let start = Instant::now();

        for _ in 0..iterations {
            let req = http::Request::builder()
                .method(Method::POST)
                .uri("http://localhost/test.Bench/LeakCheck")
                .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, mut send) = client.send_request(req, false).unwrap();
            send.send_data(frozen_payload.clone(), true).unwrap();

            let (parts, mut body) = resp_fut.await.unwrap().into_parts();
            assert_eq!(parts.status, http::StatusCode::OK);

            while let Some(chunk_res) = body.data().await {
                let chunk = chunk_res.unwrap();
                let len = chunk.len();
                let _ = body.flow_control().release_capacity(len);
            }
            let _ = body.trailers().await.unwrap();
        }

        let elapsed = start.elapsed();
        let (allocs, bytes) = ALLOCATOR.snapshot();
        let net_allocs = ALLOCATOR.net_allocs();
        let net_bytes = ALLOCATOR.net_bytes();

        (elapsed, allocs, bytes, net_allocs, net_bytes)
    });

    let (elapsed, allocs, bytes, net_allocs, net_bytes) = client_task.await.unwrap();
    server_task.abort();

    println!(
        "| Iterations: {:>5} | Duration: {:>9} | Throughput: {:>12} | Net Growth: {:>4} B | Net Allocs: {:>2} | Total: {} B ({allocs} allocs) |",
        iterations,
        format_duration(elapsed),
        format_throughput(iterations, elapsed),
        net_bytes,
        net_allocs,
        bytes,
    );

    // Verify heap stability: net retained bytes after all requests finished must be bounded by H2 stream window cache (<= 16KB)
    assert!(
        net_bytes.abs() < 16384,
        "Potential memory leak detected: net retained bytes = {net_bytes}"
    );
}
