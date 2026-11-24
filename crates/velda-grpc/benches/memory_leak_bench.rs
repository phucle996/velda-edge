//! Velda gRPC — Zero-Leak & Memory Compaction Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use common::{CountingAllocator, format_duration, format_throughput};
use http::header::CONTENT_TYPE;
use http::{Method, Uri};
use tokio::io::duplex;
use velda_core::{GrpcStatus, HeaderWorkspace};
use velda_grpc::config::GrpcConfig;
use velda_grpc::frame::encode_grpc_frame;
use velda_grpc::tcp::client::connector::GrpcUpstreamConnector;
use velda_grpc::tcp::server::connection::GrpcServerConnection;
use velda_grpc::wire::GrpcWire;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

const TEST_CONFIG: GrpcConfig = GrpcConfig::from_hardware(4, 8 * 1024 * 1024 * 1024);

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
            let _ = stream
                .respond
                .send_unary_response(GrpcStatus::Ok, Some(b"leak_check_ack"), None)
                .await;
        }
    });

    let client_task = tokio::spawn(async move {
        let mut client = GrpcUpstreamConnector::handshake_stream(client_io, &TEST_CONFIG, None)
            .await
            .unwrap();

        // 1. Warmup phase (50 requests) to stabilize initial H2 connection allocations
        let mut warmup_body = BytesMut::new();
        encode_grpc_frame(b"warmup", false, &mut warmup_body);
        let frozen_warmup = warmup_body.freeze();
        let warmup_uri = Uri::from_static("http://localhost/test.Bench/Warmup");

        for _ in 0..50 {
            let mut req_ws = HeaderWorkspace::empty(2);
            req_ws.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);
            client.ready().await.unwrap();
            let (resp_fut, mut send) = client
                .open_stream(Method::POST, warmup_uri.clone(), req_ws, false)
                .await
                .unwrap();
            send.send_chunk(frozen_warmup.clone(), true).await.unwrap();
            let mut resp = resp_fut.await.unwrap();
            while resp.body.recv_chunk().await.unwrap().is_some() {}
            let _ = resp.body.trailers().await.unwrap();
        }

        // 2. Measure phase
        let mut payload_buf = BytesMut::new();
        encode_grpc_frame(b"memory_leak_test_payload", false, &mut payload_buf);
        let frozen_payload = payload_buf.freeze();
        let uri = Uri::from_static("http://localhost/test.Bench/LeakCheck");

        ALLOCATOR.reset();
        let start = Instant::now();

        for _ in 0..iterations {
            let mut req_ws = HeaderWorkspace::empty(2);
            req_ws.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);
            client.ready().await.unwrap();
            let (resp_fut, mut send) = client
                .open_stream(Method::POST, uri.clone(), req_ws, false)
                .await
                .unwrap();
            send.send_chunk(frozen_payload.clone(), true).await.unwrap();

            let mut resp = resp_fut.await.unwrap();
            assert_eq!(resp.status, http::StatusCode::OK);

            while resp.body.recv_chunk().await.unwrap().is_some() {}
            let _ = resp.body.trailers().await.unwrap();
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
