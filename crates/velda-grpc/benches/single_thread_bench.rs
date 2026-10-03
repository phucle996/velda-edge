//! Velda gRPC — Single-Thread Latency, Allocation & Framing Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use common::{CountingAllocator, format_duration, format_throughput};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, Method, StatusCode};
use tokio::io::duplex;
use velda_core::MemoryTier;
use velda_grpc::config::GrpcConfig;
use velda_grpc::frame::{decode_grpc_frame, encode_grpc_frame};
use velda_grpc::server::GrpcServerConnection;
use velda_grpc::status::GrpcStatus;
use velda_grpc::wire::GrpcWire;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

const TEST_CONFIG: GrpcConfig = GrpcConfig::for_tier(MemoryTier::Medium);

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-GRPC: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_frame_encoding_and_decoding();
    bench_unary_roundtrip_turnaround().await;
    bench_streaming_response_turnaround().await;

    println!("================================================================================\n");
}

fn bench_frame_encoding_and_decoding() {
    println!("### 1. Length-Prefixed Message (LPM) Framing Performance\n");
    println!("| Operation | Payload Size | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    for size in [64, 1024, 65536] {
        let payload = vec![0xABu8; size];
        const ITERS: u64 = 100_000;

        // 1. Encode into pre-allocated buffer
        let mut buf = BytesMut::with_capacity((size + 5) * ITERS as usize);
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..ITERS {
            encode_grpc_frame(&payload, false, &mut buf);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        println!(
            "| encode_grpc_frame | {} B | {} | {:.2} | {} |",
            size,
            format_duration(elapsed / ITERS as u32),
            allocs as f64 / ITERS as f64,
            format_throughput(ITERS, elapsed)
        );

        // 2. Decode from pre-encoded buffer
        ALLOCATOR.reset();
        let start = Instant::now();
        let mut decode_buf = buf;
        let mut decoded_count = 0u64;
        while let Ok(Some((_compressed, _frame))) = decode_grpc_frame(&mut decode_buf) {
            decoded_count += 1;
            if decoded_count >= ITERS {
                break;
            }
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        println!(
            "| decode_grpc_frame | {} B | {} | {:.2} | {} |",
            size,
            format_duration(elapsed / ITERS as u32),
            allocs as f64 / ITERS as f64,
            format_throughput(ITERS, elapsed)
        );
    }
    println!();
}

async fn bench_unary_roundtrip_turnaround() {
    println!("### 2. End-to-End Unary RPC Turnaround (Duplex In-Memory)\n");
    println!("| Iterations | Payload Size | Latency / op | Allocs / op | Net Bytes | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    for iters in [1_000u64, 10_000u64] {
        let (client_io, server_io) = duplex(256 * 1024);

        let server_task = tokio::spawn(async move {
            let mut server = GrpcServerConnection::handshake(server_io, &TEST_CONFIG)
                .await
                .unwrap();
            while let Some(mut stream) = server.accept().await.unwrap() {
                let msg = stream.read_unary_message(1024 * 1024).await.unwrap();
                let reply_data = msg.as_deref().unwrap_or(b"pong");
                stream
                    .respond
                    .send_unary_response(GrpcStatus::Ok, Some(reply_data), None)
                    .unwrap();
            }
        });

        let client_task = tokio::spawn(async move {
            let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
            tokio::spawn(async move {
                let _ = h2_conn.await;
            });

            // Warmup
            let mut warmup_body = BytesMut::new();
            encode_grpc_frame(b"ping", false, &mut warmup_body);
            let req = http::Request::builder()
                .method(Method::POST)
                .uri("http://localhost/test.Bench/Echo")
                .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, mut send) = client.send_request(req, false).unwrap();
            send.send_data(warmup_body.freeze(), true).unwrap();
            let _ = resp_fut.await.unwrap();

            let payload = b"benchmark_unary_payload";
            let mut req_frame = BytesMut::new();
            encode_grpc_frame(payload, false, &mut req_frame);
            let frozen_req = req_frame.freeze();

            ALLOCATOR.reset();
            let start = Instant::now();

            for _ in 0..iters {
                let req = http::Request::builder()
                    .method(Method::POST)
                    .uri("http://localhost/test.Bench/Echo")
                    .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
                    .body(())
                    .unwrap();
                client = client.ready().await.unwrap();
                let (resp_fut, mut send) = client.send_request(req, false).unwrap();
                send.send_data(frozen_req.clone(), true).unwrap();

                let (resp_parts, mut body) = resp_fut.await.unwrap().into_parts();
                assert_eq!(resp_parts.status, StatusCode::OK);

                while let Some(chunk) = body.data().await {
                    let _ = chunk.unwrap();
                }
                let _ = body.trailers().await.unwrap();
            }

            let elapsed = start.elapsed();
            let (allocs, _) = ALLOCATOR.snapshot();
            let net_bytes = ALLOCATOR.net_bytes();

            (elapsed, allocs, net_bytes)
        });

        let (elapsed, allocs, net_bytes) = client_task.await.unwrap();
        server_task.abort();

        println!(
            "| {} | {} B | {} | {:.1} | {} B | {} |",
            iters,
            23,
            format_duration(elapsed / iters as u32),
            allocs as f64 / iters as f64,
            net_bytes,
            format_throughput(iters, elapsed)
        );
    }
    println!();
}

async fn bench_streaming_response_turnaround() {
    println!("### 3. Server-Streaming Progressive Frame Turnaround\n");
    println!("| Streams | Chunks / Stream | Chunk Size | Latency / chunk | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    const STREAMS: u64 = 1_000;
    const CHUNKS: u64 = 5;
    const CHUNK_SIZE: usize = 256;

    let (client_io, server_io) = duplex(256 * 1024);

    let server_task = tokio::spawn(async move {
        let mut server = GrpcServerConnection::handshake(server_io, &TEST_CONFIG)
            .await
            .unwrap();
        while let Some(mut stream) = server.accept().await.unwrap() {
            let resp = http::Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
                .body(())
                .unwrap();
            let mut send = stream.respond.send_response(resp, false).unwrap();

            let chunk_data = vec![0xEEu8; CHUNK_SIZE];
            let mut frame = BytesMut::new();
            encode_grpc_frame(&chunk_data, false, &mut frame);
            let frozen = frame.freeze();

            for _ in 0..CHUNKS {
                send.send_data(frozen.clone(), false).unwrap();
            }

            let mut trailers = HeaderMap::new();
            trailers.insert(GrpcWire::STATUS_NAME, "0".parse().unwrap());
            send.send_trailers(trailers).unwrap();
        }
    });

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let start = Instant::now();

        for _ in 0..STREAMS {
            let req = http::Request::builder()
                .method(Method::POST)
                .uri("http://localhost/test.Stream/Events")
                .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, _) = client.send_request(req, true).unwrap();

            let (resp_parts, mut body) = resp_fut.await.unwrap().into_parts();
            assert_eq!(resp_parts.status, StatusCode::OK);

            let mut received_chunks = 0;
            while let Some(chunk_res) = body.data().await {
                let chunk = chunk_res.unwrap();
                let len = chunk.len();
                received_chunks += 1;
                let _ = body.flow_control().release_capacity(len);
            }
            assert_eq!(received_chunks, CHUNKS);
            let _ = body.trailers().await.unwrap();
        }

        start.elapsed()
    });

    let elapsed = client_task.await.unwrap();
    server_task.abort();

    let total_chunks = STREAMS * CHUNKS;
    println!(
        "| {} | {} | {} B | {} | {} |",
        STREAMS,
        CHUNKS,
        CHUNK_SIZE,
        format_duration(elapsed / total_chunks as u32),
        format_throughput(total_chunks, elapsed)
    );
    println!();
}
