//! Velda gRPC — Adversarial, Boundary & Attack Resilience Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use common::{format_duration, format_throughput};
use http::header::CONTENT_TYPE;
use http::{Method, Uri};
use tokio::io::duplex;
use velda_core::HeaderWorkspace;
use velda_grpc::config::GrpcConfig;
use velda_grpc::error::GrpcError;
use velda_grpc::frame::{decode_grpc_frame, encode_grpc_frame};
use velda_grpc::tcp::client::connector::GrpcUpstreamConnector;
use velda_grpc::tcp::server::connection::GrpcServerConnection;
use velda_grpc::wire::GrpcWire;

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-GRPC: ADVERSARIAL & ATTACK RESILIENCE BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_oversized_payload_rejection().await;
    bench_corrupted_compression_flag_rejection();
    bench_truncated_frame_rejection();

    println!("================================================================================");
}

async fn bench_oversized_payload_rejection() {
    println!("### 1. Oversized Payload Early Rejection Latency (5-byte LPM Guard)\n");

    const ITERS: u64 = 5_000;
    const MAX_SIZE: usize = 64;
    let config = GrpcConfig::from_hardware(1, 512 * 1024 * 1024).with_max_message_size(MAX_SIZE);

    let (client_io, server_io) = duplex(256 * 1024);

    // Create a 1MB payload declared in the LPM header
    let mut oversized_frame = BytesMut::new();
    encode_grpc_frame(&vec![0xAA; 1024 * 1024], false, &mut oversized_frame);
    let frozen_oversized = oversized_frame.freeze();

    let server_task = tokio::spawn(async move {
        let mut server = GrpcServerConnection::handshake(server_io, &config)
            .await
            .unwrap();
        while let Some(mut stream) = server.accept().await.unwrap() {
            // Early guard rejects on the first chunk with PayloadTooLarge
            let res = stream.read_unary_message(MAX_SIZE).await;
            assert!(matches!(res, Err(GrpcError::PayloadTooLarge(_))));
            let _ = stream.respond.send_trailers_only(
                velda_core::GrpcStatus::ResourceExhausted,
                Some("message exceeds limit"),
            );
        }
    });

    let client_task = tokio::spawn(async move {
        let mut client = GrpcUpstreamConnector::handshake_stream(client_io, &config, None)
            .await
            .unwrap();

        let uri = Uri::from_static("http://localhost/test.Attack/TooLarge");
        let start = Instant::now();
        for _ in 0..ITERS {
            let mut req_ws = HeaderWorkspace::empty(2);
            req_ws.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);

            client.ready().await.unwrap();
            let (resp_fut, mut send) = client
                .open_stream(Method::POST, uri.clone(), req_ws, false)
                .await
                .unwrap();
            // Send only the first chunk of the oversized frame (header + partial payload)
            send.send_chunk(frozen_oversized.slice(0..128), true)
                .await
                .unwrap();

            let resp = resp_fut.await.unwrap();
            assert_eq!(resp.status, http::StatusCode::OK);
            assert_eq!(resp.headers.get(GrpcWire::STATUS_HEADER).unwrap(), "8"); // RESOURCE_EXHAUSTED
        }
        start.elapsed()
    });

    let elapsed = client_task.await.unwrap();
    server_task.abort();

    println!(
        "| Iterations: {:>5} | Max Size: {:>3} B | Declared Size: 1 MB | Latency / op: {:>8} | Throughput: {:>12} |",
        ITERS,
        MAX_SIZE,
        format_duration(elapsed / ITERS as u32),
        format_throughput(ITERS, elapsed)
    );
    println!();
}

fn bench_corrupted_compression_flag_rejection() {
    println!("### 2. Malformed Frame Flag Rejection Performance\n");

    const ITERS: u64 = 200_000;
    let mut corrupt_buf = BytesMut::new();
    // Byte 0 is invalid flag (0x02 is invalid, only 0 or 1 allowed)
    corrupt_buf.extend_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x10]);
    corrupt_buf.extend_from_slice(&[0x00; 16]);
    let frozen = corrupt_buf.freeze();

    let start = Instant::now();
    for _ in 0..ITERS {
        let mut test_buf = BytesMut::from(&frozen[..]);
        let res = decode_grpc_frame(&mut test_buf);
        assert!(matches!(res, Err(GrpcError::Protocol(_))));
    }
    let elapsed = start.elapsed();

    println!(
        "| Iterations: {:>6} | Invalid Flag: 0x02 | Latency / op: {:>8} | Rejection Rate: {:>12} |",
        ITERS,
        format_duration(elapsed / ITERS as u32),
        format_throughput(ITERS, elapsed)
    );
    println!();
}

fn bench_truncated_frame_rejection() {
    println!("### 3. Truncated Frame Boundary Detection Performance\n");

    const ITERS: u64 = 200_000;
    // 3 bytes only (incomplete 5-byte header)
    let incomplete = Bytes::from_static(&[0x00, 0x00, 0x00]);

    let start = Instant::now();
    for _ in 0..ITERS {
        let mut test_buf = BytesMut::from(&incomplete[..]);
        let res = decode_grpc_frame(&mut test_buf).unwrap();
        assert!(res.is_none());
    }
    let elapsed = start.elapsed();

    println!(
        "| Iterations: {:>6} | Truncated Header: 3 B | Latency / op: {:>8} | Scan Rate: {:>12} |",
        ITERS,
        format_duration(elapsed / ITERS as u32),
        format_throughput(ITERS, elapsed)
    );
    println!();
}
