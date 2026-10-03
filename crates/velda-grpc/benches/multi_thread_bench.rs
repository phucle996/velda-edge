//! Velda gRPC — Multi-Thread & Multiplexed Concurrency Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use common::{format_duration, format_throughput};
use http::Method;
use http::header::CONTENT_TYPE;
use tokio::io::duplex;
use tokio::task::JoinSet;
use velda_core::MemoryTier;
use velda_grpc::config::GrpcConfig;
use velda_grpc::frame::encode_grpc_frame;
use velda_grpc::server::GrpcServerConnection;
use velda_grpc::status::GrpcStatus;
use velda_grpc::wire::GrpcWire;

const TEST_CONFIG: GrpcConfig = GrpcConfig::for_tier(MemoryTier::Medium);

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-GRPC: MULTI-THREAD & MULTIPLEXED CONCURRENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    for concurrency in [4, 16, 64, 128] {
        bench_multiplexed_concurrency(concurrency, 100).await;
    }

    println!("================================================================================\n");
}

async fn bench_multiplexed_concurrency(concurrency: usize, requests_per_worker: u64) {
    let total_requests = concurrency as u64 * requests_per_worker;
    let (client_io, server_io) = duplex(1024 * 1024);

    let server_task = tokio::spawn(async move {
        let mut server = GrpcServerConnection::handshake(server_io, &TEST_CONFIG)
            .await
            .unwrap();
        while let Some(mut stream) = server.accept().await.unwrap() {
            tokio::spawn(async move {
                let _ = stream.read_unary_message(1024 * 1024).await.unwrap();
                let _ = stream.respond.send_unary_response(
                    GrpcStatus::Ok,
                    Some(b"concurrent_reply"),
                    None,
                );
            });
        }
    });

    let client_task = tokio::spawn(async move {
        let (client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // Warmup single request
        let mut warmup_c = client.clone();
        let mut warmup_body = BytesMut::new();
        encode_grpc_frame(b"warmup", false, &mut warmup_body);
        let warmup_req = http::Request::builder()
            .method(Method::POST)
            .uri("http://localhost/test.Bench/Warmup")
            .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
            .body(())
            .unwrap();
        warmup_c = warmup_c.ready().await.unwrap();
        let (resp_fut, mut send) = warmup_c.send_request(warmup_req, false).unwrap();
        send.send_data(warmup_body.freeze(), true).unwrap();
        let _ = resp_fut.await.unwrap();

        let mut workers = JoinSet::new();
        let mut payload_buf = BytesMut::new();
        encode_grpc_frame(b"concurrent_request_payload", false, &mut payload_buf);
        let frozen_payload = payload_buf.freeze();

        let start = Instant::now();

        for _ in 0..concurrency {
            let mut c = client.clone();
            let payload = frozen_payload.clone();

            workers.spawn(async move {
                for _ in 0..requests_per_worker {
                    let req = http::Request::builder()
                        .method(Method::POST)
                        .uri("http://localhost/test.Bench/Echo")
                        .header(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE)
                        .body(())
                        .unwrap();

                    c = c.ready().await.unwrap();
                    let (resp_fut, mut send) = c.send_request(req, false).unwrap();
                    send.send_data(payload.clone(), true).unwrap();

                    let (resp_parts, mut body) = resp_fut.await.unwrap().into_parts();
                    assert_eq!(resp_parts.status, http::StatusCode::OK);

                    while let Some(chunk_res) = body.data().await {
                        let chunk = chunk_res.unwrap();
                        let len = chunk.len();
                        let _ = body.flow_control().release_capacity(len);
                    }
                    let _ = body.trailers().await.unwrap();
                }
            });
        }

        while let Some(res) = workers.join_next().await {
            res.unwrap();
        }

        start.elapsed()
    });

    let elapsed = client_task.await.unwrap();
    server_task.abort();

    println!(
        "| Concurrency: {:>3} | Total Requests: {:>5} | Latency / req: {:>9} | Throughput: {:>12} |",
        concurrency,
        total_requests,
        format_duration(elapsed / total_requests as u32),
        format_throughput(total_requests, elapsed)
    );
}
