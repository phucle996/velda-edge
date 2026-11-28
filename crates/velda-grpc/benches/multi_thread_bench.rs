//! Velda gRPC — Multi-Thread & Multiplexed Concurrency Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use common::{format_duration, format_throughput};
use http::header::CONTENT_TYPE;
use http::{Method, Uri};
use tokio::io::duplex;
use tokio::task::JoinSet;
use velda_core::HeaderWorkspace;
use velda_grpc::GrpcStatus;
use velda_grpc::config::GrpcConfig;
use velda_grpc::frame::encode_grpc_frame;
use velda_grpc::tcp::client::connector::GrpcUpstreamConnector;
use velda_grpc::tcp::server::connection::GrpcServerConnection;
use velda_grpc::wire::GrpcWire;

const TEST_CONFIG: GrpcConfig = GrpcConfig::from_hardware(4, 8 * 1024 * 1024 * 1024);

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
                let _ = stream
                    .respond
                    .send_unary_response(GrpcStatus::Ok, Some(b"concurrent_reply"), None)
                    .await;
            });
        }
    });

    let client_task = tokio::spawn(async move {
        let client = GrpcUpstreamConnector::handshake_stream(client_io, &TEST_CONFIG, None)
            .await
            .unwrap();

        // Warmup single request
        let mut warmup_c = client.clone();
        let mut warmup_body = BytesMut::new();
        encode_grpc_frame(b"warmup", false, &mut warmup_body);
        let mut warmup_ws = HeaderWorkspace::empty(2);
        warmup_ws.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);
        warmup_c.ready().await.unwrap();
        let (resp_fut, mut send) = warmup_c
            .open_stream(
                Method::POST,
                Uri::from_static("http://localhost/test.Bench/Warmup"),
                warmup_ws,
                false,
            )
            .await
            .unwrap();
        send.send_chunk(warmup_body.freeze(), true).await.unwrap();
        let _ = resp_fut.await.unwrap();

        let mut workers = JoinSet::new();
        let mut payload_buf = BytesMut::new();
        encode_grpc_frame(b"concurrent_request_payload", false, &mut payload_buf);
        let frozen_payload = payload_buf.freeze();
        let uri = Uri::from_static("http://localhost/test.Bench/Echo");

        let start = Instant::now();

        for _ in 0..concurrency {
            let mut c = client.clone();
            let payload = frozen_payload.clone();
            let worker_uri = uri.clone();

            workers.spawn(async move {
                for _ in 0..requests_per_worker {
                    let mut req_ws = HeaderWorkspace::empty(2);
                    req_ws.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);

                    c.ready().await.unwrap();
                    let (resp_fut, mut send) = c
                        .open_stream(Method::POST, worker_uri.clone(), req_ws, false)
                        .await
                        .unwrap();
                    send.send_chunk(payload.clone(), true).await.unwrap();

                    let mut resp = resp_fut.await.unwrap();
                    assert_eq!(resp.status, http::StatusCode::OK);

                    while resp.body.recv_chunk().await.unwrap().is_some() {}
                    let _ = resp.body.trailers().await.unwrap();
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
