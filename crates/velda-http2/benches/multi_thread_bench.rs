//! Velda HTTP/2 — Multi-Thread & Multiplexed Concurrency Benchmark Suite.

mod common;

use std::time::Instant;

use common::format_throughput;
use http::StatusCode;
use tokio::io::duplex;
use tokio::task::JoinSet;
use velda_core::{L7Response, MemoryTier};
use velda_http2::config::Http2Config;
use velda_http2::server::Http2ServerConnection;

const TEST_CONFIG: Http2Config = Http2Config::for_tier(MemoryTier::Medium);

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-HTTP2: MULTI-THREAD & MULTIPLEXED CONCURRENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    for concurrency in [4, 16, 64, 128] {
        bench_multiplexed_concurrency(concurrency, 50).await;
    }

    println!("================================================================================\n");
}

async fn bench_multiplexed_concurrency(concurrency: usize, requests_per_worker: u64) {
    let total_requests = concurrency as u64 * requests_per_worker;
    let (client_io, server_io) = duplex(1024 * 1024);

    let client_task = tokio::spawn(async move {
        let (client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // Warmup single request
        let mut warmup_c = client.clone();
        let warmup_req = http::Request::builder()
            .method("GET")
            .uri("https://example.com/warmup")
            .body(())
            .unwrap();
        warmup_c = warmup_c.ready().await.unwrap();
        let (resp_fut, _) = warmup_c.send_request(warmup_req, true).unwrap();
        let _ = resp_fut.await;

        let mut workers = JoinSet::new();
        let start = Instant::now();

        for _ in 0..concurrency {
            let mut c = client.clone();
            workers.spawn(async move {
                for _ in 0..requests_per_worker {
                    let req = http::Request::builder()
                        .method("GET")
                        .uri("https://example.com/stream")
                        .body(())
                        .unwrap();
                    c = c.ready().await.unwrap();
                    let (resp_fut, _) = c.send_request(req, true).unwrap();
                    let (parts, mut body) = resp_fut.await.unwrap().into_parts();
                    assert_eq!(parts.status, StatusCode::OK);
                    while let Some(chunk) = body.data().await {
                        let _ = chunk.unwrap();
                    }
                }
            });
        }

        while let Some(res) = workers.join_next().await {
            res.unwrap();
        }

        start.elapsed()
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();

    while let Ok(Some((_req, responder))) = server_conn.accept_request().await {
        let resp = L7Response::from_bytes(StatusCode::OK, vec![]);
        responder.send_response(&resp).unwrap();
    }

    let elapsed = client_task.await.unwrap();
    let lat_us = (elapsed.as_nanos() as f64 / total_requests as f64) / 1_000.0;
    println!(
        "| Concurrency: {:3} streams | Total: {:6} reqs | Latency: {:6.2} µs/op | Throughput: {:>13} |",
        concurrency,
        total_requests,
        lat_us,
        format_throughput(total_requests, elapsed)
    );
}
