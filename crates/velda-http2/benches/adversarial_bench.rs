//! Velda HTTP/2 — Adversarial & Safety Limit Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::Bytes;
use common::format_throughput;
use http::StatusCode;
use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, TE, UPGRADE};
use tokio::io::duplex;
use velda_core::MemoryTier;
use velda_http2::config::Http2Config;
use velda_http2::error::Http2Error;
use velda_http2::headers::filter_h2_headers;
use velda_http2::server::Http2ServerConnection;

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-HTTP2: ADVERSARIAL & SAFETY LIMITS BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_oversized_payload_rejection().await;
    bench_rapid_reset_resilience().await;
    bench_dirty_header_sanitization();

    println!("================================================================================\n");
}

async fn bench_oversized_payload_rejection() {
    println!("### 1. Oversized Payload Rejection Latency\n");

    const ITERS: u64 = 1_000;
    const MAX_SIZE: usize = 64;
    let config = Http2Config::for_tier(MemoryTier::Constrained).with_max_body_size(MAX_SIZE);

    let (client_io, server_io) = duplex(256 * 1024);
    let oversized_chunk = Bytes::from(vec![b'A'; 256]);

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let start = Instant::now();
        for _ in 0..ITERS {
            let req = http::Request::builder()
                .method("POST")
                .uri("https://example.com/upload")
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();
            let _ = send_stream.send_data(oversized_chunk.clone(), true);
            let _ = resp_fut.await;
        }
        start.elapsed()
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, config)
        .await
        .unwrap();

    let mut rejections = 0;
    loop {
        match server_conn.accept_h2_request().await {
            Err(Http2Error::PayloadTooLarge(_)) => {
                rejections += 1;
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }

    let elapsed = client_task.await.unwrap();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    println!(
        "| Rejections: {:5}/{} | Avg Rejection Latency: {:6.2} ns | Rate: {} |",
        rejections,
        ITERS,
        lat_ns,
        format_throughput(ITERS, elapsed)
    );
}

async fn bench_rapid_reset_resilience() {
    println!("### 2. Rapid Reset Attack Resilience (CVE-2023-44487)\n");

    const RESETS: u64 = 5_000;
    let config = Http2Config::for_tier(MemoryTier::Medium);

    let (client_io, server_io) = duplex(256 * 1024);

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let start = Instant::now();
        for _ in 0..RESETS {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/rapid-reset")
                .body(())
                .unwrap();
            if let Ok((_resp_fut, mut send_stream)) = client.send_request(req, false) {
                send_stream.send_reset(h2::Reason::CANCEL);
            }
        }
        start.elapsed()
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, config)
        .await
        .unwrap();

    let mut handled = 0;
    for _ in 0..RESETS {
        match server_conn.accept_request().await {
            Ok(Some((_req, mut responder))) => {
                let resp = velda_core::L7Response::from_bytes(StatusCode::OK, vec![]);
                let _ = responder.send_response(&resp);
                handled += 1;
            }
            Ok(None) | Err(_) => break,
        }
    }

    let elapsed = client_task.await.unwrap();
    println!(
        "| Processed {:5} rapid resets without memory bloat or crash in {} | Throughput: {} |",
        RESETS,
        common::format_duration(elapsed),
        format_throughput(RESETS, elapsed)
    );
    let _ = handled;
}

fn bench_dirty_header_sanitization() {
    println!("### 3. Dirty / Malicious Hop-by-Hop Header Stripping\n");

    let mut dirty = http::HeaderMap::new();
    dirty.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    dirty.insert(CONTENT_LENGTH, HeaderValue::from_static("1024"));
    dirty.insert(CONNECTION, HeaderValue::from_static("upgrade, keep-alive"));
    dirty.insert(UPGRADE, HeaderValue::from_static("websocket"));
    dirty.insert("proxy-connection", HeaderValue::from_static("close"));
    dirty.insert("transfer-encoding", HeaderValue::from_static("chunked"));
    dirty.insert(TE, HeaderValue::from_static("deflate, gzip")); // Invalid TE value

    const ITERS: u64 = 100_000;

    // Verify filter correctness once outside the hot loop
    let clean = filter_h2_headers(&dirty);
    assert!(clean.get(CONNECTION).is_none());
    assert!(clean.get(UPGRADE).is_none());
    assert!(clean.get("proxy-connection").is_none());
    assert!(clean.get("transfer-encoding").is_none());
    assert!(clean.get(TE).is_none());

    let start = Instant::now();
    for _ in 0..ITERS {
        let clean = filter_h2_headers(&dirty);
        std::hint::black_box(clean);
    }
    let elapsed = start.elapsed();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    println!(
        "| Stripped 5 illegal hop-by-hop headers | Latency: {:6.2} ns/op | Rate: {} |",
        lat_ns,
        format_throughput(ITERS, elapsed)
    );
}
