//! Velda HTTP/2 — Single-Thread Latency, Allocation & Zero-Copy Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::Bytes;
use common::{CountingAllocator, format_throughput};
use http::header::{CONTENT_TYPE, HeaderValue, USER_AGENT};
use http::{HeaderMap, StatusCode};
use tokio::io::duplex;
use velda_core::{Body, L7Response, MemoryTier};
use velda_http2::config::Http2Config;
use velda_http2::headers::{filter_h2_headers, sanitize_h2_headers};
use velda_http2::server::Http2ServerConnection;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

const TEST_CONFIG: Http2Config = Http2Config::for_tier(MemoryTier::Medium);

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-HTTP2: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_header_sanitization();
    bench_h2_roundtrip_turnaround().await;
    bench_streaming_response_turnaround().await;
    bench_proxy_pipe_turnaround().await;

    println!("================================================================================\n");
}

fn bench_header_sanitization() {
    println!("### 1. RFC 9113 Header Sanitization Performance\n");
    println!("| Method | Headers Count | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("velda-benchmark-agent"),
    );
    headers.insert("connection", HeaderValue::from_static("keep-alive"));
    headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
    headers.insert("te", HeaderValue::from_static("trailers"));

    const ITERS: u64 = 200_000;

    // 1. In-place sanitize_h2_headers on pre-allocated maps (Isolated 0-alloc verification)
    let mut maps: Vec<HeaderMap> = (0..ITERS).map(|_| headers.clone()).collect();
    ALLOCATOR.reset();
    let start = Instant::now();
    for map in &mut maps {
        sanitize_h2_headers(map);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| sanitize_h2_headers (in-place) | 5 | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        format_throughput(ITERS, elapsed)
    );

    // 2. Cloned filter_h2_headers
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        let clean = filter_h2_headers(&headers);
        std::hint::black_box(clean);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| filter_h2_headers (copy) | 5 | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |\n",
        format_throughput(ITERS, elapsed)
    );
}

async fn bench_h2_roundtrip_turnaround() {
    println!("### 2. HTTP/2 Full Roundtrip Turnaround (Duplex Stream Loop)\n");
    println!("| Payload Size | Latency / op | Throughput |");
    println!("| :--- | :--- | :--- |");

    const ITERS: u64 = 1_000;

    // Small payload (empty body 200 OK)
    let (client_io, server_io) = duplex(256 * 1024);

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // Warmup phase (50 requests)
        for _ in 0..50 {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/test")
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
        for _ in 0..ITERS {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/test")
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

    while let Ok(Some((_req, responder))) = server_conn.accept_request().await {
        let resp = L7Response::from_bytes(StatusCode::OK, vec![]);
        responder.send_response(&resp).unwrap();
    }

    let elapsed = client_task.await.unwrap();
    let lat_us = (elapsed.as_nanos() as f64 / ITERS as f64) / 1_000.0;
    println!(
        "| Empty Body (200 OK) | {lat_us:.2} µs | {} |",
        format_throughput(ITERS, elapsed)
    );

    // 1KB payload
    let (client_io, server_io) = duplex(512 * 1024);
    let payload = vec![b'x'; 1024];
    let payload_clone = payload.clone();

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // Warmup phase (50 requests)
        for _ in 0..50 {
            let req = http::Request::builder()
                .method("POST")
                .uri("https://example.com/echo")
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();
            send_stream
                .send_data(Bytes::from(payload_clone.clone()), true)
                .unwrap();
            let (parts, mut body) = resp_fut.await.unwrap().into_parts();
            assert_eq!(parts.status, StatusCode::OK);
            while let Some(chunk) = body.data().await {
                let _ = chunk.unwrap();
            }
        }

        let start = Instant::now();
        for _ in 0..ITERS {
            let req = http::Request::builder()
                .method("POST")
                .uri("https://example.com/echo")
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();
            send_stream
                .send_data(Bytes::from(payload_clone.clone()), true)
                .unwrap();
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

    while let Ok(Some(req)) = server_conn.accept_request().await {
        let (req_data, responder) = req;
        let body_bytes = match req_data.body {
            Body::Bytes(b) => b,
            _ => Bytes::new(),
        };
        let resp = L7Response::from_bytes(StatusCode::OK, body_bytes.to_vec());
        responder.send_response(&resp).unwrap();
    }

    let elapsed = client_task.await.unwrap();
    let lat_us = (elapsed.as_nanos() as f64 / ITERS as f64) / 1_000.0;
    println!(
        "| 1 KB Body Echo | {lat_us:.2} µs | {} |\n",
        format_throughput(ITERS, elapsed)
    );
}

async fn bench_streaming_response_turnaround() {
    println!("### 3. Server Streaming Response Performance (Chunked / SSE)\n");
    println!("| Chunks per Stream | Chunk Size | Latency / Stream | Chunk Rate |");
    println!("| :--- | :--- | :--- | :--- |");

    const STREAMS: u64 = 1_000;
    const CHUNKS_PER_STREAM: usize = 5;
    const CHUNK_SIZE: usize = 128;

    let (client_io, server_io) = duplex(512 * 1024);
    let chunk_data = Bytes::from(vec![b's'; CHUNK_SIZE]);

    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // Warmup phase (20 streams)
        for _ in 0..20 {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/stream")
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, _) = client.send_request(req, true).unwrap();
            let (parts, mut body) = resp_fut.await.unwrap().into_parts();
            assert_eq!(parts.status, StatusCode::OK);
            while let Some(c) = body.data().await {
                let _ = c.unwrap();
            }
        }

        let start = Instant::now();
        for _ in 0..STREAMS {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/stream")
                .body(())
                .unwrap();
            client = client.ready().await.unwrap();
            let (resp_fut, _) = client.send_request(req, true).unwrap();
            let (parts, mut body) = resp_fut.await.unwrap().into_parts();
            assert_eq!(parts.status, StatusCode::OK);
            let mut count = 0;
            while let Some(c) = body.data().await {
                let chunk = c.unwrap();
                if !chunk.is_empty() {
                    count += 1;
                }
            }
            assert_eq!(count, CHUNKS_PER_STREAM);
        }
        start.elapsed()
    });

    let mut server_conn = Http2ServerConnection::handshake(server_io, TEST_CONFIG)
        .await
        .unwrap();

    while let Ok(Some((_req, responder))) = server_conn.accept_request().await {
        let mut sender = responder
            .send_stream_response(StatusCode::OK, &HeaderMap::new())
            .unwrap();
        for _ in 0..CHUNKS_PER_STREAM {
            sender.send_chunk(chunk_data.clone()).await.unwrap();
        }
        sender.finish().unwrap();
    }

    let elapsed = client_task.await.unwrap();
    let lat_us = (elapsed.as_nanos() as f64 / STREAMS as f64) / 1_000.0;
    let total_chunks = STREAMS * CHUNKS_PER_STREAM as u64;
    println!(
        "| {CHUNKS_PER_STREAM} | {CHUNK_SIZE} B | {lat_us:.2} µs | {} |\n",
        format_throughput(total_chunks, elapsed)
    );
}

async fn bench_proxy_pipe_turnaround() {
    println!("### 4. End-to-End Proxy Pipe Turnaround (Client <-> Velda H2 <-> Upstream)\n");
    println!("| Pipeline Flow | Payload | Latency / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- |");

    const ITERS: u64 = 1_000;
    let (client_io, gateway_server_io) = duplex(512 * 1024);
    let (gateway_client_io, upstream_server_io) = duplex(512 * 1024);

    // 1. Mock Upstream H2 Server
    let upstream_task = tokio::spawn(async move {
        let mut server = h2::server::handshake(upstream_server_io).await.unwrap();
        while let Some(res) = server.accept().await {
            let (_req, mut responder) = res.unwrap();
            let resp = http::Response::builder()
                .status(StatusCode::OK)
                .body(())
                .unwrap();
            let mut send_stream = responder.send_response(resp, false).unwrap();
            send_stream
                .send_data(Bytes::from_static(b"upstream-pong"), true)
                .unwrap();
        }
    });

    // 2. Velda Edge Gateway Task
    let gateway_task = tokio::spawn(async move {
        let (mut upstream_client, h2_conn) =
            h2::client::handshake(gateway_client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let mut server_conn = Http2ServerConnection::handshake(gateway_server_io, TEST_CONFIG)
            .await
            .unwrap();

        while let Ok(Some((head, body_rx, responder))) =
            server_conn.accept_streaming_request().await
        {
            upstream_client = upstream_client.ready().await.unwrap();
            velda_http2::pipe::pipe_buffered(
                head,
                body_rx,
                responder,
                &mut upstream_client,
                &TEST_CONFIG,
            )
            .await
            .unwrap();
        }
    });

    // 3. Client Task
    let client_task = tokio::spawn(async move {
        let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        // Warmup phase (20 requests)
        for _ in 0..20 {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/pipe")
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
        for _ in 0..ITERS {
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/pipe")
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

    let elapsed = client_task.await.unwrap();
    let lat_us = (elapsed.as_nanos() as f64 / ITERS as f64) / 1_000.0;
    println!(
        "| pipe_buffered (E2E Full Proxy Loop) | upstream-pong (13 B) | {lat_us:.2} µs | {} |\n",
        format_throughput(ITERS, elapsed)
    );

    gateway_task.abort();
    upstream_task.abort();
}
