//! Velda HTTP/3 — Multi-Thread & Concurrency Scaling Benchmark Suite.

mod common;

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use common::{CountingAllocator, format_throughput};
use http::header::{CONTENT_TYPE, HeaderValue, USER_AGENT};
use http::{HeaderMap, StatusCode};
use velda_http3::qpack::{decode_qpack, encode_qpack_response};
use velda_http3::server::build_edge_response_frames;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-HTTP3: MULTI-THREAD CONCURRENCY & SCALING BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_parallel_qpack_turnaround().await;
    bench_parallel_edge_response().await;

    println!("================================================================================\n");
}

async fn bench_parallel_qpack_turnaround() {
    println!("### 1. Parallel QPACK Header Processing Scaling (Multi-Worker)\n");
    println!("| Workers | Total Requests | Total Duration | Throughput | Scalability Factor |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let thread_counts = [1, 2, 4, 8];
    const TOTAL_REQUESTS: u64 = 200_000;

    let mut baseline_secs = 0.0f64;

    for &workers in &thread_counts {
        let iters_per_worker = TOTAL_REQUESTS / workers as u64;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(USER_AGENT, HeaderValue::from_static("velda-h3-bench/1.0"));
        headers.insert(
            "accept-encoding",
            HeaderValue::from_static("gzip, deflate, br"),
        );
        let headers = Arc::new(headers);

        ALLOCATOR.reset();
        let start = Instant::now();

        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let h = headers.clone();
            handles.push(tokio::spawn(async move {
                let mut buf = BytesMut::with_capacity(256);
                for _ in 0..iters_per_worker {
                    buf.clear();
                    encode_qpack_response(StatusCode::OK, &h, &mut buf);
                    let decoded = decode_qpack(buf.as_ref()).unwrap();
                    std::hint::black_box(decoded);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        if workers == 1 {
            baseline_secs = secs;
        }

        let speedup = baseline_secs / secs;
        println!(
            "| {workers} | {TOTAL_REQUESTS} | {:.2?} | {} | {:.2}x |",
            elapsed,
            format_throughput(TOTAL_REQUESTS, elapsed),
            speedup
        );
    }
    println!();
}

async fn bench_parallel_edge_response() {
    println!("### 2. Parallel Edge Response Generation (HEADERS + DATA Framing)\n");
    println!("| Workers | Total Responses | Total Duration | Throughput | Scalability Factor |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let thread_counts = [1, 2, 4, 8];
    const TOTAL_RESPONSES: u64 = 400_000;
    let body: &'static [u8] = b"{\"message\":\"hello from velda-http3\",\"cached\":true}";

    let mut baseline_secs = 0.0f64;

    for &workers in &thread_counts {
        let iters_per_worker = TOTAL_RESPONSES / workers as u64;

        ALLOCATOR.reset();
        let start = Instant::now();

        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            handles.push(tokio::spawn(async move {
                for _ in 0..iters_per_worker {
                    let resp = build_edge_response_frames(StatusCode::OK, body);
                    std::hint::black_box(resp);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        if workers == 1 {
            baseline_secs = secs;
        }

        let speedup = baseline_secs / secs;
        println!(
            "| {workers} | {TOTAL_RESPONSES} | {:.2?} | {} | {:.2}x |",
            elapsed,
            format_throughput(TOTAL_RESPONSES, elapsed),
            speedup
        );
    }
    println!();
}
