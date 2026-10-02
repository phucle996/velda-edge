//! Velda HTTP/3 — Zero-Leak & Memory Compaction Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use common::{CountingAllocator, format_bytes, format_duration, format_throughput};
use http::header::{CONTENT_TYPE, HeaderValue, USER_AGENT};
use http::{HeaderMap, StatusCode};
use velda_http3::frame::{Http3Frame, decode_frame, encode_frame};
use velda_http3::qpack::{decode_qpack, encode_qpack_response};
use velda_http3::server::build_edge_response_frames;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[tokio::main]
async fn main() {
    println!("================================================================================");
    println!("  VELDA-HTTP3: ZERO-LEAK & HEAP STABILITY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_qpack_zero_leak(10_000);
    bench_qpack_zero_leak(50_000);
    bench_edge_response_zero_leak(50_000);
    bench_stream_buffer_reclamation_leak(50_000);

    println!("================================================================================\n");
}

fn bench_qpack_zero_leak(iterations: u64) {
    println!("### Testing QPACK Encode/Decode Heap Stability ({iterations} iterations)");

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("velda-h3-leak-tester"));
    headers.insert(
        "accept-encoding",
        HeaderValue::from_static("gzip, deflate, br"),
    );

    let mut buf = BytesMut::with_capacity(256);

    // Warmup
    for _ in 0..100 {
        buf.clear();
        encode_qpack_response(StatusCode::OK, &headers, &mut buf);
        let _ = decode_qpack(buf.as_ref()).unwrap();
    }

    ALLOCATOR.reset();
    let start = Instant::now();

    for _ in 0..iterations {
        buf.clear();
        encode_qpack_response(StatusCode::OK, &headers, &mut buf);
        let dec = decode_qpack(buf.as_ref()).unwrap();
        std::hint::black_box(dec);
    }

    let elapsed = start.elapsed();
    let (allocs, total_bytes) = ALLOCATOR.snapshot();
    let net_bytes = ALLOCATOR.net_bytes();
    let net_allocs = ALLOCATOR.net_allocs();

    println!("  - Duration:        {}", format_duration(elapsed));
    println!(
        "  - Throughput:      {}",
        format_throughput(iterations, elapsed)
    );
    println!("  - Total Allocs:    {}", allocs);
    println!(
        "  - Total Allocated: {}",
        format_bytes(total_bytes as usize)
    );
    println!("  - Net Bytes:       {} B", net_bytes);
    println!("  - Net Allocs:      {}", net_allocs);

    assert!(
        net_bytes.abs() <= 64,
        "Memory leak detected: net bytes allocated ({net_bytes}) exceeds bounded threshold!"
    );
    assert!(
        net_allocs.abs() <= 5,
        "Memory leak detected: net alloc count ({net_allocs}) exceeds bounded threshold!"
    );
    println!("  -> Status: PASSED (ZERO LEAK)\n");
}

fn bench_edge_response_zero_leak(iterations: u64) {
    println!("### Testing Edge Response Generation Heap Stability ({iterations} iterations)");

    let payload = b"{\"leak_check\":true,\"status\":200}";

    // Warmup
    for _ in 0..100 {
        let _ = build_edge_response_frames(StatusCode::OK, payload);
    }

    ALLOCATOR.reset();
    let start = Instant::now();

    for _ in 0..iterations {
        let resp = build_edge_response_frames(StatusCode::OK, payload);
        std::hint::black_box(resp);
    }

    let elapsed = start.elapsed();
    let (allocs, total_bytes) = ALLOCATOR.snapshot();
    let net_bytes = ALLOCATOR.net_bytes();
    let net_allocs = ALLOCATOR.net_allocs();

    println!("  - Duration:        {}", format_duration(elapsed));
    println!(
        "  - Throughput:      {}",
        format_throughput(iterations, elapsed)
    );
    println!("  - Total Allocs:    {}", allocs);
    println!(
        "  - Total Allocated: {}",
        format_bytes(total_bytes as usize)
    );
    println!("  - Net Bytes:       {} B", net_bytes);
    println!("  - Net Allocs:      {}", net_allocs);

    assert!(
        net_bytes.abs() <= 64,
        "Memory leak detected: net bytes allocated ({net_bytes}) exceeds bounded threshold!"
    );
    assert!(
        net_allocs.abs() <= 5,
        "Memory leak detected: net alloc count ({net_allocs}) exceeds bounded threshold!"
    );
    println!("  -> Status: PASSED (ZERO LEAK)\n");
}

fn bench_stream_buffer_reclamation_leak(iterations: u64) {
    println!(
        "### Testing Stream Buffer Allocation & Reclamation Lifecycle ({iterations} iterations)"
    );

    ALLOCATOR.reset();
    let start = Instant::now();

    for _ in 0..iterations {
        // Simulate stream incoming frame assembly and deallocation upon stream completion
        let mut stream_buf = BytesMut::with_capacity(1024);
        let frame = Http3Frame::Data(Bytes::from_static(&[0x42; 128]));
        encode_frame(&frame, &mut stream_buf);

        // Decode frame
        let decoded = decode_frame(&mut stream_buf).unwrap().unwrap();
        if let Http3Frame::Data(data) = decoded {
            assert_eq!(data.len(), 128);
        } else {
            panic!("Expected Data frame");
        }

        // Stream finish - drop buffer
        drop(stream_buf);
    }

    let elapsed = start.elapsed();
    let (allocs, total_bytes) = ALLOCATOR.snapshot();
    let net_bytes = ALLOCATOR.net_bytes();
    let net_allocs = ALLOCATOR.net_allocs();

    println!("  - Duration:        {}", format_duration(elapsed));
    println!(
        "  - Throughput:      {}",
        format_throughput(iterations, elapsed)
    );
    println!("  - Total Allocs:    {}", allocs);
    println!(
        "  - Total Allocated: {}",
        format_bytes(total_bytes as usize)
    );
    println!("  - Net Bytes:       {} B", net_bytes);
    println!("  - Net Allocs:      {}", net_allocs);

    assert!(
        net_bytes.abs() <= 64,
        "Memory leak detected: net bytes allocated ({net_bytes}) exceeds bounded threshold!"
    );
    assert!(
        net_allocs.abs() <= 5,
        "Memory leak detected: net alloc count ({net_allocs}) exceeds bounded threshold!"
    );
    println!("  -> Status: PASSED (ZERO LEAK)\n");
}
