//! Velda HTTP/3 — Single-Thread Latency, Allocation & Zero-Copy Benchmark Suite.

mod common;

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use common::{CountingAllocator, format_throughput};
use http::header::{CONTENT_TYPE, HeaderValue, USER_AGENT};
use http::{HeaderMap, Method, StatusCode, Uri};
use velda_http3::frame::{Http3Frame, decode_frame, decode_varint_slice, encode_frame};
use velda_http3::huffman::{decode_huffman, encode_huffman};
use velda_http3::qpack::{decode_qpack, encode_qpack_request, encode_qpack_response};
use velda_http3::server::build_edge_response_frames;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn main() {
    println!("================================================================================");
    println!("  VELDA-HTTP3: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_varint_parsing();
    bench_huffman_codec();
    bench_qpack_codec();
    bench_frame_codec();
    bench_edge_response_generation();

    println!("================================================================================\n");
}

fn bench_varint_parsing() {
    println!("### 1. QUIC / HTTP/3 Variable-Length Integer Decoding (Zero-Alloc)\n");
    println!("| Varint Size | Iterations | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    const ITERS: u64 = 1_000_000;

    let test_cases: [(&str, &[u8]); 4] = [
        ("1-byte (value 25)", &[0x19]),
        ("2-byte (value 15293)", &[0x7b, 0xbd]),
        ("4-byte (value 494878333)", &[0x9d, 0x7f, 0x3e, 0x7d]),
        (
            "8-byte (value 151288809941952652)",
            &[0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c],
        ),
    ];

    for (name, buf) in test_cases {
        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..ITERS {
            let res = decode_varint_slice(buf);
            std::hint::black_box(res);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
        let allocs_per_op = allocs as f64 / ITERS as f64;
        println!(
            "| {name} | {ITERS} | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
            format_throughput(ITERS, elapsed)
        );
    }
    println!();
}

fn bench_huffman_codec() {
    println!("### 2. RFC 9204 / RFC 7541 Huffman Encoding & Decoding\n");
    println!("| Operation | Payload Size | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    const ITERS: u64 = 200_000;
    let sample_raw = b"application/json; charset=utf-8, gzip, deflate, br; q=1.0";
    let mut enc_buf = BytesMut::new();
    encode_huffman(sample_raw, &mut enc_buf);
    let encoded = enc_buf.freeze();

    // Huffman Encode
    let mut out_buf = BytesMut::with_capacity(128);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        out_buf.clear();
        encode_huffman(sample_raw, &mut out_buf);
        std::hint::black_box(&out_buf);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| Huffman Encode | {} B | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        sample_raw.len(),
        format_throughput(ITERS, elapsed)
    );

    // Huffman Decode
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        let out = decode_huffman(&encoded).unwrap();
        std::hint::black_box(out);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| Huffman Decode | {} B | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        encoded.len(),
        format_throughput(ITERS, elapsed)
    );
    println!();
}

fn bench_qpack_codec() {
    println!("### 3. RFC 9204 QPACK Header Encoding & Decoding\n");
    println!("| Operation | Headers Count | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("velda-benchmark-agent/1.0"),
    );
    headers.insert("x-request-id", HeaderValue::from_static("req-123456789"));
    headers.insert(
        "accept-encoding",
        HeaderValue::from_static("gzip, deflate, br"),
    );

    const ITERS: u64 = 100_000;

    // 1. QPACK Response Encoding
    let mut resp_buf = BytesMut::with_capacity(256);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        resp_buf.clear();
        encode_qpack_response(StatusCode::OK, &headers, &mut resp_buf);
        std::hint::black_box(&resp_buf);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| encode_qpack_response | 4 | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        format_throughput(ITERS, elapsed)
    );

    // 2. QPACK Request Encoding
    let method = Method::GET;
    let uri: Uri = "https://example.com/api/v1/bench".parse().unwrap();
    let mut req_buf = BytesMut::with_capacity(256);

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        req_buf.clear();
        encode_qpack_request(&method, &uri, &headers, &mut req_buf);
        std::hint::black_box(&req_buf);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| encode_qpack_request | 4 | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        format_throughput(ITERS, elapsed)
    );

    // 3. QPACK Decoding (Response)
    let resp_slice = resp_buf.freeze();
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        let dec = decode_qpack(resp_slice.as_ref()).unwrap();
        std::hint::black_box(dec);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| decode_qpack (response) | 4 | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        format_throughput(ITERS, elapsed)
    );

    // 4. QPACK Decoding (Request)
    let req_slice = req_buf.freeze();
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        let dec = decode_qpack(req_slice.as_ref()).unwrap();
        std::hint::black_box(dec);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| decode_qpack (request) | 4 | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        format_throughput(ITERS, elapsed)
    );
    println!();
}

fn bench_frame_codec() {
    println!("### 4. RFC 9114 HTTP/3 Frame Encoding & Decoding\n");
    println!("| Operation | Frame Type | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    const ITERS: u64 = 500_000;

    let payload = Bytes::from_static(&[0xaa; 1024]);
    let frame = Http3Frame::Data(payload);

    // Frame Encode
    let mut buf = BytesMut::with_capacity(2048);
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        buf.clear();
        encode_frame(&frame, &mut buf);
        std::hint::black_box(&buf);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| encode_frame | DATA (1KB) | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        format_throughput(ITERS, elapsed)
    );

    // Frame Decode
    let mut sample_bytes = BytesMut::new();
    encode_frame(&frame, &mut sample_bytes);
    let frozen = sample_bytes.freeze();

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        let mut cur = BytesMut::from(frozen.as_ref());
        let decoded = decode_frame(&mut cur).unwrap().unwrap();
        std::hint::black_box(decoded);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| decode_frame | DATA (1KB) | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        format_throughput(ITERS, elapsed)
    );
    println!();
}

fn bench_edge_response_generation() {
    println!("### 5. Edge Direct Response Framing (RFC 9114 HEADERS + DATA)\n");
    println!("| Payload Size | Iterations | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    const ITERS: u64 = 200_000;
    let body = b"{\"status\":\"ok\",\"service\":\"velda-edge\",\"protocol\":\"HTTP/3\"}";

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..ITERS {
        let resp = build_edge_response_frames(StatusCode::OK, body);
        std::hint::black_box(resp);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let lat_ns = elapsed.as_nanos() as f64 / ITERS as f64;
    let allocs_per_op = allocs as f64 / ITERS as f64;
    println!(
        "| {} B | {ITERS} | {lat_ns:.2} ns | {allocs_per_op:.1} | {} |",
        body.len(),
        format_throughput(ITERS, elapsed)
    );
    println!();
}
