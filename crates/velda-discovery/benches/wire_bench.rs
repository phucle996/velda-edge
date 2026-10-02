//! RFC 1035 Binary DNS Wire Protocol Benchmark Suite for velda-discovery.
//!
//! Stages:
//! 1. RFC 1035 Query Serialization Latency (Short vs Multi-level vs Deep FQDN)
//! 2. RFC 1035 Response Parsing Scaling (1, 4, 16, 64 A Records)
//! 3. Dual-Stack Parsing Overhead: IPv4 (Type A) vs IPv6 (Type AAAA)
//! 4. DNS Name Compression Pointer (0xC0) Resolution Efficiency
//! 5. Malformed & Adversarial Packet Defense Latency (Loops, Truncation, Mismatch)

mod common;

use std::net::IpAddr;
use std::time::Instant;

use common::{CountingAllocator, build_mock_dns_response, format_duration};
use velda_discovery::{build_query_packet, parse_response_packet};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: RFC 1035 Query Serialization Latency
// ============================================================================

fn bench_query_serialization() {
    println!("### 1. RFC 1035 Query Packet Serialization Latency & Allocations\n");
    println!(
        "> Evaluating raw wire serialization of DNS Query packets (Header + Label Sequence + QTYPE + QCLASS)...\n"
    );

    let iters = 500_000;
    let domains = [
        ("Short (2 labels)", "api.local"),
        ("Standard (5 labels)", "us-east-1.edge.api.velda.io"),
        (
            "Deep FQDN (7 labels)",
            "service.prod.ingress.cluster.k8s.internal.velda.edge",
        ),
    ];

    println!(
        "| Domain Hierarchy | Length | Total Time | Latency / op | Allocs / op | Bytes / op | Throughput |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");

    for (desc, domain) in domains {
        ALLOCATOR.reset();
        let start = Instant::now();

        for i in 0..iters {
            let pkt = build_query_packet((i & 0xFFFF) as u16, domain, 1);
            std::hint::black_box(pkt);
        }

        let elapsed = start.elapsed();
        let (allocs, bytes) = ALLOCATOR.snapshot();

        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| {} | {} B | {} | {:.2} ns | {:.2} | {:.1} B | {} ops/s |",
            desc,
            domain.len(),
            format_duration(elapsed),
            ns_op,
            allocs as f64 / iters as f64,
            bytes as f64 / iters as f64,
            ops_sec
        );
    }

    println!();
}

// ============================================================================
// Stage 2: RFC 1035 Response Parsing Scaling
// ============================================================================

fn bench_response_parsing_scaling() {
    println!("### 2. RFC 1035 Response Parsing Scaling (1, 4, 16, 64 A Records)\n");
    println!(
        "> Evaluating packet decoding and IPv4 extraction across varying answer record counts...\n"
    );

    let iters = 200_000;
    let host = "backend.velda.internal";
    let counts = [1, 4, 16, 64];

    println!(
        "| Answer Records | Packet Size | Total Time | Latency / op | Records / sec | Throughput |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    for count in counts {
        let ips: Vec<IpAddr> = (0..count)
            .map(|i| {
                format!("10.0.{}.{}", (i / 254) + 1, (i % 254) + 1)
                    .parse()
                    .unwrap()
            })
            .collect();

        let packet = build_mock_dns_response(0x1234, host, &ips, true);

        let start = Instant::now();
        for _ in 0..iters {
            let res = parse_response_packet(0x1234, host, &packet);
            std::hint::black_box(res).unwrap();
        }

        let elapsed = start.elapsed();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
        let records_sec = ops_sec * count as u64;

        println!(
            "| {} records | {} B | {} | {:.2} ns | {} rec/s | {} pkts/s |",
            count,
            packet.len(),
            format_duration(elapsed),
            ns_op,
            records_sec,
            ops_sec
        );
    }

    println!();
}

// ============================================================================
// Stage 3: Dual-Stack Parsing Overhead: IPv4 vs IPv6
// ============================================================================

fn bench_dual_stack_parsing_overhead() {
    println!("### 3. Dual-Stack Parsing Overhead: IPv4 (Type A) vs IPv6 (Type AAAA)\n");
    println!(
        "> Comparing 16-record response decoding between 4-byte IPv4 and 16-byte IPv6 payloads...\n"
    );

    let iters = 200_000;
    let host = "dualstack.velda.internal";

    let v4_ips: Vec<IpAddr> = (0..16)
        .map(|i| format!("10.0.0.{}", i + 1).parse().unwrap())
        .collect();
    let v6_ips: Vec<IpAddr> = (0..16)
        .map(|i| format!("2001:db8::{:x}", i + 1).parse().unwrap())
        .collect();

    let pkt_v4 = build_mock_dns_response(0x1001, host, &v4_ips, true);
    let pkt_v6 = build_mock_dns_response(0x1002, host, &v6_ips, true);

    // 1. IPv4 (Type A)
    let start_v4 = Instant::now();
    for _ in 0..iters {
        let res = parse_response_packet(0x1001, host, &pkt_v4);
        std::hint::black_box(res).unwrap();
    }
    let elapsed_v4 = start_v4.elapsed();

    // 2. IPv6 (Type AAAA)
    let start_v6 = Instant::now();
    for _ in 0..iters {
        let res = parse_response_packet(0x1002, host, &pkt_v6);
        std::hint::black_box(res).unwrap();
    }
    let elapsed_v6 = start_v6.elapsed();

    let v4_ns = elapsed_v4.as_nanos() as f64 / iters as f64;
    let v6_ns = elapsed_v6.as_nanos() as f64 / iters as f64;
    let v4_ops = (iters as f64 / elapsed_v4.as_secs_f64()) as u64;
    let v6_ops = (iters as f64 / elapsed_v6.as_secs_f64()) as u64;

    println!(
        "| Record Type | Records | Packet Size | Total Time | Latency / op | Throughput | Delta |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| IPv4 (Type A) | 16 | {} B | {} | {:.2} ns | {} ops/s | Baseline |",
        pkt_v4.len(),
        format_duration(elapsed_v4),
        v4_ns,
        v4_ops
    );
    println!(
        "| IPv6 (Type AAAA) | 16 | {} B | {} | {:.2} ns | {} ops/s | +{:.1}% |",
        pkt_v6.len(),
        format_duration(elapsed_v6),
        v6_ns,
        v6_ops,
        ((v6_ns - v4_ns) / v4_ns) * 100.0
    );

    println!();
}

// ============================================================================
// Stage 4: DNS Name Compression Pointer Resolution Efficiency
// ============================================================================

fn bench_compression_pointer_efficiency() {
    println!("### 4. DNS Name Compression Pointer (0xC0) Resolution Efficiency\n");
    println!(
        "> Comparing 16-record response with compression pointers vs repeated uncompressed domain labels...\n"
    );

    let iters = 200_000;
    let host = "deep.subdomain.cluster.internal.velda.io";
    let ips: Vec<IpAddr> = (0..16)
        .map(|i| format!("172.16.0.{}", i + 1).parse().unwrap())
        .collect();

    let pkt_compressed = build_mock_dns_response(0x2001, host, &ips, true);
    let pkt_uncompressed = build_mock_dns_response(0x2002, host, &ips, false);

    // Compressed (0xC00C pointer)
    let start_comp = Instant::now();
    for _ in 0..iters {
        let res = parse_response_packet(0x2001, host, &pkt_compressed);
        std::hint::black_box(res).unwrap();
    }
    let elapsed_comp = start_comp.elapsed();

    // Uncompressed (Repeated labels on every record)
    let start_uncomp = Instant::now();
    for _ in 0..iters {
        let res = parse_response_packet(0x2002, host, &pkt_uncompressed);
        std::hint::black_box(res).unwrap();
    }
    let elapsed_uncomp = start_uncomp.elapsed();

    let comp_ns = elapsed_comp.as_nanos() as f64 / iters as f64;
    let uncomp_ns = elapsed_uncomp.as_nanos() as f64 / iters as f64;
    let comp_ops = (iters as f64 / elapsed_comp.as_secs_f64()) as u64;
    let uncomp_ops = (iters as f64 / elapsed_uncomp.as_secs_f64()) as u64;

    let size_saving = ((pkt_uncompressed.len() - pkt_compressed.len()) as f64
        / pkt_uncompressed.len() as f64)
        * 100.0;

    println!(
        "| Wire Representation | Packet Size | Total Time | Latency / op | Throughput | Bandwidth Savings |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");
    println!(
        "| Uncompressed Labels | {} B | {} | {:.2} ns | {} ops/s | Baseline |",
        pkt_uncompressed.len(),
        format_duration(elapsed_uncomp),
        uncomp_ns,
        uncomp_ops
    );
    println!(
        "| RFC 1035 Pointer (0xC0) | {} B | {} | {:.2} ns | {} ops/s | -{:.1}% bytes |",
        pkt_compressed.len(),
        format_duration(elapsed_comp),
        comp_ns,
        comp_ops,
        size_saving
    );

    println!();
}

// ============================================================================
// Stage 5: Malformed & Adversarial Packet Defense Latency
// ============================================================================

fn bench_malformed_packet_defense() {
    println!("### 5. Malformed & Adversarial Packet Defense Latency\n");
    println!("> Benchmarking fast-fail and zero-panic defenses against corrupt wire payloads...\n");

    let iters = 500_000;
    let host = "safe.velda.internal";

    // 1. Truncated header (< 12 bytes)
    let corrupt_header = vec![0x12, 0x34, 0x81, 0x80];

    // 2. ID mismatch
    let mut mismatch_id =
        build_mock_dns_response(0x9999, host, &["1.1.1.1".parse().unwrap()], true);
    mismatch_id[0] = 0xAA;

    // 3. QR bit = 0 (Not a response)
    let mut not_response =
        build_mock_dns_response(0x1234, host, &["1.1.1.1".parse().unwrap()], true);
    not_response[2] = 0x01; // QR=0

    // 4. NXDOMAIN (RCODE = 3)
    let mut nxdomain = build_mock_dns_response(0x1234, host, &[], true);
    nxdomain[3] = (nxdomain[3] & 0xF0) | 0x03;

    // 5. Corrupted Label Length (Exceeds buffer)
    let mut corrupt_label =
        build_mock_dns_response(0x1234, host, &["1.1.1.1".parse().unwrap()], true);
    corrupt_label[12] = 250; // Label claims 250 bytes in question section

    // 6. Overflow RDLENGTH (Claims 5000 bytes answer data)
    let mut overflow_rdlength =
        build_mock_dns_response(0x1234, host, &["1.1.1.1".parse().unwrap()], true);
    let len = overflow_rdlength.len();
    overflow_rdlength[len - 6] = 0x13; // 0x1388 = 5000 bytes
    overflow_rdlength[len - 5] = 0x88;

    let scenarios = [
        ("Truncated Header (<12B)", corrupt_header),
        ("ID Mismatch Rejection", mismatch_id),
        ("Non-Response (QR=0) Filter", not_response),
        ("NXDOMAIN (RCODE=3) Fast-Path", nxdomain),
        ("Corrupt Label Length Defense", corrupt_label),
        ("Overflow RDLENGTH Defense", overflow_rdlength),
    ];

    println!(
        "| Adversarial Scenario | Total Time | Latency / op | Rejection Rate | Throughput | Invariant |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    for (name, payload) in scenarios {
        let start = Instant::now();
        let mut rejected = 0;

        for _ in 0..iters {
            if parse_response_packet(0x1234, host, &payload).is_err() {
                rejected += 1;
            }
        }

        let elapsed = start.elapsed();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        assert_eq!(rejected, iters);

        println!(
            "| {} | {} | {:.2} ns | 100.0% | {} ops/s | **SAFE** |",
            name,
            format_duration(elapsed),
            ns_op,
            ops_sec
        );
    }

    println!();
}

fn main() {
    println!("\n================================================================================");
    println!("  VELDA-DISCOVERY: RFC 1035 BINARY DNS WIRE PROTOCOL BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_query_serialization();
    bench_response_parsing_scaling();
    bench_dual_stack_parsing_overhead();
    bench_compression_pointer_efficiency();
    bench_malformed_packet_defense();

    println!("================================================================================");
    println!("  ALL RFC 1035 WIRE PROTOCOL INVARIANTS & DEFENSES SATISFIED");
    println!("================================================================================\n");
}
