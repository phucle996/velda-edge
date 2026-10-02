//! Single-Thread Performance, Latency & Zero-Allocation Benchmark Suite for velda-sync.
//!
//! Evaluates the core sync pipeline under single-thread baseline:
//! 1. Gzip Payload Decompression Throughput & Big-O Linearity across [10 KB .. 20 MB]
//! 2. SHA-256 Checksum Verification Throughput & Stack-Alloc Invariant across [10 KB .. 50 MB]
//! 3. Manifest JSON Acquisition & Deserialization overhead across [10 .. 2,500] domains
//! 4. Domain Compilers & Binary Serialization / Unpack (Routes, Upstreams, Listeners)
//! 5. SyncMetrics Atomic Hotpath Recording & Snapshotting

#[path = "common/mod.rs"]
mod common;

use std::io::Write;
use std::time::{Duration, Instant};

use common::{
    CountingAllocator, format_bytes, format_duration, format_throughput,
    generate_listeners_workload, generate_manifest_workload, generate_routes_workload,
    generate_upstreams_workload,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};
use velda_sync::post_sync::listener::{
    compile_listeners_to_binary, parse_listeners, unpack_listeners_from_binary, validate_listeners,
};
use velda_sync::post_sync::route::{
    compile_routes_to_binary, parse_routes, unpack_routes_from_binary, validate_routes,
};
use velda_sync::post_sync::upstream::{
    compile_upstreams_to_binary, parse_upstreams, unpack_upstreams_from_binary, validate_upstreams,
};
use velda_sync::provider::control_plane::decompress_payload;
use velda_sync::provider::metrics::SyncMetrics;
use velda_sync::{ManifestConfig, SyncOutcome};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn compress_data(raw: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(raw).unwrap();
    encoder.finish().unwrap()
}

// ============================================================================
// 1. Gzip Decompression Linearity
// ============================================================================

fn bench_gzip_decompression() {
    println!("### 1. Pre-Sync: Gzip Payload Decompression Scaling\n");

    let sizes = [
        ("10 KB", 10 * 1024),
        ("100 KB", 100 * 1024),
        ("1 MB", 1024 * 1024),
        ("5 MB", 5 * 1024 * 1024),
        ("10 MB", 10 * 1024 * 1024),
    ];

    println!(
        "| {:<10} | {:<12} | {:<10} | {:<14} | {:<16} | {:<14} |",
        "Raw Size", "Gzip Size", "Ratio", "Avg Latency", "Throughput", "Heap Allocs"
    );
    println!(
        "|{:-<12}|{:-<14}|{:-<12}|{:-<16}|{:-<18}|{:-<16}|",
        "", "", "", "", "", ""
    );

    for (label, size) in sizes {
        let raw_data: Vec<u8> = (0..size)
            .map(|i| {
                if i % 32 == 0 {
                    b'\n'
                } else {
                    b'a' + (i % 26) as u8
                }
            })
            .collect();

        let compressed = compress_data(&raw_data);
        let ratio = compressed.len() as f64 / raw_data.len() as f64 * 100.0;

        let iterations = match size {
            0..=102400 => 100,
            102401..=1048576 => 20,
            _ => 5,
        };

        let _ = decompress_payload(&compressed).unwrap();

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iterations {
            let decompressed = decompress_payload(&compressed).unwrap();
            std::hint::black_box(decompressed);
        }
        let elapsed = start.elapsed() / iterations as u32;
        let (allocs, _) = ALLOCATOR.snapshot();

        println!(
            "| {:<10} | {:<12} | {:<9.1}% | {:<14} | {:<16} | {:<14} |",
            label,
            format_bytes(compressed.len()),
            ratio,
            format_duration(elapsed),
            format_throughput(raw_data.len(), elapsed),
            allocs / iterations,
        );
    }
    println!();
}

// ============================================================================
// 2. SHA-256 Checksum Linearity & Stack-Alloc Verification
// ============================================================================

fn bench_sha256_integrity() {
    println!("### 2. Pre-Sync: SHA-256 Checksum Verification & Stack Invariant\n");

    let sizes = [
        ("10 KB", 10 * 1024),
        ("100 KB", 100 * 1024),
        ("1 MB", 1024 * 1024),
        ("5 MB", 5 * 1024 * 1024),
        ("25 MB", 25 * 1024 * 1024),
    ];

    println!(
        "| {:<10} | {:<12} | {:<16} | {:<16} | {:<16} |",
        "Payload", "Iterations", "Avg Latency", "Throughput", "Heap Allocs"
    );
    println!(
        "|{:-<12}|{:-<14}|{:-<18}|{:-<18}|{:-<18}|",
        "", "", "", "", ""
    );

    for (label, size) in sizes {
        let payload = vec![0x42u8; size];
        let iterations = match size {
            0..=102400 => 200,
            102401..=1048576 => 30,
            _ => 5,
        };

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iterations {
            let mut hasher = Sha256::new();
            hasher.update(&payload);
            let hash = hasher.finalize();
            std::hint::black_box(hash);
        }
        let elapsed = start.elapsed() / iterations as u32;
        let (allocs, _) = ALLOCATOR.snapshot();

        println!(
            "| {:<10} | {:<12} | {:<16} | {:<16} | {:<16} |",
            label,
            iterations,
            format_duration(elapsed),
            format_throughput(size, elapsed),
            allocs / iterations,
        );
    }
    println!();
}

// ============================================================================
// 3. Manifest JSON Acquisition & Deserialization
// ============================================================================

fn bench_manifest_decoding() {
    println!("### 3. Pre-Sync: Manifest JSON Deserialization Scaling\n");

    let domain_scales = [10, 50, 200, 1000];

    println!(
        "| {:<14} | {:<12} | {:<16} | {:<16} | {:<16} |",
        "Domains (N)", "JSON Size", "Avg Parse Time", "Heap Allocs", "Allocated Bytes"
    );
    println!(
        "|{:-<16}|{:-<14}|{:-<18}|{:-<18}|{:-<18}|",
        "", "", "", "", ""
    );

    for &n in &domain_scales {
        let (_manifest, manifest_bytes) = generate_manifest_workload(n, 100);
        let iterations = match n {
            0..=100 => 300,
            _ => 50,
        };

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iterations {
            let decoded: ManifestConfig = serde_json::from_slice(&manifest_bytes).unwrap();
            std::hint::black_box(decoded);
        }
        let elapsed = start.elapsed() / iterations as u32;
        let (allocs, bytes) = ALLOCATOR.snapshot();

        println!(
            "| {:<14} | {:<12} | {:<16} | {:<16} | {:<16} |",
            n,
            format_bytes(manifest_bytes.len()),
            format_duration(elapsed),
            allocs / iterations,
            format_bytes((bytes / iterations) as usize),
        );
    }
    println!();
}

// ============================================================================
// 4. Domain Compilers & Binary Serialization / Unpack
// ============================================================================

fn bench_domain_compilers() {
    println!("### 4. Post-Sync: Domain Compilers & Binary Serialization / Unpack\n");

    println!(
        "| {:<18} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
        "Domain Workload",
        "Items (N)",
        "Parse Time",
        "Validate Time",
        "Compile (.bin)",
        "Unpack (.bin)"
    );
    println!(
        "|{:-<20}|{:-<12}|{:-<16}|{:-<16}|{:-<16}|{:-<16}|",
        "", "", "", "", "", ""
    );

    // 1. Routes Domain
    let route_scales = [100, 1_000, 5_000];
    for &n in &route_scales {
        let (_routes, json_bytes, _) = generate_routes_workload(n);
        let iters = if n <= 100 { 100 } else { 20 };

        // Parse
        let start = Instant::now();
        for _ in 0..iters {
            let p = parse_routes(&json_bytes).unwrap();
            std::hint::black_box(p);
        }
        let parse_time = start.elapsed() / iters as u32;

        let mut parsed = parse_routes(&json_bytes).unwrap();

        // Validate
        let start = Instant::now();
        for _ in 0..iters {
            let mut clone = parsed.clone();
            validate_routes(&mut clone).unwrap();
        }
        let val_time = start.elapsed() / iters as u32;

        validate_routes(&mut parsed).unwrap();

        // Compile
        let start = Instant::now();
        let mut bin = Vec::new();
        for _ in 0..iters {
            bin = compile_routes_to_binary(&parsed, 1, [0u8; 32]).unwrap();
            std::hint::black_box(&bin);
        }
        let compile_time = start.elapsed() / iters as u32;

        // Unpack
        let start = Instant::now();
        for _ in 0..iters {
            let (_header, routes) = unpack_routes_from_binary(&bin).unwrap();
            std::hint::black_box(routes);
        }
        let unpack_time = start.elapsed() / iters as u32;

        println!(
            "| {:<18} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
            "Routes",
            n,
            format_duration(parse_time),
            format_duration(val_time),
            format_duration(compile_time),
            format_duration(unpack_time),
        );
    }

    // 2. Upstreams Domain
    let upstream_scales = [100, 1_000];
    for &n in &upstream_scales {
        let (_upstreams, json_bytes, _) = generate_upstreams_workload(n);
        let iters = 20;

        let start = Instant::now();
        for _ in 0..iters {
            let p = parse_upstreams(&json_bytes).unwrap();
            std::hint::black_box(p);
        }
        let parse_time = start.elapsed() / iters as u32;

        let mut parsed = parse_upstreams(&json_bytes).unwrap();

        let start = Instant::now();
        for _ in 0..iters {
            let mut clone = parsed.clone();
            validate_upstreams(&mut clone).unwrap();
        }
        let val_time = start.elapsed() / iters as u32;

        validate_upstreams(&mut parsed).unwrap();

        let start = Instant::now();
        let mut bin = Vec::new();
        for _ in 0..iters {
            bin = compile_upstreams_to_binary(&parsed, 1, [0u8; 32]).unwrap();
            std::hint::black_box(&bin);
        }
        let compile_time = start.elapsed() / iters as u32;

        let start = Instant::now();
        for _ in 0..iters {
            let (_header, ups) = unpack_upstreams_from_binary(&bin).unwrap();
            std::hint::black_box(ups);
        }
        let unpack_time = start.elapsed() / iters as u32;

        println!(
            "| {:<18} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
            "Upstreams",
            n,
            format_duration(parse_time),
            format_duration(val_time),
            format_duration(compile_time),
            format_duration(unpack_time),
        );
    }

    // 3. Listeners Domain
    let listener_scales = [50, 500];
    for &n in &listener_scales {
        let (_listeners, json_bytes, _) = generate_listeners_workload(n);
        let iters = 20;

        let start = Instant::now();
        for _ in 0..iters {
            let p = parse_listeners(&json_bytes).unwrap();
            std::hint::black_box(p);
        }
        let parse_time = start.elapsed() / iters as u32;

        let mut parsed = parse_listeners(&json_bytes).unwrap();

        let start = Instant::now();
        for _ in 0..iters {
            let mut clone = parsed.clone();
            validate_listeners(&mut clone).unwrap();
        }
        let val_time = start.elapsed() / iters as u32;

        validate_listeners(&mut parsed).unwrap();

        let start = Instant::now();
        let mut bin = Vec::new();
        for _ in 0..iters {
            bin = compile_listeners_to_binary(&parsed, 1, [0u8; 32]).unwrap();
            std::hint::black_box(&bin);
        }
        let compile_time = start.elapsed() / iters as u32;

        let start = Instant::now();
        for _ in 0..iters {
            let (_header, lis) = unpack_listeners_from_binary(&bin).unwrap();
            std::hint::black_box(lis);
        }
        let unpack_time = start.elapsed() / iters as u32;

        println!(
            "| {:<18} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
            "Listeners",
            n,
            format_duration(parse_time),
            format_duration(val_time),
            format_duration(compile_time),
            format_duration(unpack_time),
        );
    }
    println!();
}

// ============================================================================
// 5. In-Memory Atomic Metrics Hotpath
// ============================================================================

fn bench_sync_metrics_hotpath() {
    println!("### 5. Capability Providers: SyncMetrics Atomic Hotpath\n");

    let metrics = SyncMetrics::new();
    let outcome = SyncOutcome::Updated {
        changed_domains: vec!["route".into(), "upstream".into()],
    };
    let cycle_dur = Duration::from_micros(1250);
    let iterations = 1_000_000;

    println!(
        "| {:<28} | {:<16} | {:<16} | {:<16} |",
        "Operation", "Iterations", "Avg Latency", "Allocs / Op"
    );
    println!("|{:-<30}|{:-<18}|{:-<18}|{:-<18}|", "", "", "", "");

    // 1. record_cycle_success
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iterations {
        metrics.record_cycle_success(&outcome, cycle_dur);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    println!(
        "| {:<28} | {:<16} | {:<16} | {:<16} |",
        "record_cycle_success()",
        iterations,
        format_duration(elapsed / iterations as u32),
        format!("{:.4}", allocs as f64 / iterations as f64),
    );

    // 2. snapshot
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iterations {
        let snap = metrics.snapshot();
        std::hint::black_box(snap);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    println!(
        "| {:<28} | {:<16} | {:<16} | {:<16} |",
        "metrics.snapshot()",
        iterations,
        format_duration(elapsed / iterations as u32),
        format!("{:.4}", allocs as f64 / iterations as f64),
    );
    println!();
}

fn main() {
    println!("\n================================================================================");
    println!("  VELDA-SYNC: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_gzip_decompression();
    bench_sha256_integrity();
    bench_manifest_decoding();
    bench_domain_compilers();
    bench_sync_metrics_hotpath();

    println!("================================================================================\n");
}
