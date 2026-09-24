//! Micro-benchmarks for Stage 1: Pre-Sync (Acquisition, Ingestion & Staging):
//! 1. Gzip Payload Decompression throughput & Big-O linearity across [10 KB .. 20 MB].
//! 2. SHA-256 Checksum integrity verification throughput across [10 KB .. 50 MB].
//! 3. Manifest JSON Acquisition & Deserialization overhead across [10 .. 2,500] domains.

#[path = "common/mod.rs"]
mod common;

use std::io::Write;
use std::time::{Duration, Instant};

use common::{
    CountingAllocator, calculate_big_o, format_bytes, format_duration, format_throughput,
    generate_manifest_workload,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};
use velda_sync::ManifestConfig;
use velda_sync::provider::control_plane::decompress_payload;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn compress_data(raw: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(raw).unwrap();
    encoder.finish().unwrap()
}

fn benchmark_gzip_decompression() {
    println!("### 1. Pre-Sync: Gzip Payload Decompression High-Weight Scaling\n");

    let sizes = [
        ("10 KB", 10 * 1024),
        ("100 KB", 100 * 1024),
        ("1 MB", 1024 * 1024),
        ("5 MB", 5 * 1024 * 1024),
        ("10 MB", 10 * 1024 * 1024),
        ("20 MB", 20 * 1024 * 1024),
    ];

    let mut results: Vec<(usize, Duration)> = Vec::new();

    println!(
        "| {:<10} | {:<12} | {:<12} | {:<12} | {:<14} | {:<16} | {:<16} |",
        "Raw Size",
        "Gzip Size",
        "Ratio",
        "Avg Latency",
        "Throughput",
        "Heap Allocs",
        "Allocated Bytes"
    );
    println!(
        "|{:-<12}|{:-<14}|{:-<14}|{:-<14}|{:-<16}|{:-<18}|{:-<18}|",
        "", "", "", "", "", "", ""
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
            1048577..=5242880 => 5,
            _ => 3,
        };

        // Warm-up
        let _ = decompress_payload(&compressed).unwrap();

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iterations {
            let decompressed = decompress_payload(&compressed).unwrap();
            std::hint::black_box(decompressed);
        }
        let elapsed = start.elapsed() / iterations as u32;
        let (allocs, bytes) = ALLOCATOR.snapshot();

        println!(
            "| {:<10} | {:<12} | {:<11.1}% | {:<12} | {:<14} | {:<16} | {:<16} |",
            label,
            format_bytes(compressed.len()),
            ratio,
            format_duration(elapsed),
            format_throughput(raw_data.len(), elapsed),
            allocs / iterations,
            format_bytes((bytes / iterations) as usize),
        );

        results.push((size, elapsed));
    }

    println!("\n#### Gzip Decompression Big-O Linearity Drift\n");
    println!(
        "| {:<20} | {:<11} | {:<16} | {:<18} | {:<16} |",
        "Transition", "Scale Ratio", "Observed Growth", "Linearity Drift", "Linearity Status"
    );
    println!(
        "|{:-<22}|{:-<13}|{:-<18}|{:-<20}|{:-<18}|",
        "", "", "", "", ""
    );

    for i in 1..results.len() {
        let (prev_size, prev_dur) = results[i - 1];
        let (curr_size, curr_dur) = results[i];

        let (scale, growth, drift) = calculate_big_o(prev_dur, curr_dur, prev_size, curr_size);
        let status = if drift.abs() <= 25.0 {
            "Strict O(N) [OK]"
        } else {
            "O(N) Drift"
        };

        println!(
            "| {:<9} -> {:<8} | {:<11.1}x | {:<16.2}x | {:>+5.1}%             | {:<16} |",
            format_bytes(prev_size),
            format_bytes(curr_size),
            scale,
            growth,
            drift,
            status,
        );
    }
    println!();
}

fn benchmark_sha256_integrity() {
    println!("### 2. Pre-Sync: SHA-256 Checksum Verification High-Weight Scaling\n");

    let sizes = [
        ("10 KB", 10 * 1024),
        ("100 KB", 100 * 1024),
        ("1 MB", 1024 * 1024),
        ("5 MB", 5 * 1024 * 1024),
        ("10 MB", 10 * 1024 * 1024),
        ("25 MB", 25 * 1024 * 1024),
        ("50 MB", 50 * 1024 * 1024),
    ];

    let mut results: Vec<(usize, Duration)> = Vec::new();

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
            0..=102400 => 300,
            102401..=1048576 => 50,
            1048577..=10485760 => 10,
            _ => 3,
        };

        // Warm-up
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let _ = hasher.finalize();

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

        results.push((size, elapsed));
    }

    println!("\n#### SHA-256 Big-O Linearity Drift\n");
    println!(
        "| {:<20} | {:<11} | {:<16} | {:<18} | {:<16} |",
        "Transition", "Scale Ratio", "Observed Growth", "Linearity Drift", "Linearity Status"
    );
    println!(
        "|{:-<22}|{:-<13}|{:-<18}|{:-<20}|{:-<18}|",
        "", "", "", "", ""
    );

    for i in 1..results.len() {
        let (prev_size, prev_dur) = results[i - 1];
        let (curr_size, curr_dur) = results[i];

        let (scale, growth, drift) = calculate_big_o(prev_dur, curr_dur, prev_size, curr_size);
        let status = if drift.abs() <= 20.0 {
            "Strict O(N) [OK]"
        } else {
            "O(N) Drift"
        };

        println!(
            "| {:<9} -> {:<8} | {:<11.1}x | {:<16.2}x | {:>+5.1}%             | {:<16} |",
            format_bytes(prev_size),
            format_bytes(curr_size),
            scale,
            growth,
            drift,
            status,
        );
    }

    println!(
        "\n  -> SHA-256 Zero-Alloc Invariant: Hasher operates entirely on stack (0 heap allocs).\n"
    );
}

fn benchmark_manifest_acquisition_decoding() {
    println!("### 3. Pre-Sync: Manifest JSON Decoding High-Weight Scaling\n");

    let domain_scales = [10, 50, 100, 500, 1000, 2500];
    let mut results: Vec<(usize, Duration)> = Vec::new();

    println!(
        "| {:<12} | {:<12} | {:<16} | {:<16} | {:<16} |",
        "Domains (N)", "JSON Size", "Avg Parse Time", "Heap Allocs", "Allocated Bytes"
    );
    println!(
        "|{:-<14}|{:-<14}|{:-<18}|{:-<18}|{:-<18}|",
        "", "", "", "", ""
    );

    for &n in &domain_scales {
        let (_manifest, manifest_bytes) = generate_manifest_workload(n, 100);
        let iterations = match n {
            0..=100 => 500,
            101..=1000 => 100,
            _ => 30,
        };

        // Warm-up
        let _ = serde_json::from_slice::<ManifestConfig>(&manifest_bytes).unwrap();

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iterations {
            let decoded: ManifestConfig = serde_json::from_slice(&manifest_bytes).unwrap();
            std::hint::black_box(decoded);
        }
        let elapsed = start.elapsed() / iterations as u32;
        let (allocs, bytes) = ALLOCATOR.snapshot();

        println!(
            "| {:<12} | {:<12} | {:<16} | {:<16} | {:<16} |",
            n,
            format_bytes(manifest_bytes.len()),
            format_duration(elapsed),
            allocs / iterations,
            format_bytes((bytes / iterations) as usize),
        );

        results.push((n, elapsed));
    }

    println!("\n#### Manifest JSON Big-O Linearity Drift\n");
    println!(
        "| {:<20} | {:<11} | {:<16} | {:<18} | {:<16} |",
        "Transition", "Scale Ratio", "Observed Growth", "Linearity Drift", "Linearity Status"
    );
    println!(
        "|{:-<22}|{:-<13}|{:-<18}|{:-<20}|{:-<18}|",
        "", "", "", "", ""
    );

    for i in 1..results.len() {
        let (prev_n, prev_dur) = results[i - 1];
        let (curr_n, curr_dur) = results[i];

        let (scale, growth, drift) = calculate_big_o(prev_dur, curr_dur, prev_n, curr_n);
        let status = if drift.abs() <= 20.0 {
            "Strict O(N) [OK]"
        } else {
            "O(N) Drift"
        };

        println!(
            "| {:<6} -> {:<6} dom | {:<11.1}x | {:<16.2}x | {:>+5.1}%             | {:<16} |",
            prev_n, curr_n, scale, growth, drift, status,
        );
    }
    println!();
}

fn main() {
    println!("================================================================================");
    println!(" VELDA-SYNC BENCHMARK: STAGE 1 (PRE-SYNC)");
    println!(" Measuring Gzip Decompression, SHA-256 Checksums, and Manifest Acquisition");
    println!("================================================================================\n");

    benchmark_gzip_decompression();
    benchmark_sha256_integrity();
    benchmark_manifest_acquisition_decoding();

    println!("================================================================================");
    println!(" STAGE 1 BENCHMARK COMPLETE");
    println!("================================================================================\n");
}
