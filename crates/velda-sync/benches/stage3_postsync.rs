//! Micro-benchmarks for Stage 3: Post-Sync Domain Compilers:
//! 1. Routes Domain: Scaling across [100 .. 10,000] routes, Big-O linearity, Memory allocations.
//! 2. Upstreams Domain: Scaling across [100 .. 5,000] endpoints, Big-O linearity, Binary Unpack.
//! 3. Listeners Domain: Scaling across [50 .. 1,000] ports, Big-O linearity, Binary Unpack.

#[path = "common/mod.rs"]
mod common;

use std::time::{Duration, Instant};

use common::{
    CountingAllocator, calculate_big_o, format_bytes, format_duration, generate_listeners_workload,
    generate_routes_workload, generate_upstreams_workload,
};
use velda_sync::post_sync::listener::{
    compile_listeners_to_binary, parse_listeners, unpack_listeners_from_binary, validate_listeners,
};
use velda_sync::post_sync::route::{
    compile_routes_to_binary, parse_routes, unpack_routes_from_binary, validate_routes,
};
use velda_sync::post_sync::upstream::{
    compile_upstreams_to_binary, parse_upstreams, unpack_upstreams_from_binary, validate_upstreams,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[derive(Debug, Clone)]
struct DomainBenchResult {
    scale: usize,
    json_lines: usize,
    json_bytes_len: usize,
    bin_bytes_len: usize,

    parse_duration: Duration,
    parse_allocs: u64,
    parse_bytes: u64,

    validate_duration: Duration,
    validate_allocs: u64,
    validate_bytes: u64,

    compile_duration: Duration,
    compile_allocs: u64,

    unpack_duration: Duration,
    unpack_allocs: u64,
    unpack_bytes: u64,
}

// ============================================================================
// 1. Routes Domain
// ============================================================================

fn benchmark_routes_scale(n: usize) -> DomainBenchResult {
    let (routes, json_bytes, json_lines) = generate_routes_workload(n);

    // Warm-up
    let _ = parse_routes(&json_bytes).unwrap();
    let mut warmup_routes = routes.clone();
    let _ = validate_routes(&mut warmup_routes);
    let bin_warmup = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    let _ = unpack_routes_from_binary(&bin_warmup).unwrap();

    let iterations = match n {
        0..=100 => 30,
        101..=1000 => 10,
        1001..=5000 => 5,
        _ => 3,
    };

    // 1. Measure JSON Parse
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iterations {
        let parsed = parse_routes(&json_bytes).unwrap();
        std::hint::black_box(parsed);
    }
    let parse_duration = start.elapsed() / iterations as u32;
    let (parse_allocs, parse_bytes) = ALLOCATOR.snapshot();

    // 2. Measure Validation
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iterations {
        let mut r = routes.clone();
        validate_routes(&mut r).unwrap();
        std::hint::black_box(r);
    }
    let validate_duration = start.elapsed() / iterations as u32;
    let (validate_allocs, validate_bytes) = ALLOCATOR.snapshot();

    // 3. Measure Binary Compile
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iterations {
        let bin = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
        std::hint::black_box(bin);
    }
    let compile_duration = start.elapsed() / iterations as u32;
    let (compile_allocs, _) = ALLOCATOR.snapshot();

    // Pre-compile binary for unpack test
    let bin_bytes = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();

    // 4. Measure Binary Unpack
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iterations {
        let unpacked = unpack_routes_from_binary(&bin_bytes).unwrap();
        std::hint::black_box(unpacked);
    }
    let unpack_duration = start.elapsed() / iterations as u32;
    let (unpack_allocs, unpack_bytes) = ALLOCATOR.snapshot();

    DomainBenchResult {
        scale: n,
        json_lines,
        json_bytes_len: json_bytes.len(),
        bin_bytes_len: bin_bytes.len(),

        parse_duration,
        parse_allocs: parse_allocs / iterations,
        parse_bytes: parse_bytes / iterations,

        validate_duration,
        validate_allocs: validate_allocs / iterations,
        validate_bytes: validate_bytes / iterations,

        compile_duration,
        compile_allocs: compile_allocs / iterations,

        unpack_duration,
        unpack_allocs: unpack_allocs / iterations,
        unpack_bytes: unpack_bytes / iterations,
    }
}

fn benchmark_routes_domain() {
    println!("### 1. Routes Domain: High-Weight Scaling & Big-O Linearity Analysis\n");

    let test_scales = [100, 500, 1000, 2500, 5000, 10000];
    let mut results = Vec::new();

    for &n in &test_scales {
        let res = benchmark_routes_scale(n);
        results.push(res);
    }

    println!(
        "| {:<10} | {:<10} | {:<9} | {:<8} | {:<32} | {:<32} | {:<28} | {:<34} |",
        "Routes (N)",
        "JSON Lines",
        "JSON Size",
        "Bin Size",
        "JSON Parse Time (Allocs, Bytes)",
        "Validation Time (Allocs, Bytes)",
        "Binary Compile Time (Allocs)",
        "Binary Unpack Time (Allocs, Bytes)"
    );
    println!(
        "|{:-<12}|{:-<12}|{:-<11}|{:-<10}|{:-<34}|{:-<34}|{:-<30}|{:-<36}|",
        "", "", "", "", "", "", "", ""
    );

    for r in &results {
        println!(
            "| {:<10} | {:<10} | {:<9} | {:<8} | {:<12} ({:<5} allocs, {:<8}) | {:<12} ({:<5} allocs, {:<8}) | {:<12} ({:<5} allocs) | {:<12} ({:<5} allocs, {:<8}) |",
            r.scale,
            r.json_lines,
            format_bytes(r.json_bytes_len),
            format_bytes(r.bin_bytes_len),
            format_duration(r.parse_duration),
            r.parse_allocs,
            format_bytes(r.parse_bytes as usize),
            format_duration(r.validate_duration),
            r.validate_allocs,
            format_bytes(r.validate_bytes as usize),
            format_duration(r.compile_duration),
            r.compile_allocs,
            format_duration(r.unpack_duration),
            r.unpack_allocs,
            format_bytes(r.unpack_bytes as usize),
        );
    }

    println!("\n#### Routes Big-O Linearity Drift Analysis\n");
    println!(
        "| {:<13} | {:<11} | {:<17} | {:<17} | {:<17} | {:<17} | {:<16} |",
        "Transition",
        "Scale Ratio",
        "JSON Parse Growth",
        "Validation Growth",
        "Compile Growth",
        "Unpack Growth",
        "Linearity Status"
    );
    println!(
        "|{:-<15}|{:-<13}|{:-<19}|{:-<19}|{:-<19}|{:-<19}|{:-<18}|",
        "", "", "", "", "", "", ""
    );

    for i in 1..results.len() {
        let prev = &results[i - 1];
        let curr = &results[i];

        let (scale, parse_growth, parse_drift) = calculate_big_o(
            prev.parse_duration,
            curr.parse_duration,
            prev.scale,
            curr.scale,
        );
        let (_, val_growth, _) = calculate_big_o(
            prev.validate_duration,
            curr.validate_duration,
            prev.scale,
            curr.scale,
        );
        let (_, comp_growth, _) = calculate_big_o(
            prev.compile_duration,
            curr.compile_duration,
            prev.scale,
            curr.scale,
        );
        let (_, unpack_growth, _) = calculate_big_o(
            prev.unpack_duration,
            curr.unpack_duration,
            prev.scale,
            curr.scale,
        );

        let status = if parse_drift.abs() <= 20.0 {
            "Strict O(N) [OK]"
        } else {
            "O(N) Drift"
        };

        println!(
            "| {:<5} -> {:<5} | {:<11.1}x | {:<11.2}x ({:>+4.1}%) | {:<17.2}x | {:<17.2}x | {:<17.2}x | {:<16} |",
            prev.scale,
            curr.scale,
            scale,
            parse_growth,
            parse_drift,
            val_growth,
            comp_growth,
            unpack_growth,
            status,
        );
    }

    println!("\n#### Routes: Edge Serving Boot Advantage (Binary Unpack vs JSON Parse)\n");
    println!(
        "| {:<10} | {:<18} | {:<21} | {:<15} | {:<16} | {:<18} | {:<17} |",
        "Routes (N)",
        "JSON Parse Latency",
        "Binary Unpack Latency",
        "Latency Speedup",
        "JSON Allocations",
        "Binary Allocations",
        "Storage Reduction"
    );
    println!(
        "|{:-<12}|{:-<20}|{:-<23}|{:-<17}|{:-<18}|{:-<20}|{:-<19}|",
        "", "", "", "", "", "", ""
    );

    for r in &results {
        let speedup = r.parse_duration.as_nanos() as f64 / r.unpack_duration.as_nanos() as f64;
        let storage_reduction = 100.0 * (1.0 - (r.bin_bytes_len as f64 / r.json_bytes_len as f64));

        println!(
            "| {:<10} | {:<18} | {:<21} | {:<15.1}x | {:<16} | {:<18} | {:<17.1}% |",
            r.scale,
            format_duration(r.parse_duration),
            format_duration(r.unpack_duration),
            speedup,
            r.parse_allocs,
            r.unpack_allocs,
            storage_reduction,
        );
    }
    println!();
}

// ============================================================================
// 2. Upstreams Domain
// ============================================================================

fn benchmark_upstreams_scale(n: usize) -> DomainBenchResult {
    let (upstreams, json_bytes, json_lines) = generate_upstreams_workload(n);

    let iterations = match n {
        0..=200 => 30,
        201..=1000 => 10,
        _ => 5,
    };

    let start = Instant::now();
    for _ in 0..iterations {
        let parsed = parse_upstreams(&json_bytes).unwrap();
        std::hint::black_box(parsed);
    }
    let parse_duration = start.elapsed() / iterations as u32;

    let start = Instant::now();
    for _ in 0..iterations {
        let mut u = upstreams.clone();
        validate_upstreams(&mut u).unwrap();
        std::hint::black_box(u);
    }
    let validate_duration = start.elapsed() / iterations as u32;

    let start = Instant::now();
    for _ in 0..iterations {
        let bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
        std::hint::black_box(bin);
    }
    let compile_duration = start.elapsed() / iterations as u32;

    let bin_bytes = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
    let start = Instant::now();
    for _ in 0..iterations {
        let unpacked = unpack_upstreams_from_binary(&bin_bytes).unwrap();
        std::hint::black_box(unpacked);
    }
    let unpack_duration = start.elapsed() / iterations as u32;

    DomainBenchResult {
        scale: n,
        json_lines,
        json_bytes_len: json_bytes.len(),
        bin_bytes_len: bin_bytes.len(),
        parse_duration,
        parse_allocs: 0,
        parse_bytes: 0,
        validate_duration,
        validate_allocs: 0,
        validate_bytes: 0,
        compile_duration,
        compile_allocs: 0,
        unpack_duration,
        unpack_allocs: 0,
        unpack_bytes: 0,
    }
}

fn benchmark_upstreams_domain() {
    println!("### 2. Upstreams Domain: High-Weight Scaling & Big-O Linearity Analysis\n");

    let test_scales = [100, 500, 1000, 2500, 5000];
    let mut results = Vec::new();

    for &n in &test_scales {
        let res = benchmark_upstreams_scale(n);
        results.push(res);
    }

    println!(
        "| {:<14} | {:<10} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
        "Upstreams (N)", "JSON Size", "Bin Size", "JSON Parse", "Validate", "Compile", "Unpack"
    );
    println!(
        "|{:-<16}|{:-<12}|{:-<12}|{:-<16}|{:-<16}|{:-<16}|{:-<16}|",
        "", "", "", "", "", "", ""
    );

    for r in &results {
        println!(
            "| {:<14} | {:<10} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
            r.scale,
            format_bytes(r.json_bytes_len),
            format_bytes(r.bin_bytes_len),
            format_duration(r.parse_duration),
            format_duration(r.validate_duration),
            format_duration(r.compile_duration),
            format_duration(r.unpack_duration),
        );
    }

    println!("\n#### Upstreams Big-O Linearity Drift Analysis\n");
    println!(
        "| {:<15} | {:<11} | {:<17} | {:<17} | {:<17} | {:<17} | {:<16} |",
        "Transition",
        "Scale Ratio",
        "JSON Parse Growth",
        "Validation Growth",
        "Compile Growth",
        "Unpack Growth",
        "Linearity Status"
    );
    println!(
        "|{:-<17}|{:-<13}|{:-<19}|{:-<19}|{:-<19}|{:-<19}|{:-<18}|",
        "", "", "", "", "", "", ""
    );

    for i in 1..results.len() {
        let prev = &results[i - 1];
        let curr = &results[i];

        let (scale, parse_growth, parse_drift) = calculate_big_o(
            prev.parse_duration,
            curr.parse_duration,
            prev.scale,
            curr.scale,
        );
        let (_, val_growth, _) = calculate_big_o(
            prev.validate_duration,
            curr.validate_duration,
            prev.scale,
            curr.scale,
        );
        let (_, comp_growth, _) = calculate_big_o(
            prev.compile_duration,
            curr.compile_duration,
            prev.scale,
            curr.scale,
        );
        let (_, unpack_growth, _) = calculate_big_o(
            prev.unpack_duration,
            curr.unpack_duration,
            prev.scale,
            curr.scale,
        );

        let status = if parse_drift.abs() <= 20.0 {
            "Strict O(N) [OK]"
        } else {
            "O(N) Drift"
        };

        println!(
            "| {:<6} -> {:<6} | {:<11.1}x | {:<11.2}x ({:>+4.1}%) | {:<17.2}x | {:<17.2}x | {:<17.2}x | {:<16} |",
            prev.scale,
            curr.scale,
            scale,
            parse_growth,
            parse_drift,
            val_growth,
            comp_growth,
            unpack_growth,
            status,
        );
    }
    println!();
}

// ============================================================================
// 3. Listeners Domain
// ============================================================================

fn benchmark_listeners_scale(n: usize) -> DomainBenchResult {
    let (listeners, json_bytes, json_lines) = generate_listeners_workload(n);

    let iterations = match n {
        0..=200 => 30,
        201..=1000 => 10,
        _ => 5,
    };

    let start = Instant::now();
    for _ in 0..iterations {
        let parsed = parse_listeners(&json_bytes).unwrap();
        std::hint::black_box(parsed);
    }
    let parse_duration = start.elapsed() / iterations as u32;

    let start = Instant::now();
    for _ in 0..iterations {
        let mut l = listeners.clone();
        validate_listeners(&mut l).unwrap();
        std::hint::black_box(l);
    }
    let validate_duration = start.elapsed() / iterations as u32;

    let start = Instant::now();
    for _ in 0..iterations {
        let bin = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
        std::hint::black_box(bin);
    }
    let compile_duration = start.elapsed() / iterations as u32;

    let bin_bytes = compile_listeners_to_binary(&listeners, 1, [0u8; 32]).unwrap();
    let start = Instant::now();
    for _ in 0..iterations {
        let unpacked = unpack_listeners_from_binary(&bin_bytes).unwrap();
        std::hint::black_box(unpacked);
    }
    let unpack_duration = start.elapsed() / iterations as u32;

    DomainBenchResult {
        scale: n,
        json_lines,
        json_bytes_len: json_bytes.len(),
        bin_bytes_len: bin_bytes.len(),
        parse_duration,
        parse_allocs: 0,
        parse_bytes: 0,
        validate_duration,
        validate_allocs: 0,
        validate_bytes: 0,
        compile_duration,
        compile_allocs: 0,
        unpack_duration,
        unpack_allocs: 0,
        unpack_bytes: 0,
    }
}

fn benchmark_listeners_domain() {
    println!("### 3. Listeners Domain: High-Weight Scaling & Big-O Linearity Analysis\n");

    let test_scales = [50, 100, 250, 500, 1000];
    let mut results = Vec::new();

    for &n in &test_scales {
        let res = benchmark_listeners_scale(n);
        results.push(res);
    }

    println!(
        "| {:<14} | {:<10} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
        "Listeners (N)", "JSON Size", "Bin Size", "JSON Parse", "Validate", "Compile", "Unpack"
    );
    println!(
        "|{:-<16}|{:-<12}|{:-<12}|{:-<16}|{:-<16}|{:-<16}|{:-<16}|",
        "", "", "", "", "", "", ""
    );

    for r in &results {
        println!(
            "| {:<14} | {:<10} | {:<10} | {:<14} | {:<14} | {:<14} | {:<14} |",
            r.scale,
            format_bytes(r.json_bytes_len),
            format_bytes(r.bin_bytes_len),
            format_duration(r.parse_duration),
            format_duration(r.validate_duration),
            format_duration(r.compile_duration),
            format_duration(r.unpack_duration),
        );
    }

    println!("\n#### Listeners Big-O Linearity Drift Analysis\n");
    println!(
        "| {:<15} | {:<11} | {:<17} | {:<17} | {:<17} | {:<17} | {:<16} |",
        "Transition",
        "Scale Ratio",
        "JSON Parse Growth",
        "Validation Growth",
        "Compile Growth",
        "Unpack Growth",
        "Linearity Status"
    );
    println!(
        "|{:-<17}|{:-<13}|{:-<19}|{:-<19}|{:-<19}|{:-<19}|{:-<18}|",
        "", "", "", "", "", "", ""
    );

    for i in 1..results.len() {
        let prev = &results[i - 1];
        let curr = &results[i];

        let (scale, parse_growth, parse_drift) = calculate_big_o(
            prev.parse_duration,
            curr.parse_duration,
            prev.scale,
            curr.scale,
        );
        let (_, val_growth, _) = calculate_big_o(
            prev.validate_duration,
            curr.validate_duration,
            prev.scale,
            curr.scale,
        );
        let (_, comp_growth, _) = calculate_big_o(
            prev.compile_duration,
            curr.compile_duration,
            prev.scale,
            curr.scale,
        );
        let (_, unpack_growth, _) = calculate_big_o(
            prev.unpack_duration,
            curr.unpack_duration,
            prev.scale,
            curr.scale,
        );

        let status = if parse_drift.abs() <= 20.0 {
            "Strict O(N) [OK]"
        } else {
            "O(N) Drift"
        };

        println!(
            "| {:<6} -> {:<6} | {:<11.1}x | {:<11.2}x ({:>+4.1}%) | {:<17.2}x | {:<17.2}x | {:<17.2}x | {:<16} |",
            prev.scale,
            curr.scale,
            scale,
            parse_growth,
            parse_drift,
            val_growth,
            comp_growth,
            unpack_growth,
            status,
        );
    }
    println!();
}

fn main() {
    println!("================================================================================");
    println!(" VELDA-SYNC BENCHMARK: STAGE 3 (POST-SYNC DOMAIN COMPILERS)");
    println!(" High-Weight Scaling & Big-O Linearity Analysis across Scaling Workloads");
    println!("================================================================================\n");

    benchmark_routes_domain();
    benchmark_upstreams_domain();
    benchmark_listeners_domain();

    println!("================================================================================");
    println!(" STAGE 3 BENCHMARK COMPLETE");
    println!("================================================================================\n");
}
