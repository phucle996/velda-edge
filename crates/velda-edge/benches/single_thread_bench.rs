//! Single-Thread Performance, Latency & Zero-Allocation Benchmark Suite for velda-edge.
//!
//! Evaluates hot-path single-thread performance:
//! 1. `SharedRuntime::load()` Hot-Path Read Latency & Zero-Allocation Invariant across 100, 1,000, and 5,000 Listeners
//! 2. `PipelineTable` Protocol Dispatch Table Lookup (`tcp_pipeline`, `udp_pipeline`) across protocols
//! 3. `RuntimeProfile` Hardware Adaptation & Deep-Merge Resolution Latency
//! 4. `RuntimeConfig::active_bindings()` Transport Ingress Transformation Latency

mod common;

use std::fs;
use std::time::Instant;
use tempfile::tempdir;

use common::{CountingAllocator, build_mock_runtime};
use velda_core::hardware::HardwareTopology;
use velda_edge::runtime::new_shared_runtime;
use velda_edge::{RuntimeProfile, resolve_runtime_profile};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// Stage 1: SharedRuntime::load() Hot-Path Read Latency & Zero-Allocation
// ============================================================================

fn bench_shared_runtime_load_latency() {
    println!("### 1. `SharedRuntime::load()` Hot-Path Read Latency & Zero-Allocation Invariant\n");
    println!(
        "| Table Size | Scenario | Latency / op | Allocs / op | Throughput | Invariant Status |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 5_000_000;
    let sizes = [100, 1_000, 5_000];

    for &size in &sizes {
        let rt = build_mock_runtime(1, size, size, 50);
        let shared = new_shared_runtime(rt);

        // Warm-up to initialize thread-local ArcSwap epoch registrations
        for _ in 0..10_000 {
            let g = shared.load();
            let _ = std::hint::black_box(&g);
        }

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let guard = shared.load();
            let _ = std::hint::black_box(&guard);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        let status = if ns_op < 15.0 && allocs == 0 {
            "**PASS (< 15 ns, 0 allocs)**"
        } else if allocs == 0 {
            "**PASS (0 allocs)**"
        } else {
            "**FAIL (heap alloc)**"
        };

        println!(
            "| **N = {}** | ArcSwap lock-free read | **{:.2} ns** | **{:.2}** | {} ops/s | {} |",
            size,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec,
            status
        );
    }
    println!();
}

// ============================================================================
// Stage 2: PipelineTable Protocol Dispatch Table Lookup
// ============================================================================

fn bench_pipeline_dispatch_lookup() {
    println!("### 2. `PipelineTable` Protocol Dispatch Table Lookup\n");
    println!(
        "| Protocol Target | Lookup Type | Target ID | Latency / op | Allocs / op | Throughput |"
    );
    println!("| :--- | :--- | :--- | :--- | :--- | :--- |");

    let iters = 2_000_000;
    let rt = build_mock_runtime(1, 1_000, 1_000, 50);
    let table = &rt.pipelines;

    let scenarios = [
        ("HTTP/1.1 Cleartext", "TCP Hit", "listener_http1_0000", true),
        ("HTTP/2 TLS", "TCP Hit", "listener_http2_0005", true),
        ("gRPC Cleartext", "TCP Hit", "listener_grpc_0009", true),
        ("HTTP/3 QUIC", "UDP Hit", "listener_http3_0007", false),
        (
            "Deterministic Miss",
            "TCP Miss",
            "listener_nonexistent_9999",
            true,
        ),
    ];

    for (desc, lookup_type, id, is_tcp) in scenarios {
        ALLOCATOR.reset();
        let start = Instant::now();

        if is_tcp {
            for _ in 0..iters {
                let res = table.tcp_pipeline(id);
                let _ = std::hint::black_box(res);
            }
        } else {
            for _ in 0..iters {
                let res = table.udp_pipeline(id);
                let _ = std::hint::black_box(res);
            }
        }

        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| **{}** | {} | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
            desc,
            lookup_type,
            id,
            ns_op,
            allocs as f64 / iters as f64,
            ops_sec
        );
    }
    println!();
}

// ============================================================================
// Stage 3: RuntimeProfile Hardware Adaptation & Deep-Merge Resolution
// ============================================================================

fn bench_runtime_profile_resolution() {
    println!("### 3. `RuntimeProfile` Hardware Adaptation & Deep-Merge Resolution\n");
    println!("| Scenario | Details | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    // 1. In-Memory Hardware Profile Generation
    let iters_mem = 1_000_000;
    let hardware = HardwareTopology::with_workers_and_memory(8, 16 * 1024 * 1024 * 1024);
    ALLOCATOR.reset();
    let start_mem = Instant::now();
    for _ in 0..iters_mem {
        let profile = RuntimeProfile::from_hardware(&hardware);
        let _ = std::hint::black_box(profile);
    }
    let elapsed_mem = start_mem.elapsed();
    let (allocs_mem, _) = ALLOCATOR.snapshot();
    let ns_mem = elapsed_mem.as_nanos() as f64 / iters_mem as f64;
    let ops_mem = (iters_mem as f64 / elapsed_mem.as_secs_f64()) as u64;

    println!(
        "| **from_hardware (In-Memory)** | 16 GB Large Tier probe | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_mem,
        allocs_mem as f64 / iters_mem as f64,
        ops_mem
    );

    // 2. DNS Config Conversion
    let profile = RuntimeProfile::from_hardware(&hardware);
    ALLOCATOR.reset();
    let start_dns = Instant::now();
    for _ in 0..iters_mem {
        let dns = profile.to_dns_resolver_config();
        let _ = std::hint::black_box(dns);
    }
    let elapsed_dns = start_dns.elapsed();
    let (allocs_dns, _) = ALLOCATOR.snapshot();
    let ns_dns = elapsed_dns.as_nanos() as f64 / iters_mem as f64;
    let ops_dns = (iters_mem as f64 / elapsed_dns.as_secs_f64()) as u64;

    println!(
        "| **to_dns_resolver_config** | Struct conversion | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_dns,
        allocs_dns as f64 / iters_mem as f64,
        ops_dns
    );

    // 3. Disk Deep-Merge & Persistence (Cold vs Warm)
    let tmp = tempdir().unwrap();
    let runtime_dir = tmp.path().join("runtime");
    fs::create_dir_all(&runtime_dir).unwrap();

    let iters_disk = 10_000;
    ALLOCATOR.reset();
    let start_disk = Instant::now();
    for _ in 0..iters_disk {
        let p = resolve_runtime_profile(&runtime_dir, &hardware);
        let _ = std::hint::black_box(p);
    }
    let elapsed_disk = start_disk.elapsed();
    let (allocs_disk, _) = ALLOCATOR.snapshot();
    let ns_disk = elapsed_disk.as_nanos() as f64 / iters_disk as f64;
    let ops_disk = (iters_disk as f64 / elapsed_disk.as_secs_f64()) as u64;

    println!(
        "| **resolve_runtime_profile (Disk)** | JSON parse, tier merge, write | **{:.2} µs** | **{:.2}** | {} ops/s |",
        ns_disk / 1_000.0,
        allocs_disk as f64 / iters_disk as f64,
        ops_disk
    );
    println!();
}

// ============================================================================
// Stage 4: RuntimeConfig::active_bindings() Transformation Latency
// ============================================================================

fn bench_active_bindings_transformation() {
    println!("### 4. `RuntimeConfig::active_bindings()` Ingress Transformation\n");
    println!("| Listener Count | Total Bindings | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 10_000;
    let sizes = [100, 1_000, 5_000];

    for &size in &sizes {
        let rt = build_mock_runtime(1, size, size, 50);

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..iters {
            let bindings = rt.active_bindings().unwrap();
            let _ = std::hint::black_box(bindings);
        }
        let elapsed = start.elapsed();
        let (allocs, _) = ALLOCATOR.snapshot();
        let ns_op = elapsed.as_nanos() as f64 / iters as f64;
        let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;

        println!(
            "| **N = {}** | {} ingress ports | **{:.2} µs** | **{:.2}** | {} ops/s |",
            size,
            size,
            ns_op / 1_000.0,
            allocs as f64 / iters as f64,
            ops_sec
        );
    }
    println!();
}

fn main() {
    println!("\n================================================================================");
    println!("  VELDA-EDGE: SINGLE-THREAD PERFORMANCE & LATENCY BENCHMARK SUITE");
    println!("================================================================================\n");

    bench_shared_runtime_load_latency();
    bench_pipeline_dispatch_lookup();
    bench_runtime_profile_resolution();
    bench_active_bindings_transformation();

    println!("================================================================================\n");
}
