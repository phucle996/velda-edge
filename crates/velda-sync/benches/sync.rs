use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use velda_sync::post_sync::route::{
    RouteConfig, RouteMatch, RoutesFile, compile_routes_to_binary, parse_routes,
    unpack_routes_from_binary, validate_routes,
};

// ============================================================================
// 1. Precise Heap Allocation Counter
// ============================================================================
struct CountingAllocator {
    alloc_count: AtomicU64,
    bytes_allocated: AtomicU64,
}

impl CountingAllocator {
    const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.alloc_count.store(0, Ordering::SeqCst);
        self.bytes_allocated.store(0, Ordering::SeqCst);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.alloc_count.load(Ordering::SeqCst),
            self.bytes_allocated.load(Ordering::SeqCst),
        )
    }
}

#[allow(clippy::all)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ============================================================================
// 2. Synthetic Configuration Generator
// ============================================================================
fn generate_workload(num_routes: usize) -> (Vec<RouteConfig>, Vec<u8>, usize) {
    let routes: Vec<RouteConfig> = (0..num_routes)
        .map(|i| RouteConfig {
            id: format!("route_{i:05}"),
            kind: "l7".into(),
            listener: "http".into(),
            match_rule: RouteMatch {
                host: Some(format!("service-{}.velda.io", i % 5)),
                path: None,
                path_prefix: Some(format!("/api/v1/service_{}/resource_{i}", i % 10)),
                protocol: Some("http".into()),
            },
            timeouts: Default::default(),
            upstream: format!("upstream_{}", i % 10),
            plugins: vec![],
        })
        .collect();

    let file = RoutesFile {
        schema_version: 1,
        routes: routes.clone(),
    };

    let json_bytes = serde_json::to_vec_pretty(&file).unwrap();
    let json_lines = json_bytes.split(|&b| b == b'\n').count();

    (routes, json_bytes, json_lines)
}

// ============================================================================
// 3. Benchmark Metrics Record
// ============================================================================
#[derive(Debug, Clone)]
struct BenchResult {
    num_routes: usize,
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

fn benchmark_scale(n: usize) -> BenchResult {
    let (routes, json_bytes, json_lines) = generate_workload(n);

    // Warm-up
    let _ = parse_routes(&json_bytes).unwrap();
    let mut warmup_routes = routes.clone();
    let _ = validate_routes(&mut warmup_routes);
    let bin_warmup = compile_routes_to_binary(&routes, 1, [0u8; 32]).unwrap();
    let _ = unpack_routes_from_binary(&bin_warmup).unwrap();

    let iterations = match n {
        0..=100 => 50,
        101..=1000 => 20,
        _ => 5,
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

    BenchResult {
        num_routes: n,
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

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn format_duration(dur: Duration) -> String {
    let micros = dur.as_micros();
    if micros >= 1000 {
        format!("{:.2} ms", dur.as_secs_f64() * 1000.0)
    } else {
        format!("{micros:.1} µs")
    }
}

fn main() {
    println!("================================================================================");
    println!(" VELDA-SYNC: BIG-O COMPLEXITY & DOMAIN HEAP ALLOCATION BENCHMARK");
    println!(" Measuring Routes Domain: Parse, Validate, Compile, Unpack across Scaling N");
    println!("================================================================================\n");

    let test_scales = [50, 100, 500, 1000, 2500, 5000];
    let mut results = Vec::new();

    for &n in &test_scales {
        let res = benchmark_scale(n);
        results.push(res);
    }

    println!("### 1. Raw Execution Time & Heap Allocation Metrics\n");
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
            r.num_routes,
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

    println!("\n### 2. Big-O Growth Scaling Factor Analysis (N_prev -> N_curr)\n");
    println!(
        "| {:<11} | {:<11} | {:<17} | {:<17} | {:<21} | {:<20} | {:<16} |",
        "Transition",
        "Scale Ratio",
        "JSON Parse Growth",
        "Validation Growth",
        "Binary Compile Growth",
        "Binary Unpack Growth",
        "Expected if O(N)"
    );
    println!(
        "|{:-<13}|{:-<13}|{:-<19}|{:-<19}|{:-<23}|{:-<22}|{:-<18}|",
        "", "", "", "", "", "", ""
    );

    for i in 1..results.len() {
        let prev = &results[i - 1];
        let curr = &results[i];

        let ratio = curr.num_routes as f64 / prev.num_routes as f64;
        let parse_ratio =
            curr.parse_duration.as_nanos() as f64 / prev.parse_duration.as_nanos() as f64;
        let val_ratio =
            curr.validate_duration.as_nanos() as f64 / prev.validate_duration.as_nanos() as f64;
        let comp_ratio =
            curr.compile_duration.as_nanos() as f64 / prev.compile_duration.as_nanos() as f64;
        let unpack_ratio =
            curr.unpack_duration.as_nanos() as f64 / prev.unpack_duration.as_nanos() as f64;

        println!(
            "| {:<5} -> {:<4} | {:<11.1}x | {:<17.2}x | {:<17.2}x | {:<21.2}x | {:<20.2}x | {:<16.1}x |",
            prev.num_routes,
            curr.num_routes,
            ratio,
            parse_ratio,
            val_ratio,
            comp_ratio,
            unpack_ratio,
            ratio
        );
    }

    println!("\n### 3. Edge Serving Boot Advantage: Binary Unpack vs JSON Parse\n");
    println!(
        "| {:<10} | {:<18} | {:<21} | {:<15} | {:<16} | {:<18} | {:<16} | {:<17} |",
        "Routes (N)",
        "JSON Parse Latency",
        "Binary Unpack Latency",
        "Latency Speedup",
        "JSON Allocations",
        "Binary Allocations",
        "Alloc Reduction",
        "Storage Reduction"
    );
    println!(
        "|{:-<12}|{:-<20}|{:-<23}|{:-<17}|{:-<18}|{:-<20}|{:-<18}|{:-<19}|",
        "", "", "", "", "", "", "", ""
    );

    for r in &results {
        let speedup = r.parse_duration.as_nanos() as f64 / r.unpack_duration.as_nanos() as f64;
        let alloc_reduction = r.parse_allocs as f64 / r.unpack_allocs.max(1) as f64;
        let storage_reduction = 100.0 * (1.0 - (r.bin_bytes_len as f64 / r.json_bytes_len as f64));

        println!(
            "| {:<10} | {:<18} | {:<21} | {:<15.1}x | {:<16} | {:<18} | {:<16.1}x | {:<17.1}% |",
            r.num_routes,
            format_duration(r.parse_duration),
            format_duration(r.unpack_duration),
            speedup,
            r.parse_allocs,
            r.unpack_allocs,
            alloc_reduction,
            storage_reduction,
        );
    }

    println!("\n================================================================================");
    println!(" BENCHMARK COMPLETE");
    println!("================================================================================");
}
