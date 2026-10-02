//! Common utilities, custom counting allocator, and synthetic workload generators
//! for velda-sync micro-benchmarks.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use velda_sync::post_sync::listener::{ListenerConfig, ListenersFile};
use velda_sync::post_sync::route::{RouteConfig, RoutesFile};
use velda_sync::post_sync::upstream::{UpstreamConfig, UpstreamsFile};
use velda_sync::{ManifestConfig, ManifestFileEntry, ManifestFiles};

// ============================================================================
// 1. Precise Heap Allocation Counter & AllocSnapshot
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocSnapshot {
    pub alloc_count: u64,
    pub dealloc_count: u64,
    pub bytes_allocated: u64,
    pub bytes_deallocated: u64,
}

impl AllocSnapshot {
    pub fn net_bytes(&self) -> i64 {
        self.bytes_allocated as i64 - self.bytes_deallocated as i64
    }

    pub fn net_allocs(&self) -> i64 {
        self.alloc_count as i64 - self.dealloc_count as i64
    }
}

pub struct CountingAllocator {
    alloc_count: AtomicU64,
    dealloc_count: AtomicU64,
    bytes_allocated: AtomicU64,
    bytes_deallocated: AtomicU64,
}

impl CountingAllocator {
    pub const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
            bytes_deallocated: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.alloc_count.store(0, Ordering::SeqCst);
        self.dealloc_count.store(0, Ordering::SeqCst);
        self.bytes_allocated.store(0, Ordering::SeqCst);
        self.bytes_deallocated.store(0, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.alloc_count.load(Ordering::SeqCst),
            self.bytes_allocated.load(Ordering::SeqCst),
        )
    }

    pub fn detailed_snapshot(&self) -> AllocSnapshot {
        AllocSnapshot {
            alloc_count: self.alloc_count.load(Ordering::SeqCst),
            dealloc_count: self.dealloc_count.load(Ordering::SeqCst),
            bytes_allocated: self.bytes_allocated.load(Ordering::SeqCst),
            bytes_deallocated: self.bytes_deallocated.load(Ordering::SeqCst),
        }
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
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_deallocated
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

// ============================================================================
// Fast Deterministic PRNG for Benchmarks
// ============================================================================

pub struct FastRng {
    state: u64,
}

impl FastRng {
    pub const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0xdeadbeefcafebabe } else { seed },
        }
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[inline]
    pub fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        (self.next_u64() as usize) % max
    }
}

// ============================================================================
// 2. Formatting Utilities
// ============================================================================

pub fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_duration(dur: Duration) -> String {
    let nanos = dur.as_nanos();
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.2} µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", dur.as_secs_f64())
    }
}

pub fn format_throughput(bytes: usize, dur: Duration) -> String {
    let secs = dur.as_secs_f64();
    if secs <= 0.0 {
        return "N/A".into();
    }
    let mb_per_sec = (bytes as f64 / (1024.0 * 1024.0)) / secs;
    if mb_per_sec >= 1024.0 {
        format!("{:.2} GB/s", mb_per_sec / 1024.0)
    } else {
        format!("{mb_per_sec:.2} MB/s")
    }
}

pub fn calculate_big_o(
    prev_dur: Duration,
    curr_dur: Duration,
    prev_n: usize,
    curr_n: usize,
) -> (f64, f64, f64) {
    let scale_ratio = curr_n as f64 / prev_n as f64;
    let growth_ratio = curr_dur.as_nanos() as f64 / prev_dur.as_nanos().max(1) as f64;
    let drift_pct = (growth_ratio / scale_ratio - 1.0) * 100.0;
    (scale_ratio, growth_ratio, drift_pct)
}

// ============================================================================
// 3. Real Workload Loaders & Scaled Generators (Backed by crates/velda-sync/example)
// ============================================================================

pub fn example_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("example")
}

pub fn load_example_routes() -> RoutesFile {
    let path = example_dir().join("routes.json");
    let content =
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
    serde_json::from_slice(&content)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {e}", path.display()))
}

pub fn load_example_upstreams() -> UpstreamsFile {
    let path = example_dir().join("upstreams.json");
    let content =
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
    serde_json::from_slice(&content)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {e}", path.display()))
}

pub fn load_example_listeners() -> ListenersFile {
    let path = example_dir().join("listeners.json");
    let content =
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
    serde_json::from_slice(&content)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {e}", path.display()))
}

pub fn load_example_manifest() -> (ManifestConfig, Vec<u8>) {
    let path = example_dir().join("manifest.json");
    let content =
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
    let manifest: ManifestConfig = serde_json::from_slice(&content)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {e}", path.display()));
    (manifest, content)
}

pub fn generate_routes_workload(num_routes: usize) -> (Vec<RouteConfig>, Vec<u8>, usize) {
    let base_file = load_example_routes();
    let base_routes = base_file.routes;

    let routes: Vec<RouteConfig> = (0..num_routes)
        .map(|i| {
            let mut r = base_routes[i % base_routes.len()].clone();
            r.id = format!("{}_{i:06}", r.id);
            if let Some(ref host) = r.match_rule.host {
                r.match_rule.host = Some(format!("srv-{}.{}", i % 50, host));
            }
            if let Some(ref prefix) = r.match_rule.path_prefix {
                r.match_rule.path_prefix =
                    Some(format!("{}/item_{i}", prefix.trim_end_matches('/')));
            }
            if let Some(ref path) = r.match_rule.path {
                r.match_rule.path = Some(format!("{}/{i}", path.trim_end_matches('/')));
            }
            r.upstream = format!("{}_{}", r.upstream, i % 20);
            r
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

pub fn generate_upstreams_workload(num_upstreams: usize) -> (Vec<UpstreamConfig>, Vec<u8>, usize) {
    let base_file = load_example_upstreams();
    let base_upstreams = base_file.upstreams;

    let upstreams: Vec<UpstreamConfig> = (0..num_upstreams)
        .map(|i| {
            let mut u = base_upstreams[i % base_upstreams.len()].clone();
            u.id = format!("{}_{i:05}", u.id);
            if let Some(ref mut target) = u.target {
                target.host = format!("inst-{}.{}", i, target.host);
            }
            if let Some(ref mut tls) = u.tls.as_mut().filter(|t| t.sni.is_empty()) {
                tls.sni = vec![format!("backend-{i}.internal")];
            }
            for (idx, ep) in u.endpoints.iter_mut().enumerate() {
                ep.address = format!(
                    "10.{}.{}.{}:8080",
                    (i / 65536) % 256,
                    (i / 256) % 256,
                    (i + idx) % 256
                );
            }
            u
        })
        .collect();

    let file = UpstreamsFile {
        schema_version: 1,
        upstreams: upstreams.clone(),
    };

    let json_bytes = serde_json::to_vec_pretty(&file).unwrap();
    let json_lines = json_bytes.split(|&b| b == b'\n').count();

    (upstreams, json_bytes, json_lines)
}

pub fn generate_listeners_workload(num_listeners: usize) -> (Vec<ListenerConfig>, Vec<u8>, usize) {
    let base_file = load_example_listeners();
    let base_listeners = base_file.listeners;

    let listeners: Vec<ListenerConfig> = (0..num_listeners)
        .map(|i| {
            let mut l = base_listeners[i % base_listeners.len()].clone();
            l.id = format!("{}_{i:04}", l.id);
            let port = 8000 + (i as u16);
            l.address = format!("0.0.0.0:{port}");
            l
        })
        .collect();

    let file = ListenersFile {
        schema_version: 1,
        listeners: listeners.clone(),
    };

    let json_bytes = serde_json::to_vec_pretty(&file).unwrap();
    let json_lines = json_bytes.split(|&b| b == b'\n').count();

    (listeners, json_bytes, json_lines)
}

pub fn generate_manifest_workload(num_domains: usize, revision: u64) -> (ManifestConfig, Vec<u8>) {
    let files: Vec<ManifestFileEntry> = (0..num_domains)
        .map(|i| ManifestFileEntry {
            name: format!("domain_{i}"),
            path: format!("domains/domain_{i}.json"),
            required: true,
            revision: Some(revision),
            hash: Some(format!("hash_{revision}_{i}")),
        })
        .collect();

    let manifest = ManifestConfig {
        schema_version: 1,
        revision,
        configuration: ManifestFiles { files },
    };

    let bytes = serde_json::to_vec(&manifest).unwrap();
    (manifest, bytes)
}
