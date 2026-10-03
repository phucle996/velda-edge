//! Shared test fixtures, memory tracking allocator, and mock generators for velda-edge benchmarks.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use velda_edge::runtime::{PipelineTable, Runtime, RuntimeConfig, build_router, build_upstreams};
use velda_sync::post_sync::listener::{
    ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    compile_listeners_to_binary,
};
use velda_sync::post_sync::route::{
    RouteConfig, RouteMatch, RouteTimeouts, compile_routes_to_binary,
};
use velda_sync::post_sync::upstream::{
    EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig, UpstreamTimeouts,
    compile_upstreams_to_binary,
};

// ============================================================================
// Memory Tracking Allocator
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
// Formatting Helpers
// ============================================================================

pub fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

pub fn format_duration(d: Duration) -> String {
    let micros = d.as_micros();
    if micros < 1_000 {
        format!("{micros} µs")
    } else if micros < 1_000_000 {
        format!("{:.2} ms", micros as f64 / 1_000.0)
    } else {
        format!("{:.2} s", d.as_secs_f64())
    }
}

// ============================================================================
// Fast Deterministic PRNG for Benchmarks (Zero External Crate Dependencies)
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
// Fixture Generators
// ============================================================================

pub fn build_mock_listeners(count: usize) -> Vec<ListenerConfig> {
    let mut listeners = Vec::with_capacity(count);

    for i in 0..count {
        let (proto, is_udp) = match i % 10 {
            0..=3 => ("http1", false),
            4..=6 => ("http2", false),
            7..=8 => ("http3", true),
            _ => ("grpc", false),
        };

        let transport_str = if is_udp { "udp" } else { "tcp" };
        let port = (10_000 + (i % 50_000)) as u16;
        let addr = format!("127.0.0.1:{port}");
        let id = format!("listener_{proto}_{i:04}");

        listeners.push(ListenerConfig {
            id,
            address: addr,
            transport: ListenerTransportConfig {
                protocol: transport_str.into(),
            },
            application: ListenerApplicationConfig {
                protocol: proto.into(),
                version: None,
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            tls: ListenerTlsConfig {
                enabled: i % 2 == 1,
            },
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        });
    }

    listeners
}

pub fn build_mock_upstreams(count: usize) -> Vec<UpstreamConfig> {
    let mut upstreams = Vec::with_capacity(count);

    for i in 0..count {
        let port = (20_000 + (i % 40_000)) as u16;
        let addr = format!("10.0.0.1:{port}");
        let id = format!("upstream_{i:04}");

        upstreams.push(UpstreamConfig {
            id,
            mode: "endpoints".into(),
            protocol: UpstreamProtocolConfig {
                transport: "tcp".into(),
                application: "http1".into(),
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: addr,
                weight: 100,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: UpstreamTimeouts {
                connect_ms: 1000,
                idle_ms: 10000,
                request_ms: None,
            },
            health_check: None,
            tls: None,
        });
    }

    upstreams
}

pub fn build_mock_routes(
    count: usize,
    listeners: &[ListenerConfig],
    upstreams: &[UpstreamConfig],
) -> Vec<RouteConfig> {
    let mut routes = Vec::with_capacity(count);
    if listeners.is_empty() || upstreams.is_empty() {
        return routes;
    }

    for i in 0..count {
        let listener = &listeners[i % listeners.len()];
        let upstream = &upstreams[i % upstreams.len()];
        let id = format!("route_{i:04}");
        let prefix = format!("/api/v1/resource_{:03}", i % 50);

        routes.push(RouteConfig {
            id,
            kind: "l7".into(),
            listener: listener.id.clone(),
            match_rule: RouteMatch {
                path_prefix: Some(prefix),
                protocol: Some(listener.application.protocol.clone()),
                ..Default::default()
            },
            timeouts: RouteTimeouts::default(),
            upstream: upstream.id.clone(),
            plugins: vec![],
        });
    }

    routes
}

pub fn build_mock_runtime(
    revision: u64,
    listener_count: usize,
    route_count: usize,
    upstream_count: usize,
) -> Runtime {
    let listeners = build_mock_listeners(listener_count);
    let upstreams = build_mock_upstreams(upstream_count);
    let routes = build_mock_routes(route_count, &listeners, &upstreams);

    let router = build_router(&routes, &upstreams, &listeners).unwrap();
    let pipelines = PipelineTable::build(&listeners).unwrap();
    let upstreams_table = build_upstreams(&upstreams, None);

    let config = RuntimeConfig {
        listeners,
        routes,
        upstreams,
        plugins: vec![],
        tls: vec![],
    };

    Runtime {
        revision,
        config,
        router,
        pipelines,
        upstreams: upstreams_table,
        tls_server: None,
        tls_client: None,
    }
}

pub fn write_mock_lkg_artifacts(
    runtime_dir: &Path,
    listener_count: usize,
    route_count: usize,
    upstream_count: usize,
) -> (Vec<ListenerConfig>, Vec<RouteConfig>, Vec<UpstreamConfig>) {
    fs::create_dir_all(runtime_dir).unwrap();

    let listeners = build_mock_listeners(listener_count);
    let upstreams = build_mock_upstreams(upstream_count);
    let routes = build_mock_routes(route_count, &listeners, &upstreams);

    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0x11u8; 32]).unwrap();
    let routes_bin = compile_routes_to_binary(&routes, 1, [0x22u8; 32]).unwrap();
    let upstreams_bin = compile_upstreams_to_binary(&upstreams, 1, [0x33u8; 32]).unwrap();

    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();
    fs::write(runtime_dir.join("routes.bin"), routes_bin).unwrap();
    fs::write(runtime_dir.join("upstreams.bin"), upstreams_bin).unwrap();

    (listeners, routes, upstreams)
}
