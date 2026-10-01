//! Benchmark helper utilities, memory allocators, and realistic dataset pipeline loaders
//! for velda-router.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use velda_core::{RouteId, TransportProtocol, UpstreamId};
use velda_router::{GrpcRoute, Http1Route, L4Route, Router, RouterBuilder};
use velda_sync::post_sync::route::{
    RouteConfig, RouteMatch, RouteTimeouts, RoutesFile, compile_routes_to_binary, parse_routes,
    unpack_routes_from_binary, validate_routes,
};
use velda_sync::post_sync::upstream::{
    EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig, UpstreamTimeouts,
    UpstreamsFile, compile_upstreams_to_binary, parse_upstreams, unpack_upstreams_from_binary,
    validate_upstreams,
};

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

pub fn fnv1a_hash(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// Generates a realistic heterogeneous workload of routes and upstreams in JSON.
/// - 40% L7 HTTP Prefix routes
/// - 30% L7 HTTP Exact routes
/// - 20% L7 gRPC routes
/// - 10% L4 TCP/UDP routes
pub fn generate_realistic_workload(
    num_routes: usize,
    num_upstreams: usize,
) -> (Vec<u8>, Vec<u8>, usize, usize) {
    let mut upstreams = Vec::with_capacity(num_upstreams);
    for i in 0..num_upstreams {
        let ep1 = format!("10.{}.{}.{}:8080", (i / 256) % 256, i % 256, (i * 2) % 256);
        let ep2 = format!(
            "10.{}.{}.{}:8080",
            (i / 256) % 256,
            i % 256,
            (i * 2 + 1) % 256
        );

        upstreams.push(UpstreamConfig {
            id: format!("upstream_{i:04}"),
            mode: "endpoints".into(),
            protocol: UpstreamProtocolConfig {
                transport: if i % 10 == 0 {
                    "udp".into()
                } else {
                    "tcp".into()
                },
                application: "raw".into(),
                streaming: velda_core::StreamingMode::Disabled,
            },
            target: None,
            resolver: None,
            endpoints: vec![
                EndpointConfig {
                    address: ep1,
                    weight: 100,
                },
                EndpointConfig {
                    address: ep2,
                    weight: 100,
                },
            ],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: UpstreamTimeouts {
                connect_ms: 1000,
                idle_ms: 60000,
                request_ms: None,
            },
            health_check: None,
            tls: None,
        });
    }

    let mut routes = Vec::with_capacity(num_routes);
    for i in 0..num_routes {
        let up_id = format!("upstream_{:04}", i % num_upstreams);
        let route_type = i % 10;

        let (kind, match_rule) = match route_type {
            // 0..=3: L7 HTTP Prefix (40%)
            0..=3 => (
                "l7".to_string(),
                RouteMatch {
                    host: Some(format!("api{}.example.com", i % 50)),
                    path_prefix: Some(format!("/api/v1/service_{i:04}")),
                    path: None,
                    protocol: None,
                },
            ),
            // 4..=6: L7 HTTP Exact (30%)
            4..=6 => (
                "l7".to_string(),
                RouteMatch {
                    host: Some("api.example.com".into()),
                    path_prefix: None,
                    path: Some(format!("/endpoints/action_{i:04}/exec")),
                    protocol: None,
                },
            ),
            // 7..=8: L7 gRPC (20%)
            7..=8 => (
                "l7".to_string(),
                RouteMatch {
                    host: Some("grpc.example.com".into()),
                    path_prefix: None,
                    path: Some(format!("/service.v1.Service_{i:04}")),
                    protocol: Some("grpc".into()),
                },
            ),
            // 9: L4 TCP / UDP (10%)
            _ => (
                "l4".to_string(),
                RouteMatch {
                    host: None,
                    path_prefix: None,
                    path: None,
                    protocol: Some(if i % 2 == 0 {
                        "tcp".into()
                    } else {
                        "udp".into()
                    }),
                },
            ),
        };

        routes.push(RouteConfig {
            id: format!("route_{i:05}"),
            kind,
            listener: if route_type == 9 {
                format!("l4-in-{i}")
            } else {
                "https-in".into()
            },
            match_rule,
            timeouts: RouteTimeouts {
                downstream_idle_ms: Some(30000),
            },
            upstream: up_id,
            plugins: vec!["rate-limit".into(), "telemetry".into()],
        });
    }

    let routes_file = RoutesFile {
        schema_version: 1,
        routes,
    };
    let upstreams_file = UpstreamsFile {
        schema_version: 1,
        upstreams,
    };

    let routes_json = serde_json::to_vec_pretty(&routes_file).unwrap();
    let upstreams_json = serde_json::to_vec_pretty(&upstreams_file).unwrap();

    (routes_json, upstreams_json, num_routes, num_upstreams)
}

/// Compiles unpacked configurations into an in-memory runtime [`Router`].
pub fn compile_to_runtime_router(
    routes: &[RouteConfig],
    upstreams: &[UpstreamConfig],
) -> Result<Router, String> {
    let mut builder = RouterBuilder::new();

    for route in routes {
        let route_id = RouteId::new(fnv1a_hash(&route.id));
        let upstream_id = UpstreamId::new(fnv1a_hash(&route.upstream));

        if route.kind.eq_ignore_ascii_case("l4") {
            let proto = match route.match_rule.protocol.as_deref().unwrap_or("tcp") {
                "udp" => TransportProtocol::Udp,
                _ => TransportProtocol::Tcp,
            };

            let mut targets = Vec::new();
            if let Some(up) = upstreams.iter().find(|u| u.id == route.upstream) {
                for ep in &up.endpoints {
                    if let Ok(addr) = ep.address.parse::<SocketAddr>() {
                        targets.push(addr);
                    }
                }
            }

            let l4 = L4Route::new(
                route_id,
                &route.listener,
                proto,
                upstream_id,
                &route.upstream,
            )
            .with_target_endpoints(targets)
            .with_plugins(route.plugins.clone());

            builder = builder.add_l4_route(l4);
        } else if route.kind.eq_ignore_ascii_case("l7") {
            let is_grpc = route
                .match_rule
                .protocol
                .as_deref()
                .map(|p| p.eq_ignore_ascii_case("grpc"))
                .unwrap_or(false);

            if is_grpc {
                let raw_service = route
                    .match_rule
                    .path
                    .as_deref()
                    .or(route.match_rule.path_prefix.as_deref())
                    .unwrap_or("*");
                let service = raw_service.strip_prefix('/').unwrap_or(raw_service);

                let mut grpc_route = GrpcRoute::new(
                    route_id,
                    &route.listener,
                    service,
                    upstream_id,
                    &route.upstream,
                )
                .with_plugins(route.plugins.clone());

                if let Some(ref host) = route.match_rule.host {
                    grpc_route = grpc_route.with_authority(host.clone());
                }

                builder = builder.add_grpc_route(grpc_route);
            } else {
                let mut http_route = if let Some(ref exact) = route.match_rule.path {
                    Http1Route::new_exact(
                        route_id,
                        &route.listener,
                        exact.clone(),
                        upstream_id,
                        &route.upstream,
                    )
                } else if let Some(ref prefix) = route.match_rule.path_prefix {
                    Http1Route::new(
                        route_id,
                        &route.listener,
                        prefix.clone(),
                        upstream_id,
                        &route.upstream,
                    )
                } else {
                    Http1Route::new(route_id, &route.listener, "/", upstream_id, &route.upstream)
                }
                .with_plugins(route.plugins.clone());

                if let Some(ref host) = route.match_rule.host {
                    http_route = http_route.with_host(host.clone());
                }

                builder = builder.add_http1_route(http_route);
            }
        }
    }

    builder.build().map_err(|e| e.to_string())
}

/// End-to-end pipeline: Generates large JSON -> Compiles to .bin -> Ingests into RAM -> Builds Router.
pub fn load_router_from_large_dataset(num_routes: usize, num_upstreams: usize) -> Router {
    let (routes_json, upstreams_json, _, _) =
        generate_realistic_workload(num_routes, num_upstreams);

    let mut parsed_routes = parse_routes(&routes_json).unwrap();
    let mut parsed_upstreams = parse_upstreams(&upstreams_json).unwrap();
    validate_routes(&mut parsed_routes).unwrap();
    validate_upstreams(&mut parsed_upstreams).unwrap();

    let routes_bin = compile_routes_to_binary(&parsed_routes, 1, [0; 32]).unwrap();
    let upstreams_bin = compile_upstreams_to_binary(&parsed_upstreams, 1, [0; 32]).unwrap();

    let (_hdr_r, unpacked_routes) = unpack_routes_from_binary(&routes_bin).unwrap();
    let (_hdr_u, unpacked_upstreams) = unpack_upstreams_from_binary(&upstreams_bin).unwrap();

    compile_to_runtime_router(&unpacked_routes, &unpacked_upstreams).unwrap()
}
