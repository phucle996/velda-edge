//! Real-World Large Dataset & Binary Artifact Pipeline Benchmark for velda-router.
//!
//! Simulates:
//! 1. Generating large-scale JSON configuration files (500, 2500, 5000 routes).
//! 2. Compiling JSON -> `.bin` binary artifacts with `DomainHeader` + SHA-256 checksums.
//! 3. Ingestion from `.bin` into RAM: header verification + bincode unpacking.
//! 4. In-Memory `Router` compilation from unpacked data (Aho-Corasick automaton + hash maps).
//! 5. Request serving hot-path latency & Zero-Allocation verification under 5,000-route table.
//! 6. Multicore concurrent throughput scaling under hyperscale routing table.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use common::{CountingAllocator, format_duration};
use velda_core::{RouteId, TransportProtocol, UpstreamId};
use velda_router::{
    GrpcRoute, GrpcRouteRequest, Http1Route, Http1RouteRequest, Router, RouterBuilder, TcpRoute,
    UdpRoute,
};
use velda_sync::post_sync::route::{
    RouteConfig, RouteMatch, RouteTimeouts, RoutesFile, compile_routes_to_binary, parse_routes,
    unpack_routes_from_binary, validate_routes,
};
use velda_sync::post_sync::upstream::{
    EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig, UpstreamTimeouts,
    UpstreamsFile, compile_upstreams_to_binary, parse_upstreams, unpack_upstreams_from_binary,
    validate_upstreams,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

fn fnv1a_hash(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// Generates a realistic heterogeneous workload of routes.
/// - 40% L7 HTTP Prefix routes
/// - 30% L7 HTTP Exact routes
/// - 20% L7 gRPC routes
/// - 10% L4 TCP/UDP routes
fn generate_realistic_workload(
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
                streaming: velda_core::StreamingMode::DISABLED,
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

fn compile_to_runtime_router(
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

            match proto {
                TransportProtocol::Tcp => {
                    let tcp =
                        TcpRoute::new(route_id, &route.listener, upstream_id, &route.upstream)
                            .with_plugins(route.plugins.clone());
                    builder = builder.add_tcp_route(tcp);
                }
                TransportProtocol::Udp => {
                    let udp =
                        UdpRoute::new(route_id, &route.listener, upstream_id, &route.upstream)
                            .with_plugins(route.plugins.clone());
                    builder = builder.add_udp_route(udp);
                }
            }
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

fn bench_pipeline_scale(scale_routes: usize, scale_upstreams: usize) {
    println!(
        "========================================================================================="
    );
    println!(
        " SCALE: {} Routes, {} Upstreams (Simulated Production Environment)",
        scale_routes, scale_upstreams
    );
    println!(
        "=========================================================================================\n"
    );

    // 1. Generate JSON payload
    let gen_start = Instant::now();
    let (routes_json, upstreams_json, n_routes, _n_upstreams) =
        generate_realistic_workload(scale_routes, scale_upstreams);
    let gen_time = gen_start.elapsed();
    println!(" [1. JSON Dataset Generated]");
    println!(
        "     - Routes JSON Size:    {:.2} KB",
        routes_json.len() as f64 / 1024.0
    );
    println!(
        "     - Upstreams JSON Size: {:.2} KB",
        upstreams_json.len() as f64 / 1024.0
    );
    println!("     - Generation Time:     {}", format_duration(gen_time));

    // 2. Control Plane: Parse JSON -> Validate -> Compile to .bin
    let cp_start = Instant::now();
    let mut parsed_routes = parse_routes(&routes_json).unwrap();
    let mut parsed_upstreams = parse_upstreams(&upstreams_json).unwrap();
    validate_routes(&mut parsed_routes).unwrap();
    validate_upstreams(&mut parsed_upstreams).unwrap();

    let routes_bin = compile_routes_to_binary(&parsed_routes, 1, [0; 32]).unwrap();
    let upstreams_bin = compile_upstreams_to_binary(&parsed_upstreams, 1, [0; 32]).unwrap();
    let cp_time = cp_start.elapsed();

    println!("\n [2. Control Plane JSON -> .bin Compilation]");
    println!(
        "     - Routes .bin Size:    {:.2} KB (ratio: {:.1}% vs JSON)",
        routes_bin.len() as f64 / 1024.0,
        (routes_bin.len() as f64 / routes_json.len() as f64) * 100.0
    );
    println!(
        "     - Upstreams .bin Size: {:.2} KB (ratio: {:.1}% vs JSON)",
        upstreams_bin.len() as f64 / 1024.0,
        (upstreams_bin.len() as f64 / upstreams_json.len() as f64) * 100.0
    );
    println!("     - Compilation Time:    {}", format_duration(cp_time));

    // 3. Data Plane Ingestion: Ingest from .bin buffer in RAM
    let dp_ingest_start = Instant::now();
    let (_hdr_r, unpacked_routes) = unpack_routes_from_binary(&routes_bin).unwrap();
    let (_hdr_u, unpacked_upstreams) = unpack_upstreams_from_binary(&upstreams_bin).unwrap();
    let dp_ingest_time = dp_ingest_start.elapsed();

    let total_bin_bytes = routes_bin.len() + upstreams_bin.len();
    let ingest_throughput_mbs =
        (total_bin_bytes as f64 / (1024.0 * 1024.0)) / dp_ingest_time.as_secs_f64();

    println!("\n [3. Data Plane .bin Ingestion into RAM (Header Verification + Unpack)]");
    println!(
        "     - Total Binary Data:   {:.2} KB",
        total_bin_bytes as f64 / 1024.0
    );
    println!(
        "     - Ingestion Time:      {}",
        format_duration(dp_ingest_time)
    );
    println!(
        "     - Ingest Throughput:   {:.2} MB/s",
        ingest_throughput_mbs
    );

    // 4. Runtime Compilation into In-Memory Router
    let router_build_start = Instant::now();
    let router = compile_to_runtime_router(&unpacked_routes, &unpacked_upstreams).unwrap();
    let router_build_time = router_build_start.elapsed();

    println!("\n [4. Runtime In-Memory Router Compilation]");
    println!(
        "     - Aho-Corasick + Maps: {}",
        format_duration(router_build_time)
    );
    println!("     - Status:              Ready for request serving hot-path");

    // 5. Hot-Path Request Serving Benchmark (1,000,000 Iterations)
    println!("\n [5. Hot-Path Request Serving Latency & Zero-Allocation (1,000,000 requests)]");
    println!("| Target Scenario | Tested Target | Latency / op | Allocs / op | Throughput |");
    println!("| :--- | :--- | :--- | :--- | :--- |");

    let iters = 1_000_000;

    // A. HTTP Prefix Match (Sample middle route)
    let mid_prefix_idx = (n_routes / 10) * 2; // matches an HTTP prefix route
    let test_prefix_path = format!("/api/v1/service_{mid_prefix_idx:04}/orders/items/42");
    let req_prefix = Http1RouteRequest::new(&test_prefix_path);

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_http1("https-in", &req_prefix);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **HTTP Prefix Hit** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_prefix_path,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // B. HTTP Exact Match
    let mid_exact_idx = 5; // matches an exact route (5 % 10 = 5)
    let test_exact_path = format!("/endpoints/action_{mid_exact_idx:04}/exec");
    let req_exact = Http1RouteRequest::new(&test_exact_path).with_host("api.example.com");

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_http1("https-in", &req_exact);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **HTTP Exact Hit** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_exact_path,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // C. HTTP Miss (Strict None, No Fallback)
    let test_miss_path = "/unknown/nonexistent/endpoint/404";
    let req_miss = Http1RouteRequest::new(test_miss_path);

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_http1("https-in", &req_miss);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **HTTP Miss (None)** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_miss_path,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // D. gRPC Lookup
    let mid_grpc_idx = 7; // matches a gRPC route (7 % 10 = 7)
    let test_grpc_service = format!("service.v1.Service_{mid_grpc_idx:04}");
    let req_grpc = GrpcRouteRequest::new(&test_grpc_service, None);

    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_grpc("https-in", &req_grpc);
        let _ = std::hint::black_box(r);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **gRPC Service Hit** | `{}` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        test_grpc_service,
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // E. L4 UDP + Target Selection
    ALLOCATOR.reset();
    let start = Instant::now();
    for _ in 0..iters {
        let r = router.route_udp("l4-in-9").unwrap();
        let target = r.id;
        let _ = std::hint::black_box(target);
    }
    let elapsed = start.elapsed();
    let (allocs, _) = ALLOCATOR.snapshot();
    let ns_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_sec = (iters as f64 / elapsed.as_secs_f64()) as u64;
    println!(
        "| **L4 UDP RoundRobin** | `udp-in:53` | **{:.2} ns** | **{:.2}** | {} ops/s |",
        ns_op,
        allocs as f64 / iters as f64,
        ops_sec
    );

    // 6. Multicore Concurrency Scaling (128 Workers)
    let router_arc = Arc::new(router);
    let workers = 128;
    let ops_per_worker = 100_000;
    let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);

    for w in 0..workers {
        let r = Arc::clone(&router_arc);
        let b = Arc::clone(&barrier);
        let p_path = test_prefix_path.clone();
        let e_path = test_exact_path.clone();
        let g_srv = test_grpc_service.clone();

        handles.push(std::thread::spawn(move || {
            b.wait();
            for i in 0..ops_per_worker {
                match (i + w) % 4 {
                    0 => {
                        let req = Http1RouteRequest::new(&p_path);
                        let route = r.route_http1("https-in", &req);
                        let _ = std::hint::black_box(route);
                    }
                    1 => {
                        let req = Http1RouteRequest::new(&e_path).with_host("api.example.com");
                        let route = r.route_http1("https-in", &req);
                        let _ = std::hint::black_box(route);
                    }
                    2 => {
                        let req = GrpcRouteRequest::new(&g_srv, None);
                        let route = r.route_grpc("https-in", &req);
                        let _ = std::hint::black_box(route);
                    }
                    _ => {
                        if let Some(route) = r.route_udp("l4-in-9") {
                            let ep = route.id;
                            let _ = std::hint::black_box(ep);
                        }
                    }
                }
            }
        }));
    }

    barrier.wait();
    let mc_start = Instant::now();

    for h in handles {
        h.join().unwrap();
    }
    let mc_elapsed = mc_start.elapsed();
    let total_ops = (workers as u64) * (ops_per_worker as u64);
    let agg_m_ops = (total_ops as f64 / mc_elapsed.as_secs_f64()) / 1_000_000.0;
    let avg_ns = mc_elapsed.as_nanos() as f64 / total_ops as f64;

    println!("\n [6. Multicore Concurrency Throughput (128 Workers)]");
    println!("     - Aggregate Throughput: **{:.2} M ops/s**", agg_m_ops);
    println!("     - Average Op Latency:   **{:.2} ns**", avg_ns);
    println!("     - Total Operations:     {}", total_ops);
    println!(
        "     - Wall Clock Duration:  {}\n\n",
        format_duration(mc_elapsed)
    );
}

fn main() {
    println!(
        "\n#########################################################################################"
    );
    println!("# VELDA EDGE: END-TO-END LARGE DATASET & BINARY INGEST ROUTER BENCHMARK SUITE");
    println!(
        "#########################################################################################\n"
    );

    bench_pipeline_scale(500, 100);
    bench_pipeline_scale(2_500, 500);
    bench_pipeline_scale(5_000, 1_000);
}
