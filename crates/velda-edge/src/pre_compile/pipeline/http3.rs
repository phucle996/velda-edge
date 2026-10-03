//! Layer 7 HTTP/3 pipeline: route evaluation, upstream forwarding over QUIC,
//! and persistent packet-driven state machine management.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http3"`.
//! Routes matched via `velda-router::Http3Router`, forwarded via `velda-http3`.
//!
//! ### Zero-Disruption Reload Invariant:
//! Active HTTP/3 QUIC state machine engines persist across configuration reloads.
//! When routes, upstreams, or plugins reload, active client QUIC connections
//! are NOT disconnected; new incoming requests on existing connections seamlessly
//! evaluate against the latest swapped runtime snapshot.

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};

use rustc_hash::FxHashMap;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::sync::Mutex;
use velda_core::{L7Request, L7Response};
use velda_http3::Http3Engine;
use velda_router::Http3RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::{Datagram, UdpSocket};

use crate::error::EdgeError;
use crate::pipeline::context::IngressContext;
use crate::runtime::SharedRuntime;

/// Sharded container of HTTP/3 engines partitioned by remote peer address to eliminate
/// lock contention on multi-core hardware topologies.
#[derive(Clone)]
pub struct H3EngineShards {
    shards: Arc<Vec<Arc<Mutex<Http3Engine>>>>,
}

impl H3EngineShards {
    /// Creates a sharded pool scaled dynamically to available hardware parallelism.
    pub fn new(
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
    ) -> Self {
        let count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(2, 64)
            .next_power_of_two();

        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            let engine = Http3Engine::new_with_config(quic_cfg.clone(), h3_config);
            shards.push(Arc::new(Mutex::new(engine)));
        }
        Self {
            shards: Arc::new(shards),
        }
    }

    /// Selects an engine shard deterministically by hashing remote peer IP and port.
    #[inline]
    pub fn get_shard_for_peer(&self, peer: SocketAddr) -> Arc<Mutex<Http3Engine>> {
        let port = peer.port() as usize;
        let ip_hash = match peer.ip() {
            std::net::IpAddr::V4(v4) => u32::from_ne_bytes(v4.octets()) as usize,
            std::net::IpAddr::V6(v6) => {
                let o = v6.octets();
                u32::from_ne_bytes([o[12], o[13], o[14], o[15]]) as usize
            }
        };
        let idx = (port ^ ip_hash) % self.shards.len();
        self.shards[idx].clone()
    }

    /// Returns the primary shard (shard 0) for lifecycle checks and API stability.
    #[inline]
    pub fn primary_shard(&self) -> Arc<Mutex<Http3Engine>> {
        self.shards[0].clone()
    }
}

/// Global registry holding long-lived HTTP/3 QUIC sharded engines indexed by listener identifier.
static H3_ENGINES: OnceLock<RwLock<FxHashMap<String, H3EngineShards>>> = OnceLock::new();

fn engines_table() -> &'static RwLock<FxHashMap<String, H3EngineShards>> {
    H3_ENGINES.get_or_init(|| RwLock::new(FxHashMap::default()))
}

/// Initializes or updates the HTTP/3 QUIC engine for the given listener.
///
/// If an engine already exists for this listener, its active connection state is PRESERVED,
/// ensuring zero-downtime and zero connection drop during configuration reloads!
pub fn init_h3_engine(listener_id: &str, tls_server: &TlsServerEngine) -> Result<(), EdgeError> {
    let mut quic_cfg = tls_server.build_quic_config().map_err(EdgeError::Tls)?;
    let h3_config = velda_http3::Http3Config::auto();
    quic_cfg.transport = Arc::new(h3_config.build_transport_config());

    let table = engines_table();
    let mut write = table.write().unwrap();
    if !write.contains_key(listener_id) {
        let pool = H3EngineShards::new(Arc::new(quic_cfg), h3_config);
        write.insert(listener_id.to_string(), pool);
    }
    Ok(())
}

/// Checks whether an HTTP/3 engine is initialized for the given listener.
pub fn has_h3_engine(listener_id: &str) -> bool {
    let table = engines_table();
    table
        .read()
        .map(|l| l.contains_key(listener_id))
        .unwrap_or(false)
}

/// Clears all active HTTP/3 engines (useful for test isolation).
pub fn clear_h3_engines() {
    let table = engines_table();
    if let Ok(mut lock) = table.write() {
        lock.clear();
    }
}

/// Retrieves an existing engine or lazily initializes one from the active runtime snapshot.
pub fn get_or_init_h3_engine(
    listener_id: &str,
    runtime: &SharedRuntime,
) -> Option<Arc<Mutex<Http3Engine>>> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool.primary_shard());
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_h3_engine(listener_id, tls_server);

    let table = engines_table();
    table
        .read()
        .ok()?
        .get(listener_id)
        .map(|p| p.primary_shard())
}

/// Retrieves the specific sharded engine for a given remote peer to eliminate lock contention.
pub fn get_or_init_h3_engine_for_peer(
    listener_id: &str,
    peer: SocketAddr,
    runtime: &SharedRuntime,
) -> Option<Arc<Mutex<Http3Engine>>> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool.get_shard_for_peer(peer));
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_h3_engine(listener_id, tls_server);

    let table = engines_table();
    table
        .read()
        .ok()?
        .get(listener_id)
        .map(|p| p.get_shard_for_peer(peer))
}

/// Dispatches incoming UDP L7 handoff to the persistent HTTP/3 state machine.
///
/// Ingests the packet, drives QUIC handshake/flow control, transmits outgoing datagrams,
/// and passes decoded requests through `Http3Router`.
pub async fn handle_http3_handoff(
    datagram: Datagram,
    socket: Arc<UdpSocket>,
    context: IngressContext,
    _config: velda_http3::Http3Config,
    runtime: &SharedRuntime,
) {
    let listener_id = context.listener_id.clone();
    let peer = datagram.peer();

    let Some(engine_lock) = get_or_init_h3_engine_for_peer(&listener_id, peer, runtime) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %context.peer,
            "Received UDP L7 handoff, but no HTTP/3 engine is compiled"
        );
        return;
    };

    let now = std::time::Instant::now();
    let (outgoing, requests) = {
        let mut engine = engine_lock.lock().await;
        engine.handle_datagram(
            now,
            datagram.peer(),
            Some(datagram.local_addr().ip()),
            datagram.data(),
        )
    };

    // 1. Send all outgoing handshake / ACK datagrams immediately outside the engine lock
    for pkt in outgoing {
        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
    }

    // 2. Process complete L7 requests concurrently without blocking other datagrams
    for req_event in requests {
        let socket = socket.clone();
        let engine_lock = engine_lock.clone();
        let context = context.clone();
        let runtime = runtime.clone();

        tokio::spawn(async move {
            tracing::debug!(
                method = %req_event.request.method,
                path = %req_event.request.path(),
                peer = %context.peer,
                "Decoded HTTP/3 request from UDP"
            );

            let response = process_http3_request(&req_event.request, &context, &runtime).await;
            let resp_now = std::time::Instant::now();
            let resp_pkts = {
                let mut engine = engine_lock.lock().await;
                engine.send_response(resp_now, req_event.handle, req_event.stream_id, &response)
            };

            if let Ok(pkts) = resp_pkts {
                for pkt in pkts {
                    let _ = socket.send_to(&pkt.payload, pkt.peer).await;
                }
            }
        });
    }
}

/// Dispatches incoming UDP L7 handoff for gRPC over QUIC to the persistent state machine.
pub async fn handle_grpc_udp_handoff(
    datagram: Datagram,
    socket: Arc<UdpSocket>,
    context: IngressContext,
    config: velda_grpc::GrpcConfig,
    runtime: &SharedRuntime,
) {
    let listener_id = context.listener_id.clone();
    let peer = datagram.peer();

    let Some(engine_lock) = get_or_init_h3_engine_for_peer(&listener_id, peer, runtime) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %context.peer,
            "Received UDP L7 handoff, but no gRPC over QUIC engine is compiled"
        );
        return;
    };

    let now = std::time::Instant::now();
    let (outgoing, requests) = {
        let mut engine = engine_lock.lock().await;
        engine.handle_datagram(
            now,
            datagram.peer(),
            Some(datagram.local_addr().ip()),
            datagram.data(),
        )
    };

    // 1. Send all outgoing handshake / ACK datagrams immediately outside the engine lock
    for pkt in outgoing {
        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
    }

    // 2. Process complete gRPC requests concurrently without blocking other datagrams
    for req_event in requests {
        let socket = socket.clone();
        let engine_lock = engine_lock.clone();
        let context = context.clone();
        let runtime = runtime.clone();

        tokio::spawn(async move {
            tracing::debug!(
                method = %req_event.request.method,
                path = %req_event.request.path(),
                peer = %context.peer,
                "Decoded gRPC request from UDP"
            );

            let response =
                super::grpc::process_grpc_request(&req_event.request, &context, &config, &runtime)
                    .await;

            let resp_now = std::time::Instant::now();
            let resp_pkts = {
                let mut engine = engine_lock.lock().await;
                engine.send_response(resp_now, req_event.handle, req_event.stream_id, &response)
            };

            if let Ok(pkts) = resp_pkts {
                for pkt in pkts {
                    let _ = socket.send_to(&pkt.payload, pkt.peer).await;
                }
            }
        });
    }
}

/// Dispatches an HTTP/3 request through `Http3Router` and forwards to upstream backend.
pub async fn process_http3_request(
    req: &L7Request,
    context: &IngressContext,
    runtime: &SharedRuntime,
) -> L7Response {
    let host = req
        .host()
        .and_then(|h| h.to_str().ok())
        .or_else(|| req.uri.host());

    let mut http_req = Http3RouteRequest::new(req.path());
    if let Some(h) = host {
        http_req = http_req.with_host(h);
    }
    http_req = http_req.with_method(req.method.as_str());

    let rt = runtime.load();
    let Some(route) = rt.router.route_http3(&context.listener_id, &http_req) else {
        tracing::debug!(
            listener = %context.listener_id,
            path = %req.path(),
            host = ?host,
            method = %req.method,
            "No HTTP/3 route matched"
        );
        return L7Response::from_bytes(
            StatusCode::NOT_FOUND,
            b"404 Not Found: no matching route\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    let Some(upstream) = rt.upstreams.http3.get(&route.upstream_name) else {
        tracing::error!(
            listener = %context.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No HTTP/3 upstream configured"
        );
        return L7Response::from_bytes(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable: upstream not configured\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    let server_name = upstream.resolve_sni(req, host);
    let h3_config = velda_http3::Http3Config::auto();

    // HTTP/3 QUIC Multiplexing Invariant (RFC 9114):
    // The Edge pipeline hands off the request directly to the upstream.
    // The upstream manages single-round Load Balancing selection, persistent multiplexed QUIC
    // client reuse across concurrent requests, and candidate failover without HOL blocking.
    match upstream
        .dispatch_request(req.clone(), &server_name, &h3_config)
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                "HTTP/3 upstream dispatch request failed"
            );
            L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )
        }
    }
}
