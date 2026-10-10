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
use velda_http3::pipe::{
    Http3PipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_router::Http3RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::{Datagram, UdpSocket};

use crate::error::EdgeError;
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

/// Dispatches incoming UDP datagrams to the persistent HTTP/3 state machine.
///
/// Ingests the packet, drives QUIC handshake/flow control, transmits outgoing datagrams,
/// and passes decoded requests through `Http3Router`.
pub async fn handle_http3_udp(
    listener_id: Arc<str>,
    socket: Arc<UdpSocket>,
    datagram: Datagram,
    _config: velda_http3::Http3Config,
    runtime: &SharedRuntime,
) {
    let peer = datagram.peer();
    let local_addr = datagram.local_addr();

    let Some(engine_lock) = get_or_init_h3_engine_for_peer(&listener_id, peer, runtime) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "Received UDP datagram, but no HTTP/3 engine is compiled"
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
        let lid = listener_id.clone();
        let runtime = runtime.clone();

        tokio::spawn(async move {
            tracing::debug!(
                method = %req_event.request.method,
                path = %req_event.request.path(),
                peer = %peer,
                early_data = req_event.is_early_data,
                "Decoded HTTP/3 request from UDP"
            );

            // Fast-Fail: 0-RTT Replay Attack Defense (RFC 8470)
            // Non-idempotent methods (POST, PUT, DELETE, PATCH) MUST NOT be processed in 0-RTT early data.
            let is_mutation = matches!(
                req_event.request.method,
                http::Method::POST | http::Method::PUT | http::Method::DELETE | http::Method::PATCH
            );
            if req_event.is_early_data && is_mutation {
                tracing::warn!(
                    listener = %lid,
                    method = %req_event.request.method,
                    path = %req_event.request.path(),
                    "Rejecting non-idempotent HTTP/3 request received in 0-RTT early data (RFC 8470)"
                );
                let too_early = L7Response::from_bytes(
                    StatusCode::TOO_EARLY,
                    b"425 Too Early: non-idempotent request rejected in 0-RTT early data\n"
                        .to_vec(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                let resp_now = std::time::Instant::now();
                let resp_pkts = {
                    let mut engine = engine_lock.lock().await;
                    engine.send_response(
                        resp_now,
                        req_event.handle,
                        req_event.stream_id,
                        &too_early,
                    )
                };
                if let Ok(pkts) = resp_pkts {
                    for pkt in pkts {
                        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
                    }
                }
                return;
            }

            let mut req = req_event.request;
            if let Err(e) = velda_http3::server::path::normalize_path(&mut req.uri) {
                tracing::warn!(
                    listener = %lid,
                    error = %e,
                    path = %req.path(),
                    "Rejecting HTTP/3 request with unsafe URI path"
                );
                let bad_req = L7Response::from_bytes(
                    StatusCode::BAD_REQUEST,
                    b"400 Bad Request: unsafe or invalid URI path\n".to_vec(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                let resp_now = std::time::Instant::now();
                let resp_pkts = {
                    let mut engine = engine_lock.lock().await;
                    engine.send_response(resp_now, req_event.handle, req_event.stream_id, &bad_req)
                };
                if let Ok(pkts) = resp_pkts {
                    for pkt in pkts {
                        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
                    }
                }
                return;
            }

            velda_http3::server::header::enrich_headers(
                &mut req.headers,
                &req.uri,
                peer,
                local_addr,
            );

            let response = process_http3_request(&req, &lid, &runtime).await;
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
    listener_id: &str,
    runtime: &SharedRuntime,
) -> L7Response {
    let host = velda_http3::server::header::extract_host(&req.headers, &req.uri);
    let mut http_req = Http3RouteRequest::new(req.path());
    if let Some(h) = host {
        http_req = http_req.with_host(h);
    }
    http_req = http_req.with_method(req.method.as_str());

    let rt = runtime.load();
    let Some(route) = rt.router.route_http3(listener_id, &http_req) else {
        tracing::debug!(
            listener = %listener_id,
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

    let Some(upstream) = rt
        .upstreams
        .http
        .get(&route.upstream_name)
        .and_then(|u| u.as_http3())
    else {
        tracing::error!(
            listener = %listener_id,
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

    // Resource Saturation Circuit Breaker (Overload Protection)
    let overload_lvl = rt.overload.level();
    if overload_lvl.is_shedding() {
        tracing::warn!(
            route = %route.id,
            upstream = %route.upstream_name,
            overload = ?overload_lvl,
            "Shedding HTTP/3 request due to memory saturation"
        );
        return L7Response::from_bytes(
            StatusCode::TOO_MANY_REQUESTS,
            b"429 Too Many Requests: edge under memory pressure, please retry later\n".to_vec(),
        )
        .with_header(http::header::RETRY_AFTER, HeaderValue::from_static("1"))
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    }

    let h3_config = velda_http3::Http3Config::auto();
    let strategy = upstream.strategy;

    let client = match upstream.acquire(&h3_config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                "Failed to acquire upstream HTTP/3 connection"
            );
            return L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
        }
    };

    let mut pipe_res = match strategy {
        Http3PipeStrategy::Buffered => pipe_buffered(&client, req, &h3_config).await,
        Http3PipeStrategy::ServerStream => pipe_server_stream(&client, req, &h3_config).await,
        Http3PipeStrategy::ClientStream => pipe_client_stream(&client, req, &h3_config).await,
        Http3PipeStrategy::Duplex => pipe_duplex(&client, req, &h3_config).await,
    };

    // Self-Healing Retry (RFC 9114):
    // If upstream QUIC connection was closed or rejected before processing,
    // and downstream payload is in memory, acquire a fresh HTTP/3 client and retry once.
    if let Err(ref e) = pipe_res
        && e.is_connection_closed()
        && matches!(
            strategy,
            Http3PipeStrategy::Buffered | Http3PipeStrategy::ServerStream
        )
    {
        tracing::debug!(
            upstream = %route.upstream_name,
            error = %e,
            "HTTP/3 connection closed or rejected; self-healing with fresh connection"
        );
        if let Ok(fresh_client) = upstream.acquire_fresh(&h3_config).await {
            pipe_res = match strategy {
                Http3PipeStrategy::Buffered => pipe_buffered(&fresh_client, req, &h3_config).await,
                Http3PipeStrategy::ServerStream => {
                    pipe_server_stream(&fresh_client, req, &h3_config).await
                }
                _ => unreachable!(),
            };
        }
    }

    match pipe_res {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                "HTTP/3 upstream request failed"
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
        // Client attempts to spoof their IP, host, and SSL status
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let peer: SocketAddr = "192.0.2.30:60000".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        let mut req = L7Request::new(
            http::Method::GET,
            "https://quic.example.com/test".parse().unwrap(),
            http::Version::HTTP_3,
            headers,
            velda_core::Body::Empty,
        );

        velda_http3::server::header::enrich_headers(&mut req.headers, &req.uri, peer, local);
        let headers = req.headers;

        // Client spoofed values MUST be completely replaced with authoritative values
        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.30");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.30");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(headers.get("x-forwarded-host").unwrap(), "quic.example.com");
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.30;proto=https;by=10.0.0.1;host=\"quic.example.com\""
        );
        // RFC 9114 hop-by-hop headers MUST be stripped
        assert!(headers.get("connection").is_none());
        assert!(headers.get("keep-alive").is_none());
    }

    #[test]
    fn test_connection_closed_detection() {
        assert!(velda_http3::Http3Error::ConnectionClosed.is_connection_closed());
        assert!(velda_http3::Http3Error::H3("H3_REQUEST_REJECTED".into()).is_connection_closed());
        assert!(
            velda_http3::Http3Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_connection_closed()
        );

        // Payload size errors are not retryable
        assert!(!velda_http3::Http3Error::PayloadTooLarge(500).is_connection_closed());
    }
}
