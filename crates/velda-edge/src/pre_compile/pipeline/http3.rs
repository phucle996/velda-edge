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

/// Enriches HTTP/3 request headers with RFC 7239 and standard proxy forwarding metadata.
///
/// Anti-Spoofing Invariant:
/// Untrusted downstream clients must NEVER be permitted to spoof client IP or proxy forwarding metadata.
/// Any client-supplied `X-Forwarded-*`, `X-Real-IP`, or RFC 7239 `Forwarded` headers are stripped in-place
/// and replaced strictly with authoritative edge connection metadata (`peer.ip()`, `local_addr.port()`,
/// protocol scheme, and verified host).
/// Connection-specific RFC 9114 hop-by-hop headers are also stripped in-place.
fn enrich_http3_forwarded_headers(
    headers: &mut http::HeaderMap,
    peer: SocketAddr,
    local_addr: SocketAddr,
    host: Option<&str>,
) {
    use http::header::{HeaderName, HeaderValue};

    // 1. Strip all client-supplied untrusted forwarding headers
    if headers.keys().any(|k| {
        let s = k.as_str();
        s.starts_with("x-forwarded-")
            || s.eq_ignore_ascii_case("x-real-ip")
            || s.eq_ignore_ascii_case("forwarded")
    }) {
        let to_remove: Vec<HeaderName> = headers
            .keys()
            .filter(|k| {
                let s = k.as_str();
                s.starts_with("x-forwarded-")
                    || s.eq_ignore_ascii_case("x-real-ip")
                    || s.eq_ignore_ascii_case("forwarded")
            })
            .cloned()
            .collect();
        for name in to_remove {
            headers.remove(&name);
        }
    }

    // 2. Strip RFC 9114 connection-specific hop-by-hop headers
    static H3_HOP_BY_HOP_NAMES: [HeaderName; 5] = [
        http::header::CONNECTION,
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-connection"),
        http::header::TRANSFER_ENCODING,
        http::header::UPGRADE,
    ];
    for name in &H3_HOP_BY_HOP_NAMES {
        headers.remove(name);
    }
    if let Some(te_val) = headers.get(http::header::TE) {
        let is_trailers = te_val
            .to_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case("trailers"));
        if !is_trailers {
            headers.remove(http::header::TE);
        }
    }

    let client_ip = peer.ip();
    let proto = "https"; // HTTP/3 QUIC is always TLS 1.3 encrypted

    let mut ip_buf = [0u8; 64];
    let ip_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut ip_buf[..]);
        let _ = write!(cursor, "{}", client_ip);
        cursor.position() as usize
    };
    let client_ip_bytes = &ip_buf[..ip_len];

    // 3. Authoritative X-Forwarded-For
    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(HeaderName::from_static("x-forwarded-for"), val);
    }

    // 4. Authoritative X-Real-IP
    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(HeaderName::from_static("x-real-ip"), val);
    }

    // 5. Authoritative X-Forwarded-Proto
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );

    // 6. Authoritative X-Forwarded-Port
    let mut port_buf = [0u8; 8];
    let port_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut port_buf[..]);
        let _ = write!(cursor, "{}", local_addr.port());
        cursor.position() as usize
    };
    if let Ok(val) = HeaderValue::from_bytes(&port_buf[..port_len]) {
        headers.insert(HeaderName::from_static("x-forwarded-port"), val);
    }

    // 7. Authoritative X-Forwarded-Host
    if let Some(h) = host
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(HeaderName::from_static("x-forwarded-host"), val);
    }

    // 8. Authoritative Standard: RFC 7239 (Official IETF "Forwarded" HTTP Extension)
    let mut fwd_buf = [0u8; 256];
    let fwd_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut fwd_buf[..]);
        match client_ip {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "for={v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "for=\"[{v6}]\"");
            }
        }
        let _ = write!(cursor, ";proto={proto};by=");
        match local_addr.ip() {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "{v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "\"[{v6}]\"");
            }
        }
        if let Some(h) = host {
            let _ = write!(cursor, ";host=\"{h}\"");
        }
        cursor.position() as usize
    };
    let fwd_bytes = &fwd_buf[..fwd_len];
    if let Ok(val) = HeaderValue::from_bytes(fwd_bytes) {
        headers.insert(HeaderName::from_static("forwarded"), val);
    }
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
                "Decoded HTTP/3 request from UDP"
            );

            let mut req = req_event.request;
            let host_hdr = req.headers.get(http::header::HOST).cloned();
            let host_str = host_hdr
                .as_ref()
                .and_then(|h| h.to_str().ok())
                .or_else(|| req.uri.authority().map(|a| a.as_str()))
                .or_else(|| req.uri.host());
            enrich_http3_forwarded_headers(&mut req.headers, peer, local_addr, host_str);

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

    let Some(upstream) = rt.upstreams.http3.get(&route.upstream_name) else {
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
    fn test_enrich_http3_forwarded_headers_anti_spoofing() {
        let mut headers = HeaderMap::new();
        // Client attempts to spoof their IP, host, and SSL status
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        headers.insert("x-forwarded-ssl", "off".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let peer: SocketAddr = "192.0.2.30:60000".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        enrich_http3_forwarded_headers(&mut headers, peer, local, Some("quic.example.com"));

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
        // Untrusted extra x-forwarded-* headers MUST be stripped
        assert!(headers.get("x-forwarded-ssl").is_none());
        // RFC 9114 hop-by-hop headers MUST be stripped
        assert!(headers.get("connection").is_none());
        assert!(headers.get("keep-alive").is_none());
    }

    #[test]
    fn test_http3_connection_closed_detection() {
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
