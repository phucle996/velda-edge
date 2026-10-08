//! Layer 7 gRPC over UDP: packet-driven state machine, datagram handoff, and RPC execution.
//!
//! Powered entirely by `velda-grpc::udp`. Operates strictly for listeners explicitly configured
//! with `transport = "udp"` and `protocol = "grpc"`. Receives QUIC datagrams, decodes
//! RPC requests, and forwards to upstream backend with zero HTTP borrowing.

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};

use rustc_hash::FxHashMap;
use tokio::sync::Mutex;
use velda_core::{L7Request, L7Response};
use velda_grpc::GrpcConfig;
use velda_grpc::GrpcStatus;
use velda_grpc::udp::pipe::{
    GrpcUdpPipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_grpc::udp::quinn_proto;
use velda_grpc::udp::server::{GrpcUdpEngine, GrpcUdpServerStream};
use velda_router::GrpcRouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::{Datagram, UdpSocket};

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// Sharded container of gRPC UDP engines partitioned by remote peer address to eliminate
/// lock contention on multi-core hardware topologies.
#[derive(Clone)]
pub struct GrpcUdpEngineShards {
    shards: Arc<Vec<Arc<Mutex<GrpcUdpEngine>>>>,
}

impl GrpcUdpEngineShards {
    /// Creates a sharded pool scaled dynamically to available hardware parallelism.
    pub fn new(quic_cfg: Arc<quinn_proto::ServerConfig>, grpc_config: GrpcConfig) -> Self {
        let count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(2, 64)
            .next_power_of_two();

        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            let engine = GrpcUdpEngine::new(quic_cfg.clone(), grpc_config);
            shards.push(Arc::new(Mutex::new(engine)));
        }
        Self {
            shards: Arc::new(shards),
        }
    }

    /// Selects an engine shard deterministically by hashing remote peer IP and port.
    #[inline]
    pub fn get_shard_for_peer(&self, peer: SocketAddr) -> Arc<Mutex<GrpcUdpEngine>> {
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

    /// Returns the primary shard (shard 0) for lifecycle checks.
    #[inline]
    pub fn primary_shard(&self) -> Arc<Mutex<GrpcUdpEngine>> {
        self.shards[0].clone()
    }
}

/// Global registry holding long-lived gRPC UDP sharded engines indexed by listener identifier.
static GRPC_UDP_ENGINES: OnceLock<RwLock<FxHashMap<String, GrpcUdpEngineShards>>> = OnceLock::new();

fn engines_table() -> &'static RwLock<FxHashMap<String, GrpcUdpEngineShards>> {
    GRPC_UDP_ENGINES.get_or_init(|| RwLock::new(FxHashMap::default()))
}

/// Initializes or updates the gRPC UDP QUIC engine for the given listener.
pub fn init_grpc_udp_engine(
    listener_id: &str,
    tls_server: &TlsServerEngine,
) -> Result<(), EdgeError> {
    let quic_cfg = tls_server.build_quic_config().map_err(EdgeError::Tls)?;
    let grpc_config = GrpcConfig::auto();

    let table = engines_table();
    let mut write = table.write().unwrap();
    if !write.contains_key(listener_id) {
        let pool = GrpcUdpEngineShards::new(Arc::new(quic_cfg), grpc_config);
        write.insert(listener_id.to_string(), pool);
    }
    Ok(())
}

/// Checks whether a gRPC UDP engine is initialized for the given listener.
pub fn has_grpc_udp_engine(listener_id: &str) -> bool {
    let table = engines_table();
    table
        .read()
        .map(|l| l.contains_key(listener_id))
        .unwrap_or(false)
}

/// Clears all active gRPC UDP engines.
pub fn clear_grpc_udp_engines() {
    let table = engines_table();
    if let Ok(mut lock) = table.write() {
        lock.clear();
    }
}

/// Retrieves the specific sharded engine for a given remote peer.
pub fn get_or_init_grpc_udp_engine_for_peer(
    listener_id: &str,
    peer: SocketAddr,
    runtime: &SharedRuntime,
) -> Option<Arc<Mutex<GrpcUdpEngine>>> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool.get_shard_for_peer(peer));
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_grpc_udp_engine(listener_id, tls_server);

    let table = engines_table();
    table
        .read()
        .ok()?
        .get(listener_id)
        .map(|p| p.get_shard_for_peer(peer))
}

/// Dispatches incoming UDP datagrams for gRPC over UDP to the persistent state machine.
pub async fn handle_grpc_udp(
    listener_id: Arc<str>,
    socket: Arc<UdpSocket>,
    datagram: Datagram,
    config: GrpcConfig,
    runtime: &SharedRuntime,
) {
    let peer = datagram.peer();
    let local_addr = datagram.local_addr();

    let Some(engine_lock) = get_or_init_grpc_udp_engine_for_peer(&listener_id, peer, runtime)
    else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "Received UDP datagram, but no gRPC UDP engine is compiled"
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
        let lid = listener_id.clone();
        let runtime = runtime.clone();
        let cfg = config;

        tokio::spawn(async move {
            tracing::debug!(
                method = %req_event.request.method,
                path = %req_event.request.path(),
                peer = %peer,
                "Decoded gRPC request from UDP"
            );

            let mut stream = GrpcUdpServerStream::new(
                req_event.handle,
                req_event.stream_id,
                req_event.request,
                peer,
            );
            stream.enrich_forwarded_headers(local_addr);

            let response = process_grpc_udp_request(&stream.request, &lid, &cfg, &runtime).await;

            let resp_now = std::time::Instant::now();
            let resp_pkts = {
                let mut engine = engine_lock.lock().await;
                engine.send_response(resp_now, stream.handle, stream.stream_id, &response)
            };

            if let Ok(pkts) = resp_pkts {
                for pkt in pkts {
                    let _ = socket.send_to(&pkt.payload, pkt.peer).await;
                }
            }
        });
    }
}

/// Dispatches a single unary gRPC request through `GrpcRouter` to the upstream backend.
/// Used for unary RPC execution over UDP when the listener explicitly declares transport = "udp" and protocol = "grpc".
pub async fn process_grpc_udp_request(
    req: &L7Request,
    listener_id: &str,
    config: &GrpcConfig,
    runtime: &SharedRuntime,
) -> L7Response {
    let authority = req
        .headers
        .get(":authority")
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.host().and_then(|h| h.to_str().ok()))
        .or_else(|| req.uri.authority().map(|a| a.as_str()));

    let Some(grpc_req) = GrpcRouteRequest::from_path(req.path(), authority) else {
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for empty path"));
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_grpc(listener_id, &grpc_req) else {
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for service"));
    };

    let Some(upstream) = rt
        .upstreams
        .grpc
        .get(&route.upstream_name)
        .and_then(|u| u.as_udp())
    else {
        return GrpcStatus::Unavailable.to_l7_response(Some("upstream not configured"));
    };

    let strategy = upstream.strategy;
    let client = match upstream.acquire(config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                "Failed to acquire upstream gRPC over UDP client"
            );
            return GrpcStatus::Unavailable.to_l7_response(Some(&format!("upstream error: {e}")));
        }
    };

    let mut pipe_res = match strategy {
        GrpcUdpPipeStrategy::Buffered => pipe_buffered(&client, req, config).await,
        GrpcUdpPipeStrategy::ServerStream => pipe_server_stream(&client, req, config).await,
        GrpcUdpPipeStrategy::ClientStream => pipe_client_stream(&client, req, config).await,
        GrpcUdpPipeStrategy::Duplex => pipe_duplex(&client, req, config).await,
    };

    // Self-Healing Retry:
    // If upstream QUIC connection dropped or reset before processing,
    // and downstream payload is in memory, acquire a fresh client and retry once.
    if let Err(ref e) = pipe_res
        && e.is_stale_or_refused()
        && matches!(
            strategy,
            GrpcUdpPipeStrategy::Buffered | GrpcUdpPipeStrategy::ServerStream
        )
    {
        tracing::debug!(
            upstream = %route.upstream_name,
            error = %e,
            "gRPC over UDP connection dropped; self-healing with fresh connection"
        );
        if let Ok(fresh_client) = upstream.acquire_fresh(config).await {
            pipe_res = match strategy {
                GrpcUdpPipeStrategy::Buffered => pipe_buffered(&fresh_client, req, config).await,
                GrpcUdpPipeStrategy::ServerStream => {
                    pipe_server_stream(&fresh_client, req, config).await
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
                "gRPC over UDP stream pipe terminated with error"
            );
            GrpcStatus::Unavailable.to_l7_response(Some(&format!("upstream error: {e}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use velda_grpc::udp::server::enrich_headers;

    #[test]
    fn test_enrich_forwarded_headers() {
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

        let peer: SocketAddr = "192.0.2.50:50052".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        enrich_headers(&mut headers, peer, local, Some("grpc-udp.example.com"));

        // Client spoofed values MUST be completely replaced with authoritative values
        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.50");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.50");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(
            headers.get("x-forwarded-host").unwrap(),
            "grpc-udp.example.com"
        );
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.50;proto=https;by=10.0.0.1;host=\"grpc-udp.example.com\""
        );
        // Untrusted extra x-forwarded-* headers MUST be stripped
        assert!(headers.get("x-forwarded-ssl").is_none());
        // RFC 9113 hop-by-hop headers MUST be stripped
        assert!(headers.get("connection").is_none());
        assert!(headers.get("keep-alive").is_none());
    }
}
