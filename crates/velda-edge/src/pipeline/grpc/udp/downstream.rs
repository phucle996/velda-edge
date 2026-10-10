//! Layer 7 gRPC over UDP downstream datagram dispatch and request handling.
//!
//! Powered entirely by `velda-grpc::udp`. Operates strictly for listeners explicitly configured
//! with `transport = "udp"` and `protocol = "grpc"`. Receives QUIC datagrams, decodes
//! RPC requests, and forwards to upstream backends with zero HTTP borrowing.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::tcp::pipe::GrpcPipeStrategy;
use velda_grpc::udp::pipe::GrpcUdpPipeStrategy;
use velda_grpc::udp::server::GrpcUdpServerStream;
use velda_grpc::{GrpcConfig, GrpcStatus};
use velda_router::GrpcRouteRequest;
use velda_transport::{Datagram, UdpSocket};

use super::engine::get_or_init_grpc_udp_engine_for_peer;
use super::pipe;
use crate::runtime::SharedRuntime;
use crate::upstream::GrpcUpstream;

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
/// Supports both native gRPC over UDP and cross-transport bridge to gRPC over TCP.
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

    let Some(upstream_target) = rt.upstreams.grpc.get(&route.upstream_name) else {
        return GrpcStatus::Unavailable.to_l7_response(Some("upstream not configured"));
    };

    // Resource Saturation Circuit Breaker (Overload Protection)
    let overload_lvl = rt.overload.level();
    if overload_lvl.is_shedding() {
        tracing::warn!(
            route = %route.id,
            upstream = %route.upstream_name,
            overload = ?overload_lvl,
            "Shedding gRPC UDP request due to memory saturation"
        );
        return GrpcStatus::ResourceExhausted.to_l7_response(Some("resource exhausted"));
    }

    let pipe_res = match upstream_target.as_ref() {
        GrpcUpstream::Udp(upstream) => match upstream.strategy {
            GrpcUdpPipeStrategy::Buffered => {
                pipe::buffered_udp_udp::serve(req, upstream, config).await
            }
            GrpcUdpPipeStrategy::ServerStream => {
                pipe::server_stream_udp_udp::serve(req, upstream, config).await
            }
            GrpcUdpPipeStrategy::ClientStream => {
                pipe::client_stream_udp_udp::serve(req, upstream, config).await
            }
            GrpcUdpPipeStrategy::Duplex => pipe::duplex_udp_udp::serve(req, upstream, config).await,
        },
        GrpcUpstream::Tcp(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                pipe::buffered_udp_tcp::serve(req, upstream, config).await
            }
            GrpcPipeStrategy::ServerStream => {
                pipe::server_stream_udp_tcp::serve(req, upstream, config).await
            }
            GrpcPipeStrategy::ClientStream => {
                pipe::client_stream_udp_tcp::serve(req, upstream, config).await
            }
            GrpcPipeStrategy::Duplex => pipe::duplex_udp_tcp::serve(req, upstream, config).await,
        },
    };

    match pipe_res {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                "gRPC over UDP stream pipe terminated with error"
            );
            GrpcStatus::Unavailable.to_l7_response(Some("upstream unavailable"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

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
        // RFC 9113 hop-by-hop headers MUST be stripped
        assert!(headers.get("connection").is_none());
        assert!(headers.get("keep-alive").is_none());
    }
}
