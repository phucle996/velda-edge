//! Layer 7 gRPC over UDP: packet-driven state machine, datagram handoff, and RPC execution.
//!
//! Powered by `velda-grpc`. Operates strictly for listeners explicitly configured
//! with `transport = "udp"` and `protocol = "grpc"`. Receives QUIC datagrams, decodes
//! RPC requests, and forwards to upstream backend.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::GrpcConfig;
use velda_grpc::GrpcStatus;
use velda_router::GrpcRouteRequest;
use velda_transport::{Datagram, UdpSocket};

use crate::pipeline::http3::get_or_init_h3_engine_for_peer;
use crate::runtime::SharedRuntime;

/// Dispatches incoming UDP L7 handoff for gRPC over UDP to the persistent state machine.
pub async fn handle_grpc_udp(
    datagram: Datagram,
    socket: Arc<UdpSocket>,
    listener_id: Arc<str>,
    config: GrpcConfig,
    runtime: &SharedRuntime,
) {
    let peer = datagram.peer();

    let Some(engine_lock) = get_or_init_h3_engine_for_peer(&listener_id, peer, runtime) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "Received UDP L7 handoff, but no gRPC UDP engine is compiled"
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

            let response = process_grpc_udp_request(&req_event.request, &lid, &cfg, &runtime).await;

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

    let Some(upstream) = rt.upstreams.grpc.get(&route.upstream_name) else {
        return GrpcStatus::Unavailable.to_l7_response(Some("upstream not configured"));
    };

    match upstream.dispatch_unary(req, config).await {
        Ok(resp) => resp,
        Err(e) => GrpcStatus::Unavailable.to_l7_response(Some(&format!("upstream error: {e}"))),
    }
}
