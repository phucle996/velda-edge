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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::sync::Mutex;
use velda_composer::{ComposedDatagram, ComposerContext};
use velda_core::{L7Request, L7Response};
use velda_http3::{Http3Engine, Http3UpstreamConnector};
use velda_router::Http3RouteRequest;
use velda_tls::TlsServerEngine;

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// Global registry holding long-lived HTTP/3 QUIC engines indexed by listener identifier.
static H3_ENGINES: OnceLock<RwLock<HashMap<String, Arc<Mutex<Http3Engine>>>>> = OnceLock::new();

fn engines_table() -> &'static RwLock<HashMap<String, Arc<Mutex<Http3Engine>>>> {
    H3_ENGINES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Initializes or updates the HTTP/3 QUIC engine for the given listener.
///
/// If an engine already exists for this listener, its active connection state is PRESERVED,
/// ensuring zero-downtime and zero connection drop during configuration reloads!
pub fn init_h3_engine(listener_id: &str, tls_server: &TlsServerEngine) -> Result<(), EdgeError> {
    let quic_cfg = tls_server.build_quic_config().map_err(EdgeError::Tls)?;
    let table = engines_table();
    let mut write = table.write().unwrap();
    if !write.contains_key(listener_id) {
        let engine = Http3Engine::new(Arc::new(quic_cfg));
        write.insert(listener_id.to_string(), Arc::new(Mutex::new(engine)));
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
    if let Some(engine) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(engine);
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_h3_engine(listener_id, tls_server);

    let table = engines_table();
    table.read().ok()?.get(listener_id).cloned()
}

/// Dispatches incoming UDP L7 handoff to the persistent HTTP/3 state machine.
///
/// Ingests the packet, drives QUIC handshake/flow control, transmits outgoing datagrams,
/// and passes decoded requests through `Http3Router`.
pub async fn handle_http3_handoff(composed: ComposedDatagram, runtime: &SharedRuntime) {
    let context = composed.context().clone();
    let listener_id = context.listener_id.clone();
    let (datagram, socket) = composed.into_parts();

    let Some(engine_lock) = get_or_init_h3_engine(&listener_id, runtime) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %context.peer,
            "Received UDP L7 handoff, but no HTTP/3 engine is compiled"
        );
        return;
    };

    let now = std::time::Instant::now();
    let mut engine = engine_lock.lock().await;
    let (outgoing, requests) = engine.handle_datagram(
        now,
        datagram.peer(),
        Some(datagram.local_addr().ip()),
        datagram.data(),
    );

    // 1. Send all outgoing handshake / ACK datagrams
    for pkt in outgoing {
        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
    }

    // 2. Process complete L7 requests through the router
    for req_event in requests {
        tracing::debug!(
            method = %req_event.request.method,
            path = %req_event.request.path(),
            peer = %datagram.peer(),
            "Decoded HTTP/3 request from UDP"
        );

        let response = process_http3_request(&req_event.request, &context, runtime).await;

        if let Ok(resp_pkts) =
            engine.send_response(now, req_event.handle, req_event.stream_id, &response)
        {
            for pkt in resp_pkts {
                let _ = socket.send_to(&pkt.payload, pkt.peer).await;
            }
        }
    }
}

/// Dispatches incoming UDP L7 handoff for gRPC over QUIC to the persistent state machine.
pub async fn handle_grpc_udp_handoff(composed: ComposedDatagram, runtime: &SharedRuntime) {
    let context = composed.context().clone();
    let listener_id = context.listener_id.clone();
    let (datagram, socket) = composed.into_parts();

    let Some(engine_lock) = get_or_init_h3_engine(&listener_id, runtime) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %context.peer,
            "Received UDP L7 handoff, but no gRPC over QUIC engine is compiled"
        );
        return;
    };

    let now = std::time::Instant::now();
    let mut engine = engine_lock.lock().await;
    let (outgoing, requests) = engine.handle_datagram(
        now,
        datagram.peer(),
        Some(datagram.local_addr().ip()),
        datagram.data(),
    );

    for pkt in outgoing {
        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
    }

    for req_event in requests {
        tracing::debug!(
            method = %req_event.request.method,
            path = %req_event.request.path(),
            peer = %datagram.peer(),
            "Decoded gRPC request from UDP"
        );

        let response =
            crate::pipeline::l7::grpc::process_grpc_request(&req_event.request, &context, runtime)
                .await;

        if let Ok(resp_pkts) =
            engine.send_response(now, req_event.handle, req_event.stream_id, &response)
        {
            for pkt in resp_pkts {
                let _ = socket.send_to(&pkt.payload, pkt.peer).await;
            }
        }
    }
}

/// Forwards an HTTP/3 request over QUIC to the upstream target endpoint.
pub async fn forward_http3_request(
    req: &L7Request,
    target: SocketAddr,
) -> Result<L7Response, EdgeError> {
    Http3UpstreamConnector::forward_request(req, target)
        .await
        .map_err(|e| {
            EdgeError::Internal(format!("Failed to forward HTTP/3 request to {target}: {e}"))
        })
}

/// Dispatches an HTTP/3 request through `Http3Router` and forwards to upstream backend.
pub async fn process_http3_request(
    req: &L7Request,
    context: &ComposerContext,
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

    let target = if let Some(up) = rt.upstreams.http3.get(&route.upstream_name) {
        up.select_target().or_else(|| route.select_target())
    } else {
        route.select_target()
    };

    let Some(target) = target else {
        tracing::error!(
            listener = %context.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for HTTP/3 upstream"
        );
        return L7Response::from_bytes(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable: no healthy upstream endpoint\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    match forward_http3_request(req, target).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                target = %target,
                upstream = %route.upstream_name,
                "HTTP/3 upstream forwarding failed"
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
