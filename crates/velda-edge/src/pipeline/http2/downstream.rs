//! Layer 7 HTTP/2 Downstream Ingress Connection Engine.
//!
//! Handles downstream TLS handshake, ALPN validation, HTTP/2 connection accept loop,
//! anti-flood tracking, safe URI path normalization, in-memory route matching, and
//! stream dispatching to upstream pipeline strategies.

use std::sync::Arc;

use http::StatusCode;
use http::header::HeaderValue;
use tokio::io::{AsyncRead, AsyncWrite};
use u16;
use velda_core::{Action, L7Request, PipeKind};
use velda_http1::strategy::Http1PipeStrategy;
use velda_http2::config::Http2Config;
use velda_http2::error::Http2Error;
use velda_http2::server::connection::Http2ServerConnection;
use velda_http2::server::request::{Http2ServerRequestHead, Http2StreamReceiver};
use velda_http2::server::response::Http2Responder;
use velda_http2::strategy::Http2PipeStrategy;
use velda_http3::strategy::Http3PipeStrategy;
use velda_observability::sampling::http::HttpSampling;
use velda_observability::{StreamCompletion, global_metrics};
use velda_plugin::RoutePluginPlan;
use velda_router::Http2RouteRequest;
use velda_sock::Connection;
use velda_tls::TlsServerEngine;

use super::pipe;
use crate::pipeline::DownstreamMeta;
use crate::runtime::SharedRuntime;
use crate::upstream::HttpUpstream;

/// Asynchronous stream worker dispatching incoming HTTP/2 connections.
pub async fn handle_http2_stream(
    connection: Connection,
    listener_id: Arc<str>,
    config: Http2Config,
    tls_enabled: bool,
    alt_svc: Option<http::HeaderValue>,
    sampling: HttpSampling,
    runtime: SharedRuntime,
) {
    let _conn_guard = crate::pipeline::ConnMetricGuard::enter();
    let rt = runtime.load();
    let peer = velda_sock::raw_to_socket_addr(connection.peer_ip, connection.peer_port_be);
    let local_addr = velda_sock::raw_to_socket_addr(connection.local_ip, connection.local_port_be);

    if tls_enabled {
        let Some(tls_server) = rt.tls_server.as_ref() else {
            global_metrics().global().tls_handshake_error();
            tracing::error!(
                listener = %listener_id,
                peer = %peer,
                "TLS required for listener, but no TLS server engine is compiled; dropping connection"
            );
            return;
        };

        let ja4 =
            DownstreamMeta::peek_tcp_ja4(&connection.stream, tls_server.params().handshake_timeout)
                .await;
        match tls_server.accept_with_timeout(connection).await {
            Ok(tls_stream) => {
                // RFC 9113 Section 3.3 mandate: HTTP/2 over TLS MUST negotiate ALPN identification "h2".
                // Connections presenting no ALPN or an incompatible identification are strictly rejected
                // to prevent invalid plaintext/framing desynchronization in the downstream state machine.
                let alpn = TlsServerEngine::extract_alpn(&tls_stream);

                if alpn.as_deref() != Some("h2") {
                    global_metrics().global().tls_handshake_error();
                    tracing::warn!(
                        listener = %listener_id,
                        peer = %peer,
                        expected = "h2",
                        actual = ?alpn,
                        "Dropping connection: HTTP/2 over TLS strictly requires ALPN 'h2'"
                    );
                    return;
                }

                global_metrics().global().tls_handshake_success();
                tracing::debug!(
                    listener = %listener_id,
                    peer = %peer,
                    alpn = ?alpn,
                    "Downstream TLS handshake succeeded for HTTP/2"
                );

                let meta = DownstreamMeta::new(
                    listener_id,
                    peer,
                    local_addr,
                    true,
                    alt_svc,
                    &rt.buffer_pool,
                )
                .with_ja4(ja4);
                run_http2_loop(tls_stream, meta, config, sampling, runtime).await;
            }
            Err(e) => {
                global_metrics().global().tls_handshake_error();
                tracing::warn!(
                    error = %e,
                    listener = %listener_id,
                    peer = %peer,
                    "Downstream TLS handshake failed"
                );
            }
        }
    } else {
        let meta = DownstreamMeta::new(
            listener_id,
            peer,
            local_addr,
            false,
            alt_svc,
            &rt.buffer_pool,
        );
        run_http2_loop(connection, meta, config, sampling, runtime).await;
    }
}

/// Core HTTP/2 downstream request-response loop decoupled from transport layer.
pub async fn run_http2_loop<IO>(
    stream: IO,
    meta: DownstreamMeta,
    config: Http2Config,
    sampling: HttpSampling,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeout_duration = std::time::Duration::from_millis(config.idle_timeout_ms);
    let mut consecutive_not_founds: u32 = 0;
    const MAX_CONSECUTIVE_NOT_FOUNDS: u32 = 100;

    let conn_start_sec = velda_core::clock::coarse_sec();
    let mut requests_served: u32 = 0;
    let mut is_draining = false;

    let meta = Arc::new(meta);

    let forwarding = velda_http2::proto::hpack::H2ForwardingAddrs {
        peer_ip: meta.peer_ip,
        local_ip: meta.local_ip,
        local_port_be: meta.local_port_be,
        is_tls: meta.is_tls,
    };

    match Http2ServerConnection::handshake_with_forwarding(stream, config, forwarding).await {
        Ok(mut conn) => loop {
            let accept_result = if let Some(res) = conn.try_accept_streaming_request() {
                Ok(res)
            } else {
                tokio::time::timeout(timeout_duration, conn.accept_streaming_request()).await
            };
            match accept_result {
                Ok(Ok(Some((mut req_head, receiver, mut responder)))) => {
                    requests_served = requests_served.saturating_add(1);

                    // Graceful Connection Drain Check (keepalive requests & max connection duration)
                    if !is_draining {
                        let requests_exceeded = config.max_requests_per_connection > 0
                            && requests_served >= config.max_requests_per_connection;

                        // Amortized clock check: only sample Coarse Clock every 128 requests
                        let time_exceeded = (requests_served & 0x7F == 0)
                            && config.max_connection_duration_secs > 0
                            && velda_core::clock::coarse_sec().saturating_sub(conn_start_sec)
                                >= config.max_connection_duration_secs as u64;

                        if requests_exceeded || time_exceeded {
                            is_draining = true;
                            tracing::info!(
                                listener = %meta.listener_id,
                                peer_ip = %meta.peer_ip,
                                peer_port = u16::from_be(meta.peer_port_be),
                                requests_served,
                                "Initiating graceful HTTP/2 connection drain via GOAWAY"
                            );
                            conn.graceful_shutdown();
                        }
                    }

                    let ingress_nanos = if req_head.ingress_time_nanos > 0 {
                        req_head.ingress_time_nanos
                    } else {
                        velda_core::clock::now_unix_nanos()
                    };
                    let request_id = if req_head.request_id != 0 {
                        req_head.request_id
                    } else {
                        velda_core::request_id::next_request_id(ingress_nanos).0
                    };
                    if let Some(hdr) = req_head
                        .headers
                        .get(&crate::pipeline::X_REQUEST_ID)
                        .cloned()
                    {
                        responder.request_id = Some(hdr);
                    }
                    responder.alt_svc = meta.alt_svc_header.clone();

                    if let Err(e) = velda_http2::server::path::normalize_path(&mut req_head.uri) {
                        tracing::warn!(
                            listener = %meta.listener_id,
                            error = %e,
                            path = %req_head.uri.path(),
                            "Rejecting HTTP/2 request with unsafe URI path"
                        );
                        let _ = responder
                            .send_early_error(StatusCode::BAD_REQUEST, !receiver.is_end_stream());
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedH2H1,
                            StreamCompletion::new(
                                StatusCode::BAD_REQUEST.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                duration_ns,
                                0,
                            ),
                        );
                        if sampling.keep(StatusCode::BAD_REQUEST.as_u16()) {
                            let host = velda_http2::server::request::extract_host(
                                &req_head.headers,
                                &req_head.uri,
                            );
                            let pq = req_head
                                .uri
                                .path_and_query()
                                .map(|x| x.as_str())
                                .unwrap_or(req_head.uri.path());
                            let (log_url_len, log_url_bytes) =
                                velda_observability::TelemetryRecord::encode_url(
                                    meta.is_tls,
                                    host,
                                    pq,
                                );
                            let log_method = velda_core::HttpMethod::from_bytes(
                                req_head.method.as_str().as_bytes(),
                            );
                            velda_observability::global_telemetry_store().emit(
                                &velda_observability::TelemetryRecord {
                                    request_id: velda_core::request_id::entropy(request_id),
                                    ingress_time_nanos: ingress_nanos,
                                    duration_ns,
                                    peer_ip: meta.peer_ip,
                                    bytes_rx: 0,
                                    bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                    upstream_ip: 0,
                                    upstream_port: 0,
                                    status_code: StatusCode::BAD_REQUEST.as_u16(),
                                    route_id: velda_core::UNMATCHED_ROUTE_ID,
                                    upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                    ua_major_version: 0,
                                    rpc_msgs_or_flags: 0,
                                    pipe: velda_core::PipeKind::BufferedH2H1,
                                    completion: velda_core::CompletionReason::Complete,
                                    method: log_method,
                                    grpc_status: velda_core::GrpcStatus::NonGrpc,
                                    ua_family: velda_core::UaFamily::None,
                                    ua_os: velda_core::UaOs::Unknown,
                                    url_len: log_url_len,
                                    url_bytes: log_url_bytes,
                                },
                            );
                        }
                        continue;
                    }

                    let host = velda_http2::server::request::extract_host(
                        &req_head.headers,
                        &req_head.uri,
                    );
                    let pq = req_head
                        .uri
                        .path_and_query()
                        .map(|x| x.as_str())
                        .unwrap_or(req_head.uri.path());
                    let (log_url_len, log_url_bytes) =
                        velda_observability::TelemetryRecord::encode_url(meta.is_tls, host, pq);

                    // Route matching
                    let mut http_req = Http2RouteRequest::new(req_head.uri.path());
                    if let Some(h) = host {
                        http_req = http_req.with_host(h);
                    }
                    http_req = http_req.with_method(req_head.method.as_str());

                    let rt = runtime.load();
                    let matched_route = rt
                        .router
                        .route_http2(velda_core::fnv1a_hash_32(&meta.listener_id), &http_req);

                    let Some(route) = matched_route else {
                        consecutive_not_founds += 1;
                        if consecutive_not_founds > MAX_CONSECUTIVE_NOT_FOUNDS {
                            tracing::warn!(
                                listener = %meta.listener_id,
                                peer_ip = %meta.peer_ip,
                                peer_port = u16::from_be(meta.peer_port_be),
                                consecutive = consecutive_not_founds,
                                "Aborting HTTP/2 connection: excessive consecutive non-matching routes (possible scan/flood)"
                            );
                            break;
                        }

                        let _ = responder
                            .send_early_error(StatusCode::NOT_FOUND, !receiver.is_end_stream());
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedH2H1,
                            StreamCompletion::new(
                                StatusCode::NOT_FOUND.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                duration_ns,
                                0,
                            ),
                        );
                        if sampling.keep(StatusCode::NOT_FOUND.as_u16()) {
                            let log_method = velda_core::HttpMethod::from_bytes(
                                req_head.method.as_str().as_bytes(),
                            );
                            velda_observability::global_telemetry_store().emit(
                                &velda_observability::TelemetryRecord {
                                    request_id: velda_core::request_id::entropy(request_id),
                                    ingress_time_nanos: ingress_nanos,
                                    duration_ns,
                                    peer_ip: meta.peer_ip,
                                    bytes_rx: 0,
                                    bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                    upstream_ip: 0,
                                    upstream_port: 0,
                                    status_code: StatusCode::NOT_FOUND.as_u16(),
                                    route_id: velda_core::UNMATCHED_ROUTE_ID,
                                    upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                    ua_major_version: 0,
                                    rpc_msgs_or_flags: 0,
                                    pipe: velda_core::PipeKind::BufferedH2H1,
                                    completion: velda_core::CompletionReason::Complete,
                                    method: log_method,
                                    grpc_status: velda_core::GrpcStatus::NonGrpc,
                                    ua_family: velda_core::UaFamily::None,
                                    ua_os: velda_core::UaOs::Unknown,
                                    url_len: log_url_len,
                                    url_bytes: log_url_bytes,
                                },
                            );
                        }
                        continue;
                    };

                    consecutive_not_founds = 0;

                    let route_idx = route.id as usize;
                    let target_upstream_id =
                        rt.route_to_upstream.get(route_idx).copied().unwrap_or(0);

                    let Some(upstream_target) = rt.upstreams.http.get(target_upstream_id).cloned()
                    else {
                        tracing::error!(
                            listener = %meta.listener_id,
                            route = %route.id,
                            upstream = %target_upstream_id,
                            "No healthy backend endpoints available for HTTP upstream"
                        );
                        let _ = responder.send_early_error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            !receiver.is_end_stream(),
                        );
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedH2H1,
                            StreamCompletion::new(
                                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                duration_ns,
                                0,
                            ),
                        );
                        if sampling.keep(StatusCode::SERVICE_UNAVAILABLE.as_u16()) {
                            let log_method = velda_core::HttpMethod::from_bytes(
                                req_head.method.as_str().as_bytes(),
                            );
                            velda_observability::global_telemetry_store().emit(
                                &velda_observability::TelemetryRecord {
                                    request_id: velda_core::request_id::entropy(request_id),
                                    ingress_time_nanos: ingress_nanos,
                                    duration_ns,
                                    peer_ip: meta.peer_ip,
                                    bytes_rx: 0,
                                    bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                    upstream_ip: 0,
                                    upstream_port: 0,
                                    status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                                    route_id: route.id,
                                    upstream_id: target_upstream_id,
                                    ua_major_version: 0,
                                    rpc_msgs_or_flags: 0,
                                    pipe: velda_core::PipeKind::BufferedH2H1,
                                    completion: velda_core::CompletionReason::Complete,
                                    method: log_method,
                                    grpc_status: velda_core::GrpcStatus::NonGrpc,
                                    ua_family: velda_core::UaFamily::None,
                                    ua_os: velda_core::UaOs::Unknown,
                                    url_len: log_url_len,
                                    url_bytes: log_url_bytes,
                                },
                            );
                        }
                        continue;
                    };

                    // Overload Check
                    let overload_lvl = rt.overload.level();
                    if overload_lvl.is_shedding() {
                        tracing::warn!(
                            route = %route.id,
                            upstream = %target_upstream_id,
                            overload = ?overload_lvl,
                            "Shedding HTTP/2 request due to memory saturation"
                        );
                        let _ = responder.send_early_error(
                            StatusCode::TOO_MANY_REQUESTS,
                            !receiver.is_end_stream(),
                        );
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedH2H1,
                            StreamCompletion::new(
                                StatusCode::TOO_MANY_REQUESTS.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                duration_ns,
                                0,
                            ),
                        );
                        if sampling.keep_shed() {
                            let log_method = velda_core::HttpMethod::from_bytes(
                                req_head.method.as_str().as_bytes(),
                            );
                            velda_observability::global_telemetry_store().emit(
                                &velda_observability::TelemetryRecord {
                                    request_id: velda_core::request_id::entropy(request_id),
                                    ingress_time_nanos: ingress_nanos,
                                    duration_ns,
                                    peer_ip: meta.peer_ip,
                                    bytes_rx: 0,
                                    bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                    upstream_ip: 0,
                                    upstream_port: 0,
                                    status_code: StatusCode::TOO_MANY_REQUESTS.as_u16(),
                                    route_id: route.id,
                                    upstream_id: target_upstream_id,
                                    ua_major_version: 0,
                                    rpc_msgs_or_flags: 0,
                                    pipe: velda_core::PipeKind::BufferedH2H1,
                                    completion: velda_core::CompletionReason::Complete,
                                    method: log_method,
                                    grpc_status: velda_core::GrpcStatus::NonGrpc,
                                    ua_family: velda_core::UaFamily::None,
                                    ua_os: velda_core::UaOs::Unknown,
                                    url_len: log_url_len,
                                    url_bytes: log_url_bytes,
                                },
                            );
                        }
                        if overload_lvl.is_critical() && !is_draining {
                            is_draining = true;
                            conn.graceful_shutdown();
                        }
                        continue;
                    }

                    let ctx = Http2StreamContext {
                        meta: Arc::clone(&meta),
                        config,
                        default_upstream: upstream_target,
                        default_upstream_id: target_upstream_id,
                        http_upstreams: Arc::clone(&rt.upstreams.http),
                        plan: rt.plugins.get_plan(route.id).clone(),
                        request_id,
                        route_id: route.id,
                        ingress_time_nanos: ingress_nanos,
                        sampling,
                    };
                    tokio::spawn(async move {
                        serve_http2_stream(req_head, receiver, responder, ctx).await;
                    });
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    if matches!(e, Http2Error::FloodDetected) {
                        tracing::warn!(
                            listener = %meta.listener_id,
                            peer_ip = %meta.peer_ip,
                            peer_port = u16::from_be(meta.peer_port_be),
                            "Aborting HTTP/2 connection: control frame flood detected"
                        );
                    } else {
                        tracing::debug!(
                            listener = %meta.listener_id,
                            peer_ip = %meta.peer_ip,
                            peer_port = u16::from_be(meta.peer_port_be),
                            error = %e,
                            "HTTP/2 stream accept error"
                        );
                    }
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %meta.listener_id,
                        timeout_ms = config.idle_timeout_ms,
                        "HTTP/2 stream accept timed out"
                    );
                    break;
                }
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "Failed to complete HTTP/2 server handshake");
        }
    }
}

struct Http2StreamContext {
    meta: Arc<DownstreamMeta>,
    config: Http2Config,
    default_upstream: Arc<HttpUpstream>,
    default_upstream_id: u16,
    http_upstreams: Arc<crate::upstream::SubUpstreamTable<HttpUpstream>>,
    plan: Arc<RoutePluginPlan>,
    request_id: u128,
    route_id: u16,
    ingress_time_nanos: u64,
    sampling: HttpSampling,
}

/// Serves an individual HTTP/2 downstream multiplexed stream top-to-bottom.
async fn serve_http2_stream(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    responder: Http2Responder,
    ctx: Http2StreamContext,
) {
    let Http2StreamContext {
        meta,
        config,
        default_upstream,
        default_upstream_id,
        http_upstreams,
        plan,
        request_id,
        route_id,
        ingress_time_nanos,
        sampling,
    } = ctx;
    let mut target_mask: u32 = velda_plugin::target_init(default_upstream_id);
    let mut target_upstream = default_upstream_id;
    let peer_ip = meta.peer_ip;
    let log_method = velda_core::HttpMethod::from_bytes(req_head.method.as_str().as_bytes());
    let mut req_state = velda_core::RequestState::new(request_id)
        .with_ingress_time(ingress_time_nanos)
        .with_method(log_method);
    req_state.client_ip = peer_ip;
    req_state.ja4 = meta.ja4;
    req_state.set_route(route_id);
    req_state.set_upstream(target_upstream);
    let (ua_family, ua_os, ua_major_version) = req_head
        .headers
        .get(http::header::USER_AGENT)
        .map(|v| velda_core::lut::classify_user_agent(v.as_bytes()))
        .unwrap_or((velda_core::UaFamily::None, velda_core::UaOs::Unknown, 0));
    let (mut log_url_len, mut log_url_bytes) = {
        let host = velda_http2::server::request::extract_host(&req_head.headers, &req_head.uri);
        let pq = req_head
            .uri
            .path_and_query()
            .map(|x| x.as_str())
            .unwrap_or(req_head.uri.path());
        velda_observability::TelemetryRecord::encode_url(meta.is_tls, host, pq)
    };

    if !plan.is_empty() {
        let mut short_circuit_resp = None;

        if plan.pre_route_hooks.is_some() || plan.pre_upstream_hooks.is_some() {
            // Optimization: Zero-clone move of method, uri, and HeaderWorkspace into L7Request.
            let method = std::mem::replace(&mut req_head.method, http::Method::GET);
            let uri = std::mem::take(&mut req_head.uri);
            let headers = std::mem::replace(
                &mut req_head.headers,
                velda_core::buffer::header::HeaderWorkspace::empty(0),
            );
            let mut l7_req = L7Request::new(
                method,
                uri,
                http::Version::HTTP_2,
                headers,
                velda_core::Body::Empty,
            );
            l7_req.ingress_time_nanos = ingress_time_nanos;
            l7_req.state = req_state.clone();

            if let Some(hooks) = &plan.pre_route_hooks {
                let mut action = velda_plugin::run_pre_route_plugins(hooks, &mut l7_req);
                while let Action::Delay { duration, and_then } = action {
                    tokio::time::sleep(duration).await;
                    action = *and_then;
                }

                match action {
                    Action::Continue => {}
                    Action::Respond(resp) => {
                        short_circuit_resp = Some(resp);
                    }
                    Action::Reject(reason) => {
                        // FAST-FAIL INVARIANT: Rejections terminate immediately at the Edge boundary.
                        // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                        // retaining only MSB bypass headers while velda-http2 stamps HTTP/2 rejection fields.
                        // RFC 9113 ZERO-BODY INVARIANT: Only HEADERS frame with END_STREAM = true.
                        let mut resp = velda_http2::server::early_response::reject_response(
                            reason,
                            l7_req.headers,
                        );
                        meta.inject_response_headers(&mut resp.headers);
                        let status = resp.status.as_u16();
                        let mut responder = responder;
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&resp);
                        } else {
                            let _ = responder.send_response(&resp);
                        }
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_time_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedH2H1,
                            StreamCompletion::new(
                                status,
                                0,
                                responder.last_bytes_tx,
                                duration_ns,
                                0,
                            ),
                        );
                        if sampling.keep(status) {
                            velda_observability::global_telemetry_store().emit(
                                &velda_observability::TelemetryRecord {
                                    request_id: velda_core::request_id::entropy(request_id),
                                    ingress_time_nanos,
                                    duration_ns,
                                    peer_ip: l7_req.state.client_ip,
                                    bytes_rx: 0,
                                    bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                    upstream_ip: 0,
                                    upstream_port: 0,
                                    status_code: status,
                                    route_id,
                                    upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                    ua_major_version,
                                    rpc_msgs_or_flags: 0,
                                    pipe: velda_core::PipeKind::BufferedH2H1,
                                    completion: velda_core::CompletionReason::Complete,
                                    method: log_method,
                                    grpc_status: velda_core::GrpcStatus::NonGrpc,
                                    ua_family,
                                    ua_os,
                                    url_len: log_url_len,
                                    url_bytes: log_url_bytes,
                                },
                            );
                        }
                        return;
                    }
                    Action::Delay { .. } => unreachable!(),
                }
            }

            if short_circuit_resp.is_none()
                && let Some(hooks) = &plan.pre_upstream_hooks
            {
                let mut action = velda_plugin::run_pre_upstream_plugins_async(
                    hooks,
                    &mut l7_req,
                    &mut target_mask,
                )
                .await;
                target_upstream = velda_plugin::target_primary(target_mask)
                    .unwrap_or(velda_core::UNASSIGNED_UPSTREAM_ID);
                l7_req.state.set_upstream(target_upstream);
                while let Action::Delay { duration, and_then } = action {
                    tokio::time::sleep(duration).await;
                    action = *and_then;
                }

                match action {
                    Action::Continue => {}
                    Action::Respond(resp) => {
                        short_circuit_resp = Some(resp);
                    }
                    Action::Reject(reason) => {
                        // FAST-FAIL INVARIANT: Rejections terminate immediately at the Edge boundary.
                        // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                        // retaining only MSB bypass headers while velda-http2 stamps HTTP/2 rejection fields.
                        // RFC 9113 ZERO-BODY INVARIANT: Only HEADERS frame with END_STREAM = true.
                        let mut resp = velda_http2::server::early_response::reject_response(
                            reason,
                            l7_req.headers,
                        );
                        meta.inject_response_headers(&mut resp.headers);
                        let status = resp.status.as_u16();
                        let mut responder = responder;
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&resp);
                        } else {
                            let _ = responder.send_response(&resp);
                        }
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_time_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedH2H1,
                            StreamCompletion::new(
                                status,
                                0,
                                responder.last_bytes_tx,
                                duration_ns,
                                0,
                            ),
                        );
                        if sampling.keep(status) {
                            velda_observability::global_telemetry_store().emit(
                                &velda_observability::TelemetryRecord {
                                    request_id: velda_core::request_id::entropy(request_id),
                                    ingress_time_nanos,
                                    duration_ns,
                                    peer_ip: l7_req.state.client_ip,
                                    bytes_rx: 0,
                                    bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                    upstream_ip: 0,
                                    upstream_port: 0,
                                    status_code: status,
                                    route_id,
                                    upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                    ua_major_version,
                                    rpc_msgs_or_flags: 0,
                                    pipe: velda_core::PipeKind::BufferedH2H1,
                                    completion: velda_core::CompletionReason::Complete,
                                    method: log_method,
                                    grpc_status: velda_core::GrpcStatus::NonGrpc,
                                    ua_family,
                                    ua_os,
                                    url_len: log_url_len,
                                    url_bytes: log_url_bytes,
                                },
                            );
                        }
                        return;
                    }
                    Action::Delay { .. } => unreachable!(),
                }
            }

            if let Some(mut resp) = short_circuit_resp {
                resp.headers
                    .set_request_id(l7_req.headers.request_id().clone());
                // Edge Authority Caching: Pre-load Cache-Control: no-store if unconfigured,
                // allowing post_response_hooks (http_cache_store) to override if permitted.
                if resp.headers.get(http::header::CACHE_CONTROL).is_none() {
                    resp.headers.insert(
                        http::header::CACHE_CONTROL,
                        HeaderValue::from_static("no-store"),
                    );
                }
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &l7_req.state,
                        &l7_req.headers,
                        &mut resp,
                    );
                }
                meta.inject_response_headers(&mut resp.headers);
                let status = resp.status.as_u16();
                let mut responder = responder;
                if !receiver.is_end_stream() {
                    let _ = responder.send_response_and_cancel_upload(&resp);
                } else {
                    let _ = responder.send_response(&resp);
                }
                let duration_ns =
                    velda_core::clock::now_unix_nanos().saturating_sub(ingress_time_nanos);
                global_metrics().record_early_completion(
                    PipeKind::BufferedH2H1,
                    StreamCompletion::new(status, 0, responder.last_bytes_tx, duration_ns, 0),
                );
                if sampling.keep(status) {
                    velda_observability::global_telemetry_store().emit(
                        &velda_observability::TelemetryRecord {
                            request_id: velda_core::request_id::entropy(request_id),
                            ingress_time_nanos,
                            duration_ns,
                            peer_ip: l7_req.state.client_ip,
                            bytes_rx: 0,
                            bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                            upstream_ip: 0,
                            upstream_port: 0,
                            status_code: status,
                            route_id,
                            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                            ua_major_version,
                            rpc_msgs_or_flags: 0,
                            pipe: velda_core::PipeKind::BufferedH2H1,
                            completion: velda_core::CompletionReason::Complete,
                            method: log_method,
                            grpc_status: velda_core::GrpcStatus::NonGrpc,
                            ua_family,
                            ua_os,
                            url_len: log_url_len,
                            url_bytes: log_url_bytes,
                        },
                    );
                }
                return;
            }

            req_state = l7_req.state;
            req_head.headers = l7_req.headers;
            req_head.uri = l7_req.uri;
            req_head.method = l7_req.method;
            let (re_len, re_bytes) = {
                let host =
                    velda_http2::server::request::extract_host(&req_head.headers, &req_head.uri);
                let pq = req_head
                    .uri
                    .path_and_query()
                    .map(|x| x.as_str())
                    .unwrap_or(req_head.uri.path());
                velda_observability::TelemetryRecord::encode_url(meta.is_tls, host, pq)
            };
            log_url_len = re_len;
            log_url_bytes = re_bytes;
        }
    }

    let upstream_target = if target_upstream == default_upstream_id {
        default_upstream
    } else if let Some(target) = http_upstreams.get(target_upstream) {
        target.clone()
    } else {
        tracing::error!(
            listener = %meta.listener_id,
            upstream = %target_upstream,
            "No HTTP upstream configured"
        );
        let mut responder = responder;
        let _ =
            responder.send_early_error(StatusCode::SERVICE_UNAVAILABLE, !receiver.is_end_stream());
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_time_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedH2H1,
            StreamCompletion::new(
                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                0,
                responder.last_bytes_tx,
                duration_ns,
                0,
            ),
        );
        if sampling.keep(StatusCode::SERVICE_UNAVAILABLE.as_u16()) {
            velda_observability::global_telemetry_store().emit(
                &velda_observability::TelemetryRecord {
                    request_id: velda_core::request_id::entropy(request_id),
                    ingress_time_nanos,
                    duration_ns,
                    peer_ip: meta.peer_ip,
                    bytes_rx: 0,
                    bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                    upstream_ip: 0,
                    upstream_port: 0,
                    status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    route_id,
                    upstream_id: target_upstream,
                    ua_major_version,
                    rpc_msgs_or_flags: 0,
                    pipe: velda_core::PipeKind::BufferedH2H1,
                    completion: velda_core::CompletionReason::Complete,
                    method: log_method,
                    grpc_status: velda_core::GrpcStatus::NonGrpc,
                    ua_family,
                    ua_os,
                    url_len: log_url_len,
                    url_bytes: log_url_bytes,
                },
            );
        }
        return;
    };

    let has_upload_body = !receiver.is_end_stream();
    let is_sse = req_head.headers.get(http::header::ACCEPT).is_some_and(|a| {
        a.as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    });
    if let Some(mirror_up) = velda_plugin::target_mirror(target_mask)
        .filter(|&m| m != target_upstream)
        .and_then(|m| http_upstreams.get(m))
    {
        crate::pipeline::mirror::attach_h2_mirror(&mut receiver, &req_head, mirror_up);
    }

    let (outcome, pipe_kind) = match upstream_target.as_ref() {
        HttpUpstream::Http2(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http2PipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedH2H2;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_h2_h2::serve(
                        req_head, receiver, responder, meta, config, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http2PipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamH2H2;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_h2_h2::serve(
                        req_head, receiver, responder, meta, config, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http2PipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamH2H2;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_h2_h2::serve(
                        req_head, receiver, responder, meta, config, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http2PipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexH2H2;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_h2_h2::serve(
                        req_head, receiver, responder, meta, config, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http1PipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedH2H1;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_h2_h1::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http1PipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamH2H1;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_h2_h1::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http1PipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamH2H1;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_h2_h1::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http1PipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexH2H1;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_h2_h1::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
        },
        HttpUpstream::Http3(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http3PipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedH2H3;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_h2_h3::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http3PipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamH2H3;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_h2_h3::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http3PipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamH2H3;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_h2_h3::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
            Http3PipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexH2H3;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_h2_h3::serve(
                        req_head, receiver, responder, meta, upstream, &req_state, &plan,
                    )
                    .await,
                    PIPE,
                )
            }
        },
    };

    let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_time_nanos);
    global_metrics().record_stream_completion(
        pipe_kind,
        StreamCompletion::new(
            outcome.status_code,
            outcome.bytes_rx,
            outcome.bytes_tx,
            duration_ns,
            0,
        ),
    );
    if sampling.keep(outcome.status_code) {
        let (upstream_ip, upstream_port) = outcome
            .upstream_addr
            .map(|a| {
                (
                    if velda_core::lut::ip::is_v4_u128(a.0) {
                        (a.0 & 0xFFFF_FFFF) as u32
                    } else {
                        0
                    },
                    u16::from_be(a.1),
                )
            })
            .unwrap_or((0, 0));
        velda_observability::global_telemetry_store().emit(&velda_observability::TelemetryRecord {
            request_id: velda_core::request_id::entropy(request_id),
            ingress_time_nanos,
            duration_ns,
            peer_ip,
            bytes_rx: outcome.bytes_rx.min(u32::MAX as u64) as u32,
            bytes_tx: outcome.bytes_tx.min(u32::MAX as u64) as u32,
            upstream_ip,
            upstream_port,
            status_code: outcome.status_code,
            route_id,
            upstream_id: target_upstream,
            ua_major_version,
            rpc_msgs_or_flags: 0,
            pipe: pipe_kind,
            completion: outcome.completion,
            method: log_method,
            grpc_status: velda_core::GrpcStatus::NonGrpc,
            ua_family,
            ua_os,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut enc = velda_http2::proto::hpack::HpackEncoder::default();
        let mut wire_ws = velda_core::buffer::header::HeaderWorkspace::empty(8);
        wire_ws.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        wire_ws.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        wire_ws.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        wire_ws.insert("x-forwarded-proto", "http".parse().unwrap());
        let uri: http::Uri = "http://secure.example.com/api".parse().unwrap();

        let mut hpack_block = bytes::BytesMut::new();
        enc.encode_request(&http::Method::GET, &uri, &wire_ws, &mut hpack_block);

        let mut dec = velda_http2::proto::hpack::HpackDecoder::default();
        let decoded = dec
            .decode_ingress(
                &hpack_block,
                velda_http2::proto::hpack::H2IngressEnrich {
                    request_id: HeaderValue::from_static("req-test-1"),
                    forwarding: Some(velda_http2::proto::hpack::H2ForwardingAddrs {
                        peer_ip: velda_core::lut::ip::v4_to_u128(0xC000_0214), // 192.0.2.20
                        local_ip: velda_core::lut::ip::v4_to_u128(0x0A00_0001),
                        local_port_be: 8443u16.to_be(),
                        is_tls: true,
                    }),
                },
            )
            .unwrap();
        let head = Http2ServerRequestHead::new(
            decoded.method.unwrap(),
            decoded.uri.unwrap(),
            decoded.headers,
            None,
        );

        assert_eq!(
            head.headers.get("x-forwarded-for").unwrap(),
            "203.0.113.195, 192.0.2.20"
        );
        assert_eq!(head.headers.get("x-real-ip").unwrap(), "192.0.2.20");
        assert_eq!(head.headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(head.headers.get("x-forwarded-port").unwrap(), "8443");
        assert_eq!(
            head.headers.get("x-forwarded-host").unwrap(),
            "secure.example.com"
        );
        assert_eq!(head.headers.get("x-request-id").unwrap(), "req-test-1");
    }

    #[test]
    fn test_refused_or_goaway_detection() {
        assert!(velda_http2::error::Http2Error::ConnectionClosed.is_refused_or_goaway());
        assert!(
            velda_http2::error::Http2Error::StreamReset(
                velda_http2::proto::frame::H2ErrorCode::REFUSED_STREAM
            )
            .is_refused_or_goaway()
        );
        assert!(
            velda_http2::error::Http2Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_refused_or_goaway()
        );

        assert!(
            !velda_http2::error::Http2Error::StreamReset(
                velda_http2::proto::frame::H2ErrorCode::INTERNAL_ERROR
            )
            .is_refused_or_goaway()
        );
        assert!(!velda_http2::error::Http2Error::PayloadTooLarge(500).is_refused_or_goaway());
    }

    #[test]
    fn test_host_header_extraction_and_injection() {
        let uri: http::Uri = "http://example.com:8080/path".parse().unwrap();
        let mut headers = velda_core::buffer::header::HeaderWorkspace::empty(4);
        assert!(!headers.contains_key(http::header::HOST));

        if let Some(host) = velda_http2::server::request::extract_host(&headers, &uri)
            && let Ok(hv) = http::header::HeaderValue::from_str(host)
        {
            headers.insert(http::header::HOST, hv);
        }

        assert_eq!(headers.get(http::header::HOST).unwrap(), "example.com:8080");
    }

    #[test]
    fn test_h2_pipe_coverage() {
        // Verify all 12 pipeline strategies are declared and available
        let _ = pipe::buffered_h2_h1::serve;
        let _ = pipe::server_stream_h2_h1::serve;
        let _ = pipe::client_stream_h2_h1::serve;
        let _ = pipe::duplex_h2_h1::serve;

        let _ = pipe::buffered_h2_h2::serve;
        let _ = pipe::server_stream_h2_h2::serve;
        let _ = pipe::client_stream_h2_h2::serve;
        let _ = pipe::duplex_h2_h2::serve;

        let _ = pipe::buffered_h2_h3::serve;
        let _ = pipe::server_stream_h2_h3::serve;
        let _ = pipe::client_stream_h2_h3::serve;
        let _ = pipe::duplex_h2_h3::serve;
    }

    #[test]
    fn test_h2_canary_target_missing_resolution() {
        use rustc_hash::FxHashMap;
        let default_upstream_id = 1;
        let target_upstream = 2;

        let http_upstreams: FxHashMap<u16, Arc<HttpUpstream>> = FxHashMap::default();

        let resolved = if target_upstream == default_upstream_id {
            Some("default")
        } else if http_upstreams.contains_key(&target_upstream) {
            Some("target")
        } else {
            None // Must reject with 503, never fallback to default!
        };

        assert!(resolved.is_none());
    }
}
