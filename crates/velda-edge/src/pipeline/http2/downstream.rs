//! Layer 7 HTTP/2 Downstream Ingress Connection Engine.
//!
//! Handles downstream TLS handshake, ALPN validation, HTTP/2 connection accept loop,
//! anti-flood tracking, safe URI path normalization, in-memory route matching, and
//! stream dispatching to upstream pipeline strategies.

use std::sync::Arc;

use crate::pipeline::X_REQUEST_ID;
use http::StatusCode;
use http::header::HeaderValue;
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{Action, L7Request};
use velda_http1::Http1PipeStrategy;
use velda_http2::Http2Error;
use velda_http2::Http2PipeStrategy;
use velda_http2::config::Http2Config;
use velda_http2::server::{
    Http2Responder, Http2ServerConnection, Http2ServerRequestHead, Http2StreamReceiver,
};
use velda_http3::Http3PipeStrategy;
use velda_observability::{
    DownstreamProtocol, PipeStrategy, StreamCompletion, UpstreamProtocol, global_metrics,
};
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
    runtime: SharedRuntime,
) {
    let _conn_guard = crate::pipeline::ConnMetricGuard::enter();
    let rt = runtime.load();
    let peer = connection.peer();
    let local_addr = connection.local_addr();

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
                );
                run_http2_loop(tls_stream, meta, config, runtime).await;
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
        run_http2_loop(connection, meta, config, runtime).await;
    }
}

/// Core HTTP/2 downstream request-response loop decoupled from transport layer.
pub async fn run_http2_loop<IO>(
    stream: IO,
    meta: DownstreamMeta,
    config: Http2Config,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeout_duration = std::time::Duration::from_millis(config.idle_timeout_ms);
    let mut consecutive_not_founds: u32 = 0;
    const MAX_CONSECUTIVE_NOT_FOUNDS: u32 = 100;

    let conn_start = std::time::Instant::now();
    let mut requests_served: u32 = 0;
    let mut is_draining = false;

    let meta = Arc::new(meta);

    match Http2ServerConnection::handshake(stream, config).await {
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

                        // Amortized clock check: only sample VDSO Instant every 128 requests
                        let time_exceeded = (requests_served & 0x7F == 0)
                            && config.max_connection_duration_secs > 0
                            && conn_start.elapsed().as_secs()
                                >= config.max_connection_duration_secs as u64;

                        if requests_exceeded || time_exceeded {
                            is_draining = true;
                            tracing::info!(
                                listener = %meta.listener_id,
                                peer = %meta.peer,
                                requests_served,
                                "Initiating graceful HTTP/2 connection drain via GOAWAY"
                            );
                            conn.graceful_shutdown();
                        }
                    }

                    let (request_id, req_id_header) = velda_core::next_request_id();
                    req_head.headers.insert(X_REQUEST_ID, req_id_header.clone());
                    let host =
                        velda_http2::server::header::extract_host(&req_head.headers, &req_head.uri);
                    let pq = req_head
                        .uri
                        .path_and_query()
                        .map(|x| x.as_str())
                        .unwrap_or(req_head.uri.path());
                    let (log_url_len, log_url_bytes) =
                        velda_observability::AccessLogRecord::encode_url(meta.is_tls, host, pq);
                    responder = responder
                        .with_request_id(Some(req_id_header.clone()))
                        .with_alt_svc(meta.alt_svc_header.clone());

                    if req_head.headers.len() > velda_core::MAX_WIRE_HEADERS {
                        tracing::warn!(
                            listener = %meta.listener_id,
                            peer = %meta.peer,
                            headers = req_head.headers.len(),
                            max = velda_core::MAX_WIRE_HEADERS,
                            "Rejecting HTTP/2 request exceeding MAX_WIRE_HEADERS"
                        );
                        let _ =
                            responder.send_early_error(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
                        global_metrics().record_early_completion(
                            DownstreamProtocol::H2,
                            UpstreamProtocol::H2,
                            PipeStrategy::Buffered,
                            StreamCompletion::new(
                                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                std::time::Duration::ZERO,
                                None,
                            ),
                        );
                        let log_method = velda_observability::LogHttpMethod::from_bytes(
                            req_head.method.as_str().as_bytes(),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: 0,
                                peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                peer_port: meta.peer.port(),
                                upstream_port: 0,
                                status_code: StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE.as_u16(),
                                route_id: 0,
                                ua_major_version: 0,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                                completion: velda_observability::LogCompletionReason::Complete
                                    as u8,
                                method: log_method as u8,
                                grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                                ua_family: velda_observability::UaFamily::None as u8,
                                ua_os: velda_observability::UaOs::Unknown as u8,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            },
                        );
                        continue;
                    }

                    if let Err(e) = velda_http2::server::path::normalize_path(&mut req_head.uri) {
                        tracing::warn!(
                            listener = %meta.listener_id,
                            error = %e,
                            path = %req_head.uri.path(),
                            "Rejecting HTTP/2 request with unsafe URI path"
                        );
                        if !receiver.is_end_stream() {
                            let _ = responder
                                .send_early_error_and_cancel_upload(StatusCode::BAD_REQUEST);
                        } else {
                            let _ = responder.send_early_error(StatusCode::BAD_REQUEST);
                        }
                        global_metrics().record_early_completion(
                            DownstreamProtocol::H2,
                            UpstreamProtocol::H2,
                            PipeStrategy::Buffered,
                            StreamCompletion::new(
                                StatusCode::BAD_REQUEST.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                std::time::Duration::ZERO,
                                None,
                            ),
                        );
                        let log_method = velda_observability::LogHttpMethod::from_bytes(
                            req_head.method.as_str().as_bytes(),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: 0,
                                peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                peer_port: meta.peer.port(),
                                upstream_port: 0,
                                status_code: StatusCode::BAD_REQUEST.as_u16(),
                                route_id: 0,
                                ua_major_version: 0,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                                completion: velda_observability::LogCompletionReason::Complete
                                    as u8,
                                method: log_method as u8,
                                grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                                ua_family: velda_observability::UaFamily::None as u8,
                                ua_os: velda_observability::UaOs::Unknown as u8,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            },
                        );
                        continue;
                    }

                    // Route matching
                    let mut http_req = Http2RouteRequest::new(req_head.uri.path());
                    if let Some(h) =
                        velda_http2::server::header::extract_host(&req_head.headers, &req_head.uri)
                    {
                        http_req = http_req.with_host(h);
                    }
                    http_req = http_req.with_method(req_head.method.as_str());

                    let rt = runtime.load();
                    let matched_route = rt.router.route_http2(&meta.listener_id, &http_req);

                    let Some(route) = matched_route else {
                        consecutive_not_founds += 1;
                        if consecutive_not_founds > MAX_CONSECUTIVE_NOT_FOUNDS {
                            tracing::warn!(
                                listener = %meta.listener_id,
                                peer = %meta.peer,
                                consecutive = consecutive_not_founds,
                                "Aborting HTTP/2 connection: excessive consecutive non-matching routes (possible scan/flood)"
                            );
                            break;
                        }

                        if !receiver.is_end_stream() {
                            let _ =
                                responder.send_early_error_and_cancel_upload(StatusCode::NOT_FOUND);
                        } else {
                            let _ = responder.send_early_error(StatusCode::NOT_FOUND);
                        }
                        global_metrics().record_early_completion(
                            DownstreamProtocol::H2,
                            UpstreamProtocol::H2,
                            PipeStrategy::Buffered,
                            StreamCompletion::new(
                                StatusCode::NOT_FOUND.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                std::time::Duration::ZERO,
                                None,
                            ),
                        );
                        let log_method = velda_observability::LogHttpMethod::from_bytes(
                            req_head.method.as_str().as_bytes(),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: 0,
                                peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                peer_port: meta.peer.port(),
                                upstream_port: 0,
                                status_code: StatusCode::NOT_FOUND.as_u16(),
                                route_id: 0,
                                ua_major_version: 0,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                                completion: velda_observability::LogCompletionReason::Complete
                                    as u8,
                                method: log_method as u8,
                                grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                                ua_family: velda_observability::UaFamily::None as u8,
                                ua_os: velda_observability::UaOs::Unknown as u8,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            },
                        );
                        continue;
                    };

                    consecutive_not_founds = 0;

                    let Some(upstream_target) = rt.upstreams.http.get(route.upstream_id).cloned()
                    else {
                        tracing::error!(
                            listener = %meta.listener_id,
                            route = %route.id,
                            upstream = %route.upstream_id,
                            "No healthy backend endpoints available for HTTP upstream"
                        );
                        if !receiver.is_end_stream() {
                            let _ = responder.send_early_error_and_cancel_upload(
                                StatusCode::SERVICE_UNAVAILABLE,
                            );
                        } else {
                            let _ = responder.send_early_error(StatusCode::SERVICE_UNAVAILABLE);
                        }
                        global_metrics().record_early_completion(
                            DownstreamProtocol::H2,
                            UpstreamProtocol::H2,
                            PipeStrategy::Buffered,
                            StreamCompletion::new(
                                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                std::time::Duration::ZERO,
                                None,
                            ),
                        );
                        let log_method = velda_observability::LogHttpMethod::from_bytes(
                            req_head.method.as_str().as_bytes(),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: 0,
                                peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                peer_port: meta.peer.port(),
                                upstream_port: 0,
                                status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                                route_id: route.id.0 as u16,
                                ua_major_version: 0,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                                completion: velda_observability::LogCompletionReason::Complete
                                    as u8,
                                method: log_method as u8,
                                grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                                ua_family: velda_observability::UaFamily::None as u8,
                                ua_os: velda_observability::UaOs::Unknown as u8,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            },
                        );
                        continue;
                    };

                    // Overload Check
                    let overload_lvl = rt.overload.level();
                    if overload_lvl.is_shedding() {
                        tracing::warn!(
                            route = %route.id,
                            upstream = %route.upstream_id,
                            overload = ?overload_lvl,
                            "Shedding HTTP/2 request due to memory saturation"
                        );
                        if !receiver.is_end_stream() {
                            let _ = responder
                                .send_early_error_and_cancel_upload(StatusCode::TOO_MANY_REQUESTS);
                        } else {
                            let _ = responder.send_early_error(StatusCode::TOO_MANY_REQUESTS);
                        }
                        global_metrics().record_early_completion(
                            DownstreamProtocol::H2,
                            UpstreamProtocol::H2,
                            PipeStrategy::Buffered,
                            StreamCompletion::new(
                                StatusCode::TOO_MANY_REQUESTS.as_u16(),
                                0,
                                responder.last_bytes_tx,
                                std::time::Duration::ZERO,
                                None,
                            ),
                        );
                        let log_method = velda_observability::LogHttpMethod::from_bytes(
                            req_head.method.as_str().as_bytes(),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: 0,
                                peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                peer_port: meta.peer.port(),
                                upstream_port: 0,
                                status_code: StatusCode::TOO_MANY_REQUESTS.as_u16(),
                                route_id: route.id.0 as u16,
                                ua_major_version: 0,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                                completion: velda_observability::LogCompletionReason::Complete
                                    as u8,
                                method: log_method as u8,
                                grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                                ua_family: velda_observability::UaFamily::None as u8,
                                ua_os: velda_observability::UaOs::Unknown as u8,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            },
                        );
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
                        default_upstream_id: route.upstream_id,
                        http_upstreams: Arc::clone(&rt.upstreams.http),
                        plan: rt.plugins.get_plan(route.id).clone(),
                        request_id,
                        route_id: route.id.0 as u16,
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
                            peer = %meta.peer,
                            "Aborting HTTP/2 connection: control frame flood detected"
                        );
                    } else {
                        tracing::debug!(
                            listener = %meta.listener_id,
                            peer = %meta.peer,
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
    default_upstream_id: velda_core::UpstreamId,
    http_upstreams: Arc<crate::upstream::SubUpstreamTable<HttpUpstream>>,
    plan: Arc<RoutePluginPlan>,
    request_id: velda_core::RequestId,
    route_id: u16,
}

/// Serves an individual HTTP/2 downstream multiplexed stream top-to-bottom.
async fn serve_http2_stream(
    mut req_head: Http2ServerRequestHead,
    receiver: Http2StreamReceiver,
    responder: Http2Responder,
    ctx: Http2StreamContext,
) {
    let req_start = std::time::Instant::now();
    let Http2StreamContext {
        meta,
        config,
        default_upstream,
        default_upstream_id,
        http_upstreams,
        plan,
        request_id,
        route_id,
    } = ctx;
    let mut target_upstream = default_upstream_id;
    let peer = meta.peer;
    let log_method =
        velda_observability::LogHttpMethod::from_bytes(req_head.method.as_str().as_bytes());
    let (ua_family, ua_os, ua_major_version) = req_head
        .headers
        .get(http::header::USER_AGENT)
        .map(|v| velda_observability::classify_user_agent(v.as_bytes()))
        .unwrap_or((
            velda_observability::UaFamily::None,
            velda_observability::UaOs::Unknown,
            0,
        ));
    let (mut log_url_len, mut log_url_bytes) = {
        let host = velda_http2::server::header::extract_host(&req_head.headers, &req_head.uri);
        let pq = req_head
            .uri
            .path_and_query()
            .map(|x| x.as_str())
            .unwrap_or(req_head.uri.path());
        velda_observability::AccessLogRecord::encode_url(meta.is_tls, host, pq)
    };

    // Authoritatively enrich proxy forwarding headers (x-forwarded-*, x-real-ip)
    // directly from downstream connection metadata BEFORE pre_route and pre_upstream plugins.
    velda_http2::server::header::enrich_headers_precomputed(
        &mut req_head.headers,
        &req_head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    if !plan.is_empty() {
        let mut short_circuit_resp = None;

        if plan.pre_route_hooks.is_some() || plan.pre_upstream_hooks.is_some() {
            // Optimization: Zero-clone move of method, uri, and headers into L7Request.
            let method = std::mem::replace(&mut req_head.method, http::Method::GET);
            let uri = std::mem::take(&mut req_head.uri);
            let headers = std::mem::take(&mut req_head.headers);
            let mut l7_req = L7Request::new(
                method,
                uri,
                http::Version::HTTP_2,
                velda_core::HeaderWorkspace::from_headers(headers),
                velda_core::Body::Empty,
            );
            l7_req.state.request_id = request_id;
            l7_req.state.client_ip = Some(meta.peer.ip());

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
                    Action::ServeCached(cached) => {
                        short_circuit_resp = Some(cached.into_l7_response());
                    }
                    Action::Reject(reason) => {
                        // FAST-FAIL INVARIANT: Rejections terminate immediately at the Edge boundary.
                        // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                        // preserving only Bit-1 bypass headers and Cache-Control: no-store.
                        let mut resp = velda_http2::server::precompiled_reject_response(
                            reason,
                            Some(l7_req.headers.request_id()),
                        );
                        for (name, val) in l7_req.headers.bypass_headers() {
                            resp.headers.insert(name.clone(), val.clone());
                        }
                        resp.headers.insert(
                            http::header::CACHE_CONTROL,
                            HeaderValue::from_static("no-store"),
                        );
                        resp.headers
                            .set_request_id(l7_req.headers.request_id().clone());
                        meta.inject_response_headers(&mut resp.headers);
                        let status = resp.status.as_u16();
                        let mut responder = responder;
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&resp);
                        } else {
                            let _ = responder.send_response(&resp);
                        }
                        let elapsed = req_start.elapsed();
                        global_metrics().record_early_completion(
                            DownstreamProtocol::H2,
                            UpstreamProtocol::H2,
                            PipeStrategy::Buffered,
                            StreamCompletion::new(
                                status,
                                0,
                                responder.last_bytes_tx,
                                elapsed,
                                None,
                            ),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: elapsed.as_nanos().min(u64::MAX as u128) as u64,
                                peer_ip: velda_observability::encode_ip_u128(peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                peer_port: peer.port(),
                                upstream_port: 0,
                                status_code: status,
                                route_id,
                                ua_major_version,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                                completion: velda_observability::LogCompletionReason::Complete
                                    as u8,
                                method: log_method as u8,
                                grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                                ua_family: ua_family as u8,
                                ua_os: ua_os as u8,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            },
                        );
                        return;
                    }
                    Action::Delay { .. } => unreachable!(),
                }
            }

            if short_circuit_resp.is_none()
                && let Some(hooks) = &plan.pre_upstream_hooks
            {
                let mut action = velda_plugin::run_pre_upstream_plugins(
                    hooks,
                    &mut l7_req,
                    &mut target_upstream,
                );
                while let Action::Delay { duration, and_then } = action {
                    tokio::time::sleep(duration).await;
                    action = *and_then;
                }

                match action {
                    Action::Continue => {}
                    Action::Respond(resp) => {
                        short_circuit_resp = Some(resp);
                    }
                    Action::ServeCached(cached) => {
                        short_circuit_resp = Some(cached.into_l7_response());
                    }
                    Action::Reject(reason) => {
                        // FAST-FAIL INVARIANT: Rejections terminate immediately at the Edge boundary.
                        // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                        // preserving only Bit-1 bypass headers and Cache-Control: no-store.
                        let mut resp = velda_http2::server::precompiled_reject_response(
                            reason,
                            Some(l7_req.headers.request_id()),
                        );
                        for (name, val) in l7_req.headers.bypass_headers() {
                            resp.headers.insert(name.clone(), val.clone());
                        }
                        resp.headers.insert(
                            http::header::CACHE_CONTROL,
                            HeaderValue::from_static("no-store"),
                        );
                        resp.headers
                            .set_request_id(l7_req.headers.request_id().clone());
                        meta.inject_response_headers(&mut resp.headers);
                        let status = resp.status.as_u16();
                        let mut responder = responder;
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&resp);
                        } else {
                            let _ = responder.send_response(&resp);
                        }
                        let elapsed = req_start.elapsed();
                        global_metrics().record_early_completion(
                            DownstreamProtocol::H2,
                            UpstreamProtocol::H2,
                            PipeStrategy::Buffered,
                            StreamCompletion::new(
                                status,
                                0,
                                responder.last_bytes_tx,
                                elapsed,
                                None,
                            ),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: elapsed.as_nanos().min(u64::MAX as u128) as u64,
                                peer_ip: velda_observability::encode_ip_u128(peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                peer_port: peer.port(),
                                upstream_port: 0,
                                status_code: status,
                                route_id,
                                ua_major_version,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                                completion: velda_observability::LogCompletionReason::Complete
                                    as u8,
                                method: log_method as u8,
                                grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                                ua_family: ua_family as u8,
                                ua_os: ua_os as u8,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            },
                        );
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
                    velda_plugin::run_post_response_plugins(post_hooks, &l7_req.headers, &mut resp);
                }
                meta.inject_response_headers(&mut resp.headers);
                let status = resp.status.as_u16();
                let mut responder = responder;
                if !receiver.is_end_stream() {
                    let _ = responder.send_response_and_cancel_upload(&resp);
                } else {
                    let _ = responder.send_response(&resp);
                }
                let elapsed = req_start.elapsed();
                global_metrics().record_early_completion(
                    DownstreamProtocol::H2,
                    UpstreamProtocol::H2,
                    PipeStrategy::Buffered,
                    StreamCompletion::new(status, 0, responder.last_bytes_tx, elapsed, None),
                );
                velda_observability::global_log_store().emit(
                    &velda_observability::AccessLogRecord {
                        request_id: request_id.entropy(),
                        timestamp_sec: request_id.timestamp_secs() as u32,
                        duration_ns: elapsed.as_nanos().min(u64::MAX as u128) as u64,
                        peer_ip: velda_observability::encode_ip_u128(peer.ip()),
                        bytes_rx: 0,
                        bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
                        upstream_ip: 0,
                        peer_port: peer.port(),
                        upstream_port: 0,
                        status_code: status,
                        route_id,
                        ua_major_version,
                        rpc_msgs_or_flags: 0,
                        pipe: velda_core::pipe::PipeKind::BufferedH2H1,
                        completion: velda_observability::LogCompletionReason::Complete as u8,
                        method: log_method as u8,
                        grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
                        ua_family: ua_family as u8,
                        ua_os: ua_os as u8,
                        url_len: log_url_len,
                        url_bytes: log_url_bytes,
                    },
                );
                return;
            }

            req_head.headers = l7_req.headers.into();
            req_head.uri = l7_req.uri;
            req_head.method = l7_req.method;
            let (re_len, re_bytes) = {
                let host =
                    velda_http2::server::header::extract_host(&req_head.headers, &req_head.uri);
                let pq = req_head
                    .uri
                    .path_and_query()
                    .map(|x| x.as_str())
                    .unwrap_or(req_head.uri.path());
                velda_observability::AccessLogRecord::encode_url(meta.is_tls, host, pq)
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
        if !receiver.is_end_stream() {
            let _ = responder.send_early_error_and_cancel_upload(StatusCode::SERVICE_UNAVAILABLE);
        } else {
            let _ = responder.send_early_error(StatusCode::SERVICE_UNAVAILABLE);
        }
        let elapsed = req_start.elapsed();
        global_metrics().record_early_completion(
            DownstreamProtocol::H2,
            UpstreamProtocol::H2,
            PipeStrategy::Buffered,
            StreamCompletion::new(
                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                0,
                responder.last_bytes_tx,
                elapsed,
                None,
            ),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: request_id.entropy(),
            timestamp_sec: request_id.timestamp_secs() as u32,
            duration_ns: elapsed.as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
            bytes_rx: 0,
            bytes_tx: responder.last_bytes_tx.min(u32::MAX as u64) as u32,
            upstream_ip: 0,
            peer_port: peer.port(),
            upstream_port: 0,
            status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            route_id,
            ua_major_version,
            rpc_msgs_or_flags: 0,
            pipe: velda_core::pipe::PipeKind::BufferedH2H1,
            completion: velda_observability::LogCompletionReason::Complete as u8,
            method: log_method as u8,
            grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return;
    };

    let has_upload_body = !receiver.is_end_stream();
    let is_sse = req_head.headers.get(http::header::ACCEPT).is_some_and(|a| {
        a.as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    });

    let (upstream_protocol, pipe_strat, pipe_kind) = match upstream_target.as_ref() {
        HttpUpstream::Http2(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http2PipeStrategy::Buffered => (
                UpstreamProtocol::H2,
                PipeStrategy::Buffered,
                velda_core::pipe::PipeKind::BufferedH2H2,
            ),
            Http2PipeStrategy::ServerStream => (
                UpstreamProtocol::H2,
                PipeStrategy::ServerStream,
                velda_core::pipe::PipeKind::ServerStreamH2H2,
            ),
            Http2PipeStrategy::ClientStream => (
                UpstreamProtocol::H2,
                PipeStrategy::ClientStream,
                velda_core::pipe::PipeKind::ClientStreamH2H2,
            ),
            Http2PipeStrategy::Duplex => (
                UpstreamProtocol::H2,
                PipeStrategy::Duplex,
                velda_core::pipe::PipeKind::DuplexH2H2,
            ),
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http1PipeStrategy::Buffered => (
                UpstreamProtocol::H1,
                PipeStrategy::Buffered,
                velda_core::pipe::PipeKind::BufferedH2H1,
            ),
            Http1PipeStrategy::ServerStream => (
                UpstreamProtocol::H1,
                PipeStrategy::ServerStream,
                velda_core::pipe::PipeKind::ServerStreamH2H1,
            ),
            Http1PipeStrategy::ClientStream => (
                UpstreamProtocol::H1,
                PipeStrategy::ClientStream,
                velda_core::pipe::PipeKind::ClientStreamH2H1,
            ),
            Http1PipeStrategy::Duplex => (
                UpstreamProtocol::H1,
                PipeStrategy::Duplex,
                velda_core::pipe::PipeKind::DuplexH2H1,
            ),
        },
        HttpUpstream::Http3(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http3PipeStrategy::Buffered => (
                UpstreamProtocol::H3,
                PipeStrategy::Buffered,
                velda_core::pipe::PipeKind::BufferedH2H3,
            ),
            Http3PipeStrategy::ServerStream => (
                UpstreamProtocol::H3,
                PipeStrategy::ServerStream,
                velda_core::pipe::PipeKind::ServerStreamH2H3,
            ),
            Http3PipeStrategy::ClientStream => (
                UpstreamProtocol::H3,
                PipeStrategy::ClientStream,
                velda_core::pipe::PipeKind::ClientStreamH2H3,
            ),
            Http3PipeStrategy::Duplex => (
                UpstreamProtocol::H3,
                PipeStrategy::Duplex,
                velda_core::pipe::PipeKind::DuplexH2H3,
            ),
        },
    };

    global_metrics().record_stream_start(DownstreamProtocol::H2, upstream_protocol, pipe_strat);

    let outcome = match upstream_target.as_ref() {
        HttpUpstream::Http2(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http2PipeStrategy::Buffered => {
                pipe::buffered_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await
            }
            Http2PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await
            }
            Http2PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await
            }
            Http2PipeStrategy::Duplex => {
                pipe::duplex_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await
            }
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http1PipeStrategy::Buffered => {
                pipe::buffered_h2_h1::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await
            }
            Http1PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h1::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await
            }
            Http1PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h1::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await
            }
            Http1PipeStrategy::Duplex => {
                pipe::duplex_h2_h1::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await
            }
        },
        HttpUpstream::Http3(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http3PipeStrategy::Buffered => {
                pipe::buffered_h2_h3::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await
            }
            Http3PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h3::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await
            }
            Http3PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h3::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await
            }
            Http3PipeStrategy::Duplex => {
                pipe::duplex_h2_h3::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await
            }
        },
    };

    let elapsed = req_start.elapsed();
    global_metrics().record_stream_completion(
        DownstreamProtocol::H2,
        upstream_protocol,
        pipe_strat,
        StreamCompletion::new(
            outcome.status_code,
            outcome.bytes_rx,
            outcome.bytes_tx,
            elapsed,
            None,
        ),
    );
    let (upstream_ip, upstream_port) = outcome
        .upstream_addr
        .map(|a| {
            (
                velda_observability::encode_upstream_ip_u32(a.ip()),
                a.port(),
            )
        })
        .unwrap_or((0, 0));
    velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
        request_id: request_id.entropy(),
        timestamp_sec: request_id.timestamp_secs() as u32,
        duration_ns: elapsed.as_nanos().min(u64::MAX as u128) as u64,
        peer_ip: velda_observability::encode_ip_u128(peer.ip()),
        bytes_rx: outcome.bytes_rx.min(u32::MAX as u64) as u32,
        bytes_tx: outcome.bytes_tx.min(u32::MAX as u64) as u32,
        upstream_ip,
        peer_port: peer.port(),
        upstream_port,
        status_code: outcome.status_code,
        route_id,
        ua_major_version,
        rpc_msgs_or_flags: 0,
        pipe: pipe_kind,
        completion: outcome.completion as u8,
        method: log_method as u8,
        grpc_status: velda_observability::LogGrpcStatus::NonGrpc as u8,
        ua_family: ua_family as u8,
        ua_os: ua_os as u8,
        url_len: log_url_len,
        url_bytes: log_url_bytes,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::net::SocketAddr;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut head = Http2ServerRequestHead::new(
            http::Method::GET,
            "http://secure.example.com/api".parse().unwrap(),
            HeaderMap::new(),
            None,
        );
        head.headers
            .insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        head.headers
            .insert("x-real-ip", "203.0.113.195".parse().unwrap());
        head.headers
            .insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        head.headers
            .insert("x-forwarded-proto", "http".parse().unwrap());
        head.headers
            .insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let peer: SocketAddr = "192.0.2.20:54321".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:8443".parse().unwrap();

        head.enrich_forwarded_headers(peer, local, true);

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
        assert_eq!(
            head.headers.get("forwarded").unwrap(),
            "for=192.0.2.20;proto=https;by=10.0.0.1;host=\"secure.example.com\""
        );
    }

    #[test]
    fn test_refused_or_goaway_detection() {
        assert!(velda_http2::Http2Error::ConnectionClosed.is_refused_or_goaway());
        assert!(
            velda_http2::Http2Error::StreamReset(h2::Reason::REFUSED_STREAM).is_refused_or_goaway()
        );
        assert!(
            velda_http2::Http2Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_refused_or_goaway()
        );

        assert!(
            !velda_http2::Http2Error::StreamReset(h2::Reason::INTERNAL_ERROR)
                .is_refused_or_goaway()
        );
        assert!(!velda_http2::Http2Error::PayloadTooLarge(500).is_refused_or_goaway());
    }

    #[test]
    fn test_host_header_extraction_and_injection() {
        let uri: http::Uri = "http://example.com:8080/path".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert!(!headers.contains_key(http::header::HOST));

        if let Some(host) = velda_http2::server::header::extract_host(&headers, &uri)
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
        let default_upstream_id = velda_core::UpstreamId::new(1);
        let target_upstream = velda_core::UpstreamId::new(2);

        let http_upstreams: FxHashMap<velda_core::UpstreamId, Arc<HttpUpstream>> =
            FxHashMap::default();

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
