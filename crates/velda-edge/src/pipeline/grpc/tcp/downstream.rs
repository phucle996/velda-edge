//! Layer 7 gRPC over TCP downstream stream worker, route evaluation, and streaming forwarding.
//!
//! Powered entirely by `velda-grpc`. Operates strictly for listeners explicitly configured
//! with `transport = "tcp"` and `protocol = "grpc"`. Zero HTTP fallback, zero header sniffing,
//! and full bidirectional streaming.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{Action, L7Request, UpstreamId};
use velda_grpc::tcp::GrpcPipeStrategy;
use velda_grpc::tcp::server::{GrpcServerConnection, GrpcServerStream};
use velda_grpc::udp::GrpcUdpPipeStrategy;
use velda_grpc::{GrpcConfig, GrpcStatus};
use velda_router::GrpcRouteRequest;
use velda_sock::Connection;
use velda_tls::TlsServerEngine;

use super::pipe;
use crate::pipeline::DownstreamMeta;
use crate::runtime::SharedRuntime;
use crate::upstream::GrpcUpstream;

/// Asynchronous stream worker dispatching incoming gRPC connections over TCP.
///
/// Inspects the pre-compiled listener metadata to determine whether downstream
/// TLS termination is active. If TLS is required, negotiates the handshake, validates
/// ALPN against "h2", and passes the encrypted stream to the gRPC TCP loop.
pub async fn handle_grpc_tcp(
    connection: Connection,
    listener_id: Arc<str>,
    config: GrpcConfig,
    tls_enabled: bool,
    runtime: SharedRuntime,
) {
    let _conn_guard = crate::pipeline::ConnMetricGuard::enter();
    let rt = runtime.load();
    let peer = connection.peer();
    let local_addr = connection.local_addr();

    if tls_enabled {
        let Some(tls_server) = rt.tls_server.as_ref() else {
            velda_observability::global_metrics()
                .global()
                .tls_handshake_error();
            tracing::error!(
                listener = %listener_id,
                peer = %peer,
                "TLS required for listener, but no TLS server engine is compiled; dropping connection"
            );
            return;
        };

        match tls_server.accept_with_timeout(connection).await {
            Ok(tls_stream) => {
                // gRPC protocol specification: gRPC over TCP/TLS is strictly framed atop HTTP/2 binary framing.
                // Clients MUST successfully negotiate ALPN "h2". Missing (None) or non-h2 ALPN is dropped
                // to prevent raw binary stream misinterpretation in downstream RPC dispatching.
                let alpn = TlsServerEngine::extract_alpn(&tls_stream);

                if alpn.as_deref() != Some("h2") {
                    velda_observability::global_metrics()
                        .global()
                        .tls_handshake_error();
                    tracing::warn!(
                        listener = %listener_id,
                        peer = %peer,
                        expected = "h2",
                        actual = ?alpn,
                        "Dropping connection: gRPC over TLS strictly requires ALPN 'h2'"
                    );
                    return;
                }

                velda_observability::global_metrics()
                    .global()
                    .tls_handshake_success();

                tracing::debug!(
                    listener = %listener_id,
                    peer = %peer,
                    alpn = ?alpn,
                    "Downstream TLS handshake succeeded for gRPC"
                );

                let meta =
                    DownstreamMeta::new(listener_id, peer, local_addr, true, None, &rt.buffer_pool);
                run_grpc_tcp_loop(tls_stream, meta, config, runtime).await;
            }
            Err(e) => {
                velda_observability::global_metrics()
                    .global()
                    .tls_handshake_error();
                tracing::warn!(
                    error = %e,
                    listener = %listener_id,
                    peer = %peer,
                    "Downstream TLS handshake failed"
                );
            }
        }
    } else {
        let meta = DownstreamMeta::new(listener_id, peer, local_addr, false, None, &rt.buffer_pool);
        run_grpc_tcp_loop(connection, meta, config, runtime).await;
    }
}

/// Core gRPC downstream stream worker loop over TCP decoupled from transport layer.
pub async fn run_grpc_tcp_loop<IO>(
    stream: IO,
    meta: DownstreamMeta,
    config: GrpcConfig,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = match GrpcServerConnection::handshake(stream, &config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                listener = %meta.listener_id,
                "Failed downstream gRPC server handshake"
            );
            return;
        }
    };

    let timeout_duration = std::time::Duration::from_millis(config.idle_timeout_ms);

    while let Ok(accept_result) = tokio::time::timeout(timeout_duration, conn.accept()).await {
        match accept_result {
            Ok(Some(server_stream)) => {
                let meta = meta.clone();
                let rt = runtime.clone();
                let cfg = config;

                tokio::spawn(async move {
                    dispatch_grpc_tcp_request_stream(server_stream, meta, &cfg, &rt).await;
                });
            }
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(error = %e, "gRPC stream accept error");
                break;
            }
        }
    }
}

/// Dispatches a single downstream gRPC request stream through the router to upstream backend.
async fn dispatch_grpc_tcp_request_stream(
    mut server_stream: GrpcServerStream,
    meta: DownstreamMeta,
    config: &GrpcConfig,
    runtime: &SharedRuntime,
) {
    let req_start = std::time::Instant::now();
    let authority = server_stream.parts.uri.authority().map(|a| a.as_str());
    let path = server_stream.parts.uri.path();
    let (log_url_len, log_url_bytes) =
        velda_observability::AccessLogRecord::encode_url(meta.is_tls, authority, path);
    let (ua_family, ua_os, ua_major_version) = server_stream
        .parts
        .headers
        .get(http::header::USER_AGENT)
        .map(|v| velda_observability::classify_user_agent(v.as_bytes()))
        .unwrap_or((
            velda_observability::UaFamily::None,
            velda_observability::UaOs::Unknown,
            0,
        ));

    // [PHYSICAL WIRE CEILING: MAX_WIRE_HEADERS = 1024]
    if server_stream.parts.headers.len() > velda_core::MAX_WIRE_HEADERS {
        let (request_id, _) = velda_core::next_request_id();
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::ResourceExhausted,
            Some("too many request headers"),
        );
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H2,
            velda_observability::UpstreamProtocol::H2,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: request_id.entropy(),
            timestamp_sec: request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: meta.peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: 0,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::ResourceExhausted as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return;
    }

    // [INGRESS W3C REQUEST ID GENERATION]
    // Authoritatively generate W3C Request ID from thread-local pool (~1-2ns).
    // Overwrite any client-supplied x-request-id to enforce zero-trust Edge boundary.
    let (request_id, req_id_header) = velda_core::next_request_id();
    server_stream
        .parts
        .headers
        .insert(crate::pipeline::X_REQUEST_ID, req_id_header.clone());
    server_stream.respond = server_stream.respond.with_request_id(Some(req_id_header));

    server_stream.enrich_forwarded_headers(meta.peer, meta.local_addr, meta.is_tls);
    let authority_str = server_stream.authority();
    let path = server_stream.parts.uri.path();

    let Some(grpc_req) = GrpcRouteRequest::from_path(path, authority_str) else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for empty path"),
        );
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H2,
            velda_observability::UpstreamProtocol::H2,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: request_id.entropy(),
            timestamp_sec: request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: meta.peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: 0,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::Unimplemented as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_grpc(&*meta.listener_id, &grpc_req) else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for service"),
        );
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H2,
            velda_observability::UpstreamProtocol::H2,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: request_id.entropy(),
            timestamp_sec: request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: meta.peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: 0,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::Unimplemented as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return;
    };

    let mut target_upstream: UpstreamId = route.upstream_id;

    // ========================================================================
    // [Route Plugin Pipeline Evaluation: pre_route & pre_upstream hooks]
    // ========================================================================
    let plan = rt.plugins.get_plan(route.id);
    if !plan.is_empty() {
        let mut short_circuit_resp = None;

        if plan.pre_route_hooks.is_some() || plan.pre_upstream_hooks.is_some() {
            // Optimization: Construct L7Request ONCE across both pre_route and pre_upstream phases
            // to eliminate redundant URI/Method cloning and HeaderMap swapping.
            let mut l7_req = L7Request::new(
                server_stream.parts.method.clone(),
                server_stream.parts.uri.clone(),
                http::Version::HTTP_2,
                std::mem::take(&mut server_stream.parts.headers),
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
                        // preserving only Bit-1 bypass headers and grpc-status trailers.
                        let grpc_status = GrpcStatus::from_reject_reason(reason);
                        let log_grpc_status =
                            velda_observability::LogGrpcStatus::from_u8(grpc_status as u8);
                        let mut resp = grpc_status.to_l7_response(Some(reason.canonical_phrase()));
                        for (name, val) in l7_req.headers.bypass_headers() {
                            resp.headers.insert(name.clone(), val.clone());
                        }
                        resp.headers
                            .set_request_id(l7_req.headers.request_id().clone());
                        let mut builder = http::Response::builder()
                            .status(http::StatusCode::OK)
                            .version(http::Version::HTTP_2);
                        for (name, val) in &resp.headers {
                            builder = builder.header(name, val);
                        }
                        if let Ok(r) = builder.body(()) {
                            let _ = server_stream.respond.send_response(r, true);
                        }
                        velda_observability::global_metrics().record_early_completion(
                            velda_observability::DownstreamProtocol::H2,
                            velda_observability::UpstreamProtocol::H2,
                            velda_observability::PipeStrategy::Buffered,
                            velda_observability::StreamCompletion::new(
                                200,
                                0,
                                0,
                                req_start.elapsed(),
                                None,
                            ),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128)
                                    as u64,
                                peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: 0,
                                upstream_ip: 0,
                                peer_port: meta.peer.port(),
                                upstream_port: 0,
                                status_code: 200,
                                route_id: route.id.0 as u16,
                                ua_major_version,
                                rpc_msgs_or_flags: 1,
                                pipe: velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
                                completion: velda_observability::LogCompletionReason::Rejected
                                    as u8,
                                method: velda_observability::LogHttpMethod::Post as u8,
                                grpc_status: log_grpc_status as u8,
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
                        // preserving only Bit-1 bypass headers and grpc-status trailers.
                        let grpc_status = GrpcStatus::from_reject_reason(reason);
                        let log_grpc_status =
                            velda_observability::LogGrpcStatus::from_u8(grpc_status as u8);
                        let mut resp = grpc_status.to_l7_response(Some(reason.canonical_phrase()));
                        for (name, val) in l7_req.headers.bypass_headers() {
                            resp.headers.insert(name.clone(), val.clone());
                        }
                        resp.headers
                            .set_request_id(l7_req.headers.request_id().clone());
                        let mut builder = http::Response::builder()
                            .status(http::StatusCode::OK)
                            .version(http::Version::HTTP_2);
                        for (name, val) in &resp.headers {
                            builder = builder.header(name, val);
                        }
                        if let Ok(r) = builder.body(()) {
                            let _ = server_stream.respond.send_response(r, true);
                        }
                        velda_observability::global_metrics().record_early_completion(
                            velda_observability::DownstreamProtocol::H2,
                            velda_observability::UpstreamProtocol::H2,
                            velda_observability::PipeStrategy::Buffered,
                            velda_observability::StreamCompletion::new(
                                200,
                                0,
                                0,
                                req_start.elapsed(),
                                None,
                            ),
                        );
                        velda_observability::global_log_store().emit(
                            &velda_observability::AccessLogRecord {
                                request_id: request_id.entropy(),
                                timestamp_sec: request_id.timestamp_secs() as u32,
                                duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128)
                                    as u64,
                                peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
                                bytes_rx: 0,
                                bytes_tx: 0,
                                upstream_ip: 0,
                                peer_port: meta.peer.port(),
                                upstream_port: 0,
                                status_code: 200,
                                route_id: route.id.0 as u16,
                                ua_major_version,
                                rpc_msgs_or_flags: 1,
                                pipe: velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
                                completion: velda_observability::LogCompletionReason::Rejected
                                    as u8,
                                method: velda_observability::LogHttpMethod::Post as u8,
                                grpc_status: log_grpc_status as u8,
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
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(post_hooks, &l7_req.headers, &mut resp);
                }
                let mut builder = http::Response::builder()
                    .status(http::StatusCode::OK)
                    .version(http::Version::HTTP_2);
                for (name, val) in &resp.headers {
                    builder = builder.header(name, val);
                }
                if let Ok(r) = builder.body(()) {
                    let _ = server_stream.respond.send_response(r, true);
                }
                return;
            }

            server_stream.parts.headers = l7_req.headers.into();
            server_stream.parts.uri = l7_req.uri;
            server_stream.parts.method = l7_req.method;
        }
    }

    let Some(upstream_target) = rt.upstreams.grpc.get(target_upstream) else {
        tracing::error!(
            listener = %meta.listener_id,
            route = %route.id,
            upstream = %target_upstream,
            "No healthy backend endpoints available for gRPC upstream"
        );
        let _ = server_stream
            .respond
            .send_trailers_only(GrpcStatus::Unavailable, Some("upstream not configured"));
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H2,
            velda_observability::UpstreamProtocol::H2,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: request_id.entropy(),
            timestamp_sec: request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: meta.peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: route.id.0 as u16,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
            completion: velda_observability::LogCompletionReason::UpstreamError as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::Unavailable as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return;
    };

    // Resource Saturation Circuit Breaker (Overload Protection)
    let overload_lvl = rt.overload.level();
    if overload_lvl.is_shedding() {
        tracing::warn!(
            route = %route.id,
            upstream = %target_upstream,
            overload = ?overload_lvl,
            "Shedding gRPC TCP request due to memory saturation"
        );
        let _ = server_stream
            .respond
            .send_trailers_only(GrpcStatus::ResourceExhausted, Some("resource exhausted"));
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H2,
            velda_observability::UpstreamProtocol::H2,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: request_id.entropy(),
            timestamp_sec: request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: meta.peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: route.id.0 as u16,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::ResourceExhausted as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return;
    }

    let pipe_strat = match upstream_target.as_ref() {
        GrpcUpstream::Tcp(u) => match u.strategy {
            GrpcPipeStrategy::Buffered => velda_observability::PipeStrategy::Buffered,
            GrpcPipeStrategy::ServerStream => velda_observability::PipeStrategy::ServerStream,
            GrpcPipeStrategy::ClientStream => velda_observability::PipeStrategy::ClientStream,
            GrpcPipeStrategy::Duplex => velda_observability::PipeStrategy::Duplex,
        },
        GrpcUpstream::Udp(u) => match u.strategy {
            GrpcUdpPipeStrategy::Buffered => velda_observability::PipeStrategy::Buffered,
            GrpcUdpPipeStrategy::ServerStream => velda_observability::PipeStrategy::ServerStream,
            GrpcUdpPipeStrategy::ClientStream => velda_observability::PipeStrategy::ClientStream,
            GrpcUdpPipeStrategy::Duplex => velda_observability::PipeStrategy::Duplex,
        },
        GrpcUpstream::Rdma(u) => match u.strategy {
            GrpcPipeStrategy::Buffered => velda_observability::PipeStrategy::Buffered,
            GrpcPipeStrategy::ServerStream => velda_observability::PipeStrategy::ServerStream,
            GrpcPipeStrategy::ClientStream => velda_observability::PipeStrategy::ClientStream,
            GrpcPipeStrategy::Duplex => velda_observability::PipeStrategy::Duplex,
        },
    };

    velda_observability::global_metrics().record_stream_start(
        velda_observability::DownstreamProtocol::H2,
        velda_observability::UpstreamProtocol::H2,
        pipe_strat,
    );

    let (outcome, pipe_kind) = match upstream_target.as_ref() {
        GrpcUpstream::Tcp(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => (
                pipe::buffered_tcp_tcp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::BufferedGrpcTcpTcp,
            ),
            GrpcPipeStrategy::ServerStream => (
                pipe::server_stream_tcp_tcp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::ServerStreamGrpcTcpTcp,
            ),
            GrpcPipeStrategy::ClientStream => (
                pipe::client_stream_tcp_tcp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::ClientStreamGrpcTcpTcp,
            ),
            GrpcPipeStrategy::Duplex => (
                pipe::duplex_tcp_tcp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::DuplexGrpcTcpTcp,
            ),
        },
        GrpcUpstream::Udp(upstream) => match upstream.strategy {
            GrpcUdpPipeStrategy::Buffered => (
                pipe::buffered_tcp_udp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::BufferedGrpcTcpUdp,
            ),
            GrpcUdpPipeStrategy::ServerStream => (
                pipe::server_stream_tcp_udp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::ServerStreamGrpcTcpUdp,
            ),
            GrpcUdpPipeStrategy::ClientStream => (
                pipe::client_stream_tcp_udp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::ClientStreamGrpcTcpUdp,
            ),
            GrpcUdpPipeStrategy::Duplex => (
                pipe::duplex_tcp_udp::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::DuplexGrpcTcpUdp,
            ),
        },
        GrpcUpstream::Rdma(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => (
                pipe::buffered_tcp_rdma::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::BufferedGrpcTcpRdma,
            ),
            GrpcPipeStrategy::ServerStream => (
                pipe::server_stream_tcp_rdma::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::ServerStreamGrpcTcpRdma,
            ),
            GrpcPipeStrategy::ClientStream => (
                pipe::client_stream_tcp_rdma::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::ClientStreamGrpcTcpRdma,
            ),
            GrpcPipeStrategy::Duplex => (
                pipe::duplex_tcp_rdma::serve(server_stream, upstream, config).await,
                velda_core::pipe::PipeKind::DuplexGrpcTcpRdma,
            ),
        },
    };

    let duration = req_start.elapsed();
    let (upstream_ip, upstream_port) = match outcome.upstream_addr {
        Some(addr) => (
            velda_observability::encode_upstream_ip_u32(addr.ip()),
            addr.port(),
        ),
        None => (0, 0),
    };

    velda_observability::global_metrics().record_stream_completion(
        velda_observability::DownstreamProtocol::H2,
        velda_observability::UpstreamProtocol::H2,
        pipe_strat,
        velda_observability::StreamCompletion::new(
            outcome.status_code,
            outcome.bytes_rx,
            outcome.bytes_tx,
            duration,
            None,
        ),
    );

    velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
        request_id: request_id.entropy(),
        timestamp_sec: request_id.timestamp_secs() as u32,
        duration_ns: duration.as_nanos().min(u64::MAX as u128) as u64,
        peer_ip: velda_observability::encode_ip_u128(meta.peer.ip()),
        bytes_rx: outcome.bytes_rx.min(u32::MAX as u64) as u32,
        bytes_tx: outcome.bytes_tx.min(u32::MAX as u64) as u32,
        upstream_ip,
        peer_port: meta.peer.port(),
        upstream_port,
        status_code: outcome.status_code,
        route_id: route.id.0 as u16,
        ua_major_version,
        rpc_msgs_or_flags: 1,
        pipe: pipe_kind,
        completion: outcome.completion as u8,
        method: velda_observability::LogHttpMethod::Post as u8,
        grpc_status: outcome.grpc_status as u8,
        ua_family: ua_family as u8,
        ua_os: ua_os as u8,
        url_len: log_url_len,
        url_bytes: log_url_bytes,
    });
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use std::net::SocketAddr;
    use velda_grpc::GrpcStatus;
    use velda_grpc::tcp::server::enrich_headers;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let peer: SocketAddr = "192.0.2.40:50051".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        enrich_headers(&mut headers, peer, local, true, Some("grpc.example.com"));

        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.40");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.40");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(headers.get("x-forwarded-host").unwrap(), "grpc.example.com");
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.40;proto=https;by=10.0.0.1;host=\"grpc.example.com\""
        );
        assert!(headers.get("connection").is_none());
        assert!(headers.get("keep-alive").is_none());
    }

    #[test]
    fn test_stale_or_refused_detection() {
        assert!(velda_grpc::GrpcError::Protocol("connection closed".into()).is_stale_or_refused());
        assert!(
            velda_grpc::GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_stale_or_refused()
        );
        assert!(
            velda_grpc::GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset"
            ))
            .is_stale_or_refused()
        );

        assert!(
            !velda_grpc::GrpcError::Status(GrpcStatus::NotFound, "not found".into())
                .is_stale_or_refused()
        );
        assert!(!velda_grpc::GrpcError::PayloadTooLarge(100).is_stale_or_refused());
    }
}
