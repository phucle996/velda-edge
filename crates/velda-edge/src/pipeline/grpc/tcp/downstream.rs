//! Layer 7 gRPC over TCP downstream stream worker, route evaluation, and streaming forwarding.
//!
//! Powered natively by `velda-grpc` and `velda-http2` over [`velda_core::HeaderWorkspace`].
//! Operates strictly for listeners explicitly configured with `transport = "tcp"` and
//! `protocol = "grpc"`. Zero HTTP fallback, zero header sniffing, and full bidirectional streaming.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{Action, L7Request, PipeKind};
use velda_grpc::GrpcStatus;
use velda_grpc::config::GrpcConfig;
use velda_grpc::tcp::server::connection::GrpcServerConnection;
use velda_grpc::tcp::server::stream::GrpcServerStream;
use velda_grpc::tcp::strategy::GrpcPipeStrategy;
use velda_grpc::udp::strategy::GrpcUdpPipeStrategy;
use velda_observability::sampling::status::StatusSampling;
use velda_observability::{StreamCompletion, global_metrics, global_telemetry_store};
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
    sampling: StatusSampling,
    runtime: SharedRuntime,
) {
    let _conn_guard = crate::pipeline::ConnMetricGuard::enter();
    let rt = runtime.load();
    let peer = velda_sock::raw_to_socket_addr(connection.peer_ip, connection.peer_port_be);
    let local_addr = velda_sock::raw_to_socket_addr(connection.local_ip, connection.local_port_be);

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

        match tls_server.accept(connection).await {
            Ok(velda_tls::AcceptedTlsStream {
                stream: tls_stream,
                ja4,
                tls_traits,
            }) => {
                // gRPC protocol specification: gRPC over TCP/TLS is strictly framed atop HTTP/2 binary framing.
                // Clients MUST successfully negotiate ALPN "h2".
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
                    DownstreamMeta::new(listener_id, peer, local_addr, true, None, &rt.buffer_pool)
                        .with_tls_signals(ja4, tls_traits);
                run_grpc_tcp_loop(tls_stream, meta, config, sampling, runtime).await;
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
        run_grpc_tcp_loop(connection, meta, config, sampling, runtime).await;
    }
}

/// Core gRPC downstream stream worker loop over TCP decoupled from transport layer.
pub async fn run_grpc_tcp_loop<IO>(
    stream: IO,
    meta: DownstreamMeta,
    config: GrpcConfig,
    sampling: StatusSampling,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let forwarding = velda_http2::proto::hpack::H2ForwardingAddrs {
        peer_ip: meta.peer_ip,
        local_ip: meta.local_ip,
        local_port_be: meta.local_port_be,
        is_tls: meta.is_tls,
    };

    let mut conn =
        match GrpcServerConnection::handshake_with_forwarding(stream, &config, forwarding).await {
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
                    dispatch_grpc_tcp_request_stream(server_stream, meta, &cfg, sampling, &rt)
                        .await;
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
    sampling: StatusSampling,
    runtime: &SharedRuntime,
) {
    let ingress_nanos = if server_stream.head.ingress_time_nanos != 0 {
        server_stream.head.ingress_time_nanos
    } else {
        velda_core::clock::now_unix_nanos()
    };
    let request_id = if server_stream.head.request_id != 0 {
        server_stream.head.request_id
    } else {
        let (id, hdr) = velda_core::request_id::next_request_id(ingress_nanos);
        server_stream.head.headers.set_request_id(hdr.clone());
        server_stream.respond = server_stream.respond.with_request_id(Some(hdr));
        id
    };

    let authority = server_stream.head.uri.authority().map(|a| a.as_str());
    let path = server_stream.head.uri.path();
    let (log_url_len, log_url_bytes) =
        velda_observability::TelemetryRecord::encode_url(meta.is_tls, authority, path);
    let (ua_family, ua_os, ua_major_version) = server_stream
        .head
        .headers
        .get(http::header::USER_AGENT)
        .map(|v| velda_core::lut::classify_user_agent(v.as_bytes()))
        .unwrap_or((velda_core::UaFamily::None, velda_core::UaOs::Unknown, 0));

    // [PHYSICAL WIRE CEILING: MAX_WIRE_HEADERS = 256]
    if server_stream.head.headers.len() > velda_core::buffer::header::MAX_WIRE_HEADERS {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::ResourceExhausted,
            Some("too many request headers"),
        );
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcTcpTcp,
            StreamCompletion::new(
                velda_core::InternalStatus::HeaderFieldsTooLarge,
                0,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep(velda_core::InternalStatus::HeaderFieldsTooLarge) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: meta.peer_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                _cl0_pad: 0,
                route_id: velda_core::UNMATCHED_ROUTE_ID,
                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcTcpTcp,
                internal_status: velda_core::InternalStatus::HeaderFieldsTooLarge,
                method: velda_core::HttpMethod::Post,
                _reserved: 0,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return;
    }

    let authority_str = velda_grpc::tcp::server::header::extract_authority(
        &server_stream.head.headers,
        &server_stream.head.uri,
    );
    let path = server_stream.head.uri.path();

    let Some(grpc_req) = GrpcRouteRequest::from_path(path, authority_str) else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for empty path"),
        );
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcTcpTcp,
            StreamCompletion::new(
                velda_core::InternalStatus::RouteNotFound,
                0,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep(velda_core::InternalStatus::RouteNotFound) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: meta.peer_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                _cl0_pad: 0,
                route_id: velda_core::UNMATCHED_ROUTE_ID,
                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcTcpTcp,
                internal_status: velda_core::InternalStatus::RouteNotFound,
                method: velda_core::HttpMethod::Post,
                _reserved: 0,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt
        .router
        .route_grpc(velda_core::fnv1a_hash_32(&meta.listener_id), &grpc_req)
    else {
        let _ = server_stream.respond.send_trailers_only(
            GrpcStatus::Unimplemented,
            Some("no route matched for service"),
        );
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcTcpTcp,
            StreamCompletion::new(
                velda_core::InternalStatus::RouteNotFound,
                0,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep(velda_core::InternalStatus::RouteNotFound) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: meta.peer_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                _cl0_pad: 0,
                route_id: velda_core::UNMATCHED_ROUTE_ID,
                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcTcpTcp,
                internal_status: velda_core::InternalStatus::RouteNotFound,
                method: velda_core::HttpMethod::Post,
                _reserved: 0,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return;
    };

    let route_idx = route.id as usize;
    let default_upstream: u16 = rt.route_to_upstream.get(route_idx).copied().unwrap_or(0);
    let mut target_mask: u32 = velda_plugin::target_init(default_upstream);
    let mut target_upstream: u16 = default_upstream;
    let mut req_state = velda_core::RequestState::new(request_id)
        .with_ingress_time(ingress_nanos)
        .with_method(velda_core::lut::HttpMethod::from_http_method(
            &server_stream.head.method,
        ));
    req_state.client_ip = meta.peer_ip;
    req_state.is_tls = meta.is_tls;
    req_state.ja4 = meta.ja4;
    req_state.tls_traits = meta.tls_traits;
    req_state.set_wire_traits(server_stream.head.wire_traits);
    req_state.set_route(route.id);
    req_state.set_upstream(target_upstream);

    // ========================================================================
    // [Route Plugin Pipeline Evaluation: pre_route & pre_upstream hooks]
    // ========================================================================
    let plan = rt.plugins.get_plan(route.id);
    if !plan.is_empty() {
        let mut short_circuit_resp = None;

        if plan.pre_route_hooks.is_some() || plan.pre_upstream_hooks.is_some() {
            let mut l7_req = L7Request::new(
                server_stream.head.method.clone(),
                server_stream.head.uri.clone(),
                http::Version::HTTP_2,
                std::mem::take(&mut server_stream.head.headers),
                velda_core::Body::Empty,
            );
            l7_req.ingress_time_nanos = ingress_nanos;
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
                    Action::Status(internal_status) => {
                        let _ = velda_grpc::GrpcWire::prepare_status_headers(
                            &mut l7_req.headers,
                            internal_status,
                        );
                        let _ = server_stream.respond.send_response_headers(
                            http::StatusCode::OK,
                            &l7_req.headers,
                            true,
                        );
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedGrpcTcpTcp,
                            StreamCompletion::new(internal_status, 0, 0, duration_ns, 0),
                        );
                        if sampling.keep(internal_status) {
                            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                                request_id: velda_core::request_id::entropy(request_id),
                                ingress_time_nanos: ingress_nanos,
                                duration_ns,
                                peer_ip: l7_req.state.client_ip,
                                bytes_rx: 0,
                                bytes_tx: 0,
                                upstream_ip: 0,
                                upstream_port: 0,
                                _cl0_pad: 0,
                                route_id: route.id,
                                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                ua_major_version,
                                rpc_msgs_or_flags: 1,
                                pipe: PipeKind::BufferedGrpcTcpTcp,
                                internal_status,
                                method: velda_core::HttpMethod::Post,
                                _reserved: 0,
                                ua_family,
                                ua_os,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            });
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
                    Action::Status(internal_status) => {
                        let _ = velda_grpc::GrpcWire::prepare_status_headers(
                            &mut l7_req.headers,
                            internal_status,
                        );
                        let _ = server_stream.respond.send_response_headers(
                            http::StatusCode::OK,
                            &l7_req.headers,
                            true,
                        );
                        let duration_ns =
                            velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                        global_metrics().record_early_completion(
                            PipeKind::BufferedGrpcTcpTcp,
                            StreamCompletion::new(internal_status, 0, 0, duration_ns, 0),
                        );
                        if sampling.keep(internal_status) {
                            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                                request_id: velda_core::request_id::entropy(request_id),
                                ingress_time_nanos: ingress_nanos,
                                duration_ns,
                                peer_ip: l7_req.state.client_ip,
                                bytes_rx: 0,
                                bytes_tx: 0,
                                upstream_ip: 0,
                                upstream_port: 0,
                                _cl0_pad: 0,
                                route_id: route.id,
                                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                ua_major_version,
                                rpc_msgs_or_flags: 1,
                                pipe: PipeKind::BufferedGrpcTcpTcp,
                                internal_status,
                                method: velda_core::HttpMethod::Post,
                                _reserved: 0,
                                ua_family,
                                ua_os,
                                url_len: log_url_len,
                                url_bytes: log_url_bytes,
                            });
                        }
                        return;
                    }
                    Action::Delay { .. } => unreachable!(),
                }
            }

            if let Some(mut resp) = short_circuit_resp {
                resp.headers
                    .set_request_id(l7_req.headers.request_id().clone());
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &l7_req.state,
                        &l7_req.headers,
                        &mut resp,
                    );
                }
                let _ = server_stream.respond.inner.send_parts(
                    http::StatusCode::OK,
                    &resp.headers,
                    &resp.body,
                );
                return;
            }

            req_state = l7_req.state;
            let _ = &req_state;
            server_stream.head.headers = l7_req.headers;
            server_stream.head.uri = l7_req.uri;
            server_stream.head.method = l7_req.method;
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
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcTcpTcp,
            StreamCompletion::new(
                velda_core::InternalStatus::ServiceUnavailable,
                0,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep(velda_core::InternalStatus::ServiceUnavailable) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: meta.peer_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                _cl0_pad: 0,
                route_id: route.id,
                upstream_id: target_upstream,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcTcpTcp,
                internal_status: velda_core::InternalStatus::ServiceUnavailable,
                method: velda_core::HttpMethod::Post,
                _reserved: 0,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
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
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcTcpTcp,
            StreamCompletion::new(
                velda_core::InternalStatus::RateLimited,
                0,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep_shed() {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: meta.peer_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                _cl0_pad: 0,
                route_id: route.id,
                upstream_id: target_upstream,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcTcpTcp,
                internal_status: velda_core::InternalStatus::RateLimited,
                method: velda_core::HttpMethod::Post,
                _reserved: 0,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return;
    }

    if let Some(mirror_up) = velda_plugin::target_mirror(target_mask)
        .filter(|&m| m != target_upstream)
        .and_then(|m| rt.upstreams.grpc.get(m))
    {
        crate::pipeline::mirror::attach_grpc_tcp_mirror(
            &mut server_stream.receiver,
            &server_stream.head,
            mirror_up,
        );
    }

    let (outcome, pipe_kind) = match upstream_target.as_ref() {
        GrpcUpstream::Tcp(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedGrpcTcpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_tcp_tcp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamGrpcTcpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_tcp_tcp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamGrpcTcpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_tcp_tcp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexGrpcTcpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_tcp_tcp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
        },
        GrpcUpstream::Udp(upstream) => match upstream.strategy {
            GrpcUdpPipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedGrpcTcpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_tcp_udp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcUdpPipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamGrpcTcpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_tcp_udp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcUdpPipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamGrpcTcpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_tcp_udp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcUdpPipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexGrpcTcpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_tcp_udp::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
        },
        GrpcUpstream::Rdma(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedGrpcTcpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_tcp_rdma::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamGrpcTcpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_tcp_rdma::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamGrpcTcpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_tcp_rdma::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexGrpcTcpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_tcp_rdma::serve(server_stream, upstream, config).await,
                    PIPE,
                )
            }
        },
    };

    let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);

    global_metrics().record_stream_completion(
        pipe_kind,
        StreamCompletion::new(
            outcome.internal_status,
            outcome.bytes_rx,
            outcome.bytes_tx,
            duration_ns,
            0,
        ),
    );

    if sampling.keep(outcome.internal_status) {
        let (upstream_ip, upstream_port) = match outcome.upstream_addr {
            Some(addr) => (
                if velda_core::lut::ip::is_v4_u128(addr.0) {
                    (addr.0 & 0xFFFF_FFFF) as u32
                } else {
                    0
                },
                u16::from_be(addr.1),
            ),
            None => (0, 0),
        };
        global_telemetry_store().emit(&velda_observability::TelemetryRecord {
            request_id: velda_core::request_id::entropy(request_id),
            ingress_time_nanos: ingress_nanos,
            duration_ns,
            peer_ip: meta.peer_ip,
            bytes_rx: outcome.bytes_rx.min(u32::MAX as u64) as u32,
            bytes_tx: outcome.bytes_tx.min(u32::MAX as u64) as u32,
            upstream_ip,
            upstream_port,
            _cl0_pad: 0,
            route_id: route.id,
            upstream_id: target_upstream,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: pipe_kind,
            internal_status: outcome.internal_status,
            method: velda_core::HttpMethod::Post,
            _reserved: 0,
            ua_family,
            ua_os,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
    }
}

#[cfg(test)]
mod tests {
    use velda_core::HeaderWorkspace;
    use velda_grpc::tcp::server::header::enrich_headers;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderWorkspace::empty(8);
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        headers.insert("connection", "close".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let peer_ip = velda_core::lut::ip::v4_to_u128(0xC000_0228);
        let local_ip = velda_core::lut::ip::v4_to_u128(0x0A00_0001);
        let local_port_be = 443u16.to_be();

        enrich_headers(
            &mut headers,
            peer_ip,
            local_ip,
            local_port_be,
            true,
            Some("grpc.example.com"),
        );

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
        assert!(
            velda_grpc::outcome::GrpcOutcome::Protocol("connection closed".into())
                .is_stale_or_refused()
        );
        assert!(
            velda_grpc::outcome::GrpcOutcome::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset"
            ))
            .is_stale_or_refused()
        );
        assert!(
            !velda_grpc::outcome::GrpcOutcome::Transport {
                refused_or_goaway: false,
                detail: "bad frame".into(),
            }
            .is_stale_or_refused()
        );
    }
}
