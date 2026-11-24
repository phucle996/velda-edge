//! Layer 7 gRPC over UDP downstream datagram dispatch and request handling.
//!
//! Powered entirely by `velda-grpc::udp`. Operates strictly for listeners explicitly configured
//! with `transport = "udp"` and `protocol = "grpc"`. Receives QUIC datagrams, decodes
//! RPC requests, and forwards to upstream backends with zero HTTP borrowing.

use std::net::SocketAddr;
use std::sync::Arc;

use velda_core::{Action, GrpcStatus, L7Request, L7Response, PipeKind};
use velda_grpc::config::GrpcConfig;
use velda_grpc::tcp::strategy::GrpcPipeStrategy;
use velda_grpc::udp::strategy::GrpcUdpPipeStrategy;
use velda_observability::sampling::grpc::GrpcSampling;
use velda_observability::{StreamCompletion, global_metrics, global_telemetry_store};
use velda_router::GrpcRouteRequest;
use velda_sock::{Datagram, UdpSocket};

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
    sampling: GrpcSampling,
    runtime: &SharedRuntime,
) {
    let peer = velda_sock::raw_to_socket_addr(datagram.peer_ip, datagram.peer_port_be);

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
            datagram.peer_ip,
            datagram.peer_port_be,
            Some((datagram.local_ip, datagram.local_port_be)),
            datagram.data(),
        )
    };

    // 1. Send all outgoing handshake / ACK datagrams immediately outside the engine lock
    for pkt in outgoing {
        let _ = socket
            .send_to(&pkt.payload, pkt.peer_ip, pkt.peer_port_be)
            .await;
    }

    let peer_ip = datagram.peer_ip;

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
                path = %req_event.request.uri.path(),
                peer = %peer,
                "Decoded gRPC request from UDP"
            );

            // [INGRESS W3C REQUEST ID & FORWARDING HEADERS]
            // Stamped in 1 pass during QPACK decode into HeaderWorkspace.
            let mut req = req_event.request;
            req.state.client_ip = peer_ip;
            let req_id_header = req.headers.request_id().clone();

            let mut response =
                process_grpc_udp_request(req, peer, &lid, &cfg, sampling, &runtime).await;
            response.headers.set_request_id(req_id_header);

            let resp_now = std::time::Instant::now();
            let resp_pkts = {
                let mut engine = engine_lock.lock().await;
                engine.send_response(resp_now, req_event.handle, req_event.stream_id, &response)
            };

            if let Ok(pkts) = resp_pkts {
                for pkt in pkts {
                    let _ = socket
                        .send_to(&pkt.payload, pkt.peer_ip, pkt.peer_port_be)
                        .await;
                }
            }
        });
    }
}

/// Dispatches a single unary gRPC request through `GrpcRouter` to the upstream backend.
/// Supports both native gRPC over UDP and cross-transport bridge to gRPC over TCP.
pub async fn process_grpc_udp_request(
    mut req: L7Request,
    _peer: SocketAddr,
    listener_id: &str,
    config: &GrpcConfig,
    sampling: GrpcSampling,
    runtime: &SharedRuntime,
) -> L7Response {
    let ingress_nanos = req.ingress_time_nanos;
    let (log_url_len, log_url_bytes) = {
        let host = velda_http3::server::header::extract_host(&req.headers, &req.uri);
        velda_observability::TelemetryRecord::encode_url(true, host, req.uri.path())
    };
    let (ua_family, ua_os, ua_major_version) = req
        .headers
        .get(http::header::USER_AGENT)
        .map(|v| velda_core::lut::classify_user_agent(v.as_bytes()))
        .unwrap_or((velda_core::UaFamily::None, velda_core::UaOs::Unknown, 0));

    // [PHYSICAL WIRE CEILING: MAX_WIRE_HEADERS = 256]
    if req.headers.len() > velda_core::buffer::header::MAX_WIRE_HEADERS {
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcUdpUdp,
            StreamCompletion::new(200, 0, 0, duration_ns, 0),
        );
        if sampling.keep(velda_core::GrpcStatus::ResourceExhausted) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(req.state.request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: req.state.client_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                status_code: 200,
                route_id: velda_core::UNMATCHED_ROUTE_ID,
                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcUdpUdp,
                completion: velda_core::CompletionReason::Rejected,
                method: velda_core::HttpMethod::Post,
                grpc_status: velda_core::GrpcStatus::ResourceExhausted,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return GrpcStatus::ResourceExhausted.to_l7_response(Some("too many request headers"));
    }

    let authority = req
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| req.uri.authority().map(|a| a.as_str()));

    let Some(grpc_req) = GrpcRouteRequest::from_path(req.uri.path(), authority) else {
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcUdpUdp,
            StreamCompletion::new(200, 0, 0, duration_ns, 0),
        );
        if sampling.keep(velda_core::GrpcStatus::Unimplemented) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(req.state.request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: req.state.client_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                status_code: 200,
                route_id: velda_core::UNMATCHED_ROUTE_ID,
                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcUdpUdp,
                completion: velda_core::CompletionReason::Rejected,
                method: velda_core::HttpMethod::Post,
                grpc_status: velda_core::GrpcStatus::Unimplemented,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for empty path"));
    };

    let rt = runtime.load();
    let Some(route) = rt
        .router
        .route_grpc(velda_core::fnv1a_hash_32(listener_id), &grpc_req)
    else {
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcUdpUdp,
            StreamCompletion::new(200, 0, 0, duration_ns, 0),
        );
        if sampling.keep(velda_core::GrpcStatus::Unimplemented) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(req.state.request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: req.state.client_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                status_code: 200,
                route_id: velda_core::UNMATCHED_ROUTE_ID,
                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcUdpUdp,
                completion: velda_core::CompletionReason::Rejected,
                method: velda_core::HttpMethod::Post,
                grpc_status: velda_core::GrpcStatus::Unimplemented,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for service"));
    };

    let route_idx = route.id as usize;
    let mut target_upstream: u16 = rt.route_to_upstream.get(route_idx).copied().unwrap_or(0);

    // ========================================================================
    // [Route Plugin Pipeline Evaluation: pre_route & pre_upstream hooks]
    // ========================================================================
    let plan = rt.plugins.get_plan(route.id);
    if !plan.is_empty() {
        let mut short_circuit_resp = None;

        if let Some(hooks) = &plan.pre_route_hooks {
            let mut action = velda_plugin::run_pre_route_plugins(hooks, &mut req);
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
                    // transforming HeaderWorkspace in-place to preserve only Bit-1 bypass headers.
                    let grpc_status = GrpcStatus::from_reject_reason(reason);
                    let log_grpc_status = grpc_status;
                    req.headers
                        .prepare_for_grpc_rejection(grpc_status, Some(reason.canonical_phrase()));
                    let resp = velda_core::L7Response {
                        status: http::StatusCode::OK,
                        version: http::Version::HTTP_3,
                        headers: req.headers,
                        body: velda_core::Body::Empty,
                    };
                    let duration_ns =
                        velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                    global_metrics().record_early_completion(
                        PipeKind::BufferedGrpcUdpUdp,
                        StreamCompletion::new(200, 0, 0, duration_ns, 0),
                    );
                    if sampling.keep(log_grpc_status) {
                        global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                            request_id: velda_core::request_id::entropy(req.state.request_id),
                            ingress_time_nanos: ingress_nanos,
                            duration_ns,
                            peer_ip: req.state.client_ip,
                            bytes_rx: 0,
                            bytes_tx: 0,
                            upstream_ip: 0,
                            upstream_port: 0,
                            status_code: 200,
                            route_id: route.id,
                            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                            ua_major_version,
                            rpc_msgs_or_flags: 1,
                            pipe: PipeKind::BufferedGrpcUdpUdp,
                            completion: velda_core::CompletionReason::Rejected,
                            method: velda_core::HttpMethod::Post,
                            grpc_status: log_grpc_status,
                            ua_family,
                            ua_os,
                            url_len: log_url_len,
                            url_bytes: log_url_bytes,
                        });
                    }
                    return resp;
                }
                Action::Delay { .. } => unreachable!(),
            }
        }

        if short_circuit_resp.is_none()
            && let Some(hooks) = &plan.pre_upstream_hooks
        {
            let mut action =
                velda_plugin::run_pre_upstream_plugins(hooks, &mut req, &mut target_upstream);
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
                    // transforming HeaderWorkspace in-place to preserve only Bit-1 bypass headers.
                    let grpc_status = GrpcStatus::from_reject_reason(reason);
                    let log_grpc_status = grpc_status;
                    req.headers
                        .prepare_for_grpc_rejection(grpc_status, Some(reason.canonical_phrase()));
                    let resp = velda_core::L7Response {
                        status: http::StatusCode::OK,
                        version: http::Version::HTTP_3,
                        headers: req.headers,
                        body: velda_core::Body::Empty,
                    };
                    let duration_ns =
                        velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                    global_metrics().record_early_completion(
                        PipeKind::BufferedGrpcUdpUdp,
                        StreamCompletion::new(200, 0, 0, duration_ns, 0),
                    );
                    if sampling.keep(log_grpc_status) {
                        global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                            request_id: velda_core::request_id::entropy(req.state.request_id),
                            ingress_time_nanos: ingress_nanos,
                            duration_ns,
                            peer_ip: req.state.client_ip,
                            bytes_rx: 0,
                            bytes_tx: 0,
                            upstream_ip: 0,
                            upstream_port: 0,
                            status_code: 200,
                            route_id: route.id,
                            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                            ua_major_version,
                            rpc_msgs_or_flags: 1,
                            pipe: PipeKind::BufferedGrpcUdpUdp,
                            completion: velda_core::CompletionReason::Rejected,
                            method: velda_core::HttpMethod::Post,
                            grpc_status: log_grpc_status,
                            ua_family,
                            ua_os,
                            url_len: log_url_len,
                            url_bytes: log_url_bytes,
                        });
                    }
                    return resp;
                }
                Action::Delay { .. } => unreachable!(),
            }
        }

        if let Some(mut resp) = short_circuit_resp {
            resp.headers
                .set_request_id(req.headers.request_id().clone());
            if let Some(post_hooks) = &plan.post_response_hooks {
                velda_plugin::run_post_response_plugins(post_hooks, &req.headers, &mut resp);
            }
            let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
            let bytes_rx = req.body.len() as u64;
            let bytes_tx = resp.body.len() as u64;
            let status_code = resp.status.as_u16();
            let grpc_status = resp
                .headers
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u8>().ok())
                .map(velda_core::GrpcStatus::from_u8)
                .unwrap_or(velda_core::GrpcStatus::Ok);
            global_metrics().record_early_completion(
                PipeKind::BufferedGrpcUdpUdp,
                StreamCompletion::new(status_code, bytes_rx, bytes_tx, duration_ns, 0),
            );
            if sampling.keep(grpc_status) {
                global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                    request_id: velda_core::request_id::entropy(req.state.request_id),
                    ingress_time_nanos: ingress_nanos,
                    duration_ns,
                    peer_ip: req.state.client_ip,
                    bytes_rx: bytes_rx.min(u32::MAX as u64) as u32,
                    bytes_tx: bytes_tx.min(u32::MAX as u64) as u32,
                    upstream_ip: 0,
                    upstream_port: 0,
                    status_code,
                    route_id: route.id,
                    upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                    ua_major_version,
                    rpc_msgs_or_flags: 1,
                    pipe: PipeKind::BufferedGrpcUdpUdp,
                    completion: velda_core::CompletionReason::Complete,
                    method: velda_core::HttpMethod::Post,
                    grpc_status,
                    ua_family,
                    ua_os,
                    url_len: log_url_len,
                    url_bytes: log_url_bytes,
                });
            }
            return resp;
        }
    }

    let Some(upstream_target) = rt.upstreams.grpc.get(target_upstream) else {
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcUdpUdp,
            StreamCompletion::new(200, 0, 0, duration_ns, 0),
        );
        if sampling.keep(velda_core::GrpcStatus::Unavailable) {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(req.state.request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: req.state.client_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                status_code: 200,
                route_id: route.id,
                upstream_id: target_upstream,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcUdpUdp,
                completion: velda_core::CompletionReason::Rejected,
                method: velda_core::HttpMethod::Post,
                grpc_status: velda_core::GrpcStatus::Unavailable,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return GrpcStatus::Unavailable.to_l7_response(Some("upstream not configured"));
    };

    // Resource Saturation Circuit Breaker (Overload Protection)
    let overload_lvl = rt.overload.level();
    if overload_lvl.is_shedding() {
        tracing::warn!(
            route = %route.id,
            upstream = %target_upstream,
            overload = ?overload_lvl,
            "Shedding gRPC UDP request due to memory saturation"
        );
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedGrpcUdpUdp,
            StreamCompletion::new(200, 0, 0, duration_ns, 0),
        );
        if sampling.keep_shed() {
            global_telemetry_store().emit(&velda_observability::TelemetryRecord {
                request_id: velda_core::request_id::entropy(req.state.request_id),
                ingress_time_nanos: ingress_nanos,
                duration_ns,
                peer_ip: req.state.client_ip,
                bytes_rx: 0,
                bytes_tx: 0,
                upstream_ip: 0,
                upstream_port: 0,
                status_code: 200,
                route_id: route.id,
                upstream_id: target_upstream,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: PipeKind::BufferedGrpcUdpUdp,
                completion: velda_core::CompletionReason::Rejected,
                method: velda_core::HttpMethod::Post,
                grpc_status: velda_core::GrpcStatus::ResourceExhausted,
                ua_family,
                ua_os,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
        }
        return GrpcStatus::ResourceExhausted.to_l7_response(Some("resource exhausted"));
    }

    let (pipe_res, pipe_kind) = match upstream_target.as_ref() {
        GrpcUpstream::Udp(upstream) => match upstream.strategy {
            GrpcUdpPipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedGrpcUdpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_udp_udp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcUdpPipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamGrpcUdpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_udp_udp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcUdpPipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamGrpcUdpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_udp_udp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcUdpPipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexGrpcUdpUdp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_udp_udp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
        },
        GrpcUpstream::Tcp(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedGrpcUdpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_udp_tcp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamGrpcUdpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_udp_tcp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamGrpcUdpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_udp_tcp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexGrpcUdpTcp;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_udp_tcp::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
        },
        GrpcUpstream::Rdma(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedGrpcUdpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::buffered_udp_rdma::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamGrpcUdpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_udp_rdma::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamGrpcUdpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::client_stream_udp_rdma::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
            GrpcPipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexGrpcUdpRdma;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_udp_rdma::serve(&req, upstream, config).await,
                    PIPE,
                )
            }
        },
    };

    let (mut resp, upstream_addr, completion_reason) = match pipe_res {
        Ok((resp, addr)) => (resp, addr, velda_core::CompletionReason::Complete),
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %target_upstream,
                "gRPC over UDP stream pipe terminated with error"
            );
            (
                GrpcStatus::Unavailable.to_l7_response(Some("upstream unavailable")),
                None,
                velda_core::CompletionReason::UpstreamError,
            )
        }
    };

    if let Some(post_hooks) = &plan.post_response_hooks {
        velda_plugin::run_post_response_plugins(post_hooks, &req.headers, &mut resp);
    }

    let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
    let bytes_rx = req.body.len() as u64;
    let bytes_tx = resp.body.len() as u64;
    let status_code = resp.status.as_u16();
    let grpc_status = resp
        .headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u8>().ok())
        .map(velda_core::GrpcStatus::from_u8)
        .unwrap_or(velda_core::GrpcStatus::Ok);

    global_metrics().record_stream_completion(
        pipe_kind,
        StreamCompletion::new(status_code, bytes_rx, bytes_tx, duration_ns, 0),
    );

    if sampling.keep(grpc_status) {
        let (upstream_ip, upstream_port) = match upstream_addr {
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
            request_id: velda_core::request_id::entropy(req.state.request_id),
            ingress_time_nanos: ingress_nanos,
            duration_ns,
            peer_ip: req.state.client_ip,
            bytes_rx: bytes_rx.min(u32::MAX as u64) as u32,
            bytes_tx: bytes_tx.min(u32::MAX as u64) as u32,
            upstream_ip,
            upstream_port,
            status_code,
            route_id: route.id,
            upstream_id: target_upstream,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: pipe_kind,
            completion: completion_reason,
            method: velda_core::HttpMethod::Post,
            grpc_status,
            ua_family,
            ua_os,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
    }

    resp
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use http::HeaderValue;
    use velda_grpc::udp::wire::{
        GrpcUdpForwardingAddrs, GrpcUdpIngressEnrich, decode_qpack_ingress, encode_prefixed_int,
        encode_string_literal,
    };

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x00);
        buf.put_u8(0x00);
        encode_prefixed_int(20, 6, 0xc0, &mut buf); // :method: POST
        encode_prefixed_int(23, 6, 0xc0, &mut buf); // :scheme: https
        encode_prefixed_int(0, 4, 0x50, &mut buf); // :authority
        encode_string_literal(b"grpc-udp.example.com", &mut buf);
        encode_prefixed_int(1, 4, 0x50, &mut buf); // :path
        encode_string_literal(b"/pkg.Svc/Unary", &mut buf);

        let push_lit = |dst: &mut BytesMut, k: &[u8], v: &[u8]| {
            encode_prefixed_int(k.len() as u64, 3, 0x20, dst);
            dst.put_slice(k);
            encode_string_literal(v, dst);
        };
        // Client attempts to spoof their IP, host, and SSL status
        push_lit(&mut buf, b"x-forwarded-for", b"203.0.113.195");
        push_lit(&mut buf, b"x-real-ip", b"203.0.113.195");
        push_lit(&mut buf, b"x-forwarded-host", b"evil.attacker.com");
        push_lit(&mut buf, b"x-forwarded-proto", b"http");
        push_lit(&mut buf, b"connection", b"close");
        push_lit(&mut buf, b"keep-alive", b"timeout=5");
        push_lit(&mut buf, b"forwarded", b"for=203.0.113.195;proto=http");

        let peer_ip = velda_core::lut::ip::v4_to_u128(0xC000_0232);
        let local_ip = velda_core::lut::ip::v4_to_u128(0x0A00_0001);
        let local_port_be = 443u16.to_be();

        let dec = decode_qpack_ingress(
            &buf,
            GrpcUdpIngressEnrich {
                request_id: HeaderValue::from_static("auth-req-id"),
                forwarding: Some(GrpcUdpForwardingAddrs {
                    peer_ip,
                    local_ip,
                    local_port_be,
                }),
            },
        )
        .unwrap();
        let headers = dec.headers;

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
