//! Layer 7 gRPC over UDP downstream datagram dispatch and request handling.
//!
//! Powered entirely by `velda-grpc::udp`. Operates strictly for listeners explicitly configured
//! with `transport = "udp"` and `protocol = "grpc"`. Receives QUIC datagrams, decodes
//! RPC requests, and forwards to upstream backends with zero HTTP borrowing.

use std::net::SocketAddr;
use std::sync::Arc;

use velda_core::{Action, L7Request, L7Response, UpstreamId};
use velda_grpc::tcp::GrpcPipeStrategy;
use velda_grpc::udp::GrpcUdpPipeStrategy;
use velda_grpc::udp::server::GrpcUdpServerStream;
use velda_grpc::{GrpcConfig, GrpcStatus};
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

            // [INGRESS W3C REQUEST ID]
            // Authoritative W3C Request ID is already stamped at QPACK decode into Slot 0.
            let mut req = req_event.request;
            req.state.client_ip = Some(peer.ip());
            let req_id_header = req.headers.request_id().clone();

            let mut stream =
                GrpcUdpServerStream::new(req_event.handle, req_event.stream_id, req, peer);
            stream.enrich_forwarded_headers(local_addr);

            let mut response =
                process_grpc_udp_request(stream.request, peer, &lid, &cfg, &runtime).await;
            response.headers.set_request_id(req_id_header);

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
    mut req: L7Request,
    peer: SocketAddr,
    listener_id: &str,
    config: &GrpcConfig,
    runtime: &SharedRuntime,
) -> L7Response {
    let req_start = std::time::Instant::now();
    let (log_url_len, log_url_bytes) = {
        let host = velda_http3::server::header::extract_host(&req.headers, &req.uri);
        velda_observability::AccessLogRecord::encode_url(true, host, req.path())
    };
    let (ua_family, ua_os, ua_major_version) = req
        .headers
        .get(http::header::USER_AGENT)
        .map(|v| velda_observability::classify_user_agent(v.as_bytes()))
        .unwrap_or((
            velda_observability::UaFamily::None,
            velda_observability::UaOs::Unknown,
            0,
        ));

    // [PHYSICAL WIRE CEILING: MAX_WIRE_HEADERS = 1024]
    if req.headers.len() > velda_core::MAX_WIRE_HEADERS {
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H3,
            velda_observability::UpstreamProtocol::H3,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: req.state.request_id.entropy(),
            timestamp_sec: req.state.request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: 0,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::ResourceExhausted as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return GrpcStatus::ResourceExhausted.to_l7_response(Some("too many request headers"));
    }

    let authority = req
        .headers
        .get(":authority")
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.host().and_then(|h| h.to_str().ok()))
        .or_else(|| req.uri.authority().map(|a| a.as_str()));

    let Some(grpc_req) = GrpcRouteRequest::from_path(req.path(), authority) else {
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H3,
            velda_observability::UpstreamProtocol::H3,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: req.state.request_id.entropy(),
            timestamp_sec: req.state.request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: 0,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::Unimplemented as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for empty path"));
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_grpc(listener_id, &grpc_req) else {
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H3,
            velda_observability::UpstreamProtocol::H3,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: req.state.request_id.entropy(),
            timestamp_sec: req.state.request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: 0,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::Unimplemented as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return GrpcStatus::Unimplemented.to_l7_response(Some("no route matched for service"));
    };

    let mut target_upstream: UpstreamId = route.upstream_id;

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
                    // preserving only Bit-1 bypass headers and grpc-status trailers.
                    let grpc_status = GrpcStatus::from_reject_reason(reason);
                    let log_grpc_status =
                        velda_observability::LogGrpcStatus::from_u8(grpc_status as u8);
                    let mut resp = grpc_status.to_l7_response(Some(reason.canonical_phrase()));
                    for (name, val) in req.headers.bypass_headers() {
                        resp.headers.insert(name.clone(), val.clone());
                    }
                    resp.headers
                        .set_request_id(req.headers.request_id().clone());
                    velda_observability::global_metrics().record_early_completion(
                        velda_observability::DownstreamProtocol::H3,
                        velda_observability::UpstreamProtocol::H3,
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
                            request_id: req.state.request_id.entropy(),
                            timestamp_sec: req.state.request_id.timestamp_secs() as u32,
                            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128)
                                as u64,
                            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
                            bytes_rx: 0,
                            bytes_tx: 0,
                            upstream_ip: 0,
                            peer_port: peer.port(),
                            upstream_port: 0,
                            status_code: 200,
                            route_id: route.id.0 as u16,
                            ua_major_version,
                            rpc_msgs_or_flags: 1,
                            pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
                            completion: velda_observability::LogCompletionReason::Rejected as u8,
                            method: velda_observability::LogHttpMethod::Post as u8,
                            grpc_status: log_grpc_status as u8,
                            ua_family: ua_family as u8,
                            ua_os: ua_os as u8,
                            url_len: log_url_len,
                            url_bytes: log_url_bytes,
                        },
                    );
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
                    // preserving only Bit-1 bypass headers and grpc-status trailers.
                    let grpc_status = GrpcStatus::from_reject_reason(reason);
                    let log_grpc_status =
                        velda_observability::LogGrpcStatus::from_u8(grpc_status as u8);
                    let mut resp = grpc_status.to_l7_response(Some(reason.canonical_phrase()));
                    for (name, val) in req.headers.bypass_headers() {
                        resp.headers.insert(name.clone(), val.clone());
                    }
                    resp.headers
                        .set_request_id(req.headers.request_id().clone());
                    velda_observability::global_metrics().record_early_completion(
                        velda_observability::DownstreamProtocol::H3,
                        velda_observability::UpstreamProtocol::H3,
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
                            request_id: req.state.request_id.entropy(),
                            timestamp_sec: req.state.request_id.timestamp_secs() as u32,
                            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128)
                                as u64,
                            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
                            bytes_rx: 0,
                            bytes_tx: 0,
                            upstream_ip: 0,
                            peer_port: peer.port(),
                            upstream_port: 0,
                            status_code: 200,
                            route_id: route.id.0 as u16,
                            ua_major_version,
                            rpc_msgs_or_flags: 1,
                            pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
                            completion: velda_observability::LogCompletionReason::Rejected as u8,
                            method: velda_observability::LogHttpMethod::Post as u8,
                            grpc_status: log_grpc_status as u8,
                            ua_family: ua_family as u8,
                            ua_os: ua_os as u8,
                            url_len: log_url_len,
                            url_bytes: log_url_bytes,
                        },
                    );
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
            let duration = req_start.elapsed();
            let bytes_rx = req.body.len() as u64;
            let bytes_tx = resp.body.len() as u64;
            let status_code = resp.status.as_u16();
            let grpc_status = resp
                .headers
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u8>().ok())
                .map(velda_observability::LogGrpcStatus::from_u8)
                .unwrap_or(velda_observability::LogGrpcStatus::Ok);
            velda_observability::global_metrics().record_early_completion(
                velda_observability::DownstreamProtocol::H3,
                velda_observability::UpstreamProtocol::H3,
                velda_observability::PipeStrategy::Buffered,
                velda_observability::StreamCompletion::new(
                    status_code,
                    bytes_rx,
                    bytes_tx,
                    duration,
                    None,
                ),
            );
            velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
                request_id: req.state.request_id.entropy(),
                timestamp_sec: req.state.request_id.timestamp_secs() as u32,
                duration_ns: duration.as_nanos().min(u64::MAX as u128) as u64,
                peer_ip: velda_observability::encode_ip_u128(peer.ip()),
                bytes_rx: bytes_rx.min(u32::MAX as u64) as u32,
                bytes_tx: bytes_tx.min(u32::MAX as u64) as u32,
                upstream_ip: 0,
                peer_port: peer.port(),
                upstream_port: 0,
                status_code,
                route_id: route.id.0 as u16,
                ua_major_version,
                rpc_msgs_or_flags: 1,
                pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
                completion: velda_observability::LogCompletionReason::Complete as u8,
                method: velda_observability::LogHttpMethod::Post as u8,
                grpc_status: grpc_status as u8,
                ua_family: ua_family as u8,
                ua_os: ua_os as u8,
                url_len: log_url_len,
                url_bytes: log_url_bytes,
            });
            return resp;
        }
    }

    let Some(upstream_target) = rt.upstreams.grpc.get(target_upstream) else {
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H3,
            velda_observability::UpstreamProtocol::H3,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: req.state.request_id.entropy(),
            timestamp_sec: req.state.request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: route.id.0 as u16,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::Unavailable as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
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
        velda_observability::global_metrics().record_early_completion(
            velda_observability::DownstreamProtocol::H3,
            velda_observability::UpstreamProtocol::H3,
            velda_observability::PipeStrategy::Buffered,
            velda_observability::StreamCompletion::new(200, 0, 0, req_start.elapsed(), None),
        );
        velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
            request_id: req.state.request_id.entropy(),
            timestamp_sec: req.state.request_id.timestamp_secs() as u32,
            duration_ns: req_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            peer_ip: velda_observability::encode_ip_u128(peer.ip()),
            bytes_rx: 0,
            bytes_tx: 0,
            upstream_ip: 0,
            peer_port: peer.port(),
            upstream_port: 0,
            status_code: 200,
            route_id: route.id.0 as u16,
            ua_major_version,
            rpc_msgs_or_flags: 1,
            pipe: velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
            completion: velda_observability::LogCompletionReason::Rejected as u8,
            method: velda_observability::LogHttpMethod::Post as u8,
            grpc_status: velda_observability::LogGrpcStatus::ResourceExhausted as u8,
            ua_family: ua_family as u8,
            ua_os: ua_os as u8,
            url_len: log_url_len,
            url_bytes: log_url_bytes,
        });
        return GrpcStatus::ResourceExhausted.to_l7_response(Some("resource exhausted"));
    }

    let (metric_upstream_proto, metric_pipe_strat, pipe_kind) = match upstream_target.as_ref() {
        GrpcUpstream::Udp(u) => (
            velda_observability::UpstreamProtocol::H3,
            match u.strategy {
                GrpcUdpPipeStrategy::Buffered => velda_observability::PipeStrategy::Buffered,
                GrpcUdpPipeStrategy::ServerStream => {
                    velda_observability::PipeStrategy::ServerStream
                }
                GrpcUdpPipeStrategy::ClientStream => {
                    velda_observability::PipeStrategy::ClientStream
                }
                GrpcUdpPipeStrategy::Duplex => velda_observability::PipeStrategy::Duplex,
            },
            match u.strategy {
                GrpcUdpPipeStrategy::Buffered => velda_core::pipe::PipeKind::BufferedGrpcUdpUdp,
                GrpcUdpPipeStrategy::ServerStream => {
                    velda_core::pipe::PipeKind::ServerStreamGrpcUdpUdp
                }
                GrpcUdpPipeStrategy::ClientStream => {
                    velda_core::pipe::PipeKind::ClientStreamGrpcUdpUdp
                }
                GrpcUdpPipeStrategy::Duplex => velda_core::pipe::PipeKind::DuplexGrpcUdpUdp,
            },
        ),
        GrpcUpstream::Tcp(u) => (
            velda_observability::UpstreamProtocol::H2,
            match u.strategy {
                GrpcPipeStrategy::Buffered => velda_observability::PipeStrategy::Buffered,
                GrpcPipeStrategy::ServerStream => velda_observability::PipeStrategy::ServerStream,
                GrpcPipeStrategy::ClientStream => velda_observability::PipeStrategy::ClientStream,
                GrpcPipeStrategy::Duplex => velda_observability::PipeStrategy::Duplex,
            },
            match u.strategy {
                GrpcPipeStrategy::Buffered => velda_core::pipe::PipeKind::BufferedGrpcUdpTcp,
                GrpcPipeStrategy::ServerStream => {
                    velda_core::pipe::PipeKind::ServerStreamGrpcUdpTcp
                }
                GrpcPipeStrategy::ClientStream => {
                    velda_core::pipe::PipeKind::ClientStreamGrpcUdpTcp
                }
                GrpcPipeStrategy::Duplex => velda_core::pipe::PipeKind::DuplexGrpcUdpTcp,
            },
        ),
        GrpcUpstream::Rdma(u) => (
            velda_observability::UpstreamProtocol::H2,
            match u.strategy {
                GrpcPipeStrategy::Buffered => velda_observability::PipeStrategy::Buffered,
                GrpcPipeStrategy::ServerStream => velda_observability::PipeStrategy::ServerStream,
                GrpcPipeStrategy::ClientStream => velda_observability::PipeStrategy::ClientStream,
                GrpcPipeStrategy::Duplex => velda_observability::PipeStrategy::Duplex,
            },
            match u.strategy {
                GrpcPipeStrategy::Buffered => velda_core::pipe::PipeKind::BufferedGrpcUdpRdma,
                GrpcPipeStrategy::ServerStream => {
                    velda_core::pipe::PipeKind::ServerStreamGrpcUdpRdma
                }
                GrpcPipeStrategy::ClientStream => {
                    velda_core::pipe::PipeKind::ClientStreamGrpcUdpRdma
                }
                GrpcPipeStrategy::Duplex => velda_core::pipe::PipeKind::DuplexGrpcUdpRdma,
            },
        ),
    };

    velda_observability::global_metrics().record_stream_start(
        velda_observability::DownstreamProtocol::H3,
        metric_upstream_proto,
        metric_pipe_strat,
    );

    let pipe_res = match upstream_target.as_ref() {
        GrpcUpstream::Udp(upstream) => match upstream.strategy {
            GrpcUdpPipeStrategy::Buffered => {
                pipe::buffered_udp_udp::serve(&req, upstream, config).await
            }
            GrpcUdpPipeStrategy::ServerStream => {
                pipe::server_stream_udp_udp::serve(&req, upstream, config).await
            }
            GrpcUdpPipeStrategy::ClientStream => {
                pipe::client_stream_udp_udp::serve(&req, upstream, config).await
            }
            GrpcUdpPipeStrategy::Duplex => {
                pipe::duplex_udp_udp::serve(&req, upstream, config).await
            }
        },
        GrpcUpstream::Tcp(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                pipe::buffered_udp_tcp::serve(&req, upstream, config).await
            }
            GrpcPipeStrategy::ServerStream => {
                pipe::server_stream_udp_tcp::serve(&req, upstream, config).await
            }
            GrpcPipeStrategy::ClientStream => {
                pipe::client_stream_udp_tcp::serve(&req, upstream, config).await
            }
            GrpcPipeStrategy::Duplex => pipe::duplex_udp_tcp::serve(&req, upstream, config).await,
        },
        GrpcUpstream::Rdma(upstream) => match upstream.strategy {
            GrpcPipeStrategy::Buffered => {
                pipe::buffered_udp_rdma::serve(&req, upstream, config).await
            }
            GrpcPipeStrategy::ServerStream => {
                pipe::server_stream_udp_rdma::serve(&req, upstream, config).await
            }
            GrpcPipeStrategy::ClientStream => {
                pipe::client_stream_udp_rdma::serve(&req, upstream, config).await
            }
            GrpcPipeStrategy::Duplex => pipe::duplex_udp_rdma::serve(&req, upstream, config).await,
        },
    };

    let (mut resp, upstream_addr, completion_reason) = match pipe_res {
        Ok((resp, addr)) => (
            resp,
            addr,
            velda_observability::LogCompletionReason::Complete,
        ),
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_id,
                "gRPC over UDP stream pipe terminated with error"
            );
            (
                GrpcStatus::Unavailable.to_l7_response(Some("upstream unavailable")),
                None,
                velda_observability::LogCompletionReason::UpstreamError,
            )
        }
    };

    if let Some(post_hooks) = &plan.post_response_hooks {
        velda_plugin::run_post_response_plugins(post_hooks, &req.headers, &mut resp);
    }

    let duration = req_start.elapsed();
    let bytes_rx = req.body.len() as u64;
    let bytes_tx = resp.body.len() as u64;
    let status_code = resp.status.as_u16();
    let (upstream_ip, upstream_port) = match upstream_addr {
        Some(addr) => (
            velda_observability::encode_upstream_ip_u32(addr.ip()),
            addr.port(),
        ),
        None => (0, 0),
    };
    let grpc_status = resp
        .headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u8>().ok())
        .map(velda_observability::LogGrpcStatus::from_u8)
        .unwrap_or(velda_observability::LogGrpcStatus::Ok);

    velda_observability::global_metrics().record_stream_completion(
        velda_observability::DownstreamProtocol::H3,
        metric_upstream_proto,
        metric_pipe_strat,
        velda_observability::StreamCompletion::new(status_code, bytes_rx, bytes_tx, duration, None),
    );

    velda_observability::global_log_store().emit(&velda_observability::AccessLogRecord {
        request_id: req.state.request_id.entropy(),
        timestamp_sec: req.state.request_id.timestamp_secs() as u32,
        duration_ns: duration.as_nanos().min(u64::MAX as u128) as u64,
        peer_ip: velda_observability::encode_ip_u128(peer.ip()),
        bytes_rx: bytes_rx.min(u32::MAX as u64) as u32,
        bytes_tx: bytes_tx.min(u32::MAX as u64) as u32,
        upstream_ip,
        peer_port: peer.port(),
        upstream_port,
        status_code,
        route_id: route.id.0 as u16,
        ua_major_version,
        rpc_msgs_or_flags: 1,
        pipe: pipe_kind,
        completion: completion_reason as u8,
        method: velda_observability::LogHttpMethod::Post as u8,
        grpc_status: grpc_status as u8,
        ua_family: ua_family as u8,
        ua_os: ua_os as u8,
        url_len: log_url_len,
        url_bytes: log_url_bytes,
    });

    resp
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use http::HeaderMap;
    use velda_core::HeaderWorkspace;
    use velda_grpc::udp::server::enrich_headers;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut map = HeaderMap::new();
        // Client attempts to spoof their IP, host, and SSL status
        map.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        map.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        map.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        map.insert("x-forwarded-proto", "http".parse().unwrap());
        map.insert("connection", "close".parse().unwrap());
        map.insert("keep-alive", "timeout=5".parse().unwrap());
        map.insert("forwarded", "for=203.0.113.195;proto=http".parse().unwrap());

        let mut headers = HeaderWorkspace::from_headers(map);

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
