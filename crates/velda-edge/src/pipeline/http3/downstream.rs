//! Downstream HTTP/3 UDP Datagram & QUIC Ingress Engine.
//!
//! Handles UDP packet ingestion, 0-RTT anti-replay defense, request decoding,
//! route matching, overload shedding, and pipeline dispatching across all upstream backends.

use std::sync::Arc;

use http::StatusCode;
use http::header::HeaderValue;
use u16;
use velda_core::{Action, L7Request, PipeKind};
use velda_http1::strategy::Http1PipeStrategy;
use velda_http2::strategy::Http2PipeStrategy;
use velda_http3::server::connection::OutgoingDatagram;
use velda_http3::strategy::Http3PipeStrategy;
use velda_observability::sampling::status::StatusSampling;
use velda_observability::{StreamCompletion, global_metrics};
use velda_router::Http3RouteRequest;
use velda_sock::{Datagram, UdpSocket};

use super::engine::{Http3WorkerPool, get_or_init_h3_workers};
use super::pipe;
use crate::pipeline::PipeOutcome;
use crate::runtime::SharedRuntime;
use crate::upstream::HttpUpstream;

/// Cache-line aligned (64-byte) 64-bit atomic counter to completely eliminate false sharing.
///
/// # Hardware Rationale
/// On modern multi-core x86-64 CPUs, adjacent 8-byte atomic variables in static memory share
/// the same 64-byte L1 data cache line. If ingress threads mutate `H3_TOTAL_DECODED` while response
/// writer threads mutate `H3_TOTAL_RESP_SENT`, the cache line bounces between cores in the MESI
/// Modified state, causing severe L1 cache invalidation and bus locking under high packet load.
/// Aligning each counter to 64 bytes isolates them into dedicated cache lines.
#[repr(align(64))]
pub struct CacheAlignedAtomicU64(pub std::sync::atomic::AtomicU64);

pub static H3_TOTAL_DECODED: CacheAlignedAtomicU64 =
    CacheAlignedAtomicU64(std::sync::atomic::AtomicU64::new(0));
pub static H3_TOTAL_RESP_SENT: CacheAlignedAtomicU64 =
    CacheAlignedAtomicU64(std::sync::atomic::AtomicU64::new(0));

/// Sends outgoing QUIC packets using GSO-aware path (UDP_SEGMENT cmsg) when available,
/// and `sendmmsg` batching for non-GSO control, ACK, and individual datagrams.
pub(crate) async fn send_outgoing_packets(socket: &UdpSocket, packets: Vec<OutgoingDatagram>) {
    if packets.is_empty() {
        return;
    }

    let mut non_gso_batch: [(&[u8], u128, u16); 64] = [(&[], 0, 0); 64];
    let mut batch_count = 0;

    for pkt in &packets {
        let (peer_ip, peer_port_be) = (pkt.peer_ip, pkt.peer_port_be);
        if let Some(seg) = pkt.segment_size
            && seg > 0
            && pkt.payload.len() > seg as usize
        {
            if batch_count > 0 {
                flush_non_gso_batch(socket, &non_gso_batch[..batch_count]).await;
                batch_count = 0;
            }

            if let Some(res) =
                socket.try_send_gso_aware(&pkt.payload, Some(seg), peer_ip, peer_port_be)
            {
                if let Err(e) = res {
                    tracing::warn!(
                        peer = %velda_sock::format_raw_addr(peer_ip, peer_port_be),
                        segment_size = seg,
                        error = %e,
                        "Failed to send outgoing QUIC packet via try_send_gso_aware"
                    );
                }
            } else if let Err(e) = socket
                .send_gso_aware(&pkt.payload, Some(seg), peer_ip, peer_port_be)
                .await
            {
                tracing::warn!(
                    peer = %velda_sock::format_raw_addr(peer_ip, peer_port_be),
                    segment_size = seg,
                    error = %e,
                    "Failed to send outgoing QUIC packet via send_gso_aware"
                );
            }
        } else {
            non_gso_batch[batch_count] = (&pkt.payload, peer_ip, peer_port_be);
            batch_count += 1;
            if batch_count == 64 {
                flush_non_gso_batch(socket, &non_gso_batch[..batch_count]).await;
                batch_count = 0;
            }
        }
    }

    if batch_count > 0 {
        flush_non_gso_batch(socket, &non_gso_batch[..batch_count]).await;
    }
}

async fn flush_non_gso_batch(socket: &UdpSocket, batch: &[(&[u8], u128, u16)]) {
    let mut sent = socket.try_send_batch(batch);
    while sent < batch.len() {
        let (buf, target_ip, target_port_be) = batch[sent];
        if let Err(e) = socket.send_to(buf, target_ip, target_port_be).await {
            tracing::warn!(
                peer = %velda_sock::format_raw_addr(target_ip, target_port_be),
                error = %e,
                "Failed to send packet via fallback send_to in flush_non_gso_batch"
            );
        }
        sent += 1;
    }
}

/// Dispatches incoming UDP datagrams to the persistent HTTP/3 state machine.
///
/// Ingests the packet, drives QUIC handshake/flow control, transmits outgoing datagrams,
/// and passes decoded requests through `Http3Router`.
pub async fn handle_http3_udp(
    listener_id: Arc<str>,
    socket: Arc<UdpSocket>,
    datagram: Datagram,
    _config: velda_http3::config::Http3Config,
    sampling: StatusSampling,
    runtime: &SharedRuntime,
    precomputed_workers: Option<&Http3WorkerPool>,
) {
    let peer = velda_sock::raw_to_socket_addr(datagram.peer_ip, datagram.peer_port_be);

    let worker = if let Some(pool) = precomputed_workers {
        pool.get_worker_for_socket(&socket)
    } else {
        let Some(pool) = get_or_init_h3_workers(&listener_id, runtime) else {
            tracing::warn!(
                listener = %listener_id,
                peer = %peer,
                "Received UDP datagram, but no HTTP/3 engine is compiled"
            );
            return;
        };
        pool.get_worker_for_socket(&socket)
    };

    worker.register_socket(&socket);

    let now = std::time::Instant::now();
    let (outgoing, requests) = {
        let mut engine = worker.engine.lock();
        engine.handle_datagram(
            now,
            datagram.peer_ip,
            datagram.peer_port_be,
            Some((datagram.local_ip, datagram.local_port_be)),
            datagram.data(),
        )
    };
    worker.notify.notify_one();

    // 1. Send all outgoing handshake / ACK datagrams immediately outside the engine lock
    //    using GSO-aware path (UDP_SEGMENT cmsg) when quinn-proto provides segment_size.
    send_outgoing_packets(&socket, outgoing).await;

    let n_reqs = requests.len() as u64;
    if n_reqs > 0 {
        let dec = H3_TOTAL_DECODED
            .0
            .fetch_add(n_reqs, std::sync::atomic::Ordering::Relaxed)
            + n_reqs;
        let sent = H3_TOTAL_RESP_SENT
            .0
            .load(std::sync::atomic::Ordering::Relaxed);
        tracing::trace!(
            decoded = dec,
            sent = sent,
            in_flight = dec - sent,
            "H3 INGRESS STATS"
        );
    }

    let peer_ip = velda_core::lut::ip::encode_ip_u128(peer.ip());

    // 2. Process complete L7 requests concurrently without blocking other datagrams
    for req_event in requests {
        let worker = worker.clone();
        let lid = listener_id.clone();
        let runtime = runtime.clone();

        tokio::spawn(async move {
            // [INGRESS W3C REQUEST ID]
            // Authoritative W3C Request ID is already stamped at QPACK decode into Slot 0.
            let mut req = req_event.request;
            req.state.client_ip = peer_ip;
            let req_id_header = req.headers.request_id().clone();
            let (log_url_len, log_url_bytes) = {
                let host = velda_http3::server::header::extract_host(&req.headers, &req.uri);
                let pq = req
                    .uri
                    .path_and_query()
                    .map(|x| x.as_str())
                    .unwrap_or(req.uri.path());
                velda_observability::TelemetryRecord::encode_url(true, host, pq)
            };

            let responder = super::engine::Http3Responder::new(
                req_event.handle,
                req_event.stream_id,
                worker.response_tx.clone(),
            )
            .with_request_id(Some(req_id_header.clone()));

            tracing::debug!(
                method = %req.method,
                path = %req.uri.path(),
                peer = %peer,
                early_data = req_event.is_early_data,
                "Decoded HTTP/3 request from UDP"
            );

            // Fast-Fail: 0-RTT Replay Attack Defense (RFC 8470)
            // Non-idempotent methods (POST, PUT, DELETE, PATCH) MUST NOT be processed in 0-RTT early data.
            let is_mutation = matches!(
                req.method,
                http::Method::POST | http::Method::PUT | http::Method::DELETE | http::Method::PATCH
            );
            if req_event.is_early_data && is_mutation {
                tracing::warn!(
                    listener = %lid,
                    method = %req.method,
                    path = %req.uri.path(),
                    "Rejecting non-idempotent HTTP/3 request received in 0-RTT early data (RFC 8470)"
                );
                let _ = responder
                    .send_response(
                        velda_http3::server::early_response::precompiled_early_response(
                            StatusCode::TOO_EARLY,
                            Some(&req_id_header),
                        ),
                    )
                    .await;
                let duration_ns =
                    velda_core::clock::now_unix_nanos().saturating_sub(req.ingress_time_nanos);
                global_metrics().record_early_completion(
                    PipeKind::BufferedH3H1,
                    StreamCompletion::new(
                        velda_core::InternalStatus::BadRequest,
                        req.body.len() as u64,
                        0,
                        duration_ns,
                        0,
                    ),
                );
                if sampling.keep(velda_core::InternalStatus::BadRequest) {
                    let log_method =
                        velda_core::HttpMethod::from_bytes(req.method.as_str().as_bytes());
                    velda_observability::global_telemetry_store().emit(
                        &velda_observability::TelemetryRecord {
                            request_id: velda_core::request_id::entropy(req.state.request_id),
                            ingress_time_nanos: req.ingress_time_nanos,
                            duration_ns,
                            peer_ip: req.state.client_ip,
                            bytes_rx: req.body.len() as u32,
                            bytes_tx: 0,
                            upstream_ip: 0,
                            upstream_port: 0,
                            _cl0_pad: 0,
                            route_id: velda_core::UNMATCHED_ROUTE_ID,
                            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                            ua_major_version: 0,
                            rpc_msgs_or_flags: 0,
                            pipe: velda_core::PipeKind::BufferedH3H1,
                            internal_status: velda_core::InternalStatus::BadRequest,
                            method: log_method,
                            _reserved: 0,
                            ua_family: velda_core::UaFamily::None,
                            ua_os: velda_core::UaOs::Unknown,
                            url_len: log_url_len,
                            url_bytes: log_url_bytes,
                        },
                    );
                }
                return;
            }
            if let Err(e) = velda_http3::server::path::normalize_path(&mut req.uri) {
                tracing::warn!(
                    listener = %lid,
                    error = %e,
                    path = %req.uri.path(),
                    "Rejecting HTTP/3 request with unsafe URI path"
                );
                let _ = responder
                    .send_response(
                        velda_http3::server::early_response::precompiled_early_response(
                            StatusCode::BAD_REQUEST,
                            Some(&req_id_header),
                        ),
                    )
                    .await;
                let duration_ns =
                    velda_core::clock::now_unix_nanos().saturating_sub(req.ingress_time_nanos);
                global_metrics().record_early_completion(
                    PipeKind::BufferedH3H1,
                    StreamCompletion::new(
                        velda_core::InternalStatus::BadRequest,
                        req.body.len() as u64,
                        0,
                        duration_ns,
                        0,
                    ),
                );
                if sampling.keep(velda_core::InternalStatus::BadRequest) {
                    let log_method =
                        velda_core::HttpMethod::from_bytes(req.method.as_str().as_bytes());
                    velda_observability::global_telemetry_store().emit(
                        &velda_observability::TelemetryRecord {
                            request_id: velda_core::request_id::entropy(req.state.request_id),
                            ingress_time_nanos: req.ingress_time_nanos,
                            duration_ns,
                            peer_ip: req.state.client_ip,
                            bytes_rx: req.body.len() as u32,
                            bytes_tx: 0,
                            upstream_ip: 0,
                            upstream_port: 0,
                            _cl0_pad: 0,
                            route_id: velda_core::UNMATCHED_ROUTE_ID,
                            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                            ua_major_version: 0,
                            rpc_msgs_or_flags: 0,
                            pipe: velda_core::PipeKind::BufferedH3H1,
                            internal_status: velda_core::InternalStatus::BadRequest,
                            method: log_method,
                            _reserved: 0,
                            ua_family: velda_core::UaFamily::None,
                            ua_os: velda_core::UaOs::Unknown,
                            url_len: log_url_len,
                            url_bytes: log_url_bytes,
                        },
                    );
                }
                return;
            }

            process_http3_request(req, &lid, sampling, &runtime, responder).await;
        });
    }
}

/// Dispatches an HTTP/3 request through `Http3Router` and forwards to upstream backend.
async fn process_http3_request(
    mut req: L7Request,
    listener_id: &str,
    sampling: StatusSampling,
    runtime: &SharedRuntime,
    responder: super::engine::Http3Responder,
) {
    let ingress_nanos = req.ingress_time_nanos;
    let host = velda_http3::server::header::extract_host(&req.headers, &req.uri);
    let mut http_req = Http3RouteRequest::new(req.uri.path());
    if let Some(h) = host {
        http_req = http_req.with_host(h);
    }
    http_req = http_req.with_method(req.method.as_str());

    let (ua_family, ua_os, ua_major_version) = req
        .headers
        .get(http::header::USER_AGENT)
        .map(|v| velda_core::lut::classify_user_agent(v.as_bytes()))
        .unwrap_or((velda_core::UaFamily::None, velda_core::UaOs::Unknown, 0));
    let log_method = velda_core::HttpMethod::from_bytes(req.method.as_str().as_bytes());
    let pq = req
        .uri
        .path_and_query()
        .map(|x| x.as_str())
        .unwrap_or(req.uri.path());
    let (mut log_url_len, mut log_url_bytes) =
        velda_observability::TelemetryRecord::encode_url(true, host, pq);

    let rt = runtime.load();
    let Some(route) = rt
        .router
        .route_http3(velda_core::fnv1a_hash_32(listener_id), &http_req)
    else {
        tracing::debug!(
            listener = %listener_id,
            path = %req.uri.path(),
            host = ?host,
            method = %req.method,
            "No HTTP/3 route matched"
        );
        let _ = responder
            .send_response(
                velda_http3::server::early_response::precompiled_early_response(
                    StatusCode::NOT_FOUND,
                    Some(req.headers.request_id()),
                ),
            )
            .await;
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedH3H1,
            StreamCompletion::new(
                velda_core::InternalStatus::RouteNotFound,
                req.body.len() as u64,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep(velda_core::InternalStatus::RouteNotFound) {
            velda_observability::global_telemetry_store().emit(
                &velda_observability::TelemetryRecord {
                    request_id: velda_core::request_id::entropy(req.state.request_id),
                    ingress_time_nanos: ingress_nanos,
                    duration_ns,
                    peer_ip: req.state.client_ip,
                    bytes_rx: req.body.len() as u32,
                    bytes_tx: 0,
                    upstream_ip: 0,
                    upstream_port: 0,
                    _cl0_pad: 0,
                    route_id: velda_core::UNMATCHED_ROUTE_ID,
                    upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                    ua_major_version,
                    rpc_msgs_or_flags: 0,
                    pipe: velda_core::PipeKind::BufferedH3H1,
                    internal_status: velda_core::InternalStatus::RouteNotFound,
                    method: log_method,
                    _reserved: 0,
                    ua_family,
                    ua_os,
                    url_len: log_url_len,
                    url_bytes: log_url_bytes,
                },
            );
        }
        return;
    };

    let route_idx = route.id as usize;
    let default_upstream: u16 = rt.route_to_upstream.get(route_idx).copied().unwrap_or(0);
    let mut target_mask: u32 = velda_plugin::target_init(default_upstream);
    let mut target_upstream: u16 = default_upstream;
    req.state.set_route(route.id);
    req.state.set_upstream(target_upstream);

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
                Action::Status(internal_status) => {
                    // FAST-FAIL INVARIANT: Status responses terminate immediately at the Edge boundary.
                    // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                    // retaining only MSB bypass headers while velda-http3 stamps HTTP/3 fields.
                    let resp = velda_http3::status_response(internal_status, req.headers);
                    let bytes_tx = resp.body.len() as u64;
                    let _ = responder.send_response(resp).await;
                    let duration_ns =
                        velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                    global_metrics().record_early_completion(
                        PipeKind::BufferedH3H1,
                        StreamCompletion::new(
                            internal_status,
                            req.body.len() as u64,
                            bytes_tx,
                            duration_ns,
                            0,
                        ),
                    );
                    if sampling.keep(internal_status) {
                        velda_observability::global_telemetry_store().emit(
                            &velda_observability::TelemetryRecord {
                                request_id: velda_core::request_id::entropy(req.state.request_id),
                                ingress_time_nanos: ingress_nanos,
                                duration_ns,
                                peer_ip: req.state.client_ip,
                                bytes_rx: req.body.len() as u32,
                                bytes_tx: bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                upstream_port: 0,
                                _cl0_pad: 0,
                                route_id: route.id,
                                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                ua_major_version,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::PipeKind::BufferedH3H1,
                                internal_status,
                                method: log_method,
                                _reserved: 0,
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
            let mut action =
                velda_plugin::run_pre_upstream_plugins_async(hooks, &mut req, &mut target_mask)
                    .await;
            target_upstream = velda_plugin::target_primary(target_mask)
                .unwrap_or(velda_core::UNASSIGNED_UPSTREAM_ID);
            req.state.set_upstream(target_upstream);
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
                    // FAST-FAIL INVARIANT: Status responses terminate immediately at the Edge boundary.
                    // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                    // retaining only MSB bypass headers while velda-http3 stamps HTTP/3 fields.
                    let resp = velda_http3::status_response(internal_status, req.headers);
                    let bytes_tx = resp.body.len() as u64;
                    let _ = responder.send_response(resp).await;
                    let duration_ns =
                        velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
                    global_metrics().record_early_completion(
                        PipeKind::BufferedH3H1,
                        StreamCompletion::new(
                            internal_status,
                            req.body.len() as u64,
                            bytes_tx,
                            duration_ns,
                            0,
                        ),
                    );
                    if sampling.keep(internal_status) {
                        velda_observability::global_telemetry_store().emit(
                            &velda_observability::TelemetryRecord {
                                request_id: velda_core::request_id::entropy(req.state.request_id),
                                ingress_time_nanos: ingress_nanos,
                                duration_ns,
                                peer_ip: req.state.client_ip,
                                bytes_rx: req.body.len() as u32,
                                bytes_tx: bytes_tx.min(u32::MAX as u64) as u32,
                                upstream_ip: 0,
                                upstream_port: 0,
                                _cl0_pad: 0,
                                route_id: route.id,
                                upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                                ua_major_version,
                                rpc_msgs_or_flags: 0,
                                pipe: velda_core::PipeKind::BufferedH3H1,
                                internal_status,
                                method: log_method,
                                _reserved: 0,
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
                .set_request_id(req.headers.request_id().clone());
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
                    &req.state,
                    &req.headers,
                    &mut resp,
                );
            }
            let internal_status = crate::pipeline::internal_status_from_http(resp.status);
            let bytes_tx = resp.body.len() as u64;
            let _ = responder.send_response(resp).await;
            let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
            global_metrics().record_early_completion(
                PipeKind::BufferedH3H1,
                StreamCompletion::new(
                    internal_status,
                    req.body.len() as u64,
                    bytes_tx,
                    duration_ns,
                    0,
                ),
            );
            if sampling.keep(internal_status) {
                velda_observability::global_telemetry_store().emit(
                    &velda_observability::TelemetryRecord {
                        request_id: velda_core::request_id::entropy(req.state.request_id),
                        ingress_time_nanos: ingress_nanos,
                        duration_ns,
                        peer_ip: req.state.client_ip,
                        bytes_rx: req.body.len() as u32,
                        bytes_tx: bytes_tx.min(u32::MAX as u64) as u32,
                        upstream_ip: 0,
                        upstream_port: 0,
                        _cl0_pad: 0,
                        route_id: route.id,
                        upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
                        ua_major_version,
                        rpc_msgs_or_flags: 0,
                        pipe: velda_core::PipeKind::BufferedH3H1,
                        internal_status,
                        method: log_method,
                        _reserved: 0,
                        ua_family,
                        ua_os,
                        url_len: log_url_len,
                        url_bytes: log_url_bytes,
                    },
                );
            }
            return;
        }
    }

    let (re_len, re_bytes) = {
        let host = velda_http3::server::header::extract_host(&req.headers, &req.uri);
        let pq = req
            .uri
            .path_and_query()
            .map(|x| x.as_str())
            .unwrap_or(req.uri.path());
        velda_observability::TelemetryRecord::encode_url(true, host, pq)
    };
    log_url_len = re_len;
    log_url_bytes = re_bytes;

    let Some(upstream_target) = rt.upstreams.http.get(target_upstream) else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %target_upstream,
            "No HTTP upstream configured"
        );
        let _ = responder
            .send_response(
                velda_http3::server::early_response::precompiled_early_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Some(req.headers.request_id()),
                ),
            )
            .await;
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedH3H1,
            StreamCompletion::new(
                velda_core::InternalStatus::ServiceUnavailable,
                req.body.len() as u64,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep(velda_core::InternalStatus::ServiceUnavailable) {
            velda_observability::global_telemetry_store().emit(
                &velda_observability::TelemetryRecord {
                    request_id: velda_core::request_id::entropy(req.state.request_id),
                    ingress_time_nanos: ingress_nanos,
                    duration_ns,
                    peer_ip: req.state.client_ip,
                    bytes_rx: req.body.len() as u32,
                    bytes_tx: 0,
                    upstream_ip: 0,
                    upstream_port: 0,
                    _cl0_pad: 0,
                    route_id: route.id,
                    upstream_id: target_upstream,
                    ua_major_version,
                    rpc_msgs_or_flags: 0,
                    pipe: velda_core::PipeKind::BufferedH3H1,
                    internal_status: velda_core::InternalStatus::ServiceUnavailable,
                    method: log_method,
                    _reserved: 0,
                    ua_family,
                    ua_os,
                    url_len: log_url_len,
                    url_bytes: log_url_bytes,
                },
            );
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
            "Shedding HTTP/3 request due to memory saturation"
        );
        let _ = responder
            .send_response(
                velda_http3::server::early_response::precompiled_early_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    Some(req.headers.request_id()),
                ),
            )
            .await;
        let duration_ns = velda_core::clock::now_unix_nanos().saturating_sub(ingress_nanos);
        global_metrics().record_early_completion(
            PipeKind::BufferedH3H1,
            StreamCompletion::new(
                velda_core::InternalStatus::RateLimited,
                req.body.len() as u64,
                0,
                duration_ns,
                0,
            ),
        );
        if sampling.keep_shed() {
            velda_observability::global_telemetry_store().emit(
                &velda_observability::TelemetryRecord {
                    request_id: velda_core::request_id::entropy(req.state.request_id),
                    ingress_time_nanos: ingress_nanos,
                    duration_ns,
                    peer_ip: req.state.client_ip,
                    bytes_rx: req.body.len() as u32,
                    bytes_tx: 0,
                    upstream_ip: 0,
                    upstream_port: 0,
                    _cl0_pad: 0,
                    route_id: route.id,
                    upstream_id: target_upstream,
                    ua_major_version,
                    rpc_msgs_or_flags: 0,
                    pipe: velda_core::PipeKind::BufferedH3H1,
                    internal_status: velda_core::InternalStatus::RateLimited,
                    method: log_method,
                    _reserved: 0,
                    ua_family,
                    ua_os,
                    url_len: log_url_len,
                    url_bytes: log_url_bytes,
                },
            );
        }
        return;
    }

    // ========================================================================
    // Upstream Forwarding & Pipe Strategy Dispatch
    // ========================================================================
    let has_upload_body = !req.body.is_empty()
        || req
            .headers
            .get(http::header::CONTENT_LENGTH)
            .is_some_and(|cl| cl.to_str().map(|s| s.trim() != "0").unwrap_or(false))
        || req
            .headers
            .get(http::header::TRANSFER_ENCODING)
            .is_some_and(|te| {
                te.as_bytes()
                    .windows(b"chunked".len())
                    .any(|w| w.eq_ignore_ascii_case(b"chunked"))
            });
    let is_sse = req.headers.get(http::header::ACCEPT).is_some_and(|a| {
        a.as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    });

    if let Some(mirror_id) = velda_plugin::target_mirror(target_mask)
        && mirror_id != target_upstream
        && let Some(mirror_up) = rt.upstreams.http.get(mirror_id)
    {
        crate::pipeline::mirror::dispatch_http_mirror(
            Arc::clone(mirror_up),
            req.method.clone(),
            req.uri.clone(),
            req.version,
            req.headers.clone(),
            req.body.clone(),
        );
    }

    let (outcome, pipe_kind): (PipeOutcome, PipeKind) = match upstream_target.as_ref() {
        HttpUpstream::Http3(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http3PipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedH3H3;
                global_metrics().record_stream_start(PIPE);
                let (mut resp, upstream_addr, completion) =
                    pipe::buffered_h3_h3::serve(&req, upstream).await;
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &req.state,
                        &req.headers,
                        &mut resp,
                    );
                }
                let bytes_tx = resp.body.len() as u64;
                let _ = responder.send_response(resp).await;
                (
                    PipeOutcome::http(req.body.len() as u64, bytes_tx, upstream_addr, completion),
                    PIPE,
                )
            }
            Http3PipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamH3H3;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_h3_h3::serve(&req, responder, upstream, plan).await,
                    PIPE,
                )
            }
            Http3PipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamH3H3;
                global_metrics().record_stream_start(PIPE);
                let (mut resp, upstream_addr, completion) =
                    pipe::client_stream_h3_h3::serve(&req, upstream).await;
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &req.state,
                        &req.headers,
                        &mut resp,
                    );
                }
                let bytes_tx = resp.body.len() as u64;
                let _ = responder.send_response(resp).await;
                (
                    PipeOutcome::http(req.body.len() as u64, bytes_tx, upstream_addr, completion),
                    PIPE,
                )
            }
            Http3PipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexH3H3;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_h3_h3::serve(&req, responder, upstream, plan).await,
                    PIPE,
                )
            }
        },
        HttpUpstream::Http2(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http2PipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedH3H2;
                global_metrics().record_stream_start(PIPE);
                let (mut resp, upstream_addr, completion) =
                    pipe::buffered_h3_h2::serve(&req, upstream).await;
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &req.state,
                        &req.headers,
                        &mut resp,
                    );
                }
                let bytes_tx = resp.body.len() as u64;
                let _ = responder.send_response(resp).await;
                (
                    PipeOutcome::http(req.body.len() as u64, bytes_tx, upstream_addr, completion),
                    PIPE,
                )
            }
            Http2PipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamH3H2;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_h3_h2::serve(&req, responder, upstream, plan).await,
                    PIPE,
                )
            }
            Http2PipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamH3H2;
                global_metrics().record_stream_start(PIPE);
                let (mut resp, upstream_addr, completion) =
                    pipe::client_stream_h3_h2::serve(&req, upstream).await;
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &req.state,
                        &req.headers,
                        &mut resp,
                    );
                }
                let bytes_tx = resp.body.len() as u64;
                let _ = responder.send_response(resp).await;
                (
                    PipeOutcome::http(req.body.len() as u64, bytes_tx, upstream_addr, completion),
                    PIPE,
                )
            }
            Http2PipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexH3H2;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_h3_h2::serve(&req, responder, upstream, plan).await,
                    PIPE,
                )
            }
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy.effective(has_upload_body, is_sse)
        {
            Http1PipeStrategy::Buffered => {
                const PIPE: PipeKind = PipeKind::BufferedH3H1;
                global_metrics().record_stream_start(PIPE);
                let (mut resp, upstream_addr, completion) =
                    pipe::buffered_h3_h1::serve(&req, upstream).await;
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &req.state,
                        &req.headers,
                        &mut resp,
                    );
                }
                let bytes_tx = resp.body.len() as u64;
                let _ = responder.send_response(resp).await;
                (
                    PipeOutcome::http(req.body.len() as u64, bytes_tx, upstream_addr, completion),
                    PIPE,
                )
            }
            Http1PipeStrategy::ServerStream => {
                const PIPE: PipeKind = PipeKind::ServerStreamH3H1;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::server_stream_h3_h1::serve(&req, responder, upstream, plan).await,
                    PIPE,
                )
            }
            Http1PipeStrategy::ClientStream => {
                const PIPE: PipeKind = PipeKind::ClientStreamH3H1;
                global_metrics().record_stream_start(PIPE);
                let (mut resp, upstream_addr, completion) =
                    pipe::client_stream_h3_h1::serve(&req, upstream).await;
                if let Some(post_hooks) = &plan.post_response_hooks {
                    velda_plugin::run_post_response_plugins(
                        post_hooks,
                        &req.state,
                        &req.headers,
                        &mut resp,
                    );
                }
                let bytes_tx = resp.body.len() as u64;
                let _ = responder.send_response(resp).await;
                (
                    PipeOutcome::http(req.body.len() as u64, bytes_tx, upstream_addr, completion),
                    PIPE,
                )
            }
            Http1PipeStrategy::Duplex => {
                const PIPE: PipeKind = PipeKind::DuplexH3H1;
                global_metrics().record_stream_start(PIPE);
                (
                    pipe::duplex_h3_h1::serve(&req, responder, upstream, plan).await,
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
            request_id: velda_core::request_id::entropy(req.state.request_id),
            ingress_time_nanos: ingress_nanos,
            duration_ns,
            peer_ip: req.state.client_ip,
            bytes_rx: outcome.bytes_rx.min(u32::MAX as u64) as u32,
            bytes_tx: outcome.bytes_tx.min(u32::MAX as u64) as u32,
            upstream_ip,
            upstream_port,
            _cl0_pad: 0,
            route_id: route.id,
            upstream_id: target_upstream,
            ua_major_version,
            rpc_msgs_or_flags: 0,
            pipe: pipe_kind,
            internal_status: outcome.internal_status,
            method: log_method,
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
    use super::*;
    use bytes::{BufMut, BytesMut};
    use velda_http3::proto::qpack::{
        H3ForwardingAddrs, H3IngressEnrich, decode_qpack_ingress, encode_literal_name_and_value,
        encode_prefixed_int, encode_string_literal,
    };

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x00);
        buf.put_u8(0x00);
        encode_prefixed_int(17, 6, 0xc0, &mut buf); // :method: GET
        encode_prefixed_int(23, 6, 0xc0, &mut buf); // :scheme: https
        encode_prefixed_int(0, 4, 0x50, &mut buf); // :authority
        encode_string_literal(b"quic.example.com", &mut buf);
        encode_prefixed_int(1, 4, 0x50, &mut buf); // :path
        encode_string_literal(b"/test", &mut buf);
        // Client attempts to spoof their IP, host, and SSL status
        encode_literal_name_and_value(b"x-forwarded-for", b"203.0.113.195", &mut buf);
        encode_literal_name_and_value(b"x-real-ip", b"203.0.113.195", &mut buf);
        encode_literal_name_and_value(b"x-forwarded-host", b"evil.attacker.com", &mut buf);
        encode_literal_name_and_value(b"x-forwarded-proto", b"http", &mut buf);
        encode_literal_name_and_value(b"connection", b"close", &mut buf);
        encode_literal_name_and_value(b"keep-alive", b"timeout=5", &mut buf);
        encode_literal_name_and_value(b"forwarded", b"for=203.0.113.195;proto=http", &mut buf);

        let peer_ip = velda_core::lut::ip::v4_to_u128(0xC000_021E);
        let local_ip = velda_core::lut::ip::v4_to_u128(0x0A00_0001);
        let local_port_be = 443u16.to_be();

        let dec = decode_qpack_ingress(
            &buf,
            H3IngressEnrich {
                request_id: HeaderValue::from_static("auth-req-id"),
                forwarding: Some(H3ForwardingAddrs {
                    peer_ip,
                    local_ip,
                    local_port_be,
                }),
            },
        )
        .unwrap();
        let headers = dec.headers;

        // Client spoofed values MUST be completely replaced with authoritative values
        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.30");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.30");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "443");
        assert_eq!(headers.get("x-forwarded-host").unwrap(), "quic.example.com");
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.30;proto=https;by=10.0.0.1;host=\"quic.example.com\""
        );
        // RFC 9114 hop-by-hop headers MUST be stripped
        assert!(headers.get("connection").is_none());
        assert!(headers.get("keep-alive").is_none());
    }

    #[test]
    fn test_connection_closed_detection() {
        assert!(velda_http3::outcome::Http3Outcome::ConnectionClosed.is_connection_closed());
        assert!(
            velda_http3::outcome::Http3Outcome::H3("H3_REQUEST_REJECTED".into())
                .is_connection_closed()
        );
        assert!(
            velda_http3::outcome::Http3Outcome::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_connection_closed()
        );

        // Payload size errors are not retryable
        assert!(!velda_http3::outcome::Http3Outcome::PayloadTooLarge(500).is_connection_closed());
    }

    #[test]
    fn test_h3_pipe_coverage() {
        // Verify all 12 pipeline strategies are declared and available
        let _ = pipe::buffered_h3_h1::serve;
        let _ = pipe::server_stream_h3_h1::serve;
        let _ = pipe::client_stream_h3_h1::serve;
        let _ = pipe::duplex_h3_h1::serve;

        let _ = pipe::buffered_h3_h2::serve;
        let _ = pipe::server_stream_h3_h2::serve;
        let _ = pipe::client_stream_h3_h2::serve;
        let _ = pipe::duplex_h3_h2::serve;

        let _ = pipe::buffered_h3_h3::serve;
        let _ = pipe::server_stream_h3_h3::serve;
        let _ = pipe::client_stream_h3_h3::serve;
        let _ = pipe::duplex_h3_h3::serve;
    }
}
