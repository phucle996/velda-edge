//! Downstream HTTP/3 UDP Datagram & QUIC Ingress Engine.
//!
//! Handles UDP packet ingestion, 0-RTT anti-replay defense, request decoding,
//! route matching, overload shedding, and pipeline dispatching across all upstream backends.

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http1::Http1PipeStrategy;
use velda_http2::Http2PipeStrategy;
use velda_http3::Http3PipeStrategy;
use velda_http3::server::connection::OutgoingDatagram;
use velda_router::Http3RouteRequest;
use velda_transport::{Datagram, UdpSocket};

use super::engine::{Http3WorkerPool, get_or_init_h3_workers};
use super::pipe;
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

    let mut non_gso_batch: [(&[u8], std::net::SocketAddr); 64] = [(&[], packets[0].peer); 64];
    let mut batch_count = 0;

    for pkt in &packets {
        if let Some(seg) = pkt.segment_size
            && seg > 0
            && pkt.payload.len() > seg as usize
        {
            if batch_count > 0 {
                flush_non_gso_batch(socket, &non_gso_batch[..batch_count]).await;
                batch_count = 0;
            }

            if let Some(res) = socket.try_send_gso_aware(&pkt.payload, Some(seg), pkt.peer) {
                if let Err(e) = res {
                    tracing::warn!(
                        peer = %pkt.peer,
                        segment_size = seg,
                        error = %e,
                        "Failed to send outgoing QUIC packet via try_send_gso_aware"
                    );
                }
            } else if let Err(e) = socket
                .send_gso_aware(&pkt.payload, Some(seg), pkt.peer)
                .await
            {
                tracing::warn!(
                    peer = %pkt.peer,
                    segment_size = seg,
                    error = %e,
                    "Failed to send outgoing QUIC packet via send_gso_aware"
                );
            }
        } else {
            non_gso_batch[batch_count] = (&pkt.payload, pkt.peer);
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

async fn flush_non_gso_batch(socket: &UdpSocket, batch: &[(&[u8], std::net::SocketAddr)]) {
    let mut sent = socket.try_send_batch(batch);
    while sent < batch.len() {
        let (buf, target) = batch[sent];
        if let Err(e) = socket.send_to(buf, target).await {
            tracing::warn!(
                peer = %target,
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
    _config: velda_http3::Http3Config,
    runtime: &SharedRuntime,
    precomputed_workers: Option<&Http3WorkerPool>,
) {
    let peer = datagram.peer();
    let local_addr = datagram.local_addr();

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
            datagram.peer(),
            Some(datagram.local_addr().ip()),
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

    // 2. Process complete L7 requests concurrently without blocking other datagrams
    for req_event in requests {
        let worker = worker.clone();
        let lid = listener_id.clone();
        let runtime = runtime.clone();

        tokio::spawn(async move {
            tracing::debug!(
                method = %req_event.request.method,
                path = %req_event.request.path(),
                peer = %peer,
                early_data = req_event.is_early_data,
                "Decoded HTTP/3 request from UDP"
            );

            // Fast-Fail: 0-RTT Replay Attack Defense (RFC 8470)
            // Non-idempotent methods (POST, PUT, DELETE, PATCH) MUST NOT be processed in 0-RTT early data.
            let is_mutation = matches!(
                req_event.request.method,
                http::Method::POST | http::Method::PUT | http::Method::DELETE | http::Method::PATCH
            );
            if req_event.is_early_data && is_mutation {
                tracing::warn!(
                    listener = %lid,
                    method = %req_event.request.method,
                    path = %req_event.request.path(),
                    "Rejecting non-idempotent HTTP/3 request received in 0-RTT early data (RFC 8470)"
                );
                let responder = super::engine::Http3Responder::new(
                    req_event.handle,
                    req_event.stream_id,
                    worker.response_tx.clone(),
                );
                let _ = responder.send_response(make_too_early_response()).await;
                return;
            }

            let responder = super::engine::Http3Responder::new(
                req_event.handle,
                req_event.stream_id,
                worker.response_tx.clone(),
            );

            let mut req = req_event.request;
            if let Err(e) = velda_http3::server::path::normalize_path(&mut req.uri) {
                tracing::warn!(
                    listener = %lid,
                    error = %e,
                    path = %req.path(),
                    "Rejecting HTTP/3 request with unsafe URI path"
                );
                let _ = responder.send_response(make_bad_request_response()).await;
                return;
            }

            velda_http3::server::header::enrich_headers(
                &mut req.headers,
                &req.uri,
                peer,
                local_addr,
            );

            process_http3_request(&req, &lid, &runtime, responder).await;
        });
    }
}

/// Dispatches an HTTP/3 request through `Http3Router` and forwards to upstream backend.
async fn process_http3_request(
    req: &L7Request,
    listener_id: &str,
    runtime: &SharedRuntime,
    responder: super::engine::Http3Responder,
) {
    let host = velda_http3::server::header::extract_host(&req.headers, &req.uri);
    let mut http_req = Http3RouteRequest::new(req.path());
    if let Some(h) = host {
        http_req = http_req.with_host(h);
    }
    http_req = http_req.with_method(req.method.as_str());

    let rt = runtime.load();
    let Some(route) = rt.router.route_http3(listener_id, &http_req) else {
        tracing::debug!(
            listener = %listener_id,
            path = %req.path(),
            host = ?host,
            method = %req.method,
            "No HTTP/3 route matched"
        );
        let _ = responder.send_response(make_not_found_response()).await;
        return;
    };

    let Some(upstream_target) = rt.upstreams.http.get(&route.upstream_name) else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No HTTP upstream configured"
        );
        let _ = responder
            .send_response(make_service_unavailable_response())
            .await;
        return;
    };

    // Resource Saturation Circuit Breaker (Overload Protection)
    let overload_lvl = rt.overload.level();
    if overload_lvl.is_shedding() {
        tracing::warn!(
            route = %route.id,
            upstream = %route.upstream_name,
            overload = ?overload_lvl,
            "Shedding HTTP/3 request due to memory saturation"
        );
        let _ = responder.send_response(make_overload_response()).await;
        return;
    }

    // ========================================================================
    // Upstream Forwarding & Pipe Strategy Dispatch
    // ========================================================================
    match upstream_target.as_ref() {
        HttpUpstream::Http3(upstream) => match upstream.strategy {
            Http3PipeStrategy::Buffered => {
                let resp = pipe::buffered_h3_h3::serve(req, upstream).await;
                let _ = responder.send_response(resp).await;
            }
            Http3PipeStrategy::ServerStream => {
                pipe::server_stream_h3_h3::serve(req, responder, upstream).await;
            }
            Http3PipeStrategy::ClientStream => {
                let resp = pipe::client_stream_h3_h3::serve(req, upstream).await;
                let _ = responder.send_response(resp).await;
            }
            Http3PipeStrategy::Duplex => {
                pipe::duplex_h3_h3::serve(req, responder, upstream).await;
            }
        },
        HttpUpstream::Http2(upstream) => match upstream.strategy {
            Http2PipeStrategy::Buffered => {
                let resp = pipe::buffered_h3_h2::serve(req, upstream).await;
                let _ = responder.send_response(resp).await;
            }
            Http2PipeStrategy::ServerStream => {
                pipe::server_stream_h3_h2::serve(req, responder, upstream).await;
            }
            Http2PipeStrategy::ClientStream => {
                let resp = pipe::client_stream_h3_h2::serve(req, upstream).await;
                let _ = responder.send_response(resp).await;
            }
            Http2PipeStrategy::Duplex => {
                pipe::duplex_h3_h2::serve(req, responder, upstream).await;
            }
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy {
            Http1PipeStrategy::Buffered => {
                let resp = pipe::buffered_h3_h1::serve(req, upstream).await;
                let _ = responder.send_response(resp).await;
            }
            Http1PipeStrategy::ServerStream => {
                pipe::server_stream_h3_h1::serve(req, responder, upstream).await;
            }
            Http1PipeStrategy::ClientStream => {
                let resp = pipe::client_stream_h3_h1::serve(req, upstream).await;
                let _ = responder.send_response(resp).await;
            }
            Http1PipeStrategy::Duplex => {
                pipe::duplex_h3_h1::serve(req, responder, upstream).await;
            }
        },
    }
}

#[cold]
#[inline(never)]
fn make_too_early_response() -> L7Response {
    L7Response::from_bytes(
        StatusCode::TOO_EARLY,
        Bytes::from_static(b"425 Too Early: non-idempotent request rejected in 0-RTT early data\n"),
    )
    .with_header(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    )
}

#[cold]
#[inline(never)]
fn make_bad_request_response() -> L7Response {
    L7Response::from_bytes(
        StatusCode::BAD_REQUEST,
        Bytes::from_static(b"400 Bad Request: unsafe or invalid URI path\n"),
    )
    .with_header(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    )
}

#[cold]
#[inline(never)]
fn make_not_found_response() -> L7Response {
    L7Response::from_bytes(
        StatusCode::NOT_FOUND,
        Bytes::from_static(b"404 Not Found: no matching route\n"),
    )
    .with_header(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    )
}

#[cold]
#[inline(never)]
fn make_service_unavailable_response() -> L7Response {
    L7Response::from_bytes(
        StatusCode::SERVICE_UNAVAILABLE,
        Bytes::from_static(b"503 Service Unavailable: upstream not configured\n"),
    )
    .with_header(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    )
}

#[cold]
#[inline(never)]
fn make_overload_response() -> L7Response {
    L7Response::from_bytes(
        StatusCode::TOO_MANY_REQUESTS,
        Bytes::from_static(b"429 Too Many Requests\n"),
    )
    .with_header(http::header::RETRY_AFTER, HeaderValue::from_static("1"))
    .with_header(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::net::SocketAddr;

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

        let peer: SocketAddr = "192.0.2.30:60000".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:443".parse().unwrap();

        let mut req = L7Request::new(
            http::Method::GET,
            "https://quic.example.com/test".parse().unwrap(),
            http::Version::HTTP_3,
            headers,
            velda_core::Body::Empty,
        );

        velda_http3::server::header::enrich_headers(&mut req.headers, &req.uri, peer, local);
        let headers = req.headers;

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
        assert!(velda_http3::Http3Error::ConnectionClosed.is_connection_closed());
        assert!(velda_http3::Http3Error::H3("H3_REQUEST_REJECTED".into()).is_connection_closed());
        assert!(
            velda_http3::Http3Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_connection_closed()
        );

        // Payload size errors are not retryable
        assert!(!velda_http3::Http3Error::PayloadTooLarge(500).is_connection_closed());
    }
}
