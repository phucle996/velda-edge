//! Downstream HTTP/3 UDP Datagram & QUIC Ingress Engine.
//!
//! Handles UDP packet ingestion, 0-RTT anti-replay defense, request decoding,
//! route matching, overload shedding, and pipeline dispatching across all upstream backends.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http1::Http1PipeStrategy;
use velda_http2::Http2PipeStrategy;
use velda_http3::pipe::Http3PipeStrategy;
use velda_router::Http3RouteRequest;
use velda_transport::{Datagram, UdpSocket};

use super::engine::get_or_init_h3_engine_for_peer;
use super::pipe;
use crate::runtime::SharedRuntime;
use crate::upstream::HttpUpstream;

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
) {
    let peer = datagram.peer();
    let local_addr = datagram.local_addr();

    let Some(engine_lock) = get_or_init_h3_engine_for_peer(&listener_id, peer, runtime) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "Received UDP datagram, but no HTTP/3 engine is compiled"
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

    // 2. Process complete L7 requests concurrently without blocking other datagrams
    for req_event in requests {
        let socket = socket.clone();
        let engine_lock = engine_lock.clone();
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
                let too_early = L7Response::from_bytes(
                    StatusCode::TOO_EARLY,
                    b"425 Too Early: non-idempotent request rejected in 0-RTT early data\n"
                        .to_vec(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                let resp_now = std::time::Instant::now();
                let resp_pkts = {
                    let mut engine = engine_lock.lock().await;
                    engine.send_response(
                        resp_now,
                        req_event.handle,
                        req_event.stream_id,
                        &too_early,
                    )
                };
                if let Ok(pkts) = resp_pkts {
                    for pkt in pkts {
                        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
                    }
                }
                return;
            }

            let mut req = req_event.request;
            if let Err(e) = velda_http3::server::path::normalize_path(&mut req.uri) {
                tracing::warn!(
                    listener = %lid,
                    error = %e,
                    path = %req.path(),
                    "Rejecting HTTP/3 request with unsafe URI path"
                );
                let bad_req = L7Response::from_bytes(
                    StatusCode::BAD_REQUEST,
                    b"400 Bad Request: unsafe or invalid URI path\n".to_vec(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                let resp_now = std::time::Instant::now();
                let resp_pkts = {
                    let mut engine = engine_lock.lock().await;
                    engine.send_response(resp_now, req_event.handle, req_event.stream_id, &bad_req)
                };
                if let Ok(pkts) = resp_pkts {
                    for pkt in pkts {
                        let _ = socket.send_to(&pkt.payload, pkt.peer).await;
                    }
                }
                return;
            }

            velda_http3::server::header::enrich_headers(
                &mut req.headers,
                &req.uri,
                peer,
                local_addr,
            );

            let response = process_http3_request(&req, &lid, &runtime).await;
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

/// Dispatches an HTTP/3 request through `Http3Router` and forwards to upstream backend.
pub async fn process_http3_request(
    req: &L7Request,
    listener_id: &str,
    runtime: &SharedRuntime,
) -> L7Response {
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
        return L7Response::from_bytes(
            StatusCode::NOT_FOUND,
            b"404 Not Found: no matching route\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    let Some(upstream_target) = rt.upstreams.http.get(&route.upstream_name) else {
        tracing::error!(
            listener = %listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No HTTP upstream configured"
        );
        return L7Response::from_bytes(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable: upstream not configured\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
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
        return L7Response::from_bytes(
            StatusCode::TOO_MANY_REQUESTS,
            b"429 Too Many Requests\n".to_vec(),
        )
        .with_header(http::header::RETRY_AFTER, HeaderValue::from_static("1"))
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    }

    // ========================================================================
    // Upstream Forwarding & Pipe Strategy Dispatch
    // ========================================================================
    match upstream_target.as_ref() {
        HttpUpstream::Http3(upstream) => match upstream.strategy {
            Http3PipeStrategy::Buffered => pipe::buffered_h3_h3::serve(req, upstream).await,
            Http3PipeStrategy::ServerStream => {
                pipe::server_stream_h3_h3::serve(req, upstream).await
            }
            Http3PipeStrategy::ClientStream => {
                pipe::client_stream_h3_h3::serve(req, upstream).await
            }
            Http3PipeStrategy::Duplex => pipe::duplex_h3_h3::serve(req, upstream).await,
        },
        HttpUpstream::Http2(upstream) => match upstream.strategy {
            Http2PipeStrategy::Buffered => pipe::buffered_h3_h2::serve(req, upstream).await,
            Http2PipeStrategy::ServerStream => {
                pipe::server_stream_h3_h2::serve(req, upstream).await
            }
            Http2PipeStrategy::ClientStream => {
                pipe::client_stream_h3_h2::serve(req, upstream).await
            }
            Http2PipeStrategy::Duplex => pipe::duplex_h3_h2::serve(req, upstream).await,
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy {
            Http1PipeStrategy::Buffered => pipe::buffered_h3_h1::serve(req, upstream).await,
            Http1PipeStrategy::ServerStream => {
                pipe::server_stream_h3_h1::serve(req, upstream).await
            }
            Http1PipeStrategy::ClientStream => {
                pipe::client_stream_h3_h1::serve(req, upstream).await
            }
            Http1PipeStrategy::Duplex => pipe::duplex_h3_h1::serve(req, upstream).await,
        },
    }
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
