//! Layer 7 HTTP/2 pipeline: downstream stream worker, route evaluation, and upstream forwarding.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http2"`.
//! Routes matched via `velda-router::Http2Router`, forwarded via `velda-http2`.

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::L7Response;
use velda_http2::Http2Error;
use velda_http2::config::Http2Config;
use velda_http2::pipe::{
    Http2PipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_http2::server::{
    Http2Responder, Http2ServerConnection, Http2ServerRequestHead, Http2StreamReceiver,
};
use velda_router::Http2RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use std::sync::Arc;

use super::DownstreamMeta;
use crate::pre_compile::upstream::{Http1Upstream, Http2Upstream, HttpUpstream};
use crate::runtime::SharedRuntime;

/// Asynchronous stream worker dispatching incoming HTTP/2 connections.
///
/// Inspects listener configuration to determine whether downstream TLS termination
/// is active. If TLS is required, negotiates handshake, validates ALPN against "http2",
/// and passes the encrypted stream to the HTTP/2 loop.
pub async fn handle_http2_stream(
    connection: Connection,
    listener_id: Arc<str>,
    config: Http2Config,
    tls_enabled: bool,
    runtime: SharedRuntime,
) {
    let rt = runtime.load();
    let peer = connection.peer();
    let local_addr = connection.local_addr();

    if tls_enabled {
        let Some(tls_server) = rt.tls_server.as_ref() else {
            tracing::error!(
                listener = %listener_id,
                peer = %peer,
                "TLS required for listener, but no TLS server engine is compiled; dropping connection"
            );
            return;
        };

        match tls_server.accept_with_timeout(connection).await {
            Ok(tls_stream) => {
                let handshake_info = TlsServerEngine::extract_handshake_info(&tls_stream);
                tracing::debug!(
                    listener = %listener_id,
                    peer = %peer,
                    sni = ?handshake_info.sni,
                    alpn = ?handshake_info.alpn,
                    "Downstream TLS handshake succeeded"
                );

                if let Some(alpn) = handshake_info.alpn.as_deref()
                    && alpn != "h2"
                {
                    tracing::warn!(
                        listener = %listener_id,
                        peer = %peer,
                        expected = "h2",
                        actual = alpn,
                        "Dropping connection due to protocol ALPN mismatch"
                    );
                    return;
                }

                let meta = DownstreamMeta::new(listener_id, peer, local_addr, true);
                run_http2_loop(tls_stream, meta, config, runtime).await;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    listener = %listener_id,
                    peer = %peer,
                    "Downstream TLS handshake failed"
                );
            }
        }
    } else {
        let meta = DownstreamMeta::new(listener_id, peer, local_addr, false);
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
    let config_arc = Arc::new(config);

    match Http2ServerConnection::handshake(stream, *config_arc).await {
        Ok(mut conn) => loop {
            let accept_result =
                tokio::time::timeout(timeout_duration, conn.accept_streaming_request()).await;
            match accept_result {
                Ok(Ok(Some((mut head, receiver, mut responder)))) => {
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

                    if let Err(e) = velda_http2::server::path::normalize_path(&mut head.uri) {
                        tracing::warn!(
                            listener = %meta.listener_id,
                            error = %e,
                            path = %head.uri.path(),
                            "Rejecting HTTP/2 request with unsafe URI path"
                        );
                        let bad_req = L7Response::from_bytes(
                            StatusCode::BAD_REQUEST,
                            b"400 Bad Request: unsafe or invalid URI path\n".to_vec(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        );
                        let _ = responder.send_response(&bad_req);
                        continue;
                    }

                    // ========================================================
                    // [PHASE 2: Pre-Route Hook Placeholder]
                    // Flat workflow execution before entering narrow Router waist.
                    // Extension point for velda-plugin: IP access control,
                    // rate limiting, and early security filtering.
                    // Contract: Action::Continue | Action::Respond(L7Response) | Action::Reject
                    // ========================================================

                    let mut http_req = Http2RouteRequest::new(head.uri.path());
                    if let Some(h) =
                        velda_http2::server::header::extract_host(&head.headers, &head.uri)
                    {
                        http_req = http_req.with_host(h);
                    }
                    http_req = http_req.with_method(head.method.as_str());

                    // ========================================================
                    // [PHASE 3: Narrow Waist Router Lookup]
                    // Zero-IO in-memory route matching: (listener_id, req_head) -> upstream_id
                    // ========================================================
                    let rt = runtime.load();
                    let matched_route = rt.router.route_http2(&meta.listener_id, &http_req);

                    // ========================================================
                    // FAST-PATH 404 EVALUATION (Zero Tokio Task Allocation)
                    //
                    // Non-matching routes (including misdirected gRPC requests
                    // or scanner path-fuzzing) are rejected immediately in-place
                    // without spawning a new task or allocating heap futures.
                    // ========================================================
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

                        let is_grpc = head
                            .headers
                            .get(CONTENT_TYPE)
                            .is_some_and(|ct| ct.as_bytes().starts_with(b"application/grpc"));

                        let body_text = if is_grpc {
                            b"404 Not Found: no matching HTTP/2 route (this listener serves HTTP/2 web traffic, not gRPC)\n".as_slice()
                        } else {
                            b"404 Not Found: no matching route\n".as_slice()
                        };

                        let not_found =
                            L7Response::from_bytes(StatusCode::NOT_FOUND, body_text.to_vec())
                                .with_header(
                                    CONTENT_TYPE,
                                    HeaderValue::from_static("text/plain; charset=utf-8"),
                                );

                        // If peer has not finished transmitting the request body,
                        // immediately issue RST_STREAM to terminate peer upload and
                        // free socket buffers instantly.
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&not_found);
                        } else {
                            let _ = responder.send_response(&not_found);
                        }
                        continue;
                    };

                    consecutive_not_founds = 0;

                    // Resolve upstream target directly within rt read lock (Zero second rt.load / zero second HashMap lookup)
                    let Some(upstream_target) =
                        rt.upstreams.http.get(&route.upstream_name).cloned()
                    else {
                        tracing::error!(
                            listener = %meta.listener_id,
                            route = %route.id,
                            upstream = %route.upstream_name,
                            "No healthy backend endpoints available for HTTP upstream"
                        );
                        let no_backend = L7Response::from_bytes(
                            StatusCode::SERVICE_UNAVAILABLE,
                            b"503 Service Unavailable: upstream not configured\n".to_vec(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        );
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&no_backend);
                        } else {
                            let _ = responder.send_response(&no_backend);
                        }
                        continue;
                    };

                    let meta_clone = Arc::clone(&meta);
                    let cfg_clone = Arc::clone(&config_arc);
                    tokio::spawn(async move {
                        serve_http2_stream(
                            head,
                            receiver,
                            responder,
                            meta_clone,
                            cfg_clone,
                            upstream_target,
                        )
                        .await;
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
                        tracing::debug!(error = %e, "HTTP/2 stream accept error");
                    }
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %meta.listener_id,
                        timeout_ms = config_arc.idle_timeout_ms,
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

/// Serves an individual HTTP/2 downstream multiplexed stream top-to-bottom.
async fn serve_http2_stream(
    head: Http2ServerRequestHead,
    receiver: Http2StreamReceiver,
    responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Arc<Http2Config>,
    upstream_target: Arc<HttpUpstream>,
) {
    match upstream_target.as_ref() {
        HttpUpstream::Http2(upstream) => {
            serve_http2_to_http2(head, receiver, responder, meta, config, upstream).await;
        }
        HttpUpstream::Http1(upstream) => {
            serve_http2_to_http1_bridge(head, receiver, responder, meta, upstream).await;
        }
        HttpUpstream::Http3(_) => {
            let mut responder = responder;
            let not_impl = L7Response::from_bytes(
                StatusCode::NOT_IMPLEMENTED,
                b"501 Not Implemented: HTTP/2 to HTTP/3 bridging not supported\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&not_impl);
        }
    }
}

/// Forwards an HTTP/2 downstream stream to an HTTP/2 upstream multiplexed backend.
async fn serve_http2_to_http2(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Arc<Http2Config>,
    upstream: &Arc<Http2Upstream>,
) {
    // ========================================================================
    // [PHASE 4: Pre-Upstream Hook Placeholder]
    // Flat workflow execution after route/upstream resolution, before stream lease.
    // Extension point for velda-plugin: JWT/OAuth2 token claims inspection,
    // tracing context propagation, and request header transformation.
    // Contract: Action::Continue | Action::Respond(L7Response) | Action::Reject
    // ========================================================================

    let strategy = upstream.strategy;
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // Fail-fast: Acquire upstream multiplexed connection before reading downstream body
    let (mut client, _lease) = match upstream.acquire(&config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            if !receiver.is_end_stream() {
                let _ = responder.send_response_and_cancel_upload(&err_resp);
            } else {
                let _ = responder.send_response(&err_resp);
            }
            return;
        }
    };

    let pipe_res = match strategy {
        Http2PipeStrategy::ClientStream => {
            if let Err(e) =
                pipe_client_stream(head, receiver, responder, &mut client, &config).await
            {
                tracing::warn!(
                    error = %e,
                    upstream = %upstream.id(),
                    "HTTP/2 upstream pipe failed"
                );
            }
            return;
        }
        Http2PipeStrategy::Duplex => {
            if let Err(e) = pipe_duplex(head, receiver, responder, &mut client, &config).await {
                tracing::warn!(
                    error = %e,
                    upstream = %upstream.id(),
                    "HTTP/2 upstream pipe failed"
                );
            }
            return;
        }
        Http2PipeStrategy::ServerStream => {
            let body = match receiver.consume_all().await {
                Ok(b) => b,
                Err(e) => {
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_REQUEST,
                        format!("400 Bad Request: {e}\n").into_bytes(),
                    )
                    .with_header(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    );
                    let _ = responder.send_response(&err_resp);
                    return;
                }
            };
            let mut res =
                pipe_server_stream(&head, &body, &mut responder, &mut client, &config).await;
            if let Err(ref e) = res
                && e.is_refused_or_goaway()
            {
                tracing::debug!(
                    upstream = %upstream.id(),
                    error = %e,
                    "HTTP/2 server-stream refused or connection closed; self-healing with fresh connection"
                );
                if let Ok((mut fresh_client, _fresh_lease)) = upstream.acquire_fresh(&config).await
                {
                    res = pipe_server_stream(
                        &head,
                        &body,
                        &mut responder,
                        &mut fresh_client,
                        &config,
                    )
                    .await;
                }
            }
            res
        }
        Http2PipeStrategy::Buffered => {
            let body = match receiver.consume_all().await {
                Ok(b) => b,
                Err(e) => {
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_REQUEST,
                        format!("400 Bad Request: {e}\n").into_bytes(),
                    )
                    .with_header(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    );
                    let _ = responder.send_response(&err_resp);
                    return;
                }
            };
            let mut res = pipe_buffered(&head, &body, &mut responder, &mut client, &config).await;
            if let Err(ref e) = res
                && e.is_refused_or_goaway()
            {
                tracing::debug!(
                    upstream = %upstream.id(),
                    error = %e,
                    "HTTP/2 buffered stream refused or connection closed; self-healing with fresh connection"
                );
                if let Ok((mut fresh_client, _fresh_lease)) = upstream.acquire_fresh(&config).await
                {
                    res = pipe_buffered(&head, &body, &mut responder, &mut fresh_client, &config)
                        .await;
                }
            }
            res
        }
    };

    // ========================================================================
    // [PHASE 6: Post-Response Hook Placeholder]
    // Flat workflow execution after successful upstream response delivery.
    // Extension point for velda-plugin: CORS header injection, compression,
    // and stream metrics recording.
    // Contract: Action::Continue | Action::Respond(L7Response)
    // ========================================================================

    if let Err(e) = pipe_res {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "HTTP/2 upstream pipe failed"
        );
        let err_resp = L7Response::from_bytes(
            StatusCode::BAD_GATEWAY,
            format!("502 Bad Gateway: {e}\n").into_bytes(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        let _ = responder.send_response(&err_resp);
    }
}

/// Bridges an incoming HTTP/2 downstream stream to an HTTP/1.1 upstream backend.
///
/// Implements RFC 9113 §8 (HTTP/2 to HTTP/1 translation):
/// - Decodes HTTP/2 HEADERS + DATA from downstream.
/// - Acquires an active connection from the HTTP/1 keep-alive pool.
/// - Forwards the request formatted as RFC 9112 text stream.
/// - Reads the backend response and frames it into HTTP/2 HEADERS + DATA frames to client.
async fn serve_http2_to_http1_bridge(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
) {
    let body = match receiver.consume_all().await {
        Ok(b) => b,
        Err(e) => {
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    let mut lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection for HTTP/2 bridge"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    let cfg = velda_http1::config::Http1Config::auto();
    let mut write_buf = bytes::BytesMut::with_capacity(cfg.upstream_write_base);
    let send_res = velda_http1::client::send_request_parts(
        &head.method,
        &head.uri,
        &head.headers,
        &body,
        &mut *lease,
        &mut write_buf,
    )
    .await;

    if let Err(e) = send_res {
        lease.mark_closed();
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to send bridged HTTP/1.1 request to upstream"
        );
        let err_resp = L7Response::from_bytes(
            StatusCode::BAD_GATEWAY,
            format!("502 Bad Gateway: {e}\n").into_bytes(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        let _ = responder.send_response(&err_resp);
        return;
    }

    let mut read_buf = bytes::BytesMut::with_capacity(cfg.upstream_read_capacity);
    loop {
        match tokio::io::AsyncReadExt::read_buf(&mut *lease, &mut read_buf).await {
            Ok(0) => {
                if let Ok(Some(resp)) = velda_http1::client::decode_response(&mut read_buf, &cfg) {
                    let l7_resp = resp.into_l7_response();
                    let _ = responder.send_response(&l7_resp);
                } else {
                    lease.mark_closed();
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        b"502 Bad Gateway: upstream closed connection prematurely\n".to_vec(),
                    );
                    let _ = responder.send_response(&err_resp);
                }
                break;
            }
            Ok(_) => match velda_http1::client::decode_response(&mut read_buf, &cfg) {
                Ok(Some(resp)) => {
                    let l7_resp = resp.into_l7_response();
                    let _ = responder.send_response(&l7_resp);
                    break;
                }
                Ok(None) => continue,
                Err(e) => {
                    lease.mark_closed();
                    tracing::warn!(
                        error = %e,
                        upstream = %upstream.id(),
                        "Failed to decode upstream HTTP/1.1 response"
                    );
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        format!("502 Bad Gateway: {e}\n").into_bytes(),
                    );
                    let _ = responder.send_response(&err_resp);
                    break;
                }
            },
            Err(e) => {
                lease.mark_closed();
                tracing::warn!(
                    error = %e,
                    upstream = %upstream.id(),
                    "Error reading upstream HTTP/1.1 response"
                );
                let err_resp = L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                );
                let _ = responder.send_response(&err_resp);
                break;
            }
        }
    }
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
        // Client attempts to spoof their IP, host, and SSL status
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

        // Client spoofed values MUST be completely replaced with authoritative values
        assert_eq!(head.headers.get("x-forwarded-for").unwrap(), "192.0.2.20");
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

        // Stream resets with other reasons are not safe to retry
        assert!(
            !velda_http2::Http2Error::StreamReset(h2::Reason::INTERNAL_ERROR)
                .is_refused_or_goaway()
        );
        assert!(!velda_http2::Http2Error::PayloadTooLarge(500).is_refused_or_goaway());
    }
}
