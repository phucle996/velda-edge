//! Layer 7 HTTP/1.1 pipeline: downstream stream worker, route evaluation, and upstream connection handoff.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http1"`.
//! Routes matched via `velda-router::Http1Router`, protocol lifecycle and wire streaming
//! piped via `velda-http1`.
//!
//! Follows strict architectural boundary: `velda-edge` orchestrates the ingress pipeline
//! (`accept head -> route -> target -> enrich -> connect -> pipe`), while all HTTP/1.1
//! wire protocol mechanics, hop-by-hop sanitization, and progressive streaming pumps
//! belong entirely inside `velda-http1`.

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::{
    Http1BodyFraming, Http1Config, Http1Error, Http1PipeStrategy, Http1ServerConnection,
    Http1ServerRequest, Http1ServerResponse, pipe_buffered, pipe_client_stream, pipe_duplex,
    pipe_server_stream,
};
use velda_router::Http1RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use std::sync::Arc;

use super::DownstreamMeta;
use crate::runtime::SharedRuntime;

/// Asynchronous stream worker dispatching incoming HTTP/1.1 connections.
///
/// Inspects the pre-compiled listener metadata to determine whether downstream
/// TLS termination is active. If TLS is required, negotiates the handshake, validates
/// ALPN against "http/1.1", and passes the encrypted stream to the HTTP/1.1 loop.
pub async fn handle_http1_stream(
    connection: Connection,
    listener_id: Arc<str>,
    config: Http1Config,
    tls_enabled: bool,
    streaming: velda_core::StreamingMode,
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
                    && alpn != "http/1.1"
                {
                    tracing::warn!(
                        listener = %listener_id,
                        peer = %peer,
                        expected = "http/1.1",
                        actual = alpn,
                        "Dropping connection due to protocol ALPN mismatch"
                    );
                    return;
                }

                let meta = DownstreamMeta::new(listener_id, peer, local_addr, true);
                run_http1_loop(tls_stream, meta, streaming, config, runtime).await;
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
        run_http1_loop(connection, meta, streaming, config, runtime).await;
    }
}

/// Core HTTP/1.1 downstream request-response loop decoupled from transport layer.
pub async fn run_http1_loop<IO>(
    stream: IO,
    meta: DownstreamMeta,
    streaming: velda_core::StreamingMode,
    config: Http1Config,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = Http1ServerConnection::new(stream, config);
    let mut requests_served: u32 = 0;

    loop {
        // Phase 1: decode request head (fail-fast, header inspection, Slowloris protection)
        let (mut head, framing) = match conn.next_request_head().await {
            Ok(Some(parts)) => parts,
            Ok(None) => break,
            Err(Http1Error::Timeout) => {
                tracing::debug!(
                    listener = %meta.listener_id,
                    timeout_ms = config.header_read_timeout_ms,
                    "HTTP/1.1 request head read timed out (Slowloris protection)"
                );
                let err_resp = Http1ServerResponse::from_bytes(
                    StatusCode::REQUEST_TIMEOUT,
                    b"408 Request Timeout: header read timeout exceeded\n".to_vec(),
                );
                conn.mark_close();
                let _ = conn.send_response(&err_resp).await;
                conn.lingering_close().await;
                break;
            }
            Err(e) => {
                tracing::debug!(error = %e, "HTTP/1.1 request head decode error");
                let status = match e {
                    Http1Error::HeaderTooLarge(_) | Http1Error::TooManyHeaders(_) => {
                        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
                    }
                    Http1Error::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
                    _ => StatusCode::BAD_REQUEST,
                };
                let err_resp =
                    Http1ServerResponse::from_bytes(status, format!("{status}\n").into_bytes());
                conn.mark_close();
                let _ = conn.send_response(&err_resp).await;
                conn.lingering_close().await;
                break;
            }
        };

        requests_served += 1;
        let reach_max_keepalive = requests_served >= config.max_keepalive_requests;
        if reach_max_keepalive {
            conn.mark_close();
        }
        let rt = runtime.load();

        // Enforce Ingress Streaming Policy (Option A):
        // If client sends chunked upload but listener has streaming.client disabled, reject immediately!
        if framing == Http1BodyFraming::Chunked && !streaming.client {
            tracing::warn!(
                listener = %meta.listener_id,
                "Rejecting chunked request: streaming upload is disabled on this listener"
            );
            let rejected = Http1ServerResponse::from_bytes(
                StatusCode::FORBIDDEN,
                b"403 Forbidden: streaming upload is disabled on this listener\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&rejected).await;
            conn.lingering_close().await;
            break;
        }

        if let Err(e) = velda_http1::server::path::normalize_path(&mut head.uri) {
            tracing::warn!(
                listener = %meta.listener_id,
                error = %e,
                path = %head.uri.path(),
                "Rejecting HTTP/1.1 request with unsafe URI path"
            );
            let bad_req = Http1ServerResponse::from_bytes(
                StatusCode::BAD_REQUEST,
                b"400 Bad Request: unsafe or invalid URI path\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&bad_req).await;
            conn.lingering_close().await;
            break;
        }

        // ========================================================================
        // [PHASE 2: Pre-Route Hook Placeholder]
        // Flat workflow execution before entering the narrow Router waist.
        // Extension point for velda-plugin: IP whitelist/blacklist, DDoS shield,
        // and early path/header inspection.
        // Contract: Action::Continue | Action::Respond(L7Response) | Action::Reject
        // ========================================================================

        let mut http_req = Http1RouteRequest::new(head.uri.path());
        if let Some(h) = velda_http1::server::header::extract_host(&head.headers, &head.uri) {
            http_req = http_req.with_host(h);
        }
        http_req = http_req.with_method(head.method.as_str());

        // ========================================================================
        // [PHASE 3: Narrow Waist Router Lookup]
        // Zero-IO in-memory route matching: (listener_id, req_head) -> upstream_id
        // ========================================================================
        let Some(route) = rt.router.route_http1(&meta.listener_id, &http_req) else {
            tracing::debug!(
                listener = %meta.listener_id,
                path = %head.uri.path(),
                host = ?velda_http1::server::header::extract_host(&head.headers, &head.uri),
                method = %head.method,
                "No HTTP/1.1 route matched"
            );
            let not_found = Http1ServerResponse::from_bytes(
                StatusCode::NOT_FOUND,
                b"404 Not Found: no matching route\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&not_found).await;
            conn.lingering_close().await;
            break;
        };

        let Some(upstream) = rt
            .upstreams
            .http
            .get(&route.upstream_name)
            .and_then(|u| u.as_http1())
        else {
            tracing::error!(
                listener = %meta.listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                "No HTTP/1.1 upstream configured"
            );
            let no_backend = Http1ServerResponse::from_bytes(
                StatusCode::SERVICE_UNAVAILABLE,
                b"503 Service Unavailable: upstream not configured\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&no_backend).await;
            conn.lingering_close().await;
            break;
        };

        // Resource Saturation Circuit Breaker (Overload Protection)
        let overload_lvl = rt.overload.level();
        if overload_lvl.is_shedding() {
            tracing::warn!(
                route = %route.id,
                upstream = %route.upstream_name,
                overload = ?overload_lvl,
                "Shedding HTTP/1.1 request due to memory saturation"
            );
            let shed_resp = Http1ServerResponse::from_bytes(
                StatusCode::TOO_MANY_REQUESTS,
                b"429 Too Many Requests\n".to_vec(),
            )
            .with_header(http::header::RETRY_AFTER, HeaderValue::from_static("1"))
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&shed_resp).await;
            conn.lingering_close().await;
            break;
        }

        // ========================================================================
        // [PHASE 4: Pre-Upstream Hook Placeholder]
        // Flat workflow execution after route/upstream resolution, before lease/forward.
        // Extension point for velda-plugin: JWT/OAuth2 verification, Distributed
        // Tracing span injection, and header mutation.
        // Contract: Action::Continue | Action::Respond(L7Response) | Action::Reject
        // ========================================================================

        let strategy = upstream.strategy;
        velda_http1::server::header::enrich_headers(
            &mut head.headers,
            &head.uri,
            meta.peer,
            meta.local_addr,
            meta.is_tls,
        );
        let cfg = *conn.config();

        // If downstream client sent `Expect: 100-continue`, transmit interim 100 Continue
        // response before body read/forwarding to unblock transmission (RFC 9110 §10.1.1).
        if head.is_expect_100_continue() && framing != Http1BodyFraming::Empty {
            if let Err(e) = conn.send_100_continue().await {
                tracing::debug!(error = %e, "Failed to send 100 Continue downstream");
                conn.lingering_close().await;
                break;
            }
            head.headers.remove(http::header::EXPECT);
        }

        // HTTP/1.1 Pipeline Invariant (RFC 9112 & AGENTS.md §2.3):
        // Stream mode dictates upstream lease lifecycle:
        // - Buffered & ServerStream: Downstream body is completely read into RAM first.
        //   Upstream connection is ONLY acquired when request is ready to forward,
        //   completely eliminating upstream pool starvation from slow downstream uploads.
        // - ClientStream & Duplex: Body is streamed in real-time to upstream, so upstream
        //   connection is acquired before streaming begins.
        let req_body = match strategy {
            Http1PipeStrategy::Buffered | Http1PipeStrategy::ServerStream => {
                match conn.read_body(framing).await {
                    Ok(b) => Some(b),
                    Err(Http1Error::Timeout) => {
                        let err_resp = Http1ServerResponse::from_bytes(
                            StatusCode::REQUEST_TIMEOUT,
                            b"408 Request Timeout: client body read idle timeout exceeded\n"
                                .to_vec(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        );
                        conn.mark_close();
                        let _ = conn.send_response(&err_resp).await;
                        conn.lingering_close().await;
                        break;
                    }
                    Err(e) => {
                        let err_resp = Http1ServerResponse::from_bytes(
                            StatusCode::BAD_REQUEST,
                            format!("400 Bad Request: {e}\n").into_bytes(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        );
                        conn.mark_close();
                        let _ = conn.send_response(&err_resp).await;
                        conn.lingering_close().await;
                        break;
                    }
                }
            }
            Http1PipeStrategy::ClientStream | Http1PipeStrategy::Duplex => None,
        };

        let mut upstream_lease = match upstream.acquire().await {
            Ok(lease) => lease,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    upstream = %route.upstream_name,
                    "Failed to acquire upstream HTTP/1.1 connection"
                );
                let err_resp = Http1ServerResponse::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                conn.mark_close();
                let _ = conn.send_response(&err_resp).await;
                conn.lingering_close().await;
                break;
            }
        };

        let pipe_result = match strategy {
            Http1PipeStrategy::Buffered => {
                let mut req =
                    Http1ServerRequest::from_parts(head, req_body.expect("buffered requires body"));
                let mut res = pipe_buffered(&mut conn, &mut req, &mut *upstream_lease, &cfg).await;

                // Self-Healing Retry (RFC 9112):
                if let Err(ref e) = res
                    && upstream_lease.is_reused()
                    && e.is_stale_connection()
                {
                    upstream_lease.mark_closed();
                    drop(upstream_lease);

                    tracing::debug!(
                        upstream = %route.upstream_name,
                        "Stale pooled HTTP/1.1 connection detected; self-healing with fresh connection"
                    );

                    match upstream.acquire_fresh().await {
                        Ok(mut fresh_lease) => {
                            res = pipe_buffered(&mut conn, &mut req, &mut *fresh_lease, &cfg).await;
                            if res.is_err() {
                                fresh_lease.mark_closed();
                            }
                        }
                        Err(fresh_err) => {
                            tracing::warn!(
                                error = %fresh_err,
                                upstream = %route.upstream_name,
                                "Failed to acquire fresh connection for self-healing retry"
                            );
                        }
                    }
                } else if res.is_err() {
                    upstream_lease.mark_closed();
                }
                res
            }
            Http1PipeStrategy::ServerStream => {
                let mut req = Http1ServerRequest::from_parts(
                    head,
                    req_body.expect("server_stream requires body"),
                );
                let mut res =
                    pipe_server_stream(&mut conn, &mut req, &mut *upstream_lease, &cfg).await;

                // Self-Healing Retry (RFC 9112):
                if let Err(ref e) = res
                    && upstream_lease.is_reused()
                    && e.is_stale_connection()
                {
                    upstream_lease.mark_closed();
                    drop(upstream_lease);

                    tracing::debug!(
                        upstream = %route.upstream_name,
                        "Stale pooled HTTP/1.1 connection detected; self-healing with fresh connection"
                    );

                    match upstream.acquire_fresh().await {
                        Ok(mut fresh_lease) => {
                            res = pipe_server_stream(&mut conn, &mut req, &mut *fresh_lease, &cfg)
                                .await;
                            if res.is_err() {
                                fresh_lease.mark_closed();
                            }
                        }
                        Err(fresh_err) => {
                            tracing::warn!(
                                error = %fresh_err,
                                upstream = %route.upstream_name,
                                "Failed to acquire fresh connection for self-healing retry"
                            );
                        }
                    }
                } else if res.is_err() {
                    upstream_lease.mark_closed();
                }
                res
            }
            Http1PipeStrategy::ClientStream => {
                let res =
                    pipe_client_stream(&mut conn, &mut head, &mut *upstream_lease, &cfg).await;
                if res.is_err() {
                    upstream_lease.mark_closed();
                }
                res
            }
            Http1PipeStrategy::Duplex => {
                let res = pipe_duplex(&mut conn, &mut head, &mut *upstream_lease, &cfg).await;
                if res.is_err() {
                    upstream_lease.mark_closed();
                }
                res
            }
        };

        if let Err(e) = pipe_result {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                strategy = ?strategy,
                "HTTP/1.1 upstream stream pipe failed"
            );
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            conn.lingering_close().await;
            break;
        }

        // ========================================================================
        // [PHASE 6: Post-Response Hook Placeholder]
        // Flat workflow execution after successful upstream response delivery.
        // Extension point for velda-plugin: CORS header injection, compression,
        // and audit access logging.
        // Contract: Action::Continue | Action::Respond(L7Response)
        // ========================================================================

        if reach_max_keepalive || conn.is_closed() {
            tracing::debug!(
                listener = %meta.listener_id,
                requests_served,
                max = config.max_keepalive_requests,
                "Closing HTTP/1.1 connection (reached max keepalive requests or connection closed)"
            );
            conn.lingering_close().await;
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use std::net::SocketAddr;
    use velda_http1::Http1ServerRequestHead;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
        // Client attempts to spoof their IP, host, and SSL status
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert(
            "forwarded",
            "for=203.0.113.195;proto=https".parse().unwrap(),
        );

        let peer: SocketAddr = "192.0.2.10:45678".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:8080".parse().unwrap();

        let mut head = Http1ServerRequestHead::new(
            http::Method::GET,
            http::Uri::from_static("http://api.example.com/test"),
            http::Version::HTTP_11,
            headers,
        );
        head.enrich_forwarded_headers(peer, local, false);
        let headers = head.headers;

        // Client spoofed values MUST be completely replaced with authoritative values
        assert_eq!(headers.get("x-forwarded-for").unwrap(), "192.0.2.10");
        assert_eq!(headers.get("x-real-ip").unwrap(), "192.0.2.10");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "http");
        assert_eq!(headers.get("x-forwarded-port").unwrap(), "8080");
        assert_eq!(headers.get("x-forwarded-host").unwrap(), "api.example.com");
        assert_eq!(
            headers.get("forwarded").unwrap(),
            "for=192.0.2.10;proto=http;by=10.0.0.1;host=\"api.example.com\""
        );
    }

    #[test]
    fn test_stale_connection_detection() {
        assert!(velda_http1::Http1Error::ConnectionClosed.is_stale_connection());
        assert!(
            velda_http1::Http1Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe"
            ))
            .is_stale_connection()
        );
        assert!(
            velda_http1::Http1Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset by peer"
            ))
            .is_stale_connection()
        );
        assert!(
            velda_http1::Http1Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected eof"
            ))
            .is_stale_connection()
        );

        // Protocol errors are not stale connections (must not be retried)
        assert!(!velda_http1::Http1Error::InvalidMethod("FOO".into()).is_stale_connection());
        assert!(!velda_http1::Http1Error::PayloadTooLarge(100).is_stale_connection());
    }
}
