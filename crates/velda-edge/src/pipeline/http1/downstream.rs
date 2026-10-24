//! Layer 7 HTTP/1.1 Downstream Ingress Connection Engine.
//!
//! Handles downstream TLS handshake, ALPN validation, HTTP/1.1 connection accept loop,
//! Slowloris protection, URI path normalization, in-memory route matching, and
//! stream dispatching to upstream pipeline strategies.

use std::sync::Arc;

use crate::pipeline::X_REQUEST_ID;
use http::StatusCode;
use http::header::HeaderValue;
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::{
    Http1BodyFraming, Http1Config, Http1Error, Http1PipeStrategy, Http1ServerConnection,
    Http1ServerResponse,
};
use velda_router::Http1RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use super::pipe;
use crate::pipeline::DownstreamMeta;
use crate::runtime::SharedRuntime;
use crate::upstream::HttpUpstream;
use velda_core::{Action, L7Request};
use velda_http2::Http2PipeStrategy;
use velda_http3::Http3PipeStrategy;

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
    alt_svc: Option<http::HeaderValue>,
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
                // RFC 9112 Section 2: Dedicated HTTP/1.1 over TLS listener strictly requires ALPN "http/1.1".
                let alpn = TlsServerEngine::extract_alpn(&tls_stream);

                if alpn.as_deref() != Some("http/1.1") {
                    tracing::warn!(
                        listener = %listener_id,
                        peer = %peer,
                        expected = "http/1.1",
                        actual = ?alpn,
                        "Dropping connection: HTTP/1.1 over TLS strictly requires ALPN 'http/1.1'"
                    );
                    return;
                }

                tracing::debug!(
                    listener = %listener_id,
                    peer = %peer,
                    alpn = ?alpn,
                    "Downstream TLS handshake succeeded for HTTP/1.1"
                );

                let meta = DownstreamMeta::new(listener_id, peer, local_addr, true, alt_svc);
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
        let meta = DownstreamMeta::new(listener_id, peer, local_addr, false, alt_svc);
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
    conn.set_alt_svc(meta.alt_svc_header.clone());
    let mut requests_served: u32 = 0;

    loop {
        // Phase 1: decode request head (fail-fast, header inspection, Slowloris protection)
        let (req_head, framing) = match conn.next_request_head().await {
            Ok(Some(parts)) => parts,
            Ok(None) => break,
            Err(Http1Error::Timeout) => {
                tracing::debug!(
                    listener = %meta.listener_id,
                    timeout_ms = config.header_read_timeout_ms,
                    "HTTP/1.1 request head read timed out (Slowloris protection)"
                );
                let (_req_id, req_id_header) = velda_core::next_request_id();
                conn.set_request_id(Some(req_id_header));
                conn.mark_close();
                let _ = conn.send_early_error(StatusCode::REQUEST_TIMEOUT).await;
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
                let (_req_id, req_id_header) = velda_core::next_request_id();
                conn.set_request_id(Some(req_id_header));
                conn.mark_close();
                let _ = conn.send_early_error(status).await;
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

        // [INGRESS W3C REQUEST ID GENERATION]
        // Authoritatively generate W3C Request ID from thread-local pool (~1-2ns).
        // Overwrite any client-supplied x-request-id to enforce zero-trust Edge boundary.
        let (_request_id, req_id_header) = velda_core::next_request_id();
        conn.set_request_id(Some(req_id_header.clone()));
        let mut req_head = req_head;
        req_head.headers.insert(X_REQUEST_ID, req_id_header.clone());

        // Enforce Ingress Streaming Policy:
        // If client sends chunked upload but listener has streaming.client disabled, reject immediately!
        if framing == Http1BodyFraming::Chunked && !streaming.client {
            tracing::warn!(
                listener = %meta.listener_id,
                "Rejecting chunked request: streaming upload is disabled on this listener"
            );
            conn.mark_close();
            let _ = conn.send_early_error(StatusCode::FORBIDDEN).await;
            conn.lingering_close().await;
            break;
        }

        if let Err(e) = velda_http1::server::path::normalize_path(&mut req_head.uri) {
            tracing::warn!(
                listener = %meta.listener_id,
                error = %e,
                path = %req_head.uri.path(),
                "Rejecting HTTP/1.1 request with unsafe URI path"
            );
            conn.mark_close();
            let _ = conn.send_early_error(StatusCode::BAD_REQUEST).await;
            conn.lingering_close().await;
            break;
        }

        // Authoritatively enrich proxy forwarding headers (x-forwarded-*, x-real-ip)
        // directly from downstream connection metadata BEFORE route matching and pre_route plugins.
        velda_http1::server::header::enrich_headers_cached(
            &mut req_head.headers,
            &req_head.uri,
            &meta.client_ip_header,
            &meta.client_port_header,
            meta.is_tls,
        );

        // ========================================================================
        // [PHASE 3: Narrow Waist Router Lookup]
        // Zero-IO in-memory route matching: (listener_id, req_head) -> upstream_id
        // ========================================================================
        let mut http_req = Http1RouteRequest::new(req_head.uri.path());
        if let Some(h) = velda_http1::server::header::extract_host(&req_head.headers, &req_head.uri)
        {
            http_req = http_req.with_host(h);
        }
        http_req = http_req.with_method(req_head.method.as_str());

        let route = rt
            .router
            .http1_listener(&meta.listener_id)
            .and_then(|lr| lr.route(&http_req));
        let Some(route) = route else {
            tracing::debug!(
                listener = %meta.listener_id,
                path = %req_head.uri.path(),
                host = ?velda_http1::server::header::extract_host(&req_head.headers, &req_head.uri),
                method = %req_head.method,
                "No HTTP/1.1 route matched"
            );
            conn.mark_close();
            let _ = conn.send_early_error(StatusCode::NOT_FOUND).await;
            conn.lingering_close().await;
            break;
        };

        let mut target_upstream: Arc<str> = Arc::from(route.upstream_name.as_str());

        let plan = rt.plugins.get_plan(route.id);
        if !plan.is_empty() {
            let mut short_circuit_resp = None;

            if plan.pre_route_hooks.is_some() || plan.pre_upstream_hooks.is_some() {
                // Optimization: Construct L7Request ONCE across both pre_route and pre_upstream phases
                // to eliminate redundant URI/Method cloning and HeaderMap swapping.
                let mut l7_req = L7Request::new(
                    req_head.method.clone(),
                    req_head.uri.clone(),
                    req_head.version,
                    std::mem::take(&mut req_head.headers),
                    velda_core::Body::Empty,
                );

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
                        Action::Reject(reason) => {
                            // FAST-FAIL INVARIANT: Rejections terminate immediately at the Edge boundary.
                            // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                            // preserving only Bit-1 bypass headers and Cache-Control: no-store.
                            let mut resp = velda_http1::server::precompiled_reject_response(
                                reason,
                                Some(&req_id_header),
                            );
                            for (name, val) in l7_req.headers.bypass_headers() {
                                resp.headers.insert(name.clone(), val.clone());
                            }
                            resp.headers.set_request_id(req_id_header.clone());
                            resp.headers.insert(
                                http::header::CACHE_CONTROL,
                                HeaderValue::from_static("no-store"),
                            );

                            let mut server_resp = Http1ServerResponse::from_l7_response(resp);
                            meta.inject_response_headers(&mut server_resp.headers);
                            if framing == Http1BodyFraming::Empty && !conn.close_requested {
                                let _ = conn.send_response(&server_resp).await;
                                continue;
                            } else {
                                conn.mark_close();
                                let _ = conn.send_response(&server_resp).await;
                                conn.lingering_close().await;
                                break;
                            }
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
                        Action::Reject(reason) => {
                            // FAST-FAIL INVARIANT: Rejections terminate immediately at the Edge boundary.
                            // Strictly bypasses post_response_hooks (anti-CPDoS & Zero-DDoS CPU burn),
                            // preserving only Bit-1 bypass headers and Cache-Control: no-store.
                            let mut resp = velda_http1::server::precompiled_reject_response(
                                reason,
                                Some(&req_id_header),
                            );
                            for (name, val) in l7_req.headers.bypass_headers() {
                                resp.headers.insert(name.clone(), val.clone());
                            }
                            resp.headers.set_request_id(req_id_header.clone());
                            resp.headers.insert(
                                http::header::CACHE_CONTROL,
                                HeaderValue::from_static("no-store"),
                            );

                            let mut server_resp = Http1ServerResponse::from_l7_response(resp);
                            meta.inject_response_headers(&mut server_resp.headers);
                            if framing == Http1BodyFraming::Empty && !conn.close_requested {
                                let _ = conn.send_response(&server_resp).await;
                                continue;
                            } else {
                                conn.mark_close();
                                let _ = conn.send_response(&server_resp).await;
                                conn.lingering_close().await;
                                break;
                            }
                        }
                        Action::Delay { .. } => unreachable!(),
                    }
                }

                if let Some(mut resp) = short_circuit_resp {
                    resp.headers.set_request_id(req_id_header.clone());
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
                            &l7_req.headers,
                            &mut resp,
                        );
                    }
                    let mut server_resp = Http1ServerResponse::from_l7_response(resp);
                    meta.inject_response_headers(&mut server_resp.headers);
                    if framing == Http1BodyFraming::Empty && !conn.close_requested {
                        let _ = conn.send_response(&server_resp).await;
                        continue;
                    } else {
                        conn.mark_close();
                        let _ = conn.send_response(&server_resp).await;
                        conn.lingering_close().await;
                        break;
                    }
                }

                req_head.headers = l7_req.headers.into();
                req_head.uri = l7_req.uri;
                req_head.method = l7_req.method;
            }
        }

        let Some(upstream_target) = rt.upstreams.http.get(target_upstream.as_ref()) else {
            tracing::error!(
                listener = %meta.listener_id,
                route = %route.id,
                upstream = %target_upstream,
                "No HTTP upstream configured"
            );
            conn.mark_close();
            let _ = conn.send_early_error(StatusCode::SERVICE_UNAVAILABLE).await;
            conn.lingering_close().await;
            break;
        };

        // Resource Saturation Circuit Breaker (Overload Protection)
        let overload_lvl = rt.overload.level();
        if overload_lvl.is_shedding() {
            tracing::warn!(
                route = %route.id,
                upstream = %target_upstream,
                overload = ?overload_lvl,
                "Shedding HTTP/1.1 request due to memory saturation"
            );
            conn.mark_close();
            let _ = conn.send_early_error(StatusCode::TOO_MANY_REQUESTS).await;
            conn.lingering_close().await;
            break;
        }

        // ========================================================================
        // [PHASE 5: Upstream Forwarding & Pipe Dispatch]
        // ========================================================================
        let is_chunked = framing == Http1BodyFraming::Chunked;
        let is_sse = req_head.headers.get(http::header::ACCEPT).is_some_and(|a| {
            a.as_bytes()
                .windows(b"text/event-stream".len())
                .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
        });

        let pipe_result = match upstream_target.as_ref() {
            HttpUpstream::Http1(upstream) => {
                match upstream.strategy.effective(is_chunked, is_sse) {
                    Http1PipeStrategy::Buffered => {
                        pipe::buffered_h1_h1::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http1PipeStrategy::ServerStream => {
                        pipe::server_stream_h1_h1::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http1PipeStrategy::ClientStream => {
                        pipe::client_stream_h1_h1::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http1PipeStrategy::Duplex => {
                        pipe::duplex_h1_h1::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                }
            }
            HttpUpstream::Http2(upstream) => {
                match upstream.strategy.effective(is_chunked, is_sse) {
                    Http2PipeStrategy::Buffered => {
                        pipe::buffered_h1_h2::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http2PipeStrategy::ServerStream => {
                        pipe::server_stream_h1_h2::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http2PipeStrategy::ClientStream => {
                        pipe::client_stream_h1_h2::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http2PipeStrategy::Duplex => {
                        pipe::duplex_h1_h2::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                }
            }
            HttpUpstream::Http3(upstream) => {
                match upstream.strategy.effective(is_chunked, is_sse) {
                    Http3PipeStrategy::Buffered => {
                        pipe::buffered_h1_h3::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http3PipeStrategy::ServerStream => {
                        pipe::server_stream_h1_h3::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http3PipeStrategy::ClientStream => {
                        pipe::client_stream_h1_h3::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                    Http3PipeStrategy::Duplex => {
                        pipe::duplex_h1_h3::serve(
                            &mut conn, req_head, framing, upstream, &meta, plan,
                        )
                        .await
                    }
                }
            }
        };

        if let Err(e) = pipe_result {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                "HTTP/1.1 upstream stream pipe failed"
            );
            conn.mark_close();
            let _ = conn.send_early_error(StatusCode::BAD_GATEWAY).await;
            conn.lingering_close().await;
            break;
        }

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
    use super::*;
    use http::HeaderMap;
    use std::net::SocketAddr;
    use velda_http1::Http1ServerRequestHead;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
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

        assert_eq!(
            headers.get("x-forwarded-for").unwrap(),
            "203.0.113.195, 192.0.2.10"
        );
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

        assert!(!velda_http1::Http1Error::InvalidMethod("FOO".into()).is_stale_connection());
        assert!(!velda_http1::Http1Error::PayloadTooLarge(100).is_stale_connection());
    }

    #[test]
    fn test_h1_pipe_coverage() {
        let _ = pipe::buffered_h1_h1::serve::<tokio::io::DuplexStream>;
        let _ = pipe::server_stream_h1_h1::serve::<tokio::io::DuplexStream>;
        let _ = pipe::client_stream_h1_h1::serve::<tokio::io::DuplexStream>;
        let _ = pipe::duplex_h1_h1::serve::<tokio::io::DuplexStream>;

        let _ = pipe::buffered_h1_h2::serve::<tokio::io::DuplexStream>;
        let _ = pipe::server_stream_h1_h2::serve::<tokio::io::DuplexStream>;
        let _ = pipe::client_stream_h1_h2::serve::<tokio::io::DuplexStream>;
        let _ = pipe::duplex_h1_h2::serve::<tokio::io::DuplexStream>;

        let _ = pipe::buffered_h1_h3::serve::<tokio::io::DuplexStream>;
        let _ = pipe::server_stream_h1_h3::serve::<tokio::io::DuplexStream>;
        let _ = pipe::client_stream_h1_h3::serve::<tokio::io::DuplexStream>;
        let _ = pipe::duplex_h1_h3::serve::<tokio::io::DuplexStream>;
    }

    #[test]
    fn test_dynamic_strategy_resolution() {
        let strategy = Http1PipeStrategy::Duplex;

        // Standard REST request (neither chunked upload nor text/event-stream) -> Buffered
        assert_eq!(
            strategy.effective(false, false),
            Http1PipeStrategy::Buffered
        );

        // Client chunked upload on Duplex listener -> ClientStream
        assert_eq!(
            strategy.effective(true, false),
            Http1PipeStrategy::ClientStream
        );

        // Client SSE Accept header on Duplex listener -> ServerStream
        assert_eq!(
            strategy.effective(false, true),
            Http1PipeStrategy::ServerStream
        );

        // Both chunked upload and SSE -> Duplex
        assert_eq!(strategy.effective(true, true), Http1PipeStrategy::Duplex);
    }
}
