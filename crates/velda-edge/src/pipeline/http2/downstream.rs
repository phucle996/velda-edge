//! Layer 7 HTTP/2 Downstream Ingress Connection Engine.
//!
//! Handles downstream TLS handshake, ALPN validation, HTTP/2 connection accept loop,
//! anti-flood tracking, safe URI path normalization, in-memory route matching, and
//! stream dispatching to upstream pipeline strategies.

use std::sync::Arc;

use crate::pipeline::X_REQUEST_ID;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{Action, ErrorKind, L7Request, L7Response};
use velda_http1::Http1PipeStrategy;
use velda_http2::Http2Error;
use velda_http2::Http2PipeStrategy;
use velda_http2::config::Http2Config;
use velda_http2::server::{
    Http2Responder, Http2ServerConnection, Http2ServerRequestHead, Http2StreamReceiver,
};
use velda_http3::Http3PipeStrategy;
use velda_plugin::RoutePluginPlan;
use velda_router::Http2RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use super::pipe;
use crate::pipeline::DownstreamMeta;
use crate::runtime::SharedRuntime;
use crate::upstream::HttpUpstream;

/// Asynchronous stream worker dispatching incoming HTTP/2 connections.
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

    match Http2ServerConnection::handshake(stream, config).await {
        Ok(mut conn) => loop {
            let accept_result = if let Some(res) = conn.try_accept_streaming_request() {
                Ok(res)
            } else {
                tokio::time::timeout(timeout_duration, conn.accept_streaming_request()).await
            };
            match accept_result {
                Ok(Ok(Some((mut req_head, receiver, mut responder)))) => {
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

                    let (_request_id, req_id_header) = velda_core::next_request_id();
                    req_head.headers.insert(X_REQUEST_ID, req_id_header.clone());
                    responder = responder.with_request_id(Some(req_id_header.clone()));

                    if let Err(e) = velda_http2::server::path::normalize_path(&mut req_head.uri) {
                        tracing::warn!(
                            listener = %meta.listener_id,
                            error = %e,
                            path = %req_head.uri.path(),
                            "Rejecting HTTP/2 request with unsafe URI path"
                        );
                        let bad_req = L7Response::from_bytes(
                            StatusCode::BAD_REQUEST,
                            b"400 Bad Request: unsafe or invalid URI path\n".to_vec(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        )
                        .with_header(X_REQUEST_ID, req_id_header.clone());
                        let _ = responder.send_response(&bad_req);
                        continue;
                    }

                    // Route matching
                    let mut http_req = Http2RouteRequest::new(req_head.uri.path());
                    if let Some(h) =
                        velda_http2::server::header::extract_host(&req_head.headers, &req_head.uri)
                    {
                        http_req = http_req.with_host(h);
                    }
                    http_req = http_req.with_method(req_head.method.as_str());

                    let rt = runtime.load();
                    let matched_route = rt.router.route_http2(&meta.listener_id, &http_req);

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

                        let is_grpc = req_head
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
                                )
                                .with_header(X_REQUEST_ID, req_id_header.clone());

                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&not_found);
                        } else {
                            let _ = responder.send_response(&not_found);
                        }
                        continue;
                    };

                    consecutive_not_founds = 0;

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
                            b"503 Service Unavailable: no healthy upstream endpoints\n".to_vec(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        )
                        .with_header(X_REQUEST_ID, req_id_header.clone());
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&no_backend);
                        } else {
                            let _ = responder.send_response(&no_backend);
                        }
                        continue;
                    };

                    // Overload Check
                    let overload_lvl = rt.overload.level();
                    if overload_lvl.is_shedding() {
                        tracing::warn!(
                            route = %route.id,
                            upstream = %route.upstream_name,
                            overload = ?overload_lvl,
                            "Shedding HTTP/2 request due to memory saturation"
                        );
                        let shed_resp = L7Response::from_bytes(
                            StatusCode::TOO_MANY_REQUESTS,
                            b"429 Too Many Requests\n".to_vec(),
                        )
                        .with_header(http::header::RETRY_AFTER, HeaderValue::from_static("1"))
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        )
                        .with_header(X_REQUEST_ID, req_id_header.clone());
                        if !receiver.is_end_stream() {
                            let _ = responder.send_response_and_cancel_upload(&shed_resp);
                        } else {
                            let _ = responder.send_response(&shed_resp);
                        }
                        if overload_lvl.is_critical() && !is_draining {
                            is_draining = true;
                            conn.graceful_shutdown();
                        }
                        continue;
                    }

                    let plan = rt.plugins.get_plan(route.id).clone();
                    let meta_clone = Arc::clone(&meta);
                    let upstreams_table = Arc::new(rt.upstreams.http.clone());
                    let default_upstream_name: Arc<str> = Arc::from(route.upstream_name.as_str());
                    tokio::spawn(async move {
                        serve_http2_stream(
                            req_head,
                            receiver,
                            responder,
                            meta_clone,
                            config,
                            upstream_target,
                            default_upstream_name,
                            upstreams_table,
                            plan,
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
                        tracing::debug!(
                            listener = %meta.listener_id,
                            peer = %meta.peer,
                            error = %e,
                            "HTTP/2 stream accept error"
                        );
                    }
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %meta.listener_id,
                        timeout_ms = config.idle_timeout_ms,
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
#[allow(clippy::too_many_arguments)]
async fn serve_http2_stream(
    mut req_head: Http2ServerRequestHead,
    receiver: Http2StreamReceiver,
    responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Http2Config,
    default_upstream: Arc<HttpUpstream>,
    default_upstream_name: Arc<str>,
    http_upstreams: Arc<crate::upstream::SubUpstreamTable<HttpUpstream>>,
    plan: Arc<RoutePluginPlan>,
) {
    let mut target_upstream: Arc<str> = Arc::clone(&default_upstream_name);

    if !plan.is_empty() {
        let mut short_circuit_resp = None;

        if plan.pre_route_hooks.is_some() || plan.pre_upstream_hooks.is_some() {
            // Optimization: Construct L7Request ONCE across both pre_route and pre_upstream phases
            // to eliminate redundant URI/Method cloning and HeaderMap swapping.
            let mut l7_req = L7Request::new(
                req_head.method.clone(),
                req_head.uri.clone(),
                http::Version::HTTP_2,
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
                    Action::Reject(err) => {
                        let (status, body): (StatusCode, &'static [u8]) = match err.kind() {
                            ErrorKind::Rejected => (StatusCode::FORBIDDEN, b"403 Forbidden\n"),
                            ErrorKind::InvalidRequest => {
                                (StatusCode::BAD_REQUEST, b"400 Bad Request\n")
                            }
                            _ => (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                b"500 Internal Server Error\n",
                            ),
                        };
                        short_circuit_resp = Some(L7Response::from_bytes(status, body));
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
                    Action::Reject(err) => {
                        let (status, body): (StatusCode, &'static [u8]) = match err.kind() {
                            ErrorKind::Rejected => (StatusCode::FORBIDDEN, b"403 Forbidden\n"),
                            ErrorKind::InvalidRequest => {
                                (StatusCode::BAD_REQUEST, b"400 Bad Request\n")
                            }
                            _ => (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                b"500 Internal Server Error\n",
                            ),
                        };
                        short_circuit_resp = Some(L7Response::from_bytes(status, body));
                    }
                    Action::Delay { .. } => unreachable!(),
                }
            }

            req_head.headers = l7_req.headers;
            req_head.uri = l7_req.uri;
            req_head.method = l7_req.method;
        }

        if let Some(mut resp) = short_circuit_resp {
            if let Some(post_hooks) = &plan.post_response_hooks {
                velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut resp);
            }
            let mut responder = responder;
            if !receiver.is_end_stream() {
                let _ = responder.send_response_and_cancel_upload(&resp);
            } else {
                let _ = responder.send_response(&resp);
            }
            return;
        }
    }

    let upstream_target = if target_upstream.as_ref() == default_upstream_name.as_ref() {
        default_upstream
    } else if let Some(target) = http_upstreams.get(target_upstream.as_ref()) {
        target.clone()
    } else {
        tracing::error!(
            listener = %meta.listener_id,
            upstream = %target_upstream,
            "No HTTP upstream configured"
        );
        let resp = L7Response::from_bytes(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable: upstream not configured\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        let mut responder = responder;
        if !receiver.is_end_stream() {
            let _ = responder.send_response_and_cancel_upload(&resp);
        } else {
            let _ = responder.send_response(&resp);
        }
        return;
    };

    let is_chunked = !receiver.is_end_stream()
        && (req_head.headers.get(http::header::CONTENT_LENGTH).is_none()
            || req_head
                .headers
                .get(http::header::TRANSFER_ENCODING)
                .is_some_and(|te| {
                    te.as_bytes()
                        .windows(b"chunked".len())
                        .any(|w| w.eq_ignore_ascii_case(b"chunked"))
                }));
    let is_sse = req_head.headers.get(http::header::ACCEPT).is_some_and(|a| {
        a.as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    });

    match upstream_target.as_ref() {
        HttpUpstream::Http2(upstream) => match upstream.strategy.effective(is_chunked, is_sse) {
            Http2PipeStrategy::Buffered => {
                pipe::buffered_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await;
            }
            Http2PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await;
            }
            Http2PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await;
            }
            Http2PipeStrategy::Duplex => {
                pipe::duplex_h2_h2::serve(
                    req_head, receiver, responder, meta, config, upstream, &plan,
                )
                .await;
            }
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy.effective(is_chunked, is_sse) {
            Http1PipeStrategy::Buffered => {
                pipe::buffered_h2_h1::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await;
            }
            Http1PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h1::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await;
            }
            Http1PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h1::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await;
            }
            Http1PipeStrategy::Duplex => {
                pipe::duplex_h2_h1::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await;
            }
        },
        HttpUpstream::Http3(upstream) => match upstream.strategy.effective(is_chunked, is_sse) {
            Http3PipeStrategy::Buffered => {
                pipe::buffered_h2_h3::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await;
            }
            Http3PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h3::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await;
            }
            Http3PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h3::serve(
                    req_head, receiver, responder, meta, upstream, &plan,
                )
                .await;
            }
            Http3PipeStrategy::Duplex => {
                pipe::duplex_h2_h3::serve(req_head, receiver, responder, meta, upstream, &plan)
                    .await;
            }
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
        let mut head = Http2ServerRequestHead::new(
            http::Method::GET,
            "http://secure.example.com/api".parse().unwrap(),
            HeaderMap::new(),
            None,
        );
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

        assert!(
            !velda_http2::Http2Error::StreamReset(h2::Reason::INTERNAL_ERROR)
                .is_refused_or_goaway()
        );
        assert!(!velda_http2::Http2Error::PayloadTooLarge(500).is_refused_or_goaway());
    }

    #[test]
    fn test_host_header_extraction_and_injection() {
        let uri: http::Uri = "http://example.com:8080/path".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert!(!headers.contains_key(http::header::HOST));

        if let Some(host) = velda_http2::server::header::extract_host(&headers, &uri)
            && let Ok(hv) = HeaderValue::from_str(host)
        {
            headers.insert(http::header::HOST, hv);
        }

        assert_eq!(headers.get(http::header::HOST).unwrap(), "example.com:8080");
    }

    #[test]
    fn test_h2_pipe_coverage() {
        // Verify all 12 pipeline strategies are declared and available
        let _ = pipe::buffered_h2_h1::serve;
        let _ = pipe::server_stream_h2_h1::serve;
        let _ = pipe::client_stream_h2_h1::serve;
        let _ = pipe::duplex_h2_h1::serve;

        let _ = pipe::buffered_h2_h2::serve;
        let _ = pipe::server_stream_h2_h2::serve;
        let _ = pipe::client_stream_h2_h2::serve;
        let _ = pipe::duplex_h2_h2::serve;

        let _ = pipe::buffered_h2_h3::serve;
        let _ = pipe::server_stream_h2_h3::serve;
        let _ = pipe::client_stream_h2_h3::serve;
        let _ = pipe::duplex_h2_h3::serve;
    }

    #[test]
    fn test_h2_canary_target_missing_resolution() {
        use rustc_hash::FxHashMap;
        let default_upstream_name: Arc<str> = Arc::from("main_upstream");
        let target_upstream: Arc<str> = Arc::from("canary_v2_upstream");

        let http_upstreams: FxHashMap<String, Arc<HttpUpstream>> = FxHashMap::default();

        let resolved = if target_upstream.as_ref() == default_upstream_name.as_ref() {
            Some("default")
        } else if http_upstreams.contains_key(target_upstream.as_ref()) {
            Some("target")
        } else {
            None // Must reject with 503, never fallback to default!
        };

        assert!(resolved.is_none());
    }
}
