//! Layer 7 HTTP/2 Downstream Ingress Connection Engine.
//!
//! Handles downstream TLS handshake, ALPN validation, HTTP/2 connection accept loop,
//! anti-flood tracking, safe URI path normalization, in-memory route matching, and
//! stream dispatching to upstream pipeline strategies.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::L7Response;
use velda_http1::pipe::Http1PipeStrategy;
use velda_http2::Http2Error;
use velda_http2::config::Http2Config;
use velda_http2::pipe::Http2PipeStrategy;
use velda_http2::server::{
    Http2Responder, Http2ServerConnection, Http2ServerRequestHead, Http2StreamReceiver,
};
use velda_http3::pipe::Http3PipeStrategy;
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

                    // Route matching
                    let mut http_req = Http2RouteRequest::new(head.uri.path());
                    if let Some(h) =
                        velda_http2::server::header::extract_host(&head.headers, &head.uri)
                    {
                        http_req = http_req.with_host(h);
                    }
                    http_req = http_req.with_method(head.method.as_str());

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
                        );
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
                        );
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
        HttpUpstream::Http2(upstream) => match upstream.strategy {
            Http2PipeStrategy::Buffered => {
                pipe::buffered_h2_h2::serve(head, receiver, responder, meta, config, upstream)
                    .await;
            }
            Http2PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h2::serve(head, receiver, responder, meta, config, upstream)
                    .await;
            }
            Http2PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h2::serve(head, receiver, responder, meta, config, upstream)
                    .await;
            }
            Http2PipeStrategy::Duplex => {
                pipe::duplex_h2_h2::serve(head, receiver, responder, meta, config, upstream).await;
            }
        },
        HttpUpstream::Http1(upstream) => match upstream.strategy {
            Http1PipeStrategy::Buffered => {
                pipe::buffered_h2_h1::serve(head, receiver, responder, meta, upstream).await;
            }
            Http1PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h1::serve(head, receiver, responder, meta, upstream).await;
            }
            Http1PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h1::serve(head, receiver, responder, meta, upstream).await;
            }
            Http1PipeStrategy::Duplex => {
                pipe::duplex_h2_h1::serve(head, receiver, responder, meta, upstream).await;
            }
        },
        HttpUpstream::Http3(upstream) => match upstream.strategy {
            Http3PipeStrategy::Buffered => {
                pipe::buffered_h2_h3::serve(head, receiver, responder, meta, upstream).await;
            }
            Http3PipeStrategy::ServerStream => {
                pipe::server_stream_h2_h3::serve(head, receiver, responder, meta, upstream).await;
            }
            Http3PipeStrategy::ClientStream => {
                pipe::client_stream_h2_h3::serve(head, receiver, responder, meta, upstream).await;
            }
            Http3PipeStrategy::Duplex => {
                pipe::duplex_h2_h3::serve(head, receiver, responder, meta, upstream).await;
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
}
