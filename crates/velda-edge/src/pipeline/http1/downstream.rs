//! Layer 7 HTTP/1.1 Downstream Ingress Connection Engine.
//!
//! Handles downstream TLS handshake, ALPN validation, HTTP/1.1 connection accept loop,
//! Slowloris protection, URI path normalization, in-memory route matching, and
//! stream dispatching to upstream pipeline strategies.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
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
use velda_http2::pipe::Http2PipeStrategy;
use velda_http3::pipe::Http3PipeStrategy;

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
        let (head, framing) = match conn.next_request_head().await {
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

        // Enforce Ingress Streaming Policy:
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

        let mut head = head;
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
        // [PHASE 3: Narrow Waist Router Lookup]
        // Zero-IO in-memory route matching: (listener_id, req_head) -> upstream_id
        // ========================================================================
        let mut http_req = Http1RouteRequest::new(head.uri.path());
        if let Some(h) = velda_http1::server::header::extract_host(&head.headers, &head.uri) {
            http_req = http_req.with_host(h);
        }
        http_req = http_req.with_method(head.method.as_str());

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

        let Some(upstream_target) = rt.upstreams.http.get(&route.upstream_name) else {
            tracing::error!(
                listener = %meta.listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                "No HTTP upstream configured"
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
        // [PHASE 5: Upstream Forwarding & Pipe Dispatch]
        // ========================================================================
        let pipe_result = match upstream_target.as_ref() {
            HttpUpstream::Http1(upstream) => match upstream.strategy {
                Http1PipeStrategy::Buffered => {
                    pipe::buffered_h1_h1::serve(&mut conn, head, framing, upstream, &meta).await
                }
                Http1PipeStrategy::ServerStream => {
                    pipe::server_stream_h1_h1::serve(&mut conn, head, framing, upstream, &meta)
                        .await
                }
                Http1PipeStrategy::ClientStream => {
                    pipe::client_stream_h1_h1::serve(&mut conn, head, framing, upstream, &meta)
                        .await
                }
                Http1PipeStrategy::Duplex => {
                    pipe::duplex_h1_h1::serve(&mut conn, head, framing, upstream, &meta).await
                }
            },
            HttpUpstream::Http2(upstream) => match upstream.strategy {
                Http2PipeStrategy::Buffered => {
                    pipe::buffered_h1_h2::serve(&mut conn, head, framing, upstream, &meta).await
                }
                Http2PipeStrategy::ServerStream => {
                    pipe::server_stream_h1_h2::serve(&mut conn, head, framing, upstream, &meta)
                        .await
                }
                Http2PipeStrategy::ClientStream => {
                    pipe::client_stream_h1_h2::serve(&mut conn, head, framing, upstream, &meta)
                        .await
                }
                Http2PipeStrategy::Duplex => {
                    pipe::duplex_h1_h2::serve(&mut conn, head, framing, upstream, &meta).await
                }
            },
            HttpUpstream::Http3(upstream) => match upstream.strategy {
                Http3PipeStrategy::Buffered => {
                    pipe::buffered_h1_h3::serve(&mut conn, head, framing, upstream, &meta).await
                }
                Http3PipeStrategy::ServerStream => {
                    pipe::server_stream_h1_h3::serve(&mut conn, head, framing, upstream, &meta)
                        .await
                }
                Http3PipeStrategy::ClientStream => {
                    pipe::client_stream_h1_h3::serve(&mut conn, head, framing, upstream, &meta)
                        .await
                }
                Http3PipeStrategy::Duplex => {
                    pipe::duplex_h1_h3::serve(&mut conn, head, framing, upstream, &meta).await
                }
            },
        };

        if let Err(e) = pipe_result {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
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
}
