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
    Http1BodyFraming, Http1Config, Http1Error, Http1PipeStrategy, Http1Request, Http1Response,
    Http1ServerConnection, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_router::Http1RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::runtime::SharedRuntime;

/// Enriches HTTP/1.1 request headers with RFC 7239 and standard proxy forwarding metadata.
///
/// Anti-Spoofing Invariant:
/// Untrusted downstream clients must NEVER be permitted to spoof client IP or proxy forwarding metadata.
/// Any client-supplied `X-Forwarded-*`, `X-Real-IP`, or RFC 7239 `Forwarded` headers are stripped in-place
/// and replaced strictly with authoritative edge connection metadata (`peer.ip()`, `local_addr.port()`,
/// protocol scheme, and verified host).
fn enrich_http1_forwarded_headers(
    headers: &mut http::HeaderMap,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
    host: Option<&str>,
) {
    use http::header::{HeaderName, HeaderValue};

    // 1. Strip all standard client-supplied untrusted forwarding headers (Zero-alloc O(1) removals)
    static UNTRUSTED_FORWARDED_HEADERS: [HeaderName; 6] = [
        HeaderName::from_static("x-forwarded-for"),
        HeaderName::from_static("x-forwarded-proto"),
        HeaderName::from_static("x-forwarded-host"),
        HeaderName::from_static("x-forwarded-port"),
        HeaderName::from_static("x-real-ip"),
        HeaderName::from_static("forwarded"),
    ];
    for name in &UNTRUSTED_FORWARDED_HEADERS {
        headers.remove(name);
    }

    // 2. Defensive sweep: strip any custom "x-forwarded-*" headers without heap allocation
    let mut custom_to_remove: [Option<HeaderName>; 8] = [const { None }; 8];
    let mut count = 0;
    for key in headers.keys() {
        if key.as_str().starts_with("x-forwarded-") && count < custom_to_remove.len() {
            custom_to_remove[count] = Some(key.clone());
            count += 1;
        }
    }
    for name in custom_to_remove[..count].iter().flatten() {
        headers.remove(name);
    }

    let client_ip = peer.ip();
    let proto = if is_tls { "https" } else { "http" };

    // Format peer IP directly into stack buffer
    let mut ip_buf = [0u8; 64];
    let ip_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut ip_buf[..]);
        let _ = write!(cursor, "{}", client_ip);
        cursor.position() as usize
    };
    let client_ip_bytes = &ip_buf[..ip_len];

    // 2. Authoritative X-Forwarded-For
    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(HeaderName::from_static("x-forwarded-for"), val);
    }

    // 3. Authoritative X-Real-IP
    if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(HeaderName::from_static("x-real-ip"), val);
    }

    // 4. Authoritative X-Forwarded-Proto
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );

    // 5. Authoritative X-Forwarded-Port
    let mut port_buf = [0u8; 8];
    let port_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut port_buf[..]);
        let _ = write!(cursor, "{}", local_addr.port());
        cursor.position() as usize
    };
    if let Ok(val) = HeaderValue::from_bytes(&port_buf[..port_len]) {
        headers.insert(HeaderName::from_static("x-forwarded-port"), val);
    }

    // 6. Authoritative X-Forwarded-Host
    if let Some(h) = host
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(HeaderName::from_static("x-forwarded-host"), val);
    }

    // 7. Authoritative Standard: RFC 7239 (Official IETF "Forwarded" HTTP Extension)
    let mut fwd_buf = [0u8; 256];
    let fwd_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut fwd_buf[..]);
        match client_ip {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "for={v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "for=\"[{v6}]\"");
            }
        }
        let _ = write!(cursor, ";proto={proto};by=");
        match local_addr.ip() {
            std::net::IpAddr::V4(v4) => {
                let _ = write!(cursor, "{v4}");
            }
            std::net::IpAddr::V6(v6) => {
                let _ = write!(cursor, "\"[{v6}]\"");
            }
        }
        if let Some(h) = host {
            let _ = write!(cursor, ";host=\"{h}\"");
        }
        cursor.position() as usize
    };
    let fwd_bytes = &fwd_buf[..fwd_len];
    if let Ok(val) = HeaderValue::from_bytes(fwd_bytes) {
        headers.insert(HeaderName::from_static("forwarded"), val);
    }
}

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

                run_http1_loop(
                    tls_stream,
                    listener_id,
                    peer,
                    local_addr,
                    true,
                    streaming,
                    config,
                    runtime,
                )
                .await;
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
        run_http1_loop(
            connection,
            listener_id,
            peer,
            local_addr,
            false,
            streaming,
            config,
            runtime,
        )
        .await;
    }
}

/// Core HTTP/1.1 downstream request-response loop decoupled from transport layer.
#[allow(clippy::too_many_arguments)]
pub async fn run_http1_loop<IO>(
    stream: IO,
    listener_id: Arc<str>,
    peer: SocketAddr,
    local_addr: SocketAddr,
    is_tls: bool,
    streaming: velda_core::StreamingMode,
    config: Http1Config,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let header_timeout =
        std::time::Duration::from_millis(config.header_read_timeout_ms.min(config.idle_timeout_ms));
    let mut conn = Http1ServerConnection::new(stream, config);
    let mut requests_served: u32 = 0;

    loop {
        // Phase 1: decode request head (fail-fast, header inspection, Slowloris protection)
        let (mut head, framing) =
            match tokio::time::timeout(header_timeout, conn.next_request_head()).await {
                Ok(Ok(Some(parts))) => parts,
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "HTTP/1.1 request head decode error");
                    let status = match e {
                        Http1Error::HeaderTooLarge(_) | Http1Error::TooManyHeaders(_) => {
                            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
                        }
                        Http1Error::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
                        _ => StatusCode::BAD_REQUEST,
                    };
                    let err_resp =
                        Http1Response::from_bytes(status, format!("{status}\n").into_bytes());
                    let _ = conn.send_response(&err_resp).await;
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %listener_id,
                        timeout_ms = config.header_read_timeout_ms,
                        "HTTP/1.1 request head read timed out"
                    );
                    break;
                }
            };

        requests_served += 1;
        let reach_max_keepalive = requests_served >= config.max_keepalive_requests;
        let rt = runtime.load();

        // Enforce Ingress Streaming Policy (Option A):
        // If client sends chunked upload but listener has streaming.client disabled, reject immediately!
        if framing == Http1BodyFraming::Chunked && !streaming.client {
            tracing::warn!(
                listener = %listener_id,
                "Rejecting chunked request: streaming upload is disabled on this listener"
            );
            let rejected = Http1Response::from_bytes(
                StatusCode::FORBIDDEN,
                b"403 Forbidden: streaming upload is disabled on this listener\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&rejected).await;
            break;
        }

        if !velda_router::is_clean_path(head.path().as_bytes()) {
            match velda_router::normalize_path(head.path()) {
                Ok(normalized) => {
                    let mut parts = head.uri.clone().into_parts();
                    let new_path_and_query = match parts.path_and_query {
                        Some(ref pq) => {
                            if let Some(q) = pq.query() {
                                format!("{normalized}?{q}")
                                    .parse::<http::uri::PathAndQuery>()
                                    .ok()
                            } else {
                                normalized.parse::<http::uri::PathAndQuery>().ok()
                            }
                        }
                        None => normalized.parse::<http::uri::PathAndQuery>().ok(),
                    };
                    if let Some(pq) = new_path_and_query {
                        parts.path_and_query = Some(pq);
                        if let Ok(new_uri) = http::Uri::from_parts(parts) {
                            head.uri = new_uri;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        listener = %listener_id,
                        error = %e,
                        path = %head.path(),
                        "Rejecting HTTP/1.1 request with unsafe URI path"
                    );
                    let bad_req = Http1Response::from_bytes(
                        StatusCode::BAD_REQUEST,
                        b"400 Bad Request: unsafe or invalid URI path\n".to_vec(),
                    )
                    .with_header(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    );
                    let _ = conn.send_response(&bad_req).await;
                    break;
                }
            }
        }

        let clean_path = head.path();
        let host_hdr = head.headers.get(http::header::HOST).cloned();
        let host_str = host_hdr
            .as_ref()
            .and_then(|v| v.to_str().ok())
            .or_else(|| head.uri.host());

        let mut http_req = Http1RouteRequest::new(clean_path);
        if let Some(h) = host_str {
            http_req = http_req.with_host(h);
        }
        http_req = http_req.with_method(head.method.as_str());

        let Some(route) = rt.router.route_http1(&listener_id, &http_req) else {
            tracing::debug!(
                listener = %listener_id,
                path = %head.path(),
                host = ?host_str,
                method = %head.method,
                "No HTTP/1.1 route matched"
            );
            let not_found = Http1Response::from_bytes(
                StatusCode::NOT_FOUND,
                b"404 Not Found: no matching route\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&not_found).await;
            break;
        };

        let Some(upstream) = rt.upstreams.http1.get(&route.upstream_name) else {
            tracing::error!(
                listener = %listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                "No HTTP/1.1 upstream configured"
            );
            let no_backend = Http1Response::from_bytes(
                StatusCode::SERVICE_UNAVAILABLE,
                b"503 Service Unavailable: upstream not configured\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&no_backend).await;
            break;
        };

        let strategy = upstream.strategy;
        enrich_http1_forwarded_headers(&mut head.headers, peer, local_addr, is_tls, host_str);
        let cfg = *conn.config();

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
                        let err_resp = Http1Response::from_bytes(
                            StatusCode::REQUEST_TIMEOUT,
                            b"408 Request Timeout: client body read idle timeout exceeded\n"
                                .to_vec(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        );
                        let _ = conn.send_response(&err_resp).await;
                        break;
                    }
                    Err(e) => {
                        let err_resp = Http1Response::from_bytes(
                            StatusCode::BAD_REQUEST,
                            format!("400 Bad Request: {e}\n").into_bytes(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        );
                        let _ = conn.send_response(&err_resp).await;
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
                let err_resp = Http1Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                let _ = conn.send_response(&err_resp).await;
                break;
            }
        };

        let mut pipe_result = match (strategy, req_body.as_ref()) {
            (Http1PipeStrategy::Buffered, Some(body)) => {
                let req = Http1Request::from_parts(head.clone(), body.clone());
                pipe_buffered(&mut conn, req, &mut *upstream_lease, &cfg).await
            }
            (Http1PipeStrategy::ServerStream, Some(body)) => {
                let req = Http1Request::from_parts(head.clone(), body.clone());
                pipe_server_stream(&mut conn, req, &mut *upstream_lease, &cfg).await
            }
            (Http1PipeStrategy::ClientStream, None) => {
                pipe_client_stream(&mut conn, head.clone(), &mut *upstream_lease, &cfg).await
            }
            (Http1PipeStrategy::Duplex, None) => {
                pipe_duplex(&mut conn, head.clone(), &mut *upstream_lease, &cfg).await
            }
            _ => unreachable!(),
        };

        // Self-Healing Retry (RFC 9112):
        // If a reused connection failed due to a stale keep-alive connection race,
        // and the request payload is in RAM (Buffered or ServerStream), transparently
        // acquire a fresh upstream connection and retry once.
        if let Err(ref e) = pipe_result
            && upstream_lease.is_reused()
            && e.is_stale_connection()
            && matches!(
                strategy,
                Http1PipeStrategy::Buffered | Http1PipeStrategy::ServerStream
            )
        {
            upstream_lease.mark_closed();
            drop(upstream_lease);

            tracing::debug!(
                upstream = %route.upstream_name,
                "Stale pooled HTTP/1.1 connection detected; self-healing with fresh connection"
            );

            match upstream.acquire_fresh().await {
                Ok(mut fresh_lease) => {
                    let body = req_body.as_ref().unwrap();
                    let retry_req = Http1Request::from_parts(head, body.clone());
                    pipe_result = match strategy {
                        Http1PipeStrategy::Buffered => {
                            pipe_buffered(&mut conn, retry_req, &mut *fresh_lease, &cfg).await
                        }
                        Http1PipeStrategy::ServerStream => {
                            pipe_server_stream(&mut conn, retry_req, &mut *fresh_lease, &cfg).await
                        }
                        _ => unreachable!(),
                    };
                    if pipe_result.is_err() {
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
        } else if pipe_result.is_err() {
            upstream_lease.mark_closed();
        }

        if let Err(e) = pipe_result {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                strategy = ?strategy,
                "HTTP/1.1 upstream stream pipe failed"
            );
            let err_resp = Http1Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&err_resp).await;
            break;
        }

        if reach_max_keepalive || conn.is_closed() {
            tracing::debug!(
                listener = %listener_id,
                requests_served,
                max = config.max_keepalive_requests,
                "Closing HTTP/1.1 connection (reached max keepalive requests or connection closed)"
            );
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    #[test]
    fn test_enrich_forwarded_headers() {
        let mut headers = HeaderMap::new();
        // Client attempts to spoof their IP, host, and SSL status
        headers.insert("x-forwarded-for", "203.0.113.195".parse().unwrap());
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.attacker.com".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-ssl", "on".parse().unwrap());
        headers.insert(
            "forwarded",
            "for=203.0.113.195;proto=https".parse().unwrap(),
        );

        let peer: SocketAddr = "192.0.2.10:45678".parse().unwrap();
        let local: SocketAddr = "10.0.0.1:8080".parse().unwrap();

        enrich_http1_forwarded_headers(&mut headers, peer, local, false, Some("api.example.com"));

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
        // Untrusted extra x-forwarded-* headers MUST be stripped
        assert!(headers.get("x-forwarded-ssl").is_none());
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
