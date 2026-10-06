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

use crate::pipeline::context::{IngressContext, TlsMetadata};
use crate::runtime::SharedRuntime;

pub use crate::runtime::upstream::UpstreamHttp1Stream;

/// Enriches HTTP/1.1 request headers with RFC 7239 and standard proxy forwarding metadata.
fn enrich_http1_forwarded_headers(
    headers: &mut http::HeaderMap,
    context: &IngressContext,
    host: Option<&str>,
) {
    use http::header::{HeaderName, HeaderValue};

    let client_ip = context.peer.ip();
    let is_tls = context.tls_enabled || context.tls.is_some();
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

    // 1. X-Forwarded-For
    let x_forwarded_for = HeaderName::from_static("x-forwarded-for");
    if let Some(existing) = headers.get(&x_forwarded_for) {
        if let Ok(existing_bytes) = existing.to_str() {
            let mut combined = Vec::with_capacity(existing_bytes.len() + 2 + ip_len);
            combined.extend_from_slice(existing_bytes.as_bytes());
            combined.extend_from_slice(b", ");
            combined.extend_from_slice(client_ip_bytes);
            if let Ok(val) = HeaderValue::from_bytes(&combined) {
                headers.insert(x_forwarded_for, val);
            }
        }
    } else if let Ok(val) = HeaderValue::from_bytes(client_ip_bytes) {
        headers.insert(x_forwarded_for, val);
    }

    // 2. X-Forwarded-Proto
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );

    // 3. X-Forwarded-Port
    let mut port_buf = [0u8; 8];
    let port_len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut port_buf[..]);
        let _ = write!(cursor, "{}", context.local_addr.port());
        cursor.position() as usize
    };
    if let Ok(val) = HeaderValue::from_bytes(&port_buf[..port_len]) {
        headers.insert(HeaderName::from_static("x-forwarded-port"), val);
    }

    // 4. X-Forwarded-Host
    let x_forwarded_host = HeaderName::from_static("x-forwarded-host");
    if !headers.contains_key(&x_forwarded_host)
        && let Some(h) = host
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(x_forwarded_host, val);
    }

    // 5. X-Real-IP
    let x_real_ip = HeaderName::from_static("x-real-ip");
    if !headers.contains_key(&x_real_ip)
        && let Ok(val) = HeaderValue::from_bytes(client_ip_bytes)
    {
        headers.insert(x_real_ip, val);
    }

    // 6. Standard: RFC 7239 (Official IETF "Forwarded" HTTP Extension)
    let forwarded = HeaderName::from_static("forwarded");
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
        match context.local_addr.ip() {
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

    if let Some(existing) = headers.get(&forwarded) {
        if let Ok(existing_bytes) = existing.to_str() {
            let mut combined = Vec::with_capacity(existing_bytes.len() + 2 + fwd_len);
            combined.extend_from_slice(existing_bytes.as_bytes());
            combined.extend_from_slice(b", ");
            combined.extend_from_slice(fwd_bytes);
            if let Ok(val) = HeaderValue::from_bytes(&combined) {
                headers.insert(forwarded, val);
            }
        }
    } else if let Ok(val) = HeaderValue::from_bytes(fwd_bytes) {
        headers.insert(forwarded, val);
    }
}

/// Asynchronous stream worker dispatching incoming HTTP/1.1 connections.
///
/// Inspects the pre-compiled listener metadata to determine whether downstream
/// TLS termination is active. If TLS is required, negotiates the handshake, validates
/// ALPN against the declared protocol, and passes the encrypted stream to the HTTP/1.1 loop.
pub async fn handle_http1_stream(
    connection: Connection,
    context: IngressContext,
    config: Http1Config,
    runtime: SharedRuntime,
) {
    let rt = runtime.load();

    if context.tls_enabled {
        let Some(tls_server) = rt.tls_server.as_ref() else {
            tracing::error!(
                listener = %context.listener_id,
                peer = %context.peer,
                "TLS required for listener, but no TLS server engine is compiled; dropping connection"
            );
            return;
        };

        match tls_server.accept_with_timeout(connection).await {
            Ok(tls_stream) => {
                let handshake_info = TlsServerEngine::extract_handshake_info(&tls_stream);
                tracing::debug!(
                    listener = %context.listener_id,
                    peer = %context.peer,
                    sni = ?handshake_info.sni,
                    alpn = ?handshake_info.alpn,
                    "Downstream TLS handshake succeeded"
                );
                let enriched_context =
                    context.with_tls_metadata(TlsMetadata::from_handshake(handshake_info));

                if let Err(err) = enriched_context.validate_alpn("http1") {
                    tracing::warn!(
                        error = %err,
                        listener = %enriched_context.listener_id,
                        peer = %enriched_context.peer,
                        "Dropping connection due to protocol ALPN mismatch"
                    );
                    return;
                }

                run_http1_loop(tls_stream, enriched_context, config, runtime).await;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    listener = %context.listener_id,
                    peer = %context.peer,
                    "Downstream TLS handshake failed"
                );
            }
        }
    } else {
        run_http1_loop(connection, context, config, runtime).await;
    }
}

/// Core HTTP/1.1 downstream request-response loop decoupled from transport layer.
pub async fn run_http1_loop<IO>(
    stream: IO,
    context: IngressContext,
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
                        listener = %context.listener_id,
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
        if framing == Http1BodyFraming::Chunked && !context.streaming.client {
            tracing::warn!(
                listener = %context.listener_id,
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

        let host_hdr = head.headers.get(http::header::HOST).cloned();
        let host_str = host_hdr
            .as_ref()
            .and_then(|v| v.to_str().ok())
            .or_else(|| head.uri.host());

        let mut http_req = Http1RouteRequest::new(head.path());
        if let Some(h) = host_str {
            http_req = http_req.with_host(h);
        }
        http_req = http_req.with_method(head.method.as_str());

        let Some(route) = rt.router.route_http1(&context.listener_id, &http_req) else {
            tracing::debug!(
                listener = %context.listener_id,
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
                listener = %context.listener_id,
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
        enrich_http1_forwarded_headers(&mut head.headers, &context, host_str);
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

        let mut upstream_lease = match upstream.acquire_stream(host_str).await {
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

        let pipe_result = match (strategy, req_body) {
            (Http1PipeStrategy::Buffered, Some(body)) => {
                let req = Http1Request::from_parts(head, body);
                pipe_buffered(&mut conn, req, &mut *upstream_lease, &cfg).await
            }
            (Http1PipeStrategy::ServerStream, Some(body)) => {
                let req = Http1Request::from_parts(head, body);
                pipe_server_stream(&mut conn, req, &mut *upstream_lease, &cfg).await
            }
            (Http1PipeStrategy::ClientStream, None) => {
                pipe_client_stream(&mut conn, head, &mut *upstream_lease, &cfg).await
            }
            (Http1PipeStrategy::Duplex, None) => {
                pipe_duplex(&mut conn, head, &mut *upstream_lease, &cfg).await
            }
            _ => unreachable!(),
        };

        if let Err(e) = pipe_result {
            upstream_lease.mark_closed();
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
                listener = %context.listener_id,
                requests_served,
                max = config.max_keepalive_requests,
                "Closing HTTP/1.1 connection (reached max keepalive requests or connection closed)"
            );
            break;
        }
    }
}
