//! Layer 7 HTTP/2 pipeline: downstream stream worker, route evaluation, and upstream forwarding.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http2"`.
//! Routes matched via `velda-router::Http2Router`, forwarded via `velda-http2`.

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::L7Response;
use velda_http2::config::Http2Config;
use velda_http2::pipe::{
    Http2PipeStrategy, pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_http2::server::{
    Http2RequestHead, Http2Responder, Http2ServerConnection, Http2StreamReceiver,
};
use velda_router::Http2RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use crate::pipeline::context::{IngressContext, TlsMetadata};
use crate::runtime::SharedRuntime;

/// Enriches downstream request headers with standard proxy forwarding metadata.
fn enrich_forwarded_headers(
    headers: &mut http::HeaderMap,
    context: &IngressContext,
    host: Option<&str>,
) {
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
}

/// Asynchronous stream worker dispatching incoming HTTP/2 connections.
///
/// Inspects listener configuration to determine whether downstream TLS termination
/// is active. If TLS is required, negotiates handshake, validates ALPN against "http2",
/// and passes the encrypted stream to the HTTP/2 loop.
pub async fn handle_http2_stream(
    connection: Connection,
    context: IngressContext,
    config: Http2Config,
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

                if let Err(err) = enriched_context.validate_alpn("http2") {
                    tracing::warn!(
                        error = %err,
                        listener = %enriched_context.listener_id,
                        peer = %enriched_context.peer,
                        "Dropping connection due to protocol ALPN mismatch"
                    );
                    return;
                }

                run_http2_loop(tls_stream, enriched_context, config, runtime).await;
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
        run_http2_loop(connection, context, config, runtime).await;
    }
}

/// Core HTTP/2 downstream request-response loop decoupled from transport layer.
pub async fn run_http2_loop<IO>(
    stream: IO,
    context: IngressContext,
    config: Http2Config,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeout_duration = std::time::Duration::from_millis(config.idle_timeout_ms);
    let mut consecutive_not_founds: u32 = 0;
    const MAX_CONSECUTIVE_NOT_FOUNDS: u32 = 100;

    match Http2ServerConnection::handshake(stream, config).await {
        Ok(mut conn) => loop {
            let accept_result =
                tokio::time::timeout(timeout_duration, conn.accept_streaming_request()).await;
            match accept_result {
                Ok(Ok(Some((head, receiver, responder)))) => {
                    let mut host_buf = [0u8; 128];
                    let mut host_len = 0;
                    let mut heap_host = None;
                    if let Some(h) = head.host() {
                        if h.len() <= host_buf.len() {
                            host_buf[..h.len()].copy_from_slice(h.as_bytes());
                            host_len = h.len();
                        } else {
                            heap_host = Some(h.to_string());
                        }
                    }
                    let host_str: Option<&str> = if host_len > 0 {
                        std::str::from_utf8(&host_buf[..host_len]).ok()
                    } else {
                        heap_host.as_deref()
                    };

                    let mut http_req = Http2RouteRequest::new(head.path());
                    if let Some(h) = host_str {
                        http_req = http_req.with_host(h);
                    }
                    http_req = http_req.with_method(head.method.as_str());

                    let rt = runtime.load();
                    let matched_route = rt
                        .router
                        .route_http2(&context.listener_id, &http_req)
                        .cloned();

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
                                listener = %context.listener_id,
                                peer = %context.peer,
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
                    let ctx_clone = context.clone();
                    let rt_clone = runtime.clone();
                    let cfg_clone = config;
                    tokio::spawn(async move {
                        serve_http2_stream(
                            head, receiver, responder, ctx_clone, cfg_clone, rt_clone, route,
                        )
                        .await;
                    });
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "HTTP/2 stream accept error");
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %context.listener_id,
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
async fn serve_http2_stream(
    mut head: Http2RequestHead,
    receiver: Http2StreamReceiver,
    responder: Http2Responder,
    context: IngressContext,
    config: Http2Config,
    runtime: SharedRuntime,
    route: velda_router::Http2Route,
) {
    let rt = runtime.load();
    let Some(upstream) = rt.upstreams.http2.get(&route.upstream_name) else {
        tracing::error!(
            listener = %context.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for HTTP/2 upstream"
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
        return;
    };

    let strategy = upstream.strategy;
    let mut host_buf = [0u8; 128];
    let host_len = if let Some(h) = head.host() {
        let len = h.len().min(host_buf.len());
        host_buf[..len].copy_from_slice(&h.as_bytes()[..len]);
        len
    } else {
        0
    };
    let host_str = if host_len > 0 {
        std::str::from_utf8(&host_buf[..host_len]).ok()
    } else {
        None
    };
    enrich_forwarded_headers(&mut head.headers, &context, host_str);

    // HTTP/2 Pipeline Invariant (RFC 9113):
    // The Edge pipeline hands off the request processing closure directly to the upstream.
    // The upstream manages single-round Load Balancing selection, persistent multiplexed client
    // reuse, ready check, error recovery, and zero-overhead binary framing.
    let upstream_cfg = config;
    let pipe_res = upstream
        .dispatch_pipe(&upstream_cfg, move |mut client| async move {
            match strategy {
                Http2PipeStrategy::Buffered => {
                    pipe_buffered(head, receiver, responder, &mut client, &config).await
                }
                Http2PipeStrategy::ServerStream => {
                    pipe_server_stream(head, receiver, responder, &mut client, &config).await
                }
                Http2PipeStrategy::ClientStream => {
                    pipe_client_stream(head, receiver, responder, &mut client, &config).await
                }
                Http2PipeStrategy::Duplex => {
                    pipe_duplex(head, receiver, responder, &mut client, &config).await
                }
            }
        })
        .await;

    if let Err(e) = pipe_res {
        tracing::warn!(
            error = %e,
            upstream = %route.upstream_name,
            "HTTP/2 upstream dispatch pipe failed"
        );
    }
}
