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
    let client_ip_str = context.peer.ip().to_string();
    let is_tls = context.tls_enabled || context.tls.is_some();
    let proto = if is_tls { "https" } else { "http" };

    // 1. X-Forwarded-For
    let x_forwarded_for = HeaderName::from_static("x-forwarded-for");
    if let Some(existing) = headers.get(&x_forwarded_for) {
        if let Ok(existing_str) = existing.to_str() {
            let combined = format!("{existing_str}, {client_ip_str}");
            if let Ok(val) = HeaderValue::from_str(&combined) {
                headers.insert(x_forwarded_for, val);
            }
        }
    } else if let Ok(val) = HeaderValue::from_str(&client_ip_str) {
        headers.insert(x_forwarded_for, val);
    }

    // 2. X-Forwarded-Proto
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );

    // 3. X-Forwarded-Port
    let port_str = context.local_addr.port().to_string();
    if let Ok(val) = HeaderValue::from_str(&port_str) {
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
        && let Ok(val) = HeaderValue::from_str(&client_ip_str)
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
    match Http2ServerConnection::handshake(stream, config.clone()).await {
        Ok(mut conn) => loop {
            let accept_result =
                tokio::time::timeout(timeout_duration, conn.accept_streaming_request()).await;
            match accept_result {
                Ok(Ok(Some((head, receiver, responder)))) => {
                    let ctx_clone = context.clone();
                    let rt_clone = runtime.clone();
                    let cfg_clone = config.clone();
                    tokio::spawn(async move {
                        serve_http2_stream(
                            head, receiver, responder, ctx_clone, cfg_clone, rt_clone,
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
    head: Http2RequestHead,
    receiver: Http2StreamReceiver,
    responder: Http2Responder,
    context: IngressContext,
    config: Http2Config,
    runtime: SharedRuntime,
) {
    let host_str = head.host().map(|s| s.to_string());

    let mut http_req = Http2RouteRequest::new(head.path());
    if let Some(ref h) = host_str {
        http_req = http_req.with_host(h);
    }
    http_req = http_req.with_method(head.method.as_str());

    let rt = runtime.load();
    let Some(route) = rt.router.route_http2(&context.listener_id, &http_req) else {
        tracing::debug!(
            listener = %context.listener_id,
            path = %head.path(),
            host = ?host_str,
            method = %head.method,
            "No HTTP/2 route matched"
        );
        let not_found = L7Response::from_bytes(
            StatusCode::NOT_FOUND,
            b"404 Not Found: no matching route\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        let _ = responder.send_response(&not_found);
        return;
    };

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
        let _ = responder.send_response(&no_backend);
        return;
    };

    let strategy = upstream.strategy;
    let mut head = head;
    enrich_forwarded_headers(&mut head.headers, &context, host_str.as_deref());

    // HTTP/2 Pipeline Invariant (RFC 9113):
    // The Edge pipeline hands off the request processing closure directly to the upstream.
    // The upstream manages single-round Load Balancing selection, persistent multiplexed client
    // reuse, ready check, error recovery, and zero-overhead binary framing.
    let upstream_cfg = config.clone();
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
