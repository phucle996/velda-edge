//! Layer 7 HTTP/1.1 pipeline: downstream stream worker, route evaluation, and upstream forwarding.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http1"`.
//! Routes matched via `velda-router::Http1Router`, forwarded via `velda-http1`.

use std::net::SocketAddr;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_composer::ComposerContext;
use velda_http1::{Http1Request, Http1Response, Http1ServerConnection, Http1UpstreamConnector};
use velda_router::Http1RouteRequest;

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// Forwards an HTTP/1.1 request over cleartext TCP to the upstream target endpoint.
///
/// Opens a fresh TCP connection per call. When `velda-connection-pool` is integrated,
/// replace `TcpStream::connect` with a pooled connection lease.
pub async fn forward_http1_request(
    req: &Http1Request,
    target: SocketAddr,
    limits: &velda_core::IngressLimits,
) -> Result<Http1Response, EdgeError> {
    let mut stream = tokio::net::TcpStream::connect(target).await.map_err(|e| {
        EdgeError::Internal(format!(
            "Failed to connect to HTTP/1.1 upstream {target}: {e}"
        ))
    })?;
    Http1UpstreamConnector::forward_on_stream(req, &mut stream, limits)
        .await
        .map_err(|e| {
            EdgeError::Internal(format!(
                "Failed to forward HTTP/1.1 request to {target}: {e}"
            ))
        })
}

/// Dispatches an HTTP/1.1 request through `Http1Router` and forwards to upstream backend.
pub async fn process_http1_request(
    req: &Http1Request,
    context: &ComposerContext,
    runtime: &SharedRuntime,
) -> Http1Response {
    let host = req
        .host()
        .and_then(|h| h.to_str().ok())
        .or_else(|| req.uri.host());

    let mut http_req = Http1RouteRequest::new(req.path());
    if let Some(h) = host {
        http_req = http_req.with_host(h);
    }
    http_req = http_req.with_method(req.method.as_str());

    let rt = runtime.load();
    let Some(route) = rt.router.route_http1(&context.listener_id, &http_req) else {
        tracing::debug!(
            listener = %context.listener_id,
            path = %req.path(),
            host = ?host,
            method = %req.method,
            "No HTTP/1.1 route matched"
        );
        return Http1Response::from_bytes(
            StatusCode::NOT_FOUND,
            b"404 Not Found: no matching route\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    let target = if let Some(up) = rt.upstreams.http1.get(&route.upstream_name) {
        up.select_target().or_else(|| route.select_target())
    } else {
        route.select_target()
    };

    let Some(target) = target else {
        tracing::error!(
            listener = %context.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for HTTP/1.1 upstream"
        );
        return Http1Response::from_bytes(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable: no healthy upstream endpoint\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    match forward_http1_request(req, target, &context.limits).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                target = %target,
                upstream = %route.upstream_name,
                "HTTP/1.1 upstream forwarding failed"
            );
            Http1Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )
        }
    }
}

/// Asynchronous stream worker dispatching incoming HTTP/1.1 connections.
pub async fn handle_http1_stream<IO>(stream: IO, context: ComposerContext, runtime: SharedRuntime)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeout_duration = std::time::Duration::from_millis(context.limits.idle_timeout_ms);
    let mut conn = Http1ServerConnection::new(stream, context.limits);
    loop {
        // Phase 1: decode request head (fail-fast, header inspection, plugin hook point)
        let (head, framing) =
            match tokio::time::timeout(timeout_duration, conn.next_request_head()).await {
                Ok(Ok(Some(parts))) => parts,
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "HTTP/1.1 request head decode error");
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %context.listener_id,
                        timeout_ms = context.limits.idle_timeout_ms,
                        "HTTP/1.1 request read timed out"
                    );
                    break;
                }
            };

        // Phase 2: decode request body
        let body = match tokio::time::timeout(timeout_duration, conn.read_body(framing)).await {
            Ok(Ok(body)) => body,
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "HTTP/1.1 request body decode error");
                break;
            }
            Err(_) => {
                tracing::debug!(
                    listener = %context.listener_id,
                    timeout_ms = context.limits.idle_timeout_ms,
                    "HTTP/1.1 request body read timed out"
                );
                break;
            }
        };

        let req = Http1Request::from_parts(head, body);
        tracing::debug!(
            method = %req.method,
            path = %req.path(),
            listener = %context.listener_id,
            "Decoded HTTP/1.1 request"
        );
        let response = process_http1_request(&req, &context, &runtime).await;
        if let Err(e) = conn.send_response(&response).await {
            tracing::warn!(error = %e, "Failed to send HTTP/1.1 response to client");
            break;
        }
        if conn.is_closed() {
            break;
        }
    }
}
