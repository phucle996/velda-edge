//! Layer 7 HTTP/2 pipeline: downstream stream worker, route evaluation, and upstream forwarding.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http2"`.
//! Routes matched via `velda-router::Http2Router`, forwarded via `velda-http2`.

use std::net::SocketAddr;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_composer::ComposerContext;
use velda_core::{L7Request, L7Response};
use velda_http2::Http2UpstreamConnector;
use velda_router::Http2RouteRequest;

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// Forwards an HTTP/2 request over cleartext TCP to the upstream target endpoint.
pub async fn forward_http2_request(
    req: &L7Request,
    target: SocketAddr,
    limits: &velda_core::IngressLimits,
) -> Result<L7Response, EdgeError> {
    Http2UpstreamConnector::forward_request(req, target, limits)
        .await
        .map_err(|e| {
            EdgeError::Internal(format!("Failed to forward HTTP/2 request to {target}: {e}"))
        })
}

/// Dispatches an HTTP/2 request through `Http2Router` and forwards to upstream backend.
pub async fn process_http2_request(
    req: &L7Request,
    context: &ComposerContext,
    runtime: &SharedRuntime,
) -> L7Response {
    let host = req
        .host()
        .and_then(|h| h.to_str().ok())
        .or_else(|| req.uri.host());

    let mut http_req = Http2RouteRequest::new(req.path());
    if let Some(h) = host {
        http_req = http_req.with_host(h);
    }
    http_req = http_req.with_method(req.method.as_str());

    let rt = runtime.load();
    let Some(route) = rt.router.route_http2(&context.listener_id, &http_req) else {
        tracing::debug!(
            listener = %context.listener_id,
            path = %req.path(),
            host = ?host,
            method = %req.method,
            "No HTTP/2 route matched"
        );
        return L7Response::from_bytes(
            StatusCode::NOT_FOUND,
            b"404 Not Found: no matching route\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    let target = if let Some(up) = rt.upstreams.http2.get(&route.upstream_name) {
        up.select_target().or_else(|| route.select_target())
    } else {
        route.select_target()
    };

    let Some(target) = target else {
        tracing::error!(
            listener = %context.listener_id,
            route = %route.id,
            upstream = %route.upstream_name,
            "No healthy backend endpoints available for HTTP/2 upstream"
        );
        return L7Response::from_bytes(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable: no healthy upstream endpoint\n".to_vec(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    };

    match forward_http2_request(req, target, &context.limits).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                target = %target,
                upstream = %route.upstream_name,
                "HTTP/2 upstream forwarding failed"
            );
            L7Response::from_bytes(
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

/// Asynchronous stream worker dispatching incoming HTTP/2 connections.
pub async fn handle_http2_stream<IO>(stream: IO, context: ComposerContext, runtime: SharedRuntime)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeout_duration = std::time::Duration::from_millis(context.limits.request_timeout_ms);
    match velda_http2::Http2ServerConnection::handshake(stream, context.limits).await {
        Ok(mut conn) => loop {
            let accept_result = tokio::time::timeout(timeout_duration, conn.accept_request()).await;
            match accept_result {
                Ok(Ok(Some((req, responder)))) => {
                    tracing::debug!(
                        method = %req.method,
                        path = %req.path(),
                        listener = %context.listener_id,
                        "Decoded HTTP/2 request"
                    );
                    let ctx_clone = context.clone();
                    let rt_clone = runtime.clone();
                    tokio::spawn(async move {
                        let response = process_http2_request(&req, &ctx_clone, &rt_clone).await;
                        if let Err(e) = responder.send_response(&response) {
                            tracing::warn!(error = %e, "Failed to send HTTP/2 response to client");
                        }
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
                        timeout_ms = context.limits.request_timeout_ms,
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
