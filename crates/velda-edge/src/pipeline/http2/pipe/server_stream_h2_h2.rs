//! Layer 7 HTTP/2 to HTTP/2 Server-Stream Pipeline.
//!
//! Designed for Server-Sent Events (SSE), LLM token streaming, and chunked push,
//! streaming upstream response frames back to downstream as they arrive.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::L7Response;
use velda_http2::config::Http2Config;
use velda_http2::pipe::pipe_server_stream;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a server-streaming HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Arc<Http2Config>,
    upstream: &Arc<Http2Upstream>,
) {
    // 1. Consume downstream body
    let body = match receiver.consume_all().await {
        Ok(b) => b,
        Err(e) => {
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 2. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 3. Acquire upstream multiplexed connection
    let (mut client, _lease) = match upstream.acquire(&config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 4. Pipe server-streaming response
    let mut res = pipe_server_stream(&head, &body, &mut responder, &mut client, &config).await;
    if let Err(ref e) = res
        && e.is_refused_or_goaway()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "HTTP/2 server-stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((mut fresh_client, _fresh_lease)) = upstream.acquire_fresh(&config).await {
            res =
                pipe_server_stream(&head, &body, &mut responder, &mut fresh_client, &config).await;
        }
    }

    if let Err(e) = res {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "HTTP/2 upstream pipe failed"
        );
        let err_resp = L7Response::from_bytes(
            StatusCode::BAD_GATEWAY,
            format!("502 Bad Gateway: {e}\n").into_bytes(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        let _ = responder.send_response(&err_resp);
    }
}
