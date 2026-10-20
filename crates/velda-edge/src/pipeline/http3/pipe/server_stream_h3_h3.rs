//! Layer 7 HTTP/3 to HTTP/3 Server Streaming Pipeline.
//!
//! Handles progressive server-streaming responses (SSE, chunked push) forwarded natively over upstream HTTP/3.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http3`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{Body, L7Request, L7Response};
use velda_plugin::RoutePluginPlan;

use crate::pipeline::http3::engine::Http3Responder;
use crate::upstream::Http3Upstream;

/// Serves a server-streaming HTTP/3 downstream request forwarded to an HTTP/3 upstream backend.
pub async fn serve(
    req: &L7Request,
    responder: Http3Responder,
    upstream: &Arc<Http3Upstream>,
    plan: &Arc<RoutePluginPlan>,
) {
    let (client, lease) = match upstream.acquire(&upstream.config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 connection"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(err_resp).await;
            return;
        }
    };

    let mut stream_resp = match client.send_stream_request_ref(req).await {
        Ok(r) => r,
        Err(e) => {
            drop(lease);
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to dispatch HTTP/3 streaming request to upstream"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(err_resp).await;
            return;
        }
    };

    let is_head = req.method == http::Method::HEAD;
    let is_no_body = stream_resp.status.is_informational()
        || stream_resp.status == StatusCode::NO_CONTENT
        || stream_resp.status == StatusCode::NOT_MODIFIED;

    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = L7Response::new(
            stream_resp.status,
            stream_resp.version,
            stream_resp.headers,
            Body::Empty,
        );
        velda_plugin::run_post_response_plugins(post_hooks, &req.headers, &mut l7_resp);
        stream_resp.status = l7_resp.status;
        stream_resp.headers = l7_resp.headers;
    }

    if is_head || is_no_body {
        let resp = L7Response::new(
            stream_resp.status,
            stream_resp.version,
            stream_resp.headers,
            Body::Empty,
        );
        let _ = responder.send_response(resp).await;
        return;
    }

    let mut stream_sender = match responder
        .send_stream_response(stream_resp.status, &stream_resp.headers)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to initiate downstream HTTP/3 streaming response"
            );
            return;
        }
    };

    while let Some(chunk_res) = stream_resp.next_chunk().await {
        let chunk = match chunk_res {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "HTTP/3 upstream chunk read error");
                return;
            }
        };

        if stream_sender.send_chunk(chunk).await.is_err() {
            tracing::debug!("Downstream client disconnected during H3 streaming");
            return;
        }
    }

    let _ = stream_sender.finish().await;
}
