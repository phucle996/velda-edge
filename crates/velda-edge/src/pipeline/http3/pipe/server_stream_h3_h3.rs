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
use velda_http3::{Http3Client, Http3Config, Http3Error};

use crate::upstream::Http3Upstream;

/// Forward a server-streaming request over active HTTP/3 client.
async fn forward_server_stream(
    client: &Http3Client,
    req: &L7Request,
    config: &Http3Config,
) -> Result<L7Response, Http3Error> {
    if let Body::Bytes(ref b) = req.body
        && b.len() > config.max_body_size
    {
        return Err(Http3Error::PayloadTooLarge(b.len()));
    }

    let resp = client.send_request_ref(req).await?;

    if let Body::Bytes(ref b) = resp.body
        && b.len() > config.max_body_size
    {
        return Err(Http3Error::PayloadTooLarge(b.len()));
    }

    let is_head = req.method == http::Method::HEAD;
    let is_no_body = resp.status.is_informational()
        || resp.status == StatusCode::NO_CONTENT
        || resp.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body {
        return Ok(L7Response::new(
            resp.status,
            resp.version,
            resp.headers,
            Body::Empty,
        ));
    }

    Ok(resp)
}

/// Serves a server-streaming HTTP/3 downstream request forwarded to an HTTP/3 upstream backend.
pub async fn serve(req: &L7Request, upstream: &Arc<Http3Upstream>) -> L7Response {
    let (client, _lease) = match upstream.acquire(&upstream.config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 connection"
            );
            return L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
        }
    };

    let mut pipe_res = forward_server_stream(&client, req, &upstream.config).await;
    if let Err(Http3Error::ConnectionClosed) = pipe_res {
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection"
        );
        if let Ok((fresh_client, _fresh_lease)) = upstream.acquire_fresh(&upstream.config).await {
            pipe_res = forward_server_stream(&fresh_client, req, &upstream.config).await;
        }
    }

    match pipe_res {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream server stream request failed"
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
