//! Layer 7 HTTP/3 to HTTP/3 Duplex Bidirectional Pipeline.
//!
//! Handles full-duplex bidirectional streaming forwarded natively over upstream HTTP/3 QUIC connection.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http3`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{Body, L7Request, L7Response};
use velda_http3::{Http3Client, Http3Error};

use crate::upstream::Http3Upstream;

/// Forward a duplex request over active HTTP/3 client.
async fn forward_duplex(client: &Http3Client, req: &L7Request) -> Result<L7Response, Http3Error> {
    let resp = client.send_request_ref(req).await?;
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

/// Serves a full-duplex HTTP/3 downstream request forwarded to an HTTP/3 upstream backend.
pub async fn serve(req: &L7Request, upstream: &Arc<Http3Upstream>) -> L7Response {
    let (client, lease) = match upstream.acquire(&upstream.config).await {
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

    let mut pipe_res = forward_duplex(&client, req).await;
    let mut _fresh_lease_guard = None;
    if pipe_res
        .as_ref()
        .err()
        .is_some_and(|e| e.is_connection_closed())
    {
        lease.mark_goaway();
        drop(lease);
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection"
        );
        if let Ok((fresh_client, fresh_lease)) = upstream.acquire_fresh(&upstream.config).await {
            pipe_res = forward_duplex(&fresh_client, req).await;
            if pipe_res.is_err() {
                fresh_lease.mark_goaway();
            }
            _fresh_lease_guard = Some(fresh_lease);
        }
    }

    match pipe_res {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream duplex request failed"
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
