//! Layer 7 HTTP/3 to HTTP/2 Client Streaming Bridge Pipeline.
//!
//! Handles client-streaming uploads where downstream HTTP/3 requests are forwarded
//! to upstream HTTP/2 backends.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};

use crate::upstream::Http2Upstream;

/// Serves a client-streaming HTTP/3 downstream request bridged to an HTTP/2 backend.
pub async fn serve(
    req: &L7Request,
    upstream: &Arc<Http2Upstream>,
) -> (
    L7Response,
    Option<(u128, u16)>,
    velda_core::CompletionReason,
) {
    let body_bytes = req.body.to_bytes();

    let (client, lease) = match upstream.acquire(&upstream.config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection for H3->H2 client stream bridge"
            );
            return (
                L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                ),
                None,
                velda_core::CompletionReason::UpstreamError,
            );
        }
    };
    let mut upstream_addr = Some(lease.endpoint);

    let mut pipe_res = send_h2_client_stream(&client, req, &body_bytes).await;
    let mut _fresh_lease_guard = None;
    if let Err(ref e) = pipe_res
        && e.is_refused_or_goaway()
    {
        lease.mark_goaway();
        drop(lease);
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/2 stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((fresh_client, fresh_lease)) = upstream.acquire_fresh(&upstream.config).await {
            upstream_addr = Some(fresh_lease.endpoint);
            pipe_res = send_h2_client_stream(&fresh_client, req, &body_bytes).await;
            if pipe_res.is_err() {
                fresh_lease.mark_goaway();
            }
            _fresh_lease_guard = Some(fresh_lease);
        }
    }

    match pipe_res {
        Ok(resp) => (resp, upstream_addr, velda_core::CompletionReason::Complete),
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/2 upstream client-stream pipe failed"
            );
            (
                L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                ),
                upstream_addr,
                velda_core::CompletionReason::UpstreamError,
            )
        }
    }
}

async fn send_h2_client_stream(
    client: &velda_http2::client::connection::Http2Client,
    req: &L7Request,
    body: &bytes::Bytes,
) -> Result<L7Response, velda_http2::error::Http2Error> {
    let has_body = !body.is_empty();
    let (response_future, mut send_stream) = client
        .send_request(
            req.method.clone(),
            req.uri.clone(),
            req.headers.clone(),
            !has_body,
        )
        .await?;
    if has_body {
        send_stream.send_data(body.clone(), true)?;
    }

    let mut response = response_future.await?;

    let content_len = response
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4096);
    let mut resp_body = BytesMut::with_capacity(content_len.min(65536));
    while let Some(chunk) = response.body.recv_chunk().await? {
        resp_body.extend_from_slice(&chunk);
    }

    Ok(L7Response::new(
        response.status,
        http::Version::HTTP_3,
        response.headers,
        velda_core::Body::Bytes(resp_body.freeze()),
    ))
}
