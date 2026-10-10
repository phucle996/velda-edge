//! Layer 7 HTTP/3 to HTTP/2 Server Streaming Bridge Pipeline.
//!
//! Handles streaming responses where downstream HTTP/3 requests are forwarded
//! to upstream HTTP/2 backends.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http2::Http2Config;

use crate::upstream::Http2Upstream;

/// Serves a server-streaming HTTP/3 downstream request bridged to an HTTP/2 backend.
pub async fn serve(req: &L7Request, upstream: &Arc<Http2Upstream>) -> L7Response {
    let mut headers = req.headers.clone();
    velda_http2::client::sanitize_h2_headers(&mut headers);

    let body_bytes = match &req.body {
        velda_core::Body::Bytes(b) => b.clone(),
        velda_core::Body::Empty => bytes::Bytes::new(),
    };

    let h2_config = Http2Config::auto();

    let (mut client, _lease) = match upstream.acquire(&h2_config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection for H3->H2 server stream bridge"
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

    let mut pipe_res = send_h2_stream(&mut client, req, &headers, &body_bytes).await;
    if let Err(ref e) = pipe_res
        && is_h2_refused_or_goaway(e)
    {
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/2 stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((mut fresh_client, _fresh_lease)) = upstream.acquire_fresh(&h2_config).await {
            pipe_res = send_h2_stream(&mut fresh_client, req, &headers, &body_bytes).await;
        }
    }

    match pipe_res {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/2 upstream server-stream pipe failed"
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

async fn send_h2_stream(
    client: &mut h2::client::SendRequest<bytes::Bytes>,
    req: &L7Request,
    headers: &http::HeaderMap,
    body: &bytes::Bytes,
) -> Result<L7Response, h2::Error> {
    let mut h2_req = http::Request::builder()
        .method(req.method.clone())
        .uri(req.uri.clone())
        .version(http::Version::HTTP_2);
    *h2_req.headers_mut().unwrap() = headers.clone();
    let h2_req = h2_req.body(()).unwrap();

    let has_body = !body.is_empty();
    let (response_future, mut send_stream) = client.send_request(h2_req, !has_body)?;
    if has_body {
        send_stream.send_data(body.clone(), true)?;
    }

    let response = response_future.await?;
    let (parts, mut body_stream) = response.into_parts();

    let mut resp_body = BytesMut::new();
    while let Some(chunk) = body_stream.data().await {
        let chunk = chunk?;
        body_stream.flow_control().release_capacity(chunk.len())?;
        resp_body.extend_from_slice(&chunk);
    }

    Ok(L7Response::new(
        parts.status,
        http::Version::HTTP_3,
        parts.headers,
        velda_core::Body::Bytes(resp_body.freeze()),
    ))
}

#[inline]
fn is_h2_refused_or_goaway(err: &h2::Error) -> bool {
    (err.is_reset() && err.reason() == Some(h2::Reason::REFUSED_STREAM)) || err.is_go_away()
}
