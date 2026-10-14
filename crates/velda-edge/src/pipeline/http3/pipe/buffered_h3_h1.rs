//! Layer 7 HTTP/3 to HTTP/1.1 Buffered Bridge Pipeline.
//!
//! Handles standard request-response RPCs where incoming HTTP/3 QUIC requests
//! are bridged and forwarded to upstream HTTP/1.1 backends.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::AsyncReadExt;
use velda_core::{L7Request, L7Response};
use velda_http1::client::UpstreamHttp1Stream;

use crate::upstream::Http1Upstream;

/// Serves a buffered HTTP/3 downstream request bridged to an HTTP/1.1 backend.
pub async fn serve(req: &L7Request, upstream: &Arc<Http1Upstream>) -> L7Response {
    let mut lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection for H3->H1 bridge"
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

    let mut pipe_res = send_h1_buffered(&mut lease, req, &req.headers, &upstream.config).await;

    if pipe_res.is_err() {
        lease.mark_closed();
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/1.1 upstream connection failed; self-healing with fresh connection"
        );
        if let Ok(mut fresh_lease) = upstream.acquire_fresh().await {
            pipe_res =
                send_h1_buffered(&mut fresh_lease, req, &req.headers, &upstream.config).await;
            if pipe_res.is_err() {
                fresh_lease.mark_closed();
            }
        }
    }

    match pipe_res {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/1.1 upstream buffered bridge pipe failed"
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

async fn send_h1_buffered(
    stream: &mut UpstreamHttp1Stream,
    req: &L7Request,
    headers: &http::HeaderMap,
    cfg: &velda_http1::config::Http1Config,
) -> Result<L7Response, velda_http1::Http1Error> {
    let mut write_buf = BytesMut::with_capacity(1024);
    let read_cap = cfg.client_body_buffer_size.clamp(16_384, 65_536);
    let mut read_buf = BytesMut::with_capacity(read_cap);

    velda_http1::client::send_request_parts(
        &req.method,
        &req.uri,
        headers,
        &req.body,
        stream,
        &mut write_buf,
    )
    .await?;

    // 1. Read and decode response head (single-pass parsing without redundant loops)
    let (head, framing) =
        velda_http1::client::read_response_head(stream, &mut read_buf, cfg).await?;

    // 2. Read bounded response body with exact pre-allocation to prevent repeated reallocations & memcpys
    let body = match framing {
        velda_http1::server::request::Http1BodyFraming::Empty => velda_core::Body::Empty,
        velda_http1::server::request::Http1BodyFraming::ContentLength(body_len) => {
            if body_len > cfg.max_body_size {
                return Err(velda_http1::Http1Error::PayloadTooLarge(body_len));
            }
            if body_len > 0 {
                if read_buf.len() < body_len {
                    let needed = body_len - read_buf.len();
                    read_buf.reserve(needed);
                    while read_buf.len() < body_len {
                        let n = stream.read_buf(&mut read_buf).await?;
                        if n == 0 {
                            return Err(velda_http1::Http1Error::ConnectionClosed);
                        }
                    }
                }
                velda_core::Body::Bytes(read_buf.split_to(body_len).freeze())
            } else {
                velda_core::Body::Empty
            }
        }
        velda_http1::server::request::Http1BodyFraming::Chunked => {
            let mut chunked_body = BytesMut::new();
            while let Some(chunk) =
                velda_http1::client::read_next_chunk(stream, &mut read_buf).await?
            {
                if chunked_body.len() + chunk.len() > cfg.max_body_size {
                    return Err(velda_http1::Http1Error::PayloadTooLarge(
                        chunked_body.len() + chunk.len(),
                    ));
                }
                chunked_body.extend_from_slice(&chunk);
            }
            if chunked_body.is_empty() {
                velda_core::Body::Empty
            } else {
                velda_core::Body::Bytes(chunked_body.freeze())
            }
        }
    };

    Ok(L7Response::new(
        head.status,
        head.version,
        head.headers,
        body,
    ))
}
