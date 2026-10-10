//! Layer 7 HTTP/3 to HTTP/1.1 Server Streaming Bridge Pipeline.
//!
//! Handles streaming responses where incoming HTTP/3 QUIC requests
//! are bridged and forwarded to upstream HTTP/1.1 backends.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::AsyncReadExt;
use velda_core::{L7Request, L7Response};
use velda_http1::client::UpstreamHttp1Stream;

use crate::upstream::Http1Upstream;

/// Serves a server-streaming HTTP/3 downstream request bridged to an HTTP/1.1 backend.
pub async fn serve(req: &L7Request, upstream: &Arc<Http1Upstream>) -> L7Response {
    let mut headers = req.headers.clone();

    // Ensure RFC 9112 §7.1 mandatory Host header for HTTP/1.1
    if !headers.contains_key(http::header::HOST)
        && let Some(host) = velda_http3::server::header::extract_host(&headers, &req.uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        headers.insert(http::header::HOST, hv);
    }

    let mut lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection for H3->H1 server stream bridge"
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

    let cfg = velda_http1::config::Http1Config::auto();
    let mut pipe_res = send_h1_stream(&mut lease, req, &headers, &cfg).await;

    if pipe_res.is_err() {
        lease.mark_closed();
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/1.1 upstream connection failed; self-healing with fresh connection"
        );
        if let Ok(mut fresh_lease) = upstream.acquire_fresh().await {
            pipe_res = send_h1_stream(&mut fresh_lease, req, &headers, &cfg).await;
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
                "HTTP/1.1 upstream server-stream bridge pipe failed"
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

async fn send_h1_stream(
    stream: &mut UpstreamHttp1Stream,
    req: &L7Request,
    headers: &http::HeaderMap,
    cfg: &velda_http1::config::Http1Config,
) -> Result<L7Response, velda_http1::Http1Error> {
    let body_len = match &req.body {
        velda_core::Body::Bytes(b) => b.len(),
        velda_core::Body::Empty => 0,
    };
    let mut write_buf = BytesMut::with_capacity(cfg.upstream_write_base + body_len);
    let mut read_buf = BytesMut::with_capacity(cfg.upstream_read_capacity);

    velda_http1::client::send_request_parts(
        &req.method,
        &req.uri,
        headers,
        &req.body,
        stream,
        &mut write_buf,
    )
    .await?;

    loop {
        match stream.read_buf(&mut read_buf).await {
            Ok(0) => {
                if let Ok(Some(resp)) = velda_http1::client::decode_response(&mut read_buf, cfg) {
                    return Ok(resp.into_l7_response());
                } else {
                    return Err(velda_http1::Http1Error::ConnectionClosed);
                }
            }
            Ok(_) => match velda_http1::client::decode_response(&mut read_buf, cfg) {
                Ok(Some(resp)) => return Ok(resp.into_l7_response()),
                Ok(None) => continue,
                Err(e) => return Err(e),
            },
            Err(e) => return Err(velda_http1::Http1Error::Io(e)),
        }
    }
}
