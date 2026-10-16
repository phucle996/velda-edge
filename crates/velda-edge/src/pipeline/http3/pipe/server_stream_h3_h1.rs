//! Layer 7 HTTP/3 to HTTP/1.1 Server Streaming Bridge Pipeline.
//!
//! Handles progressive streaming responses (large files, SSE, chunked push) from HTTP/1.1 backends,
//! streaming them immediately as HTTP/3 DATA frames to downstream QUIC clients with flow-control backpressure.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http1::server::request::Http1BodyFraming;

use crate::pipeline::http3::engine::Http3Responder;
use crate::upstream::Http1Upstream;

/// Minimum buffer capacity for reading upstream HTTP/1.1 response headers (16 KB).
const MIN_UPSTREAM_READ_BUF_SIZE: usize = 16 * 1024;

/// Maximum buffer capacity for reading upstream HTTP/1.1 response headers (64 KB).
const MAX_UPSTREAM_READ_BUF_SIZE: usize = 64 * 1024;

/// Minimum progressive chunk size for downstream HTTP/3 DATA streaming (32 KB).
/// Avoids syscall amplification and frame overhead on high-bandwidth streams.
const MIN_STREAMING_CHUNK_SIZE: usize = 32 * 1024;

/// Maximum progressive chunk size for downstream HTTP/3 DATA streaming (128 KB).
/// Aligns with double the Linux UDP GSO segment size (64 KB) to avoid packet pacing latency spikes.
const MAX_STREAMING_CHUNK_SIZE: usize = 128 * 1024;

/// Serves a server-streaming HTTP/3 downstream request bridged to an HTTP/1.1 backend.
pub async fn serve(req: &L7Request, responder: Http3Responder, upstream: &Arc<Http1Upstream>) {
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

    let mut write_buf = bytes::BytesMut::with_capacity(1024);

    // Send HTTP/1.1 request head and body
    let send_res = velda_http1::client::send_request_parts(
        &req.method,
        &req.uri,
        &headers,
        &req.body,
        &mut *lease,
        &mut write_buf,
    )
    .await;

    if let Err(e) = send_res {
        lease.mark_closed();
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to send request head to HTTP/1.1 upstream"
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

    // Read HTTP/1.1 response head from upstream
    let read_cap = upstream
        .config
        .client_body_buffer_size
        .clamp(MIN_UPSTREAM_READ_BUF_SIZE, MAX_UPSTREAM_READ_BUF_SIZE);
    let mut read_buf = bytes::BytesMut::with_capacity(read_cap);
    let (resp_head, framing) =
        match velda_http1::client::read_response_head(&mut *lease, &mut read_buf, &upstream.config)
            .await
        {
            Ok(parts) => parts,
            Err(e) => {
                lease.mark_closed();
                tracing::warn!(
                    error = %e,
                    upstream = %upstream.id(),
                    "Failed to read HTTP/1.1 response head from upstream"
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

    let conn_close = check_connection_close(&resp_head.headers);

    // Strip hop-by-hop headers before sending over HTTP/3
    let mut clean_headers = resp_head.headers;
    velda_http1::client::sanitize_headers(&mut clean_headers);

    // Open downstream HTTP/3 streaming response
    let mut stream_sender = match responder
        .send_stream_response(resp_head.status, &clean_headers)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            lease.mark_closed();
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to initiate downstream HTTP/3 streaming response"
            );
            return;
        }
    };

    // Stream response chunks based on HTTP/1.1 framing
    let stream_res = match framing {
        Http1BodyFraming::Empty => Ok(()),
        Http1BodyFraming::Chunked => loop {
            match velda_http1::client::read_next_chunk(&mut *lease, &mut read_buf).await {
                Ok(Some(chunk)) => {
                    if let Err(e) = stream_sender.send_chunk(chunk).await {
                        break Err(e);
                    }
                }
                Ok(None) => break Ok(()),
                Err(_) => {
                    lease.mark_closed();
                    break Ok(());
                }
            }
        },
        Http1BodyFraming::ContentLength(total_len) => {
            let mut remaining = total_len;
            let mut failed = false;
            let chunk_limit = upstream
                .config
                .client_body_buffer_size
                .clamp(MIN_STREAMING_CHUNK_SIZE, MAX_STREAMING_CHUNK_SIZE);
            while remaining > 0 {
                let to_read = remaining.min(chunk_limit);
                match velda_http1::client::read_chunk_sized(&mut *lease, &mut read_buf, to_read)
                    .await
                {
                    Ok(Some(chunk)) => {
                        remaining = remaining.saturating_sub(chunk.len());
                        if let Err(e) = stream_sender.send_chunk(chunk).await {
                            tracing::debug!(error = %e, "Downstream client disconnected during H3 streaming");
                            failed = true;
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        lease.mark_closed();
                        failed = true;
                        break;
                    }
                }
            }
            if failed {
                lease.mark_closed();
            }
            Ok(())
        }
    };

    if conn_close || stream_res.is_err() {
        lease.mark_closed();
    }

    let _ = stream_sender.finish().await;
}

#[inline]
fn check_connection_close(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|val| val.eq_ignore_ascii_case("close"))
}
