//! Layer 7 HTTP/3 to HTTP/1.1 Buffered Bridge Pipeline.
//!
//! Handles standard request-response RPCs where incoming HTTP/3 QUIC requests
//! are bridged and forwarded to upstream HTTP/1.1 backends.
//!
//! Follows the Velda Edge Hourglass Architecture:
//! - Contiguous, linear workflow top-to-bottom within `serve`.
//! - Transparent self-healing retry on stale pooled connections.
//! - Direct pooled buffer reuse (`split_all()`, `split_read()`) for zero heap allocation.
//! - Protocol mechanics completely decoupled from plugin logic.

use std::sync::Arc;

use bytes::{Buf, BytesMut};
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::AsyncReadExt;
use velda_core::{L7Request, L7Response};
use velda_http1::is_connection_close;
use velda_http1::server::request::Http1BodyFraming;

use crate::upstream::Http1Upstream;

/// Serves a buffered HTTP/3 downstream request bridged to an HTTP/1.1 backend.
pub async fn serve(req: &L7Request, upstream: &Arc<Http1Upstream>) -> L7Response {
    // 1. Ensure mandatory Host header (RFC 9112 §7.1) for upstream HTTP/1.1
    let mut headers = req.headers.clone();
    if !headers.contains_key(http::header::HOST)
        && let Some(host) = velda_http3::server::header::extract_host(&headers, &req.uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        headers.insert(http::header::HOST, hv);
    }

    // 2. Acquire pooled HTTP/1.1 upstream connection
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

    // 3. Transmit request and read response head with self-healing retry on stale connection
    let (head, framing) = {
        let (stream, read_buf, write_buf) = lease.split_all();
        write_buf.clear();

        let attempt = async {
            velda_http1::client::send_request_parts(
                &req.method,
                &req.uri,
                &headers,
                &req.body,
                stream,
                write_buf,
            )
            .await?;
            velda_http1::client::read_response_head(stream, read_buf, &upstream.config).await
        };

        match attempt.await {
            Ok(parts) => parts,
            Err(e) if lease.is_reused && e.is_stale_connection() => {
                lease.mark_closed();
                tracing::debug!(
                    upstream = %upstream.id(),
                    "Stale pooled HTTP/1.1 connection detected in H3->H1 bridge; retrying with fresh connection"
                );
                match upstream.acquire_fresh().await {
                    Ok(mut fresh_lease) => {
                        let (f_stream, f_read_buf, f_write_buf) = fresh_lease.split_all();
                        f_write_buf.clear();
                        let retry_res = async {
                            velda_http1::client::send_request_parts(
                                &req.method,
                                &req.uri,
                                &headers,
                                &req.body,
                                f_stream,
                                f_write_buf,
                            )
                            .await?;
                            velda_http1::client::read_response_head(
                                f_stream,
                                f_read_buf,
                                &upstream.config,
                            )
                            .await
                        }
                        .await;

                        match retry_res {
                            Ok(parts) => {
                                lease = fresh_lease;
                                parts
                            }
                            Err(retry_err) => {
                                fresh_lease.mark_closed();
                                tracing::warn!(
                                    error = %retry_err,
                                    upstream = %upstream.id(),
                                    "HTTP/1.1 upstream buffered bridge pipe failed after retry"
                                );
                                return L7Response::from_bytes(
                                    StatusCode::BAD_GATEWAY,
                                    format!("502 Bad Gateway: {retry_err}\n").into_bytes(),
                                )
                                .with_header(
                                    CONTENT_TYPE,
                                    HeaderValue::from_static("text/plain; charset=utf-8"),
                                );
                            }
                        }
                    }
                    Err(fresh_err) => {
                        tracing::warn!(
                            error = %fresh_err,
                            upstream = %upstream.id(),
                            "Failed to acquire fresh connection for self-healing retry"
                        );
                        return L7Response::from_bytes(
                            StatusCode::BAD_GATEWAY,
                            format!("502 Bad Gateway: {fresh_err}\n").into_bytes(),
                        )
                        .with_header(
                            CONTENT_TYPE,
                            HeaderValue::from_static("text/plain; charset=utf-8"),
                        );
                    }
                }
            }
            Err(e) => {
                lease.mark_closed();
                tracing::warn!(
                    error = %e,
                    upstream = %upstream.id(),
                    "HTTP/1.1 upstream buffered bridge pipe failed"
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
        }
    };

    // 4. Mark connection closed if upstream signaled Connection: close
    if is_connection_close(&head.headers) {
        lease.mark_closed();
    }

    // 5. Read bounded response body using pre-allocated pooled buffer
    let (stream, read_buf) = lease.split_read();
    let body = match framing {
        Http1BodyFraming::Empty => velda_core::Body::Empty,
        Http1BodyFraming::ContentLength(body_len) => {
            if body_len > upstream.config.max_body_size {
                lease.mark_closed();
                return L7Response::from_bytes(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    b"413 Payload Too Large\n".to_vec(),
                );
            }
            if body_len > 0 {
                if read_buf.len() < body_len {
                    let needed = body_len - read_buf.len();
                    read_buf.reserve(needed);
                    while read_buf.len() < body_len {
                        match stream.read_buf(read_buf).await {
                            Ok(0) => {
                                lease.mark_closed();
                                return L7Response::from_bytes(
                                    StatusCode::BAD_GATEWAY,
                                    b"502 Bad Gateway: Premature connection EOF while reading body\n".to_vec(),
                                );
                            }
                            Ok(_) => {}
                            Err(e) => {
                                lease.mark_closed();
                                return L7Response::from_bytes(
                                    StatusCode::BAD_GATEWAY,
                                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                                );
                            }
                        }
                    }
                }
                // Preserve buffer uniqueness for high-capacity pool buffers
                let body_bytes = if body_len <= 4096 && read_buf.capacity() >= 16384 {
                    let b = bytes::Bytes::copy_from_slice(&read_buf[..body_len]);
                    read_buf.advance(body_len);
                    b
                } else {
                    read_buf.split_to(body_len).freeze()
                };
                velda_core::Body::Bytes(body_bytes)
            } else {
                velda_core::Body::Empty
            }
        }
        Http1BodyFraming::Chunked => {
            let mut chunked_body = BytesMut::with_capacity(4096);
            loop {
                match velda_http1::client::read_next_chunk(stream, read_buf).await {
                    Ok(Some(chunk)) => {
                        if chunked_body.len() + chunk.len() > upstream.config.max_body_size {
                            lease.mark_closed();
                            return L7Response::from_bytes(
                                StatusCode::PAYLOAD_TOO_LARGE,
                                b"413 Payload Too Large\n".to_vec(),
                            );
                        }
                        chunked_body.extend_from_slice(&chunk);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        lease.mark_closed();
                        return L7Response::from_bytes(
                            StatusCode::BAD_GATEWAY,
                            format!("502 Bad Gateway: {e}\n").into_bytes(),
                        );
                    }
                }
            }
            if chunked_body.is_empty() {
                velda_core::Body::Empty
            } else {
                velda_core::Body::Bytes(chunked_body.freeze())
            }
        }
    };

    // 6. Reset pooled read buffer and release lease back to connection pool
    read_buf.clear();
    drop(lease);

    L7Response::new(head.status, head.version, head.headers, body)
}
