//! Layer 7 HTTP/1.1 to HTTP/1.1 Buffered Pipeline.
//!
//! Handles standard request-response REST APIs where the full downstream request body
//! is buffered into RAM before an upstream connection is leased from the pool.
//! Completely eliminates upstream pool connection starvation from slow downstream uploads.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http1`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequest,
    Http1ServerRequestHead, Http1ServerResponse, is_connection_close, read_response_head,
    sanitize_headers, send_request,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a buffered HTTP/1.1 request-response transaction forwarded to an HTTP/1.1 backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut req_head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http1Upstream>,
    meta: &DownstreamMeta,
    plan: &velda_plugin::RoutePluginPlan,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 1. Enrich proxy forwarding headers using pre-computed connection metadata
    velda_http1::server::header::enrich_headers_cached(
        &mut req_head.headers,
        &req_head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 2. Handle Expect: 100-continue unblocking
    if req_head.is_expect_100_continue() && framing != Http1BodyFraming::Empty {
        if let Err(e) = conn.send_100_continue().await {
            tracing::debug!(error = %e, "Failed to send 100 Continue downstream");
            return Err(e);
        }
        req_head.headers.remove(http::header::EXPECT);
    }

    // 3. Read entire downstream body into RAM (protect upstream pool from slow uploads)
    let body = match conn.read_body(framing).await {
        Ok(b) => b,
        Err(Http1Error::Timeout) => {
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::REQUEST_TIMEOUT,
                b"408 Request Timeout: client body read idle timeout exceeded\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::Timeout);
        }
        Err(e) => {
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(e);
        }
    };

    // 4. Acquire pooled HTTP/1.1 upstream connection
    let mut upstream_lease = match upstream.acquire().await {
        Ok(lease) => lease,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection"
            );
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::ConnectionClosed);
        }
    };

    let cfg = *conn.config();
    let mut req = Http1ServerRequest::from_parts(req_head, body);
    sanitize_headers(&mut req.headers);

    // 5. Send request and read response head with self-healing retry on stale pooled connection
    let (mut resp_head, resp_framing) = {
        let exchange_attempt = async {
            send_request(&req, &mut *upstream_lease, &mut conn.upstream_write_buf).await?;
            read_response_head(&mut *upstream_lease, &mut conn.upstream_read_buf, &cfg).await
        };

        match exchange_attempt.await {
            Ok(head_and_framing) => head_and_framing,
            Err(e) if upstream_lease.is_reused && e.is_stale_connection() => {
                upstream_lease.mark_closed();
                drop(upstream_lease);

                tracing::debug!(
                    upstream = %upstream.id(),
                    "Stale pooled HTTP/1.1 connection detected; self-healing with fresh connection"
                );

                match upstream.acquire_fresh().await {
                    Ok(mut fresh_lease) => {
                        let retry_res = async {
                            send_request(&req, &mut *fresh_lease, &mut conn.upstream_write_buf)
                                .await?;
                            read_response_head(&mut *fresh_lease, &mut conn.upstream_read_buf, &cfg)
                                .await
                        }
                        .await;

                        match retry_res {
                            Ok(head_and_framing) => {
                                upstream_lease = fresh_lease;
                                head_and_framing
                            }
                            Err(retry_err) => {
                                fresh_lease.mark_closed();
                                return Err(retry_err);
                            }
                        }
                    }
                    Err(fresh_err) => {
                        tracing::warn!(
                            error = %fresh_err,
                            upstream = %upstream.id(),
                            "Failed to acquire fresh connection for self-healing retry"
                        );
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                upstream_lease.mark_closed();
                return Err(e);
            }
        }
    };

    sanitize_headers(&mut resp_head.headers);
    if let Some(req_id) = req.headers.get(&crate::pipeline::X_REQUEST_ID) {
        resp_head
            .headers
            .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
    }

    // 6. Validate that upstream does not violate buffered mode invariant
    if let Some(content_type) = resp_head.headers.get(http::header::CONTENT_TYPE)
        && content_type
            .as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    {
        upstream_lease.mark_closed();
        return Err(Http1Error::StreamingViolation(
            "Upstream returned text/event-stream (SSE), but server streaming is disabled on this listener"
                .into(),
        ));
    }

    // 7. Read bounded response body (RFC 9110 §6.4.1 & §9.3.2: HEAD, 204, 304 have no body)
    let is_head = req.method == http::Method::HEAD;
    let is_no_body_status = resp_head.status.is_informational()
        || resp_head.status == http::StatusCode::NO_CONTENT
        || resp_head.status == http::StatusCode::NOT_MODIFIED;

    let (body_len, decoded_chunk_bytes) = match resp_framing {
        Http1BodyFraming::Empty => (0, None),
        Http1BodyFraming::ContentLength(len) => {
            if len > cfg.max_body_size {
                upstream_lease.mark_closed();
                return Err(Http1Error::PayloadTooLarge(len));
            }
            if conn.upstream_read_buf.len() < len {
                let needed = len - conn.upstream_read_buf.len();
                conn.upstream_read_buf.reserve(needed);
                while conn.upstream_read_buf.len() < len {
                    let n = upstream_lease.read_buf(&mut conn.upstream_read_buf).await?;
                    if n == 0 {
                        upstream_lease.mark_closed();
                        return Err(Http1Error::Parse(
                            "Unexpected EOF while reading upstream response body: premature connection close".into(),
                        ));
                    }
                }
            }
            (len, None)
        }
        Http1BodyFraming::Chunked => {
            resp_head.headers.remove(http::header::TRANSFER_ENCODING);
            loop {
                match velda_http1::wire::parse_chunked_body(
                    &conn.upstream_read_buf,
                    cfg.max_body_size,
                )? {
                    Some((_consumed, body)) => {
                        let bytes = match body {
                            velda_core::Body::Bytes(b) => b,
                            velda_core::Body::Empty => bytes::Bytes::new(),
                        };
                        break (bytes.len(), Some(bytes));
                    }
                    None => {
                        let n = upstream_lease.read_buf(&mut conn.upstream_read_buf).await?;
                        if n == 0 {
                            upstream_lease.mark_closed();
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream chunked response body: premature connection close".into(),
                            ));
                        }
                    }
                }
            }
        }
    };

    // 8. Align response version with downstream request
    if req.version == http::Version::HTTP_10 {
        resp_head.version = http::Version::HTTP_10;
    }

    // Execute post-response plugins if attached to this route (0 overhead if None)
    let transformed_body: Option<bytes::Bytes> = if let Some(post_hooks) = &plan.post_response_hooks
    {
        // Enforce Payload Plugin Invariant:
        // If route has response plugins requiring payload access (e.g. http_cache_store, response_compression),
        // supply genuine payload slice; otherwise pass Body::Empty for 100% Zero-Copy fast-path.
        let initial_body = if plan.is_response_payload_passthrough()
            || is_head
            || is_no_body_status
            || body_len == 0
        {
            velda_core::Body::Empty
        } else if let Some(b) = &decoded_chunk_bytes {
            velda_core::Body::Bytes(b.clone())
        } else {
            velda_core::Body::Bytes(bytes::Bytes::copy_from_slice(
                &conn.upstream_read_buf[..body_len],
            ))
        };

        let mut l7_resp = velda_core::L7Response::new(
            resp_head.status,
            resp_head.version,
            std::mem::take(&mut resp_head.headers),
            initial_body,
        );
        velda_plugin::run_post_response_plugins(post_hooks, &req.headers, &mut l7_resp);
        resp_head.status = l7_resp.status;
        resp_head.headers = l7_resp.headers;
        match l7_resp.body {
            velda_core::Body::Bytes(b) => Some(b),
            velda_core::Body::Empty => None,
        }
    } else {
        None
    };

    let is_no_body = is_head || is_no_body_status;
    let actual_body_len = if is_no_body {
        0
    } else if let Some(b) = &transformed_body {
        b.len()
    } else {
        body_len
    };
    let force_close = conn.is_closed() || is_connection_close(&resp_head.headers);

    // 9. Encode response head into downstream write_buf with authoritative request ID invariant
    conn.write_buf.clear();
    conn.encode_response_head(
        resp_head.version,
        resp_head.status,
        &resp_head.headers,
        Some(actual_body_len),
        force_close,
    );

    // 10. DROP UPSTREAM LEASE IMMEDIATELY before starting downstream transmission!
    // Returning the connection to the pool early eliminates pool starvation under high concurrency.
    drop(upstream_lease);

    // 11. Zero-Copy Vectored Write downstream (writev)
    // Directly transmits headers (conn.write_buf) and body without
    // any intermediate memcpy or buffer reallocation on the hot path.
    if !is_no_body && actual_body_len > 0 {
        let (mut b1, mut b2) = if let Some(b) = &transformed_body {
            (&conn.write_buf[..], &b[..])
        } else if let Some(b) = &decoded_chunk_bytes {
            (&conn.write_buf[..], &b[..])
        } else {
            (&conn.write_buf[..], &conn.upstream_read_buf[..body_len])
        };
        while !b1.is_empty() || !b2.is_empty() {
            let bufs = [std::io::IoSlice::new(b1), std::io::IoSlice::new(b2)];
            let bufs_slice = if b1.is_empty() { &bufs[1..] } else { &bufs[..] };
            let n = conn.stream.write_vectored(bufs_slice).await?;
            if n == 0 {
                return Err(Http1Error::ConnectionClosed);
            }
            if n < b1.len() {
                b1 = &b1[n..];
            } else {
                let rem = n - b1.len();
                b1 = &[];
                b2 = &b2[rem..];
            }
        }
        conn.upstream_read_buf.clear();
    } else {
        conn.upstream_read_buf.clear();
        conn.stream.write_all(&conn.write_buf).await?;
    }
    conn.write_buf.clear();

    if force_close {
        conn.mark_close();
    }

    Ok(())
}
