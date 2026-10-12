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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use velda_core::Body;
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequest,
    Http1ServerRequestHead, Http1ServerResponse, read_response_head, sanitize_headers,
    send_request,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a buffered HTTP/1.1 request-response transaction forwarded to an HTTP/1.1 backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http1Upstream>,
    meta: &DownstreamMeta,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 1. Enrich proxy forwarding headers
    velda_http1::server::header::enrich_headers(
        &mut head.headers,
        &head.uri,
        meta.peer,
        meta.local_addr,
        meta.is_tls,
    );

    // 2. Handle Expect: 100-continue unblocking
    if head.is_expect_100_continue() && framing != Http1BodyFraming::Empty {
        if let Err(e) = conn.send_100_continue().await {
            tracing::debug!(error = %e, "Failed to send 100 Continue downstream");
            return Err(e);
        }
        head.headers.remove(http::header::EXPECT);
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
    let mut req = Http1ServerRequest::from_parts(head, body);
    sanitize_headers(&mut req.headers);

    // 5. Send request and read response head with self-healing retry on stale pooled connection
    let (mut resp_head, resp_framing) = {
        let exchange_attempt = async {
            send_request(&req, &mut *upstream_lease, &mut conn.upstream_write_buf).await?;
            read_response_head(&mut *upstream_lease, &mut conn.upstream_read_buf, &cfg).await
        };

        match exchange_attempt.await {
            Ok(head_and_framing) => head_and_framing,
            Err(e) if upstream_lease.is_reused() && e.is_stale_connection() => {
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

    // 6. Validate that upstream does not violate buffered mode invariant
    if resp_framing == Http1BodyFraming::Chunked {
        upstream_lease.mark_closed();
        return Err(Http1Error::StreamingViolation(
            "Upstream returned chunked transfer-encoding, but server streaming is disabled on this listener"
                .into(),
        ));
    }

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

    let body = if is_head || is_no_body_status {
        Body::Empty
    } else {
        match resp_framing {
            Http1BodyFraming::Empty => Body::Empty,
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
                Body::Bytes(conn.upstream_read_buf.split_to(len).freeze())
            }
            Http1BodyFraming::Chunked => unreachable!(),
        }
    };

    // 8. Send response downstream (align response version with downstream request)
    if req.version == http::Version::HTTP_10 {
        resp_head.version = http::Version::HTTP_10;
    }
    let resp =
        Http1ServerResponse::new(resp_head.status, resp_head.version, resp_head.headers, body);
    conn.send_response(&resp).await?;

    Ok(())
}
