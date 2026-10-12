//! Layer 7 HTTP/1.1 to HTTP/1.1 Client-Stream Pipeline.
//!
//! Handles client streaming uploads (e.g. chunked file uploads, log batch ingest) where
//! downstream request chunks are streamed in real-time to the upstream backend.
//! Leases upstream connection prior to transmission.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http1`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::Body;
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse, parse_single_chunk, read_response_head, sanitize_headers,
    send_chunked_end, send_request, send_request_head_chunked,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a client-streaming HTTP/1.1 transaction forwarded to an HTTP/1.1 backend.
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

    // 3. Acquire upstream connection before streaming upload begins
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
    sanitize_headers(&mut head.headers);

    // 4. If request has no body (e.g. GET, HEAD), forward directly without chunked stream
    if framing == Http1BodyFraming::Empty {
        let req = velda_http1::Http1ServerRequest::new(
            head.method,
            head.uri,
            head.version,
            head.headers,
            Body::Empty,
        );

        let res = async {
            send_request(&req, &mut *upstream_lease, &mut conn.upstream_write_buf).await?;
            let (mut resp_head, resp_framing) = read_response_head(
                &mut *upstream_lease,
                &mut conn.upstream_read_buf,
                &cfg,
            )
            .await?;
            sanitize_headers(&mut resp_head.headers);

            let is_head = req.method == http::Method::HEAD;
            let is_no_body_status = resp_head.status.is_informational()
                || resp_head.status == StatusCode::NO_CONTENT
                || resp_head.status == StatusCode::NOT_MODIFIED;

            let body = if is_head || is_no_body_status {
                Body::Empty
            } else {
                match resp_framing {
                    Http1BodyFraming::Empty => Body::Empty,
                    Http1BodyFraming::ContentLength(len) => {
                        if len > cfg.max_body_size {
                            return Err(Http1Error::PayloadTooLarge(len));
                        }
                        if conn.upstream_read_buf.len() < len {
                            let needed = len - conn.upstream_read_buf.len();
                            conn.upstream_read_buf.reserve(needed);
                            while conn.upstream_read_buf.len() < len {
                                let n = upstream_lease
                                    .read_buf(&mut conn.upstream_read_buf)
                                    .await?;
                                if n == 0 {
                                    return Err(Http1Error::Parse(
                                        "Unexpected EOF while reading upstream response body: premature connection close".into(),
                                    ));
                                }
                            }
                        }
                        Body::Bytes(conn.upstream_read_buf.split_to(len).freeze())
                    }
                    Http1BodyFraming::Chunked => {
                        return Err(Http1Error::StreamingViolation(
                            "Upstream returned chunked transfer-encoding on client-streaming route with server streaming disabled".into(),
                        ));
                    }
                }
            };

            let resp = Http1ServerResponse::new(
                resp_head.status,
                resp_head.version,
                resp_head.headers,
                body,
            );
            conn.send_response(&resp).await?;
            Ok::<(), Http1Error>(())
        }
        .await;

        if res.is_err() {
            upstream_lease.mark_closed();
        }
        return res;
    }

    // 5. Pipe streaming upload chunks to upstream
    let pipe_res = async {
        // Send chunked request head upstream reusing scratch buffer
        send_request_head_chunked(&head, &mut *upstream_lease, &mut conn.upstream_write_buf)
            .await?;

        // Stream request body chunks to upstream reusing conn.read_buf with zero heap allocations
        let mut total_body_bytes = 0usize;
        let idle_timeout = Duration::from_millis(cfg.idle_timeout_ms);
        let target_reserve = cfg.client_body_buffer_size.max(65536);
        let mut sent_terminal = false;

        loop {
            // Coalesce as many complete valid chunks as possible from conn.read_buf
            let mut coalesced_wire_len = 0usize;
            let mut coalesced_payload_bytes = 0usize;
            let mut reached_terminal = false;

            while coalesced_wire_len < conn.read_buf.len() {
                match parse_single_chunk(&conn.read_buf[coalesced_wire_len..])? {
                    Some((needed, payload, is_terminal)) => {
                        coalesced_wire_len += needed;
                        coalesced_payload_bytes += payload.len();
                        if is_terminal {
                            reached_terminal = true;
                            break;
                        }
                    }
                    None => break,
                }
            }

            if coalesced_wire_len > 0 {
                total_body_bytes = total_body_bytes
                    .checked_add(coalesced_payload_bytes)
                    .ok_or(Http1Error::PayloadTooLarge(usize::MAX))?;
                if total_body_bytes > cfg.max_body_size {
                    return Err(Http1Error::PayloadTooLarge(total_body_bytes));
                }

                // Zero-rechunking passthrough: forward the exact wire bytes directly to upstream
                upstream_lease
                    .write_all(&conn.read_buf[..coalesced_wire_len])
                    .await
                    .map_err(Http1Error::Io)?;

                let remaining_len = conn.read_buf.len() - coalesced_wire_len;
                if remaining_len == 0 {
                    conn.read_buf.clear();
                } else {
                    conn.read_buf.copy_within(coalesced_wire_len.., 0);
                    conn.read_buf.truncate(remaining_len);
                }

                if reached_terminal {
                    sent_terminal = true;
                    break;
                }
                continue;
            }

            // Incomplete chunk in buffer: ensure buffer capacity has room for next chunk
            if conn.read_buf.capacity() - conn.read_buf.len() < target_reserve {
                conn.read_buf.reserve(target_reserve);
            }

            let bytes_read = match tokio::time::timeout(
                idle_timeout,
                conn.stream.read_buf(&mut conn.read_buf),
            )
            .await
            {
                Ok(res) => res?,
                Err(_) => return Err(Http1Error::Timeout),
            };

            if bytes_read == 0 {
                if conn.read_buf.is_empty() {
                    break;
                } else {
                    return Err(Http1Error::Parse(
                        "Unexpected EOF while reading chunked body".into(),
                    ));
                }
            }
        }

        if !sent_terminal {
            send_chunked_end(&mut *upstream_lease).await?;
        }

        // Read upstream response head
        let (mut resp_head, resp_framing) = read_response_head(
            &mut *upstream_lease,
            &mut conn.upstream_read_buf,
            &cfg,
        )
        .await?;
        sanitize_headers(&mut resp_head.headers);

        // Validate response framing (client streaming expects non-streaming response)
        if resp_framing == Http1BodyFraming::Chunked {
            return Err(Http1Error::StreamingViolation(
                "Upstream returned chunked transfer-encoding on client-streaming route with server streaming disabled".into(),
            ));
        }

        let is_head = head.method == http::Method::HEAD;
        let is_no_body_status = resp_head.status.is_informational()
            || resp_head.status == StatusCode::NO_CONTENT
            || resp_head.status == StatusCode::NOT_MODIFIED;

        let body = if is_head || is_no_body_status {
            Body::Empty
        } else {
            match resp_framing {
                Http1BodyFraming::Empty => Body::Empty,
                Http1BodyFraming::ContentLength(len) => {
                    if len > cfg.max_body_size {
                        return Err(Http1Error::PayloadTooLarge(len));
                    }
                    if conn.upstream_read_buf.len() < len {
                        let needed = len - conn.upstream_read_buf.len();
                        conn.upstream_read_buf.reserve(needed);
                        while conn.upstream_read_buf.len() < len {
                            let n = upstream_lease
                                .read_buf(&mut conn.upstream_read_buf)
                                .await?;
                            if n == 0 {
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

        let resp =
            Http1ServerResponse::new(resp_head.status, resp_head.version, resp_head.headers, body);
        conn.send_response(&resp).await?;
        Ok::<(), Http1Error>(())
    }
    .await;

    if pipe_res.is_err() {
        upstream_lease.mark_closed();
    }

    pipe_res
}
