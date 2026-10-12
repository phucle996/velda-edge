//! Layer 7 HTTP/1.1 to HTTP/1.1 Duplex Pipeline.
//!
//! Handles concurrent full-duplex bidirectional streaming (WebSocket upgrade, tunnels)
//! between downstream HTTP/1.1 client and upstream HTTP/1.1 backend.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http1`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::Body;
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse, Http1ServerResponseHead, parse_single_chunk, read_chunk_sized,
    read_next_chunk, read_response_head, sanitize_headers, send_chunk, send_chunked_end,
    send_request_head_chunked,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a full-duplex HTTP/1.1 transaction forwarded to an HTTP/1.1 backend.
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

    // 3. Acquire upstream connection before duplex streaming begins
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
    let head_method = head.method.clone();

    let idle_timeout = Duration::from_millis(cfg.idle_timeout_ms);
    let target_reserve = cfg.client_body_buffer_size.max(262144);
    let max_body_size = cfg.max_body_size;

    let (mut down_read, mut down_write) = tokio::io::split(&mut conn.stream);
    let (mut up_read, mut up_write) = tokio::io::split(&mut *upstream_lease);

    let Http1ServerConnection {
        ref mut read_buf,
        ref mut write_buf,
        ref mut upstream_read_buf,
        ref mut upstream_write_buf,
        ref mut close_requested,
        ..
    } = *conn;

    // Concurrently pump client chunked upload to upstream and upstream chunked response downstream.
    let upload_task = async {
        // Send chunked request head upstream reusing scratch buffer
        send_request_head_chunked(&head, &mut up_write, upstream_write_buf).await?;

        let mut total_req_bytes = 0usize;
        let mut sent_terminal = false;

        loop {
            // Coalesce as many complete valid chunks as possible from read_buf
            let mut coalesced_wire_len = 0usize;
            let mut coalesced_payload_bytes = 0usize;
            let mut reached_terminal = false;

            while coalesced_wire_len < read_buf.len() {
                match parse_single_chunk(&read_buf[coalesced_wire_len..])? {
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
                total_req_bytes = total_req_bytes
                    .checked_add(coalesced_payload_bytes)
                    .ok_or(Http1Error::PayloadTooLarge(usize::MAX))?;
                if total_req_bytes > max_body_size {
                    return Err(Http1Error::PayloadTooLarge(total_req_bytes));
                }

                up_write.write_all(&read_buf[..coalesced_wire_len]).await?;

                let remaining_len = read_buf.len() - coalesced_wire_len;
                if remaining_len == 0 {
                    read_buf.clear();
                } else {
                    read_buf.advance(coalesced_wire_len);
                }

                if reached_terminal {
                    sent_terminal = true;
                    break;
                }
                continue;
            }

            if read_buf.capacity() - read_buf.len() < target_reserve {
                read_buf.reserve(target_reserve);
            }

            let bytes_read =
                match tokio::time::timeout(idle_timeout, down_read.read_buf(read_buf)).await {
                    Ok(res) => res?,
                    Err(_) => return Err(Http1Error::Timeout),
                };

            if bytes_read == 0 {
                if read_buf.is_empty() {
                    break;
                } else {
                    return Err(Http1Error::Parse(
                        "Unexpected EOF while reading chunked body".into(),
                    ));
                }
            }
        }

        if !sent_terminal {
            send_chunked_end(&mut up_write).await?;
        }
        up_write.flush().await?;
        Ok::<(), Http1Error>(())
    };

    let download_task = async {
        // Read upstream response head reusing connection read buffer
        let (mut resp_head, resp_framing) =
            read_response_head(&mut up_read, upstream_read_buf, &cfg).await?;
        sanitize_headers(&mut resp_head.headers);

        let is_head = head_method == http::Method::HEAD;
        let is_no_body_status = resp_head.status.is_informational()
            || resp_head.status == StatusCode::NO_CONTENT
            || resp_head.status == StatusCode::NOT_MODIFIED;

        if is_head || is_no_body_status {
            let server_head = Http1ServerResponseHead::new(
                resp_head.status,
                resp_head.version,
                resp_head.headers,
            );
            let close = velda_http1::server::response::send_response_parts(
                &mut down_write,
                write_buf,
                &server_head,
                &Body::Empty,
                *close_requested,
            )
            .await?;
            if close {
                *close_requested = true;
            }
            return Ok::<(), Http1Error>(());
        }

        let server_head =
            Http1ServerResponseHead::new(resp_head.status, resp_head.version, resp_head.headers);

        match resp_framing {
            Http1BodyFraming::ContentLength(total_len) => {
                let available = upstream_read_buf.len().min(total_len);
                if available == total_len {
                    // [FAST-PATH]: Entire response body is already present in memory!
                    let body_slice = if total_len > 0 {
                        Some(&upstream_read_buf[..total_len])
                    } else {
                        None
                    };
                    let close =
                        velda_http1::server::response::send_response_head_chunked_coalesced(
                            &mut down_write,
                            write_buf,
                            &server_head,
                            body_slice,
                            true,
                            *close_requested,
                        )
                        .await?;
                    if close {
                        *close_requested = true;
                    }
                    if total_len > 0 {
                        upstream_read_buf.advance(total_len);
                    }
                    return Ok(());
                }

                let close = velda_http1::server::response::send_response_head_chunked(
                    &mut down_write,
                    write_buf,
                    server_head.version,
                    server_head.status,
                    &server_head.headers,
                    *close_requested,
                )
                .await?;
                if close {
                    *close_requested = true;
                }

                if available > 0 {
                    let first_chunk = upstream_read_buf.split_to(available).freeze();
                    send_chunk(&mut down_write, &first_chunk).await?;
                }

                let mut remaining = total_len - available;
                while remaining > 0 {
                    let to_read = remaining.min(262144);
                    match read_chunk_sized(&mut up_read, upstream_read_buf, to_read).await? {
                        Some(chunk) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            send_chunk(&mut down_write, &chunk).await?;
                        }
                        None => {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body".into(),
                            ));
                        }
                    }
                }
            }
            Http1BodyFraming::Chunked => {
                let close = velda_http1::server::response::send_response_head_chunked(
                    &mut down_write,
                    write_buf,
                    server_head.version,
                    server_head.status,
                    &server_head.headers,
                    *close_requested,
                )
                .await?;
                if close {
                    *close_requested = true;
                }

                while let Some(chunk) = read_next_chunk(&mut up_read, upstream_read_buf).await? {
                    send_chunk(&mut down_write, &chunk).await?;
                }
            }
            Http1BodyFraming::Empty => {
                let close = velda_http1::server::response::send_response_head_chunked(
                    &mut down_write,
                    write_buf,
                    server_head.version,
                    server_head.status,
                    &server_head.headers,
                    *close_requested,
                )
                .await?;
                if close {
                    *close_requested = true;
                }
            }
        }

        send_chunked_end(&mut down_write).await?;
        down_write.flush().await?;
        Ok::<(), Http1Error>(())
    };

    let duplex_res = tokio::try_join!(upload_task, download_task);
    if duplex_res.is_err() {
        upstream_lease.mark_closed();
    }
    duplex_res.map(|_| ())
}
