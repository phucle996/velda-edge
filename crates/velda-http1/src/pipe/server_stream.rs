//! Progressive server response streaming (SSE, LLM tokens, large downloads).
//!
//! Enforces unidirectional server-to-client streaming:
//! - Request body is read completely and forwarded to upstream.
//! - Upstream response chunks are pumped downstream immediately with zero intermediate buffering.
//! - Downstream client disconnect cleanly aborts the upstream stream to stop wasting backend resources.

use bytes::Buf;
use tokio::io::{AsyncRead, AsyncWrite};

use velda_core::Body;

use super::sanitize_headers;
use crate::client::request::send_request;
use crate::client::response::{read_chunk_sized, read_next_chunk, read_response_head};
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::connection::Http1ServerConnection;
use crate::server::request::{Http1BodyFraming, Http1ServerRequest};
use crate::server::response::{
    Http1ServerResponse, Http1ServerResponseHead, send_response_head_chunked_coalesced,
};
use crate::wire::{decode_chunk, send_chunk, send_chunked_end, send_coalesced_chunks};

/// Pipes a server-streaming HTTP/1.1 request (buffered request, streaming response).
///
/// If downstream client drops the connection during streaming, terminates the loop
/// immediately so that the upstream connection drops and stops processing (e.g. stops LLM generation).
pub async fn pipe_server_stream<DownIO, UpIO>(
    conn: &mut Http1ServerConnection<DownIO>,
    req: &mut Http1ServerRequest,
    upstream: &mut UpIO,
    config: &Http1Config,
) -> Result<(), Http1Error>
where
    DownIO: AsyncRead + AsyncWrite + Unpin,
    UpIO: AsyncRead + AsyncWrite + Unpin,
{
    sanitize_headers(&mut req.headers);

    // 1. Send complete request to upstream backend reusing scratch buffer
    send_request(req, upstream, &mut conn.upstream_write_buf).await?;

    // 3. Read upstream response head reusing connection read buffer
    let (mut resp_head, resp_framing) =
        read_response_head(upstream, &mut conn.upstream_read_buf, config).await?;
    sanitize_headers(&mut resp_head.headers);

    // 4. RFC 9110 §6.4.1 & §9.3.2: HEAD, 1xx, 204, 304 have no body
    let is_head = req.method == http::Method::HEAD;
    let is_no_body_status = resp_head.status.is_informational()
        || resp_head.status == http::StatusCode::NO_CONTENT
        || resp_head.status == http::StatusCode::NOT_MODIFIED;

    if is_head || is_no_body_status {
        let resp = Http1ServerResponse::new(
            resp_head.status,
            resp_head.version,
            resp_head.headers,
            Body::Empty,
        );
        conn.send_response(&resp).await?;
        return Ok(());
    }

    // 5. Downstream framing decision (RFC 9112 §7.1):
    // HTTP/1.0 does not support chunked transfer encoding!
    let is_http10 = req.version == http::Version::HTTP_10;
    if is_http10 {
        // Downstream is HTTP/1.0: align version, strip transfer-encoding, mark close, send head, pump raw bytes
        resp_head.version = http::Version::HTTP_10;
        resp_head.headers.remove(http::header::TRANSFER_ENCODING);
        conn.mark_close();
        let server_head =
            Http1ServerResponseHead::new(resp_head.status, resp_head.version, resp_head.headers);
        conn.send_response_parts(&server_head, &Body::Empty).await?;

        let mut client_disconnected = false;
        match resp_framing {
            Http1BodyFraming::Chunked => {
                while let Some(chunk) =
                    read_next_chunk(upstream, &mut conn.upstream_read_buf).await?
                {
                    if let Err(e) = conn.send_raw_bytes(&chunk).await {
                        tracing::debug!(
                            error = %e,
                            "Downstream HTTP/1.0 client disconnected during server streaming"
                        );
                        client_disconnected = true;
                        break;
                    }
                }
            }
            Http1BodyFraming::ContentLength(total_len) => {
                let mut remaining = total_len;
                while remaining > 0 {
                    let to_read = remaining.min(65536);
                    match read_chunk_sized(upstream, &mut conn.upstream_read_buf, to_read).await? {
                        Some(chunk) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            if let Err(e) = conn.send_raw_bytes(&chunk).await {
                                tracing::debug!(
                                    error = %e,
                                    "Downstream HTTP/1.0 client disconnected during sized server streaming"
                                );
                                client_disconnected = true;
                                break;
                            }
                        }
                        None => {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body: premature connection close".into(),
                            ));
                        }
                    }
                }
            }
            Http1BodyFraming::Empty => {}
        }

        if client_disconnected {
            return Err(Http1Error::ConnectionClosed);
        }
        return Ok(());
    }

    // 6. Pump response chunks downstream with single-write coalescing and client disconnect detection
    let server_head =
        Http1ServerResponseHead::new(resp_head.status, resp_head.version, resp_head.headers);
    let mut client_disconnected = false;
    match resp_framing {
        Http1BodyFraming::ContentLength(total_len) => {
            let available = conn.upstream_read_buf.len().min(total_len);
            if available == total_len {
                // [FAST-PATH]: Entire response body is already present in memory (e.g. micro SSE ping)!
                // Coalesce Head + Chunk + Terminator (0\r\n\r\n) into a single socket write syscall!
                let body_slice = if total_len > 0 {
                    Some(&conn.upstream_read_buf[..total_len])
                } else {
                    None
                };
                let close = send_response_head_chunked_coalesced(
                    &mut conn.stream,
                    &mut conn.write_buf,
                    &server_head,
                    body_slice,
                    true,
                    conn.close_requested,
                )
                .await?;
                if close {
                    conn.close_requested = true;
                }
                if total_len > 0 {
                    conn.upstream_read_buf.advance(total_len);
                }
            } else {
                let initial_slice = if available > 0 {
                    Some(&conn.upstream_read_buf[..available])
                } else {
                    None
                };
                let close = send_response_head_chunked_coalesced(
                    &mut conn.stream,
                    &mut conn.write_buf,
                    &server_head,
                    initial_slice,
                    false,
                    conn.close_requested,
                )
                .await?;
                if close {
                    conn.close_requested = true;
                }
                if available > 0 {
                    conn.upstream_read_buf.advance(available);
                }

                let mut remaining = total_len - available;
                while remaining > 0 {
                    let to_read = remaining.min(65536);
                    match read_chunk_sized(upstream, &mut conn.upstream_read_buf, to_read).await? {
                        Some(chunk) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
                                tracing::debug!(
                                    error = %e,
                                    "Downstream client disconnected during sized server streaming"
                                );
                                client_disconnected = true;
                                break;
                            }
                        }
                        None => {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body: premature connection close".into(),
                            ));
                        }
                    }
                }

                if !client_disconnected {
                    let _ = send_chunked_end(&mut conn.stream).await;
                }
            }
        }
        Http1BodyFraming::Chunked => {
            let mut total_body_bytes = 0usize;
            let mut chunk_batch = [const { bytes::Bytes::new() }; 16];

            // Check if initial chunk(s) are already parsed in memory
            let initial_decoded = match decode_chunk(&mut conn.upstream_read_buf)? {
                Some(Some(first_chunk)) => {
                    let mut is_term = false;
                    let mut count = 1;
                    chunk_batch[0] = first_chunk;
                    let mut batch_bytes = chunk_batch[0].len();

                    while count < 16 {
                        match decode_chunk(&mut conn.upstream_read_buf)? {
                            Some(Some(next_chunk)) => {
                                batch_bytes += next_chunk.len();
                                chunk_batch[count] = next_chunk;
                                count += 1;
                            }
                            Some(None) => {
                                is_term = true;
                                break;
                            }
                            None => break,
                        }
                    }

                    total_body_bytes += batch_bytes;
                    Some((count, is_term))
                }
                Some(None) => Some((0, true)),
                None => None,
            };

            let mut already_terminated = false;
            match initial_decoded {
                Some((0, true)) => {
                    // Empty chunked stream already terminated
                    let close = send_response_head_chunked_coalesced(
                        &mut conn.stream,
                        &mut conn.write_buf,
                        &server_head,
                        None,
                        true,
                        conn.close_requested,
                    )
                    .await?;
                    if close {
                        conn.close_requested = true;
                    }
                    already_terminated = true;
                }
                Some((count, is_term)) => {
                    if count == 1 {
                        let close = send_response_head_chunked_coalesced(
                            &mut conn.stream,
                            &mut conn.write_buf,
                            &server_head,
                            Some(&chunk_batch[0]),
                            is_term,
                            conn.close_requested,
                        )
                        .await?;
                        if close {
                            conn.close_requested = true;
                        }
                    } else {
                        let close = send_response_head_chunked_coalesced(
                            &mut conn.stream,
                            &mut conn.write_buf,
                            &server_head,
                            Some(&chunk_batch[0]),
                            false,
                            conn.close_requested,
                        )
                        .await?;
                        if close {
                            conn.close_requested = true;
                        }
                        let send_res =
                            send_coalesced_chunks(&mut conn.stream, &chunk_batch[1..count]).await;
                        if let Err(e) = send_res {
                            tracing::debug!(
                                error = %e,
                                "Downstream client disconnected during batch streaming"
                            );
                            client_disconnected = true;
                        }
                        if is_term && !client_disconnected {
                            let _ = send_chunked_end(&mut conn.stream).await;
                        }
                    }
                    if is_term {
                        already_terminated = true;
                    }
                }
                None => {
                    // No chunk ready in memory yet: flush head immediately for lowest TTFB
                    let close = send_response_head_chunked_coalesced(
                        &mut conn.stream,
                        &mut conn.write_buf,
                        &server_head,
                        None,
                        false,
                        conn.close_requested,
                    )
                    .await?;
                    if close {
                        conn.close_requested = true;
                    }
                }
            }

            if !already_terminated && !client_disconnected {
                loop {
                    let Some(first_chunk) =
                        read_next_chunk(upstream, &mut conn.upstream_read_buf).await?
                    else {
                        break;
                    };
                    chunk_batch[0] = first_chunk;
                    let mut count = 1;
                    let mut batch_bytes = chunk_batch[0].len();
                    let mut reached_terminal = false;

                    while count < 16 {
                        match decode_chunk(&mut conn.upstream_read_buf)? {
                            Some(Some(next_chunk)) => {
                                batch_bytes += next_chunk.len();
                                chunk_batch[count] = next_chunk;
                                count += 1;
                            }
                            Some(None) => {
                                reached_terminal = true;
                                break;
                            }
                            None => break,
                        }
                    }

                    total_body_bytes = total_body_bytes
                        .checked_add(batch_bytes)
                        .ok_or(Http1Error::PayloadTooLarge(usize::MAX))?;
                    if total_body_bytes > config.max_body_size {
                        return Err(Http1Error::PayloadTooLarge(total_body_bytes));
                    }

                    let send_res = if count == 1 {
                        send_chunk(&mut conn.stream, &chunk_batch[0]).await
                    } else {
                        send_coalesced_chunks(&mut conn.stream, &chunk_batch[..count]).await
                    };

                    if let Err(e) = send_res {
                        tracing::debug!(
                            error = %e,
                            "Downstream client disconnected during chunked server streaming"
                        );
                        client_disconnected = true;
                        break;
                    }

                    if reached_terminal {
                        break;
                    }
                }

                if !client_disconnected {
                    let _ = send_chunked_end(&mut conn.stream).await;
                }
            }
        }
        Http1BodyFraming::Empty => {
            let close = send_response_head_chunked_coalesced(
                &mut conn.stream,
                &mut conn.write_buf,
                &server_head,
                None,
                true,
                conn.close_requested,
            )
            .await?;
            if close {
                conn.close_requested = true;
            }
        }
    }

    if client_disconnected {
        return Err(Http1Error::ConnectionClosed);
    }

    conn.compact_buffers();
    Ok(())
}
