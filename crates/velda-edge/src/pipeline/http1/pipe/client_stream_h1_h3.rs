//! Layer 7 HTTP/1.1 to HTTP/3 Client-Stream Bridge Pipeline.
//!
//! Handles progressive client-streaming uploads (e.g. file uploads, log batch ingest) where
//! downstream HTTP/1.1 chunks are progressively streamed to an upstream HTTP/3 QUIC connection,
//! and a single response is returned downstream with zero whole-payload buffering in RAM.

use std::sync::Arc;

use bytes::{Buf, BytesMut};
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_http1::wire::{find_crlf, parse_hex_usize, send_chunk, send_chunked_end};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a client-streaming HTTP/1.1 downstream request bridged to an HTTP/3 QUIC backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http3Upstream>,
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

    // 3. Sanitize connection-specific headers forbidden by RFC 9114 §4.2
    sanitize_headers(&mut head.headers);

    let h3_config = Http3Config::auto();

    // 4. Acquire pooled HTTP/3 client before upload begins
    let (client, lease) = match upstream.acquire(&h3_config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 client for client stream"
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

    // 5. Open progressive HTTP/3 stream with self-healing on stale connection drop
    let mut _lease_guard = Some(lease);
    let (sender, response_rx) = match client
        .open_stream(head.method.clone(), head.uri.clone(), head.headers.clone())
        .await
    {
        Ok(parts) => parts,
        Err(e) if e.is_connection_closed() => {
            if let Some(l) = _lease_guard.take() {
                l.mark_goaway();
            }
            tracing::debug!(
                upstream = %upstream.id(),
                "HTTP/3 connection closed; self-healing with fresh QUIC connection for client stream"
            );
            let (fresh_client, fresh_lease) = match upstream.acquire_fresh(&h3_config).await {
                Ok(res) => res,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        upstream = %upstream.id(),
                        "Failed to acquire fresh HTTP/3 connection after stale pooled drop"
                    );
                    let err_resp = Http1ServerResponse::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        format!("502 Bad Gateway: {e}\n").into_bytes(),
                    );
                    conn.mark_close();
                    let _ = conn.send_response(&err_resp).await;
                    return Err(Http1Error::ConnectionClosed);
                }
            };
            _lease_guard = Some(fresh_lease);
            match fresh_client
                .open_stream(head.method.clone(), head.uri.clone(), head.headers.clone())
                .await
            {
                Ok(parts) => parts,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        upstream = %upstream.id(),
                        "Failed to open HTTP/3 stream on fresh connection"
                    );
                    let err_resp = Http1ServerResponse::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        format!("502 Bad Gateway: {e}\n").into_bytes(),
                    );
                    conn.mark_close();
                    let _ = conn.send_response(&err_resp).await;
                    return Err(Http1Error::ConnectionClosed);
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to open HTTP/3 progressive request stream"
            );
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::ConnectionClosed);
        }
    };
    // 6. Progressive Upload Pump directly on conn without heap-allocated IO split or mutex locking
    match framing {
        Http1BodyFraming::Empty => {
            let _ = sender.finish().await;
        }
        Http1BodyFraming::Chunked => {
            let mut chunk_remaining: Option<usize> = None;
            let mut need_crlf = false;
            // Coalesce threshold: accumulate chunk payload data up to this size before
            // dispatching to the H3 driver to reduce mpsc channel round-trips.
            const COALESCE_TARGET: usize = 262144; // 256KB

            loop {
                // 1. If currently inside a chunk payload, stream available bytes immediately
                if let Some(remaining) = chunk_remaining {
                    if remaining > 0 {
                        let available = conn.read_buf.len();
                        if available > 0 {
                            let send_len = available.min(remaining);
                            // Coalesce: if we have less than COALESCE_TARGET and more data
                            // is expected from downstream, defer sending to accumulate more
                            if send_len < COALESCE_TARGET && send_len < remaining {
                                let needed =
                                    COALESCE_TARGET.min(remaining).saturating_sub(available);
                                if conn.read_buf.capacity().saturating_sub(conn.read_buf.len())
                                    < needed
                                {
                                    conn.read_buf.reserve(needed);
                                }
                                // Read more data from TCP before dispatching
                                let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                                if n == 0 {
                                    // EOF — flush whatever we have accumulated
                                    let flush_len = conn.read_buf.len().min(remaining);
                                    if flush_len > 0 {
                                        let chunk_data = conn.read_buf.split_to(flush_len).freeze();
                                        sender
                                            .send_chunk(chunk_data)
                                            .await
                                            .map_err(|_| Http1Error::ConnectionClosed)?;
                                    }
                                    break;
                                }
                                continue; // loop back to re-check accumulated size
                            }
                            let chunk_data = conn.read_buf.split_to(send_len).freeze();
                            chunk_remaining = Some(remaining - send_len);
                            sender
                                .send_chunk(chunk_data)
                                .await
                                .map_err(|_| Http1Error::ConnectionClosed)?;
                            continue;
                        }
                    } else {
                        chunk_remaining = None;
                        need_crlf = true;
                    }
                }

                // 2. Consume trailing \r\n after chunk data
                if need_crlf {
                    if conn.read_buf.len() >= 2 {
                        if &conn.read_buf[..2] == b"\r\n" {
                            conn.read_buf.advance(2);
                            need_crlf = false;
                        } else {
                            return Err(Http1Error::Parse(
                                "Malformed chunk: missing trailing CRLF".into(),
                            ));
                        }
                    } else {
                        let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                        if n == 0 {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF expecting chunk CRLF".into(),
                            ));
                        }
                        continue;
                    }
                }

                // 3. Parse next chunk header if not in the middle of a chunk
                if chunk_remaining.is_none()
                    && !need_crlf
                    && let Some(crlf_pos) = find_crlf(&conn.read_buf)
                {
                    let line = &conn.read_buf[..crlf_pos];
                    let size_part = match line.iter().position(|&b| b == b';') {
                        Some(semi) => &line[..semi],
                        None => line,
                    };
                    let chunk_size = parse_hex_usize(size_part)?;
                    let header_len = crlf_pos + 2;

                    if chunk_size == 0 {
                        // Terminal chunk reached
                        let trailer_data = &conn.read_buf[header_len..];
                        if trailer_data.starts_with(b"\r\n") {
                            conn.read_buf.advance(header_len + 2);
                            break;
                        }
                        if let Some(pos) = trailer_data.windows(4).position(|w| w == b"\r\n\r\n") {
                            conn.read_buf.advance(header_len + pos + 4);
                            break;
                        }
                        if trailer_data.len() > 8192 {
                            return Err(Http1Error::Parse("Chunk trailer exceeds limit".into()));
                        }
                    } else {
                        conn.read_buf.advance(header_len);
                        chunk_remaining = Some(chunk_size);
                        continue;
                    }
                }

                // 4. Ingest more data from downstream TCP socket
                if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < COALESCE_TARGET {
                    conn.read_buf.reserve(COALESCE_TARGET);
                }
                let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                if n == 0 {
                    if conn.read_buf.is_empty() {
                        break;
                    } else {
                        return Err(Http1Error::Parse(
                            "Unexpected EOF while reading chunked body".into(),
                        ));
                    }
                }
            }
        }
        Http1BodyFraming::ContentLength(total_len) => {
            // Coalesce multiple small TCP reads into larger batches before dispatching
            // through the mpsc channel to the H3 QUIC driver. Each channel send triggers
            // a task context switch, so fewer larger sends (up to 256KB) are significantly
            // more efficient than many small ones (typically 4-16KB from kernel).
            const COALESCE_TARGET: usize = 262144; // 256KB — balances throughput vs latency
            let mut remaining = total_len;
            while remaining > 0 {
                // Drain any data already in read_buf from prior TCP reads
                let available = conn.read_buf.len();
                if available > 0 {
                    // If we have enough data or this is the final chunk, send immediately
                    if available >= COALESCE_TARGET || available >= remaining {
                        let chunk_len = available.min(remaining);
                        let chunk = conn.read_buf.split_to(chunk_len).freeze();
                        remaining = remaining.saturating_sub(chunk_len);
                        sender
                            .send_chunk(chunk)
                            .await
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                        continue;
                    }

                    // Otherwise, try to accumulate more from downstream before dispatching
                    let target = COALESCE_TARGET.min(remaining);
                    let needed = target.saturating_sub(available);
                    if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < needed {
                        conn.read_buf.reserve(needed);
                    }

                    // Non-blocking attempt to fill buffer further from TCP socket
                    match conn.stream.read_buf(&mut conn.read_buf).await {
                        Ok(0) => {
                            // EOF — flush whatever we have accumulated
                            let chunk_len = conn.read_buf.len().min(remaining);
                            if chunk_len > 0 {
                                let chunk = conn.read_buf.split_to(chunk_len).freeze();
                                sender
                                    .send_chunk(chunk)
                                    .await
                                    .map_err(|_| Http1Error::ConnectionClosed)?;
                            }
                            break;
                        }
                        Ok(_) => continue, // loop back to check accumulated size
                        Err(e) => return Err(e.into()),
                    }
                }

                // Empty buffer: read fresh data from downstream TCP
                let to_read = COALESCE_TARGET.min(remaining);
                if conn.read_buf.capacity() < to_read {
                    conn.read_buf.reserve(to_read);
                }
                let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                if n == 0 {
                    break;
                }
                // Loop back to coalescing check rather than sending immediately
            }
        }
    }

    let _ = sender.finish().await;

    // 7. Resolve upstream response
    let mut resp_stream = response_rx
        .await
        .map_err(|_| Http1Error::ConnectionClosed)?
        .map_err(|_| Http1Error::ConnectionClosed)?;
    let should_close = conn.close_requested;

    // 8. Deliver response downstream
    let is_no_body_status = resp_stream.status.is_informational()
        || resp_stream.status == StatusCode::NO_CONTENT
        || resp_stream.status == StatusCode::NOT_MODIFIED;

    if is_no_body_status {
        let mut write_buf = BytesMut::with_capacity(256);
        velda_http1::encode_response_head(
            http::Version::HTTP_11,
            resp_stream.status,
            &resp_stream.headers,
            Some(0),
            should_close,
            &mut write_buf,
        );
        conn.stream.write_all(&write_buf).await?;
        conn.stream.flush().await?;
        return Ok(());
    }

    let cl_opt = resp_stream
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());

    if let Some(content_len) = cl_opt
        && content_len <= 65536
    {
        let mut body_buf = BytesMut::with_capacity(content_len);
        while body_buf.len() < content_len {
            match resp_stream.next_chunk().await {
                Some(Ok(chunk)) => body_buf.extend_from_slice(&chunk[..]),
                Some(Err(e)) => {
                    tracing::warn!(error = %e, "Error reading response chunk from HTTP/3 upstream");
                    return Err(Http1Error::ConnectionClosed);
                }
                None => break,
            }
        }
        conn.write_buf.clear();
        velda_http1::encode_response_head(
            http::Version::HTTP_11,
            resp_stream.status,
            &resp_stream.headers,
            Some(body_buf.len()),
            should_close,
            &mut conn.write_buf,
        );
        conn.write_buf.extend_from_slice(&body_buf);
        conn.stream.write_all(&conn.write_buf).await?;
        return Ok(());
    }

    // Progressive chunked streaming path
    let mut write_buf = BytesMut::with_capacity(512);
    velda_http1::send_response_head_chunked(
        &mut conn.stream,
        &mut write_buf,
        http::Version::HTTP_11,
        resp_stream.status,
        &resp_stream.headers,
        should_close,
    )
    .await?;

    while let Some(chunk_res) = resp_stream.next_chunk().await {
        match chunk_res {
            Ok(chunk) => {
                send_chunk(&mut conn.stream, &chunk[..]).await?;
            }
            Err(e) => {
                tracing::warn!(error = %e, "Error reading response chunk from HTTP/3 upstream");
                return Err(Http1Error::ConnectionClosed);
            }
        }
    }

    send_chunked_end(&mut conn.stream).await?;
    conn.stream.flush().await?;
    Ok(())
}
