//! Layer 7 HTTP/1.1 to HTTP/3 Duplex Streaming Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming (e.g. WebSocket over H3, bidirectional RPC)
//! where downstream HTTP/1.1 chunks and upstream HTTP/3 QUIC frames flow concurrently.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Buf;
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

/// Serves a full-duplex bidirectional streaming HTTP/1.1 downstream request bridged to an HTTP/3 QUIC backend.
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
                "Failed to acquire upstream HTTP/3 client for duplex stream"
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

    // 5. Open progressive HTTP/3 duplex stream with self-healing on stale connection drop
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
                "HTTP/3 connection closed; self-healing with fresh QUIC connection for duplex stream"
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
                        "Failed to open HTTP/3 duplex stream on fresh connection"
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
                "Failed to open HTTP/3 duplex stream"
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

    // 6. Split downstream IO into concurrent Read and Write halves
    let (mut client_read, mut client_write) = tokio::io::split(&mut conn.stream);
    let read_buf = &mut conn.read_buf;

    // Asymmetric coordination channels
    let (resp_done_tx, mut resp_done_rx) = tokio::sync::oneshot::channel::<()>();
    let upload_finished = AtomicBool::new(framing == Http1BodyFraming::Empty);

    // Task 1: Upload Pump (Downstream H1 -> Upstream H3 QUIC)
    let upload_task = async {
        let is_eos = framing == Http1BodyFraming::Empty;
        if is_eos {
            let _ = sender.finish().await;
            return Ok::<(), Http1Error>(());
        }

        match framing {
            Http1BodyFraming::Chunked => {
                let mut chunk_remaining: Option<usize> = None;
                let mut need_crlf = false;

                loop {
                    if resp_done_rx.try_recv().is_ok() {
                        break;
                    }

                    // 1. If currently inside a chunk payload, stream available bytes immediately
                    if let Some(remaining) = chunk_remaining {
                        if remaining > 0 {
                            let available = read_buf.len();
                            if available > 0 {
                                let send_len = available.min(remaining);
                                let chunk_data = read_buf.split_to(send_len).freeze();
                                chunk_remaining = Some(remaining - send_len);
                                tokio::select! {
                                    biased;
                                    _ = &mut resp_done_rx => break,
                                    send_res = sender.send_chunk(chunk_data) => {
                                        send_res.map_err(|_| Http1Error::ConnectionClosed)?;
                                    }
                                }
                                continue;
                            }
                        } else {
                            chunk_remaining = None;
                            need_crlf = true;
                        }
                    }

                    // 2. Consume trailing \r\n after chunk data
                    if need_crlf {
                        if read_buf.len() >= 2 {
                            if &read_buf[..2] == b"\r\n" {
                                read_buf.advance(2);
                                need_crlf = false;
                            } else {
                                return Err(Http1Error::Parse(
                                    "Malformed chunk: missing trailing CRLF".into(),
                                ));
                            }
                        } else {
                            tokio::select! {
                                biased;
                                _ = &mut resp_done_rx => break,
                                res = client_read.read_buf(read_buf) => {
                                    let n = res?;
                                    if n == 0 {
                                        return Err(Http1Error::Parse(
                                            "Unexpected EOF expecting chunk CRLF".into(),
                                        ));
                                    }
                                }
                            }
                            continue;
                        }
                    }

                    // 3. Parse next chunk header if not in the middle of a chunk
                    if chunk_remaining.is_none()
                        && !need_crlf
                        && let Some(crlf_pos) = find_crlf(read_buf)
                    {
                        let line = &read_buf[..crlf_pos];
                        let size_part = match line.iter().position(|&b| b == b';') {
                            Some(semi) => &line[..semi],
                            None => line,
                        };
                        let chunk_size = parse_hex_usize(size_part)?;
                        let header_len = crlf_pos + 2;

                        if chunk_size == 0 {
                            // Terminal chunk reached
                            let trailer_data = &read_buf[header_len..];
                            if trailer_data.starts_with(b"\r\n") {
                                read_buf.advance(header_len + 2);
                                upload_finished.store(true, Ordering::Release);
                                break;
                            }
                            if let Some(pos) =
                                trailer_data.windows(4).position(|w| w == b"\r\n\r\n")
                            {
                                read_buf.advance(header_len + pos + 4);
                                upload_finished.store(true, Ordering::Release);
                                break;
                            }
                            if trailer_data.len() > 8192 {
                                return Err(Http1Error::Parse(
                                    "Chunk trailer exceeds limit".into(),
                                ));
                            }
                        } else {
                            read_buf.advance(header_len);
                            chunk_remaining = Some(chunk_size);
                            continue;
                        }
                    }

                    // 4. Ingest more data from downstream TCP socket
                    const TARGET_RESERVE: usize = 262144; // 256KB — match ContentLength coalescing
                    if read_buf.capacity().saturating_sub(read_buf.len()) < TARGET_RESERVE {
                        read_buf.reserve(TARGET_RESERVE);
                    }
                    tokio::select! {
                        biased;
                        _ = &mut resp_done_rx => break,
                        res = client_read.read_buf(read_buf) => {
                            let n = res?;
                            if n == 0 {
                                if read_buf.is_empty() {
                                    break;
                                } else {
                                    return Err(Http1Error::Parse(
                                        "Unexpected EOF while reading chunked body".into(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            Http1BodyFraming::ContentLength(total_len) => {
                // Coalesce multiple small TCP reads into larger batches before dispatching
                // through the mpsc channel to the H3 QUIC driver. Each channel send triggers
                // a task context switch, so fewer larger sends (up to 256KB) are significantly
                // more efficient than many small ones (typically 4-16KB from kernel).
                const COALESCE_TARGET: usize = 262144; // 256KB
                let mut remaining = total_len;
                while remaining > 0 {
                    if resp_done_rx.try_recv().is_ok() {
                        break;
                    }

                    let available = read_buf.len();
                    if available > 0 {
                        // If we have enough data or this is the final chunk, send immediately
                        if available >= COALESCE_TARGET || available >= remaining {
                            let chunk_len = available.min(remaining);
                            let chunk = read_buf.split_to(chunk_len).freeze();
                            remaining = remaining.saturating_sub(chunk_len);
                            tokio::select! {
                                biased;
                                _ = &mut resp_done_rx => break,
                                send_res = sender.send_chunk(chunk) => {
                                    send_res.map_err(|_| Http1Error::ConnectionClosed)?;
                                }
                            }
                            continue;
                        }

                        // Otherwise, try to accumulate more from downstream before dispatching
                        let needed = COALESCE_TARGET.min(remaining).saturating_sub(available);
                        if read_buf.capacity().saturating_sub(read_buf.len()) < needed {
                            read_buf.reserve(needed);
                        }

                        tokio::select! {
                            biased;
                            _ = &mut resp_done_rx => break,
                            res = client_read.read_buf(read_buf) => {
                                let n = res?;
                                if n == 0 {
                                    // EOF — flush whatever we have accumulated
                                    let chunk_len = read_buf.len().min(remaining);
                                    if chunk_len > 0 {
                                        let chunk = read_buf.split_to(chunk_len).freeze();
                                        tokio::select! {
                                            biased;
                                            _ = &mut resp_done_rx => break,
                                            send_res = sender.send_chunk(chunk) => {
                                                send_res.map_err(|_| Http1Error::ConnectionClosed)?;
                                            }
                                        }
                                    }
                                    break;
                                }
                                continue; // loop back to re-check accumulated size
                            }
                        }
                    }

                    // Empty buffer: read fresh data from downstream TCP
                    let to_read = COALESCE_TARGET.min(remaining);
                    if read_buf.capacity().saturating_sub(read_buf.len()) < to_read {
                        read_buf.reserve(to_read);
                    }

                    tokio::select! {
                        biased;
                        _ = &mut resp_done_rx => break,
                        res = client_read.read_buf(read_buf) => {
                            let n = res?;
                            if n == 0 {
                                break;
                            }
                            // Loop back to coalescing check rather than sending immediately
                        }
                    }
                }
                if remaining == 0 {
                    upload_finished.store(true, Ordering::Release);
                }
            }
            Http1BodyFraming::Empty => {
                upload_finished.store(true, Ordering::Release);
            }
        }

        let _ = sender.finish().await;
        Ok::<(), Http1Error>(())
    };

    // Task 2: Download Pump (Upstream H3 QUIC -> Downstream H1)
    let download_task = async {
        let mut resp_stream = match response_rx.await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "Upstream HTTP/3 duplex stream failed");
                return Err(Http1Error::ConnectionClosed);
            }
            Err(_) => return Err(Http1Error::ConnectionClosed),
        };

        let should_close = conn.close_requested;

        let is_no_body_status = resp_stream.status.is_informational()
            || resp_stream.status == http::StatusCode::NO_CONTENT
            || resp_stream.status == http::StatusCode::NOT_MODIFIED;

        if is_no_body_status {
            let mut write_buf = bytes::BytesMut::with_capacity(256);
            velda_http1::encode_response_head(
                http::Version::HTTP_11,
                resp_stream.status,
                &resp_stream.headers,
                Some(0),
                should_close,
                &mut write_buf,
            );
            client_write.write_all(&write_buf).await?;
            client_write.flush().await?;
            let _ = resp_done_tx.send(());
            return Ok::<(), Http1Error>(());
        }

        // Fast-path for finite response payloads (e.g. echo or upload status like 200 OK "pong")
        let cl_opt = resp_stream
            .headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok());

        if let Some(content_len) = cl_opt
            && content_len <= 65536
        {
            let mut body_buf = bytes::BytesMut::with_capacity(content_len);
            while body_buf.len() < content_len {
                match resp_stream.next_chunk().await {
                    Some(Ok(chunk)) => body_buf.extend_from_slice(&chunk),
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "Error reading response chunk from HTTP/3 upstream");
                        return Err(Http1Error::ConnectionClosed);
                    }
                    None => break,
                }
            }
            let mut write_buf = bytes::BytesMut::with_capacity(256 + body_buf.len());
            velda_http1::encode_response_head(
                http::Version::HTTP_11,
                resp_stream.status,
                &resp_stream.headers,
                Some(body_buf.len()),
                should_close,
                &mut write_buf,
            );
            write_buf.extend_from_slice(&body_buf);
            client_write.write_all(&write_buf).await?;
            client_write.flush().await?;
            let _ = resp_done_tx.send(());
            return Ok::<(), Http1Error>(());
        }

        // True progressive chunked streaming path
        let mut write_buf = bytes::BytesMut::with_capacity(512);
        velda_http1::send_response_head_chunked(
            &mut client_write,
            &mut write_buf,
            http::Version::HTTP_11,
            resp_stream.status,
            &resp_stream.headers,
            should_close,
        )
        .await?;

        // Progressively stream chunks downstream
        while let Some(chunk_res) = resp_stream.next_chunk().await {
            match chunk_res {
                Ok(chunk) => {
                    send_chunk(&mut client_write, &chunk).await?;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Error reading response chunk from HTTP/3 upstream");
                    return Err(Http1Error::ConnectionClosed);
                }
            }
        }

        send_chunked_end(&mut client_write).await?;
        client_write.flush().await?;
        let _ = resp_done_tx.send(());
        Ok::<(), Http1Error>(())
    };

    let duplex_res = tokio::try_join!(upload_task, download_task);
    if !upload_finished.load(Ordering::Acquire) {
        conn.mark_close();
    }
    duplex_res.map(|_| ())
}
