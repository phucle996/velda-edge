//! Layer 7 HTTP/1.1 to HTTP/3 Duplex Streaming Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming (e.g. WebSocket over H3, bidirectional RPC)
//! where downstream HTTP/1.1 chunks and upstream HTTP/3 QUIC frames flow concurrently.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_http1::{
    HeadEncodeOptions, Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse, ProgressiveChunkDecoder, send_chunk, send_chunked_end,
};
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a full-duplex bidirectional streaming HTTP/1.1 downstream request bridged to an HTTP/3 QUIC backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut req_head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http3Upstream>,
    meta: &DownstreamMeta,
    plan: &velda_plugin::RoutePluginPlan,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 1. Handle Expect: 100-continue unblocking
    if req_head.is_expect_100_continue() && framing != Http1BodyFraming::Empty {
        if let Err(e) = conn.send_100_continue().await {
            tracing::debug!(error = %e, "Failed to send 100 Continue downstream");
            return Err(e);
        }
        req_head.headers.remove(http::header::EXPECT);
    }

    // 3. Sanitize connection-specific headers forbidden by RFC 9114 §4.2
    sanitize_headers(&mut req_head.headers);

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

    // 5. Opportunistically drain any already-buffered payload bytes waiting in the downstream
    // kernel socket buffer before opening the upstream stream (zero async scheduler yields).
    if framing != Http1BodyFraming::Empty {
        const INITIAL_MAX_COALESCE: usize = 131072; // 128KB
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        while conn.read_buf.len() < INITIAL_MAX_COALESCE {
            if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < 65536 {
                conn.read_buf.reserve(65536);
            }
            let mut read_buf = tokio::io::ReadBuf::uninit(conn.read_buf.spare_capacity_mut());
            match tokio::io::AsyncRead::poll_read(
                std::pin::Pin::new(&mut conn.stream),
                &mut cx,
                &mut read_buf,
            ) {
                std::task::Poll::Ready(Ok(())) if !read_buf.filled().is_empty() => {
                    let n = read_buf.filled().len();
                    unsafe {
                        let new_len = conn.read_buf.len() + n;
                        conn.read_buf.set_len(new_len);
                    }
                }
                _ => break,
            }
        }
    }

    // 6. Decode any opportunistic initial payload chunk already buffered in conn.read_buf
    let mut initial_chunk: Option<bytes::Bytes> = None;
    let mut initial_is_fin = false;
    let mut chunked_decoder = match framing {
        Http1BodyFraming::Chunked => Some(ProgressiveChunkDecoder::new()),
        _ => None,
    };

    match framing {
        Http1BodyFraming::Empty => {
            initial_is_fin = true;
        }
        Http1BodyFraming::Chunked => {
            if let Some(decoder) = chunked_decoder.as_mut() {
                let mut acc = bytes::BytesMut::new();
                while let Some(slice) = decoder.decode_step(&mut conn.read_buf)? {
                    acc.extend_from_slice(&slice);
                    if acc.len() >= 65536 {
                        break;
                    }
                }
                if decoder.is_done() {
                    initial_is_fin = true;
                }
                if !acc.is_empty() {
                    initial_chunk = Some(acc.freeze());
                }
            }
        }
        Http1BodyFraming::ContentLength(total_len) => {
            if !conn.read_buf.is_empty() {
                let chunk_len = conn.read_buf.len().min(total_len);
                initial_chunk = Some(conn.read_buf.split_to(chunk_len).freeze());
                initial_is_fin = chunk_len == total_len;
            }
        }
    }

    // 6. Open progressive HTTP/3 duplex stream with opportunistic initial payload and self-healing
    let mut _lease_guard = Some(lease);
    let (sender, response_rx) = match client
        .open_stream(
            req_head.method.clone(),
            req_head.uri.clone(),
            req_head.headers.clone(),
            initial_chunk.clone(),
            initial_is_fin,
        )
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
                .open_stream(
                    req_head.method.clone(),
                    req_head.uri.clone(),
                    req_head.headers.clone(),
                    initial_chunk.clone(),
                    initial_is_fin,
                )
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

    // 7. Split downstream IO into concurrent Read and Write halves for true asynchronous duplex
    let (mut client_read, mut client_write) = tokio::io::split(&mut conn.stream);
    let read_buf = &mut conn.read_buf;
    let write_buf = &mut conn.write_buf;
    let should_close = conn.close_requested;
    let req_id = req_head
        .headers
        .get(&crate::pipeline::X_REQUEST_ID)
        .cloned();

    let upload_finished = AtomicBool::new(initial_is_fin);

    // Task 1: Upload Pump (Downstream H1 -> Upstream H3 QUIC)
    let upload_task = async {
        if initial_is_fin {
            upload_finished.store(true, Ordering::Release);
            return Ok::<(), Http1Error>(());
        }

        match framing {
            Http1BodyFraming::Chunked => {
                let mut decoder = chunked_decoder.unwrap_or_default();
                const TARGET_RESERVE: usize = 65536;
                loop {
                    while let Some(chunk) = decoder.decode_step(read_buf)? {
                        sender
                            .send_chunk(chunk)
                            .await
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                    }

                    if decoder.is_done() {
                        upload_finished.store(true, Ordering::Release);
                        break;
                    }

                    if read_buf.capacity().saturating_sub(read_buf.len()) < TARGET_RESERVE {
                        read_buf.reserve(TARGET_RESERVE);
                    }
                    let n = client_read.read_buf(read_buf).await?;
                    if n == 0 {
                        if decoder.is_done() {
                            upload_finished.store(true, Ordering::Release);
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
                const COALESCE_TARGET: usize = 262144; // 256KB
                let initial_len = initial_chunk.as_ref().map(|c| c.len()).unwrap_or(0);
                let mut remaining = total_len.saturating_sub(initial_len);
                while remaining > 0 {
                    let available = read_buf.len();
                    if available > 0 {
                        // If we have enough data or this is the final chunk, send immediately
                        if available >= COALESCE_TARGET || available >= remaining {
                            let chunk_len = available.min(remaining);
                            let chunk = read_buf.split_to(chunk_len).freeze();
                            remaining = remaining.saturating_sub(chunk_len);
                            sender
                                .send_chunk(chunk)
                                .await
                                .map_err(|_| Http1Error::ConnectionClosed)?;
                            continue;
                        }

                        // Otherwise, try to accumulate more from downstream before dispatching
                        let needed = COALESCE_TARGET.min(remaining).saturating_sub(available);
                        if read_buf.capacity().saturating_sub(read_buf.len()) < needed {
                            read_buf.reserve(needed);
                        }

                        let n = client_read.read_buf(read_buf).await?;
                        if n == 0 {
                            // EOF — flush whatever we have accumulated
                            let chunk_len = read_buf.len().min(remaining);
                            if chunk_len > 0 {
                                let chunk = read_buf.split_to(chunk_len).freeze();
                                sender
                                    .send_chunk(chunk)
                                    .await
                                    .map_err(|_| Http1Error::ConnectionClosed)?;
                            }
                            break;
                        }
                        continue;
                    }

                    // Empty buffer: read fresh data from downstream TCP
                    let to_read = COALESCE_TARGET.min(remaining);
                    if read_buf.capacity().saturating_sub(read_buf.len()) < to_read {
                        read_buf.reserve(to_read);
                    }

                    let n = client_read.read_buf(read_buf).await?;
                    if n == 0 {
                        break;
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

        if let Some(ref rid) = req_id {
            resp_stream
                .headers
                .insert(crate::pipeline::X_REQUEST_ID, rid.clone());
        }

        // Fast-path guard: Bypasses HeaderWorkspace instantiation & plugin dispatch if route has no hooks.
        // HeaderMap::clone() clones atomic pointer handles (Bytes) in O(1) without copying string payloads.
        if let Some(post_hooks) = &plan.post_response_hooks {
            let mut l7_resp = velda_core::L7Response::new(
                resp_stream.status,
                http::Version::HTTP_11,
                std::mem::take(&mut resp_stream.headers),
                velda_core::Body::Empty,
            );
            let req_ws = velda_core::HeaderWorkspace::from_headers(req_head.headers.clone());
            velda_plugin::run_post_response_plugins(post_hooks, &req_ws, &mut l7_resp);
            resp_stream.status = l7_resp.status;
            resp_stream.headers = l7_resp.headers.into();
        }
        meta.inject_response_headers(&mut resp_stream.headers);

        let is_no_body_status = resp_stream.status.is_informational()
            || resp_stream.status == http::StatusCode::NO_CONTENT
            || resp_stream.status == http::StatusCode::NOT_MODIFIED;

        if is_no_body_status {
            write_buf.clear();
            velda_http1::encode_head(
                http::Version::HTTP_11,
                resp_stream.status,
                &resp_stream.headers,
                HeadEncodeOptions {
                    body_len: Some(0),
                    force_close: should_close,
                    request_id: req_id.as_ref(),
                },
                write_buf,
            );
            client_write.write_all(write_buf).await?;
            client_write.flush().await?;
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
            write_buf.clear();
            velda_http1::encode_head(
                http::Version::HTTP_11,
                resp_stream.status,
                &resp_stream.headers,
                HeadEncodeOptions {
                    body_len: Some(body_buf.len()),
                    force_close: should_close,
                    request_id: req_id.as_ref(),
                },
                write_buf,
            );
            write_buf.extend_from_slice(&body_buf);
            client_write.write_all(write_buf).await?;
            client_write.flush().await?;
            return Ok::<(), Http1Error>(());
        }

        // True progressive chunked streaming path
        write_buf.clear();
        velda_http1::send_chunked_head(
            &mut client_write,
            write_buf,
            http::Version::HTTP_11,
            resp_stream.status,
            &resp_stream.headers,
            HeadEncodeOptions {
                body_len: None,
                force_close: should_close,
                request_id: req_id.as_ref(),
            },
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
        Ok::<(), Http1Error>(())
    };

    let duplex_res = tokio::try_join!(upload_task, download_task);
    if !upload_finished.load(Ordering::Acquire) {
        conn.mark_close();
    }
    duplex_res.map(|_| ())
}
