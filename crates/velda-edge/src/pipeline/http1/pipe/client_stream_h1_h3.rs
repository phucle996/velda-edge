//! Layer 7 HTTP/1.1 to HTTP/3 Client-Stream Bridge Pipeline.
//!
//! Handles progressive client-streaming uploads (e.g. file uploads, log batch ingest) where
//! downstream HTTP/1.1 chunks are progressively streamed to an upstream HTTP/3 QUIC connection,
//! and a single response is returned downstream with zero whole-payload buffering in RAM.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse, ProgressiveChunkDecoder, send_chunk, send_chunked_end,
};
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a client-streaming HTTP/1.1 downstream request bridged to an HTTP/3 QUIC backend.
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

    // 5. Decode any opportunistic initial payload chunk already buffered in conn.read_buf
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
                // Opportunistically extract any initial chunk payload slices already buffered in read_buf
                let mut acc = BytesMut::new();
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

    // 6. Open progressive HTTP/3 stream with opportunistic initial payload and self-healing
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

    // 7. Progressive Upload Pump: Stream downstream chunks progressively to upstream HTTP/3
    // without buffering whole payload in RAM, enabling true concurrent network pipelining.
    if !initial_is_fin {
        match framing {
            Http1BodyFraming::Empty => {
                let _ = sender.finish().await;
            }
            Http1BodyFraming::Chunked => {
                let mut decoder = chunked_decoder.unwrap_or_default();
                const TARGET_RESERVE: usize = 65536;
                loop {
                    // 1. Drain all available chunk payload slices currently in memory
                    while let Some(chunk) = decoder.decode_step(&mut conn.read_buf)? {
                        sender
                            .send_chunk(chunk)
                            .await
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                    }

                    if decoder.is_done() {
                        break;
                    }

                    // 2. Buffer drained or delimiter incomplete: read next TCP segment from downstream
                    if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < TARGET_RESERVE
                    {
                        conn.read_buf.reserve(TARGET_RESERVE);
                    }
                    let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                    if n == 0 {
                        if decoder.is_done() {
                            break;
                        } else {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading chunked body".into(),
                            ));
                        }
                    }
                }
                let _ = sender.finish().await;
            }
            Http1BodyFraming::ContentLength(total_len) => {
                let initial_len = initial_chunk.as_ref().map(|c| c.len()).unwrap_or(0);
                let mut remaining = total_len.saturating_sub(initial_len);
                const COALESCE_TARGET: usize = 65536; // 64KB target batch
                while remaining > 0 {
                    let available = conn.read_buf.len();
                    if available > 0 {
                        let chunk_len = available.min(remaining);
                        let chunk = conn.read_buf.split_to(chunk_len).freeze();
                        remaining = remaining.saturating_sub(chunk_len);
                        let is_fin = remaining == 0;
                        sender
                            .send_chunk_fin(chunk, is_fin)
                            .await
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                        continue;
                    }

                    let to_read = remaining.min(COALESCE_TARGET);
                    if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < to_read {
                        conn.read_buf.reserve(to_read);
                    }
                    let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                    if n == 0 {
                        break;
                    }
                }
                if remaining > 0 {
                    let _ = sender.finish().await;
                }
            }
        }
    }

    // 7. Resolve upstream response
    let mut resp_stream = response_rx
        .await
        .map_err(|_| Http1Error::ConnectionClosed)?
        .map_err(|_| Http1Error::ConnectionClosed)?;
    let should_close = conn.close_requested;
    if let Some(req_id) = req_head.headers.get(&crate::pipeline::X_REQUEST_ID) {
        resp_stream
            .headers
            .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
    }

    // Fast-path guard: Bypasses HeaderWorkspace instantiation & plugin dispatch if route has no hooks.
    // req_head reaches end of life here -> mem::take transfers ownership with zero allocations.
    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = velda_core::L7Response::new(
            resp_stream.status,
            http::Version::HTTP_11,
            std::mem::take(&mut resp_stream.headers),
            velda_core::Body::Empty,
        );
        let req_ws =
            velda_core::HeaderWorkspace::from_headers(std::mem::take(&mut req_head.headers));
        velda_plugin::run_post_response_plugins(post_hooks, &req_ws, &mut l7_resp);
        resp_stream.status = l7_resp.status;
        resp_stream.headers = l7_resp.headers.into();
    }
    meta.inject_response_headers(&mut resp_stream.headers);

    // 8. Deliver response downstream
    let is_no_body_status = resp_stream.status.is_informational()
        || resp_stream.status == StatusCode::NO_CONTENT
        || resp_stream.status == StatusCode::NOT_MODIFIED;

    if is_no_body_status {
        conn.write_buf.clear();
        conn.encode_response_head(
            http::Version::HTTP_11,
            resp_stream.status,
            &resp_stream.headers,
            Some(0),
            should_close,
        );
        conn.stream.write_all(&conn.write_buf).await?;
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
        conn.encode_response_head(
            http::Version::HTTP_11,
            resp_stream.status,
            &resp_stream.headers,
            Some(body_buf.len()),
            should_close,
        );
        conn.write_buf.extend_from_slice(&body_buf);
        conn.stream.write_all(&conn.write_buf).await?;
        return Ok(());
    }

    // Progressive chunked streaming path
    conn.write_buf.clear();
    conn.encode_response_head_chunked(
        http::Version::HTTP_11,
        resp_stream.status,
        &resp_stream.headers,
        should_close,
    );
    conn.stream.write_all(&conn.write_buf).await?;

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
