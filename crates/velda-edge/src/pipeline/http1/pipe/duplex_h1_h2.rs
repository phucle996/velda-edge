//! Layer 7 HTTP/1.1 to HTTP/2 Duplex Streaming Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming (e.g. WebSocket over H2, bidirectional RPC)
//! where downstream HTTP/1.1 chunks and upstream HTTP/2 DATA frames flow concurrently.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_http1::wire::{decode_chunk, send_chunk, send_chunked_end};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http2::{Http2Config, sanitize_h2_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a full-duplex bidirectional streaming HTTP/1.1 downstream request bridged to an HTTP/2 backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http2Upstream>,
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

    // 3. Sanitize connection-specific headers forbidden by RFC 9113 §8.2.2
    sanitize_h2_headers(&mut head.headers);

    let h2_config = Http2Config::auto();

    // 4. Acquire pooled HTTP/2 client
    let (mut client, _lease) = match upstream.acquire(&h2_config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 client for duplex stream"
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

    // 5. Initiate HTTP/2 stream
    let scheme = if upstream.is_tls() { "https" } else { "http" };
    let authority = head
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| head.uri.host())
        .unwrap_or(upstream.target_host());
    let path_and_query = head
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let uri = http::Uri::builder()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(path_and_query)
        .build()
        .unwrap_or_else(|_| head.uri.clone());

    let is_eos = framing == Http1BodyFraming::Empty;
    let mut req = http::Request::builder()
        .method(head.method.clone())
        .uri(uri)
        .version(http::Version::HTTP_2);
    *req.headers_mut().unwrap() = head.headers.clone();
    let req = req.body(()).unwrap();

    let (response_future, mut send_stream) = match client.send_request(req, is_eos) {
        Ok(parts) => parts,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to initiate HTTP/2 duplex stream");
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

    // Asymmetric coordination channel
    let (resp_done_tx, mut resp_done_rx) = tokio::sync::oneshot::channel::<()>();
    let upload_finished = AtomicBool::new(is_eos);

    // Task 1: Concurrent Upload Pump (Downstream -> Upstream H2)
    let upload_task = async {
        if is_eos {
            return Ok::<(), Http1Error>(());
        }

        match framing {
            Http1BodyFraming::Chunked => loop {
                if resp_done_rx.try_recv().is_ok() {
                    break;
                }

                match decode_chunk(read_buf)? {
                    Some(Some(chunk)) => {
                        send_stream.reserve_capacity(chunk.len());
                        send_stream
                            .send_data(chunk, false)
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                    }
                    Some(None) => {
                        upload_finished.store(true, Ordering::Release);
                        break;
                    }
                    None => {
                        const TARGET_RESERVE: usize = 65536;
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
            },
            Http1BodyFraming::ContentLength(total_len) => {
                let mut remaining = total_len;
                while remaining > 0 {
                    if resp_done_rx.try_recv().is_ok() {
                        break;
                    }

                    let available = read_buf.len();
                    if available > 0 {
                        let chunk_len = available.min(remaining);
                        let chunk = read_buf.split_to(chunk_len).freeze();
                        remaining = remaining.saturating_sub(chunk_len);
                        send_stream.reserve_capacity(chunk_len);
                        send_stream
                            .send_data(chunk, false)
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                        continue;
                    }

                    let to_read = remaining.min(65536);
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
                            let chunk_len = n.min(remaining);
                            let chunk = read_buf.split_to(chunk_len).freeze();
                            remaining = remaining.saturating_sub(chunk_len);
                            send_stream.reserve_capacity(chunk_len);
                            send_stream
                                .send_data(chunk, false)
                                .map_err(|_| Http1Error::ConnectionClosed)?;
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

        // Conclude upload stream to unblock upstream response completion
        let _ = send_stream.send_data(Bytes::new(), true);
        Ok::<(), Http1Error>(())
    };

    // Task 2: Concurrent Download Pump (Upstream H2 -> Downstream)
    let download_task = async {
        let response = response_future
            .await
            .map_err(|_| Http1Error::ConnectionClosed)?;
        let (parts, mut body_stream) = response.into_parts();

        let should_close = conn.close_requested;

        let is_no_body_status = parts.status.is_informational()
            || parts.status == http::StatusCode::NO_CONTENT
            || parts.status == http::StatusCode::NOT_MODIFIED;

        if is_no_body_status {
            let mut write_buf = bytes::BytesMut::with_capacity(256);
            velda_http1::encode_response_head(
                http::Version::HTTP_11,
                parts.status,
                &parts.headers,
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
        let cl_opt = parts
            .headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok());

        if let Some(content_len) = cl_opt
            && content_len <= 65536
        {
            let mut body_buf = bytes::BytesMut::with_capacity(content_len);
            while body_buf.len() < content_len {
                match body_stream.data().await {
                    Some(Ok(chunk)) => {
                        let _ = body_stream.flow_control().release_capacity(chunk.len());
                        body_buf.extend_from_slice(&chunk);
                    }
                    Some(Err(_)) => return Err(Http1Error::ConnectionClosed),
                    None => break,
                }
            }
            let mut write_buf = bytes::BytesMut::with_capacity(256 + body_buf.len());
            velda_http1::encode_response_head(
                http::Version::HTTP_11,
                parts.status,
                &parts.headers,
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
            parts.status,
            &parts.headers,
            should_close,
        )
        .await?;

        while let Some(chunk) = body_stream.data().await {
            let chunk = chunk.map_err(|_| Http1Error::ConnectionClosed)?;
            let _ = body_stream.flow_control().release_capacity(chunk.len());
            send_chunk(&mut client_write, &chunk).await?;
        }

        send_chunked_end(&mut client_write).await?;
        client_write.flush().await?;
        let _ = resp_done_tx.send(());
        Ok::<(), Http1Error>(())
    };

    // 7. Concurrently drive upload and download with zero head-of-line blocking
    let duplex_res = tokio::try_join!(upload_task, download_task);
    if !upload_finished.load(Ordering::Acquire) {
        conn.mark_close();
    }
    duplex_res.map(|_| ())
}
