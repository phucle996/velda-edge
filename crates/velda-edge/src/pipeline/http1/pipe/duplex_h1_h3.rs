//! Layer 7 HTTP/1.1 to HTTP/3 Duplex Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming (e.g. WebSocket / bidirectional stream)
//! between downstream HTTP/1.1 client and upstream HTTP/3 QUIC connection with true
//! concurrent streaming pumps and zero whole-payload buffering.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_http1::wire::{decode_chunk, send_chunk, send_chunked_end};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a full-duplex HTTP/1.1 downstream stream bridged to an HTTP/3 QUIC backend.
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

    // 4. Acquire pooled HTTP/3 client
    let (client, _lease) = match upstream.acquire(&h3_config).await {
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

    // 5. Open progressive bidirectional HTTP/3 stream
    let (sender, response_rx) = match client
        .open_stream(head.method.clone(), head.uri.clone(), head.headers.clone())
        .await
    {
        Ok(parts) => parts,
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

    // Task 1: Upload Pump (Downstream H1 -> Upstream H3 QUIC)
    let upload_task = async {
        let is_eos = framing == Http1BodyFraming::Empty;
        if is_eos {
            let _ = sender.finish().await;
            return Ok::<(), Http1Error>(());
        }

        match framing {
            Http1BodyFraming::Chunked => loop {
                match decode_chunk(read_buf)? {
                    Some(Some(chunk)) => {
                        sender
                            .send_chunk(chunk)
                            .await
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                    }
                    Some(None) => break,
                    None => {
                        const TARGET_RESERVE: usize = 65536;
                        if read_buf.capacity().saturating_sub(read_buf.len()) < TARGET_RESERVE {
                            read_buf.reserve(TARGET_RESERVE);
                        }
                        let n = client_read.read_buf(read_buf).await?;
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
            },
            Http1BodyFraming::ContentLength(total_len) => {
                let mut remaining = total_len;
                while remaining > 0 {
                    let available = read_buf.len();
                    if available > 0 {
                        let chunk_len = available.min(remaining);
                        let chunk = read_buf.split_to(chunk_len).freeze();
                        remaining = remaining.saturating_sub(chunk_len);
                        sender
                            .send_chunk(chunk)
                            .await
                            .map_err(|_| Http1Error::ConnectionClosed)?;
                        continue;
                    }
                    let to_read = remaining.min(65536);
                    if read_buf.capacity().saturating_sub(read_buf.len()) < to_read {
                        read_buf.reserve(to_read);
                    }
                    let n = client_read.read_buf(read_buf).await?;
                    if n == 0 {
                        break;
                    }
                    let chunk_len = n.min(remaining);
                    let chunk = read_buf.split_to(chunk_len).freeze();
                    remaining = remaining.saturating_sub(chunk_len);
                    sender
                        .send_chunk(chunk)
                        .await
                        .map_err(|_| Http1Error::ConnectionClosed)?;
                }
            }
            Http1BodyFraming::Empty => {}
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
                false,
                &mut write_buf,
            );
            client_write.write_all(&write_buf).await?;
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
            let mut write_buf = bytes::BytesMut::with_capacity(256 + body_buf.len());
            velda_http1::encode_response_head(
                http::Version::HTTP_11,
                resp_stream.status,
                &resp_stream.headers,
                Some(body_buf.len()),
                false,
                &mut write_buf,
            );
            write_buf.extend_from_slice(&body_buf);
            client_write.write_all(&write_buf).await?;
            client_write.flush().await?;
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
            false,
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

    tokio::try_join!(upload_task, download_task)?;
    Ok(())
}
