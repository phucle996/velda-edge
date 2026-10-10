//! Layer 7 HTTP/2 to HTTP/1.1 Duplex Streaming Bridge Pipeline.
//!
//! Handles full bidirectional streaming (duplex) between a downstream HTTP/2 client
//! and an HTTP/1.1 backend, simultaneously uploading chunked data and streaming chunked
//! responses downstream with cancellation propagation.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, TRANSFER_ENCODING};
use tokio::io::AsyncWriteExt;
use velda_core::L7Response;
use velda_http1::server::request::Http1BodyFraming;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a full-duplex HTTP/2 downstream stream bridged to an HTTP/1.1 backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
) {
    // 1. Ensure mandatory Host header (RFC 9112 §7.1)
    if !head.headers.contains_key(http::header::HOST)
        && let Some(host) = velda_http2::server::header::extract_host(&head.headers, &head.uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        head.headers.insert(http::header::HOST, hv);
    }

    // 2. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 3. Mark request as Chunked transfer encoding towards HTTP/1.1
    head.headers
        .insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
    head.headers.remove(CONTENT_LENGTH);

    // 4. Acquire pooled HTTP/1.1 upstream connection
    let mut lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection for duplex bridge"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    let cfg = velda_http1::config::Http1Config::auto();
    let mut write_buf = bytes::BytesMut::with_capacity(cfg.upstream_write_base);

    // 5. Send HTTP/1.1 request head
    velda_http1::client::encode_request_head(
        &head.method,
        &head.uri,
        &head.headers,
        None,
        &mut write_buf,
    );

    if let Err(e) = lease.write_all(&write_buf).await {
        lease.mark_closed();
        tracing::warn!(error = %e, upstream = %upstream.id(), "Failed to send duplex request head");
        let err_resp = L7Response::from_bytes(
            StatusCode::BAD_GATEWAY,
            format!("502 Bad Gateway: {e}\n").into_bytes(),
        );
        let _ = responder.send_response(&err_resp);
        return;
    }

    // 6. Split HTTP/1.1 stream into read and write halves for concurrent duplex execution
    let (mut read_half, mut write_half) = tokio::io::split(&mut *lease);

    // Upload task: pipes downstream H2 DATA frames into upstream H1 chunked encoding
    let upload_fut = async {
        let mut hex_buf = [0u8; 32];
        loop {
            match receiver.recv_chunk().await {
                Ok(Some(chunk)) => {
                    if !chunk.is_empty() {
                        let hex_len = {
                            use std::io::Write;
                            let mut cursor = std::io::Cursor::new(&mut hex_buf[..]);
                            let _ = write!(cursor, "{:x}\r\n", chunk.len());
                            cursor.position() as usize
                        };
                        write_half.write_all(&hex_buf[..hex_len]).await?;
                        write_half.write_all(&chunk).await?;
                        write_half.write_all(b"\r\n").await?;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, e));
                }
            }
        }
        write_half.write_all(b"0\r\n\r\n").await?;
        write_half.flush().await?;
        Ok::<(), std::io::Error>(())
    };

    // Download task: reads upstream H1 response and streams H2 DATA frames downstream
    let download_fut = async {
        let mut read_buf = bytes::BytesMut::with_capacity(cfg.upstream_read_capacity);
        let (resp_head, framing) =
            velda_http1::client::read_response_head(&mut read_half, &mut read_buf, &cfg)
                .await
                .map_err(std::io::Error::other)?;

        let mut clean_headers = resp_head.headers;
        velda_http1::client::sanitize_headers(&mut clean_headers);

        let mut stream_sender = responder
            .send_stream_response(resp_head.status, &clean_headers)
            .map_err(std::io::Error::other)?;

        match framing {
            Http1BodyFraming::Empty => {}
            Http1BodyFraming::Chunked => loop {
                match velda_http1::client::read_next_chunk(&mut read_half, &mut read_buf).await {
                    Ok(Some(chunk)) => {
                        stream_sender
                            .send_chunk(chunk)
                            .await
                            .map_err(std::io::Error::other)?;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(std::io::Error::other(e));
                    }
                }
            },
            Http1BodyFraming::ContentLength(total_len) => {
                let mut remaining = total_len;
                while remaining > 0 {
                    let to_read = remaining.min(65536);
                    match velda_http1::client::read_chunk_sized(
                        &mut read_half,
                        &mut read_buf,
                        to_read,
                    )
                    .await
                    {
                        Ok(Some(chunk)) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            stream_sender
                                .send_chunk(chunk)
                                .await
                                .map_err(std::io::Error::other)?;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            return Err(std::io::Error::other(e));
                        }
                    }
                }
            }
        }

        let _ = stream_sender.finish();
        Ok::<(), std::io::Error>(())
    };

    // Concurrently drive upload and download, aborting on whichever encounters a failure
    if let Err(e) = tokio::try_join!(upload_fut, download_fut) {
        lease.mark_closed();
        tracing::warn!(error = %e, upstream = %upstream.id(), "Duplex H2 to H1 bridge failed");
    }
}
