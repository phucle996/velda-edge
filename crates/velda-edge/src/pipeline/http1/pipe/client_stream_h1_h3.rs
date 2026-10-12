//! Layer 7 HTTP/1.1 to HTTP/3 Client-Stream Bridge Pipeline.
//!
//! Handles progressive client-streaming uploads (e.g. file uploads, log batch ingest) where
//! downstream HTTP/1.1 chunks are progressively streamed to an upstream HTTP/3 QUIC connection,
//! and a single response is returned downstream with zero whole-payload buffering in RAM.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use velda_http1::wire::decode_chunk;
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
    let (client, _lease) = match upstream.acquire(&h3_config).await {
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

    // 5. Open progressive HTTP/3 stream
    let (sender, response_rx) = match client
        .open_stream(head.method.clone(), head.uri.clone(), head.headers.clone())
        .await
    {
        Ok(parts) => parts,
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

    // 6. Progressively stream client body chunks directly to HTTP/3 without buffering in RAM
    match framing {
        Http1BodyFraming::Chunked => loop {
            match decode_chunk(&mut conn.read_buf)? {
                Some(Some(chunk)) => {
                    if let Err(e) = sender.send_chunk(chunk).await {
                        tracing::warn!(error = %e, "Failed to stream chunk to HTTP/3 upstream");
                        return Err(Http1Error::ConnectionClosed);
                    }
                }
                Some(None) => break,
                None => {
                    const TARGET_RESERVE: usize = 65536;
                    if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < TARGET_RESERVE
                    {
                        conn.read_buf.reserve(TARGET_RESERVE);
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
        },
        Http1BodyFraming::ContentLength(len) => {
            let mut remaining = len;
            while remaining > 0 {
                let available = conn.read_buf.len();
                if available > 0 {
                    let chunk_len = available.min(remaining);
                    let chunk = conn.read_buf.split_to(chunk_len).freeze();
                    remaining = remaining.saturating_sub(chunk_len);
                    if let Err(e) = sender.send_chunk(chunk).await {
                        tracing::warn!(error = %e, "Failed to stream chunk to HTTP/3 upstream");
                        return Err(Http1Error::ConnectionClosed);
                    }
                    continue;
                }
                let to_read = remaining.min(65536);
                if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < to_read {
                    conn.read_buf.reserve(to_read);
                }
                let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                if n == 0 {
                    break;
                }
                let chunk_len = n.min(remaining);
                let chunk = conn.read_buf.split_to(chunk_len).freeze();
                remaining = remaining.saturating_sub(chunk_len);
                if let Err(e) = sender.send_chunk(chunk).await {
                    tracing::warn!(error = %e, "Failed to stream chunk to HTTP/3 upstream");
                    return Err(Http1Error::ConnectionClosed);
                }
            }
        }
        Http1BodyFraming::Empty => {}
    }

    let _ = sender.finish().await;

    // 6. Await response headers from upstream HTTP/3
    let mut resp_stream = match response_rx.await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "Upstream HTTP/3 stream returned error");
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::ConnectionClosed);
        }
        Err(_) => {
            return Err(Http1Error::ConnectionClosed);
        }
    };

    // 7. Consume finite response body (client streaming expects finite response like 200/201)
    let mut resp_body = BytesMut::new();
    while let Some(chunk_res) = resp_stream.next_chunk().await {
        match chunk_res {
            Ok(chunk) => resp_body.extend_from_slice(&chunk),
            Err(e) => {
                tracing::warn!(error = %e, "Error reading response chunk from HTTP/3 upstream");
                return Err(Http1Error::ConnectionClosed);
            }
        }
    }

    let body = if resp_body.is_empty() {
        velda_core::Body::Empty
    } else {
        velda_core::Body::Bytes(resp_body.freeze())
    };

    let server_resp = Http1ServerResponse::new(
        resp_stream.status,
        http::Version::HTTP_11,
        resp_stream.headers,
        body,
    );
    conn.send_response(&server_resp).await?;
    Ok(())
}
