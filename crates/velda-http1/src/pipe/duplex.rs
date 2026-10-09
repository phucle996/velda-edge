//! Bidirectional streaming (chunked upload + chunked response).
//!
//! Enforces full bidirectional streaming:
//! - Client uploads request body progressively using chunked transfer encoding.
//! - Upstream streams response body progressively using chunked transfer encoding.
//! - Cleanly propagates client disconnects to cancel active upstream stream.

use tokio::io::{AsyncRead, AsyncWrite};

use velda_core::Body;

use super::sanitize_headers;
use crate::client::request::send_request_head_chunked;
use crate::client::response::{read_chunk_sized, read_next_chunk, read_response_head};
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::connection::Http1ServerConnection;
use crate::server::request::{Http1BodyFraming, Http1ServerRequestHead};
use crate::server::response::Http1ServerResponse;
use crate::wire::{send_chunk, send_chunked_end};

/// Pipes a bidirectional streaming HTTP/1.1 request (streaming upload and streaming response).
pub async fn pipe_duplex<DownIO, UpIO>(
    conn: &mut Http1ServerConnection<DownIO>,
    head: &mut Http1ServerRequestHead,
    upstream: &mut UpIO,
    config: &Http1Config,
) -> Result<(), Http1Error>
where
    DownIO: AsyncRead + AsyncWrite + Unpin,
    UpIO: AsyncRead + AsyncWrite + Unpin,
{
    sanitize_headers(&mut head.headers);

    // 1. Send chunked request head upstream reusing scratch buffer
    send_request_head_chunked(head, upstream, &mut conn.upstream_write_buf).await?;

    // 2. Pump request body chunks from client to upstream
    while let Some(chunk) = conn.read_next_chunk().await? {
        send_chunk(upstream, &chunk).await?;
    }
    send_chunked_end(upstream).await?;

    // 3. Read upstream response head reusing connection read buffer
    let (mut resp_head, resp_framing) =
        read_response_head(upstream, &mut conn.upstream_read_buf, config).await?;
    sanitize_headers(&mut resp_head.headers);

    // 4. RFC 9110 §6.4.1 & §9.3.2: HEAD, 1xx, 204, 304 have no body
    let is_head = head.method == http::Method::HEAD;
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

    // Send chunked response head downstream
    conn.send_client_response_head_chunked(&resp_head).await?;

    // 5. Pump response chunks downstream with client disconnect detection
    let mut client_disconnected = false;
    match resp_framing {
        Http1BodyFraming::Chunked => {
            while let Some(chunk) = read_next_chunk(upstream, &mut conn.upstream_read_buf).await? {
                if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
                    tracing::debug!(
                        error = %e,
                        "Downstream client disconnected during duplex response streaming"
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
                        if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
                            tracing::debug!(
                                error = %e,
                                "Downstream client disconnected during sized duplex response streaming"
                            );
                            client_disconnected = true;
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
        Http1BodyFraming::Empty => {}
    }

    if client_disconnected {
        return Err(Http1Error::ConnectionClosed);
    }

    let _ = send_chunked_end(&mut conn.stream).await;
    Ok(())
}
