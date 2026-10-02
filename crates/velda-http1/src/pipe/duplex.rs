//! Bidirectional streaming (chunked upload + chunked response).
//!
//! Enforces full bidirectional streaming:
//! - Client uploads request body progressively using chunked transfer encoding.
//! - Upstream streams response body progressively using chunked transfer encoding.
//! - Cleanly propagates client disconnects to cancel active upstream stream.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};

use super::sanitize_hop_by_hop_headers;
use crate::client::connector::{
    read_chunk_sized, read_next_chunk, read_response_head, send_request_head_chunked,
};
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::connection::Http1ServerConnection;
use crate::server::request::{Http1BodyFraming, Http1RequestHead};
use crate::wire::{send_chunk, send_chunked_end};

/// Pipes a bidirectional streaming HTTP/1.1 request (streaming upload and streaming response).
pub async fn pipe_duplex<DownIO, UpIO>(
    conn: &mut Http1ServerConnection<DownIO>,
    mut head: Http1RequestHead,
    upstream: &mut UpIO,
    config: &Http1Config,
) -> Result<(), Http1Error>
where
    DownIO: AsyncRead + AsyncWrite + Unpin,
    UpIO: AsyncRead + AsyncWrite + Unpin,
{
    sanitize_hop_by_hop_headers(&mut head.headers);

    // 1. Send chunked request head upstream
    send_request_head_chunked(&head, upstream, config).await?;

    // 2. Pump request body chunks from client to upstream
    while let Some(chunk) = conn.read_next_chunk().await? {
        send_chunk(upstream, &chunk).await?;
    }
    send_chunked_end(upstream).await?;

    // 3. Read upstream response head
    let mut read_buf = BytesMut::with_capacity(config.upstream_read_capacity);
    let (mut resp_head, resp_framing) = read_response_head(upstream, &mut read_buf, config).await?;
    sanitize_hop_by_hop_headers(&mut resp_head.headers);

    // 4. Send chunked response head downstream
    conn.send_response_head_chunked(&resp_head).await?;

    // 5. Pump response chunks downstream with client disconnect detection
    let mut client_disconnected = false;
    match resp_framing {
        Http1BodyFraming::Chunked => {
            while let Some(chunk) = read_next_chunk(upstream, &mut read_buf).await? {
                if let Err(e) = conn.send_chunk(&chunk).await {
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
                let to_read = remaining.min(16384);
                match read_chunk_sized(upstream, &mut read_buf, to_read).await? {
                    Some(chunk) => {
                        remaining = remaining.saturating_sub(chunk.len());
                        if let Err(e) = conn.send_chunk(&chunk).await {
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

    if !client_disconnected {
        let _ = conn.send_chunked_end().await;
    }

    Ok(())
}
