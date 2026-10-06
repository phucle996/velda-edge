//! Progressive server response streaming (SSE, LLM tokens, large downloads).
//!
//! Enforces unidirectional server-to-client streaming:
//! - Request body is read completely and forwarded to upstream.
//! - Upstream response chunks are pumped downstream immediately with zero intermediate buffering.
//! - Downstream client disconnect cleanly aborts the upstream stream to stop wasting backend resources.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};

use velda_core::Body;

use super::sanitize_hop_by_hop_headers;
use crate::client::connector::{
    read_chunk_sized, read_next_chunk, read_response_head, send_request,
};
use crate::client::response::Http1Response;
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::connection::Http1ServerConnection;
use crate::server::request::{Http1BodyFraming, Http1Request};

/// Pipes a server-streaming HTTP/1.1 request (buffered request, streaming response).
///
/// If downstream client drops the connection during streaming, terminates the loop
/// immediately so that the upstream connection drops and stops processing (e.g. stops LLM generation).
pub async fn pipe_server_stream<DownIO, UpIO>(
    conn: &mut Http1ServerConnection<DownIO>,
    mut req: Http1Request,
    upstream: &mut UpIO,
    config: &Http1Config,
) -> Result<(), Http1Error>
where
    DownIO: AsyncRead + AsyncWrite + Unpin,
    UpIO: AsyncRead + AsyncWrite + Unpin,
{
    sanitize_hop_by_hop_headers(&mut req.headers);

    // 1. Send complete request to upstream backend
    send_request(&req, upstream, config).await?;

    // 3. Read upstream response head
    let mut read_buf = BytesMut::with_capacity(config.upstream_read_capacity);
    let (mut resp_head, resp_framing) = read_response_head(upstream, &mut read_buf, config).await?;
    sanitize_hop_by_hop_headers(&mut resp_head.headers);

    // 4. RFC 9110 §6.4.1 & §9.3.2: HEAD, 1xx, 204, 304 have no body
    let is_head = req.method == http::Method::HEAD;
    let is_no_body_status = resp_head.status.is_informational()
        || resp_head.status == http::StatusCode::NO_CONTENT
        || resp_head.status == http::StatusCode::NOT_MODIFIED;

    if is_head || is_no_body_status {
        let resp = Http1Response::from_parts(resp_head, Body::Empty);
        conn.send_response(&resp).await?;
        return Ok(());
    }

    // Send chunked response head downstream
    conn.send_response_head_chunked(&resp_head).await?;

    // 5. Pump response chunks downstream with client disconnect detection
    let mut client_disconnected = false;
    match resp_framing {
        Http1BodyFraming::Chunked => {
            while let Some(chunk) = read_next_chunk(upstream, &mut read_buf).await? {
                if let Err(e) = conn.send_chunk(&chunk).await {
                    tracing::debug!(
                        error = %e,
                        "Downstream client disconnected during chunked server streaming"
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
                                "Downstream client disconnected during sized server streaming"
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

    let _ = conn.send_chunked_end().await;
    Ok(())
}
