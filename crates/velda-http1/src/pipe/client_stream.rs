//! Progressive client request streaming (large file uploads, log ingestion).
//!
//! Enforces unidirectional client-to-server streaming:
//! - Client uploads request body progressively using chunked transfer encoding.
//! - Upstream receives chunks in real-time without gateway buffering the entire upload in RAM.
//! - Response from upstream is expected to be finite and buffered (e.g. 201 Created or JSON receipt).

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use velda_core::Body;

use super::sanitize_headers;
use crate::client::request::send_request_head_chunked;
use crate::client::response::read_response_head;
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::connection::Http1ServerConnection;
use crate::server::request::{Http1BodyFraming, Http1ServerRequestHead};
use crate::server::response::Http1ServerResponse;
use crate::wire::{send_chunk, send_chunked_end};

/// Pipes a client-streaming HTTP/1.1 request (streaming upload, buffered response).
pub async fn pipe_client_stream<DownIO, UpIO>(
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

    // 4. Validate response framing (client streaming expects non-streaming response)
    if resp_framing == Http1BodyFraming::Chunked {
        return Err(Http1Error::StreamingViolation(
            "Upstream returned chunked transfer-encoding on client-streaming route with server streaming disabled"
                .into(),
        ));
    }

    // 5. Read bounded response body (RFC 9110 §6.4.1 & §9.3.2: HEAD, 1xx, 204, 304 have no body)
    let is_head = head.method == http::Method::HEAD;
    let is_no_body_status = resp_head.status.is_informational()
        || resp_head.status == http::StatusCode::NO_CONTENT
        || resp_head.status == http::StatusCode::NOT_MODIFIED;

    let body = if is_head || is_no_body_status {
        Body::Empty
    } else {
        match resp_framing {
            Http1BodyFraming::Empty => Body::Empty,
            Http1BodyFraming::ContentLength(len) => {
                if len > config.max_body_size {
                    return Err(Http1Error::PayloadTooLarge(len));
                }
                if conn.upstream_read_buf.len() < len {
                    let needed = len - conn.upstream_read_buf.len();
                    conn.upstream_read_buf.reserve(needed);
                    while conn.upstream_read_buf.len() < len {
                        let n = upstream.read_buf(&mut conn.upstream_read_buf).await?;
                        if n == 0 {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body: premature connection close".into(),
                            ));
                        }
                    }
                }
                Body::Bytes(conn.upstream_read_buf.split_to(len).freeze())
            }
            Http1BodyFraming::Chunked => unreachable!(),
        }
    };

    // 6. Send response downstream
    let resp =
        Http1ServerResponse::new(resp_head.status, resp_head.version, resp_head.headers, body);
    conn.send_response(&resp).await?;

    Ok(())
}
