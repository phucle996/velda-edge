//! Pure in-memory buffered HTTP/1.1 request-response forwarding.
//!
//! Enforces zero streaming: reads full request body, forwards request,
//! expects bounded response (Content-Length or Empty), and sends response downstream.
//! Rejects upstream chunked/SSE responses if listener has server streaming disabled.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::Body;

use super::sanitize_hop_by_hop_headers;
use crate::client::connector::{read_chunk_sized, read_response_head, send_request};
use crate::client::response::Http1Response;
use crate::config::Http1Config;
use crate::error::Http1Error;
use crate::server::connection::Http1ServerConnection;
use crate::server::request::{Http1BodyFraming, Http1Request};

/// Pipes a non-streaming HTTP/1.1 request between downstream connection and upstream stream.
///
/// # Invariants
/// - Request body is read completely into RAM before forwarding.
/// - Upstream response must have finite framing (Content-Length or Empty).
/// - If upstream returns chunked encoding or SSE, returns [`Http1Error::StreamingViolation`].
pub async fn pipe_buffered<DownIO, UpIO>(
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

    // 4. Validate that upstream does not violate buffered mode invariant
    if resp_framing == Http1BodyFraming::Chunked {
        return Err(Http1Error::StreamingViolation(
            "Upstream returned chunked transfer-encoding, but server streaming is disabled on this listener"
                .into(),
        ));
    }

    if let Some(content_type) = resp_head.headers.get(http::header::CONTENT_TYPE)
        && let Ok(ct_str) = content_type.to_str()
        && ct_str.to_ascii_lowercase().contains("text/event-stream")
    {
        return Err(Http1Error::StreamingViolation(
            "Upstream returned text/event-stream (SSE), but server streaming is disabled on this listener"
                .into(),
        ));
    }

    // 5. Read bounded response body (RFC 9110 §6.4.1 & §9.3.2: HEAD, 204, 304 have no body)
    let is_head = req.method == http::Method::HEAD;
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
                let mut bytes = BytesMut::with_capacity(len);
                while bytes.len() < len {
                    let needed = len - bytes.len();
                    match read_chunk_sized(upstream, &mut read_buf, needed).await? {
                        Some(c) => bytes.extend_from_slice(&c),
                        None => break,
                    }
                }
                Body::Bytes(bytes.freeze())
            }
            Http1BodyFraming::Chunked => unreachable!(),
        }
    };

    // 6. Send response downstream
    let resp = Http1Response::from_parts(resp_head, body);
    conn.send_response(&resp).await?;

    Ok(())
}
