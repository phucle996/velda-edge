//! Layer 7 HTTP/1.1 to HTTP/2 Client-Stream Bridge Pipeline.
//!
//! Handles client-streaming uploads (chunked file upload, batch ingest) where
//! downstream HTTP/1.1 chunks are progressively streamed to an upstream HTTP/2 connection,
//! and a single buffered response is returned downstream.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::wire::decode_chunk;
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http2::Http2Config;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a client-streaming HTTP/1.1 downstream request bridged to an HTTP/2 backend.
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

    // 3. Sanitize headers disallowed by RFC 9113 for HTTP/2
    velda_http2::client::sanitize_h2_headers(&mut head.headers);

    let h2_config = Http2Config::auto();

    // 4. Acquire upstream multiplexed connection before upload begins
    let (mut client, _lease) = match upstream.acquire(&h2_config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection"
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
    let mut req = http::Request::builder()
        .method(head.method.clone())
        .uri(head.uri.clone())
        .version(http::Version::HTTP_2);
    *req.headers_mut().unwrap() = head.headers.clone();
    let req = req.body(()).unwrap();

    let (response_future, mut send_stream) = match client.send_request(req, false) {
        Ok(parts) => parts,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to initiate HTTP/2 stream for upload");
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::ConnectionClosed);
        }
    };

    // 6. Pump downstream chunks to HTTP/2 DATA frames
    match framing {
        Http1BodyFraming::Chunked => loop {
            match decode_chunk(&mut conn.upstream_read_buf)? {
                Some(Some(chunk)) => {
                    send_stream.reserve_capacity(chunk.len());
                    send_stream
                        .send_data(chunk, false)
                        .map_err(|_| Http1Error::ConnectionClosed)?;
                }
                Some(None) => break,
                None => {
                    let mut temp = [0u8; 8192];
                    use tokio::io::AsyncReadExt;
                    let n = conn.stream.read(&mut temp).await?;
                    if n == 0 {
                        break;
                    }
                    conn.upstream_read_buf.extend_from_slice(&temp[..n]);
                }
            }
        },
        Http1BodyFraming::ContentLength(len) => {
            let mut remaining = len;
            while remaining > 0 {
                let to_read = remaining.min(8192);
                let mut temp = vec![0u8; to_read];
                use tokio::io::AsyncReadExt;
                let n = conn.stream.read(&mut temp).await?;
                if n == 0 {
                    break;
                }
                remaining = remaining.saturating_sub(n);
                send_stream.reserve_capacity(n);
                send_stream
                    .send_data(Bytes::copy_from_slice(&temp[..n]), false)
                    .map_err(|_| Http1Error::ConnectionClosed)?;
            }
        }
        Http1BodyFraming::Empty => {}
    }

    // Conclude upload stream
    let _ = send_stream.send_data(Bytes::new(), true);

    // 7. Await response and return to downstream
    let response = response_future
        .await
        .map_err(|_| Http1Error::ConnectionClosed)?;
    let (parts, mut body_stream) = response.into_parts();

    let mut resp_body = BytesMut::new();
    while let Some(chunk) = body_stream.data().await {
        let chunk = chunk.map_err(|_| Http1Error::ConnectionClosed)?;
        let _ = body_stream.flow_control().release_capacity(chunk.len());
        resp_body.extend_from_slice(&chunk);
    }

    let server_resp = Http1ServerResponse::new(
        parts.status,
        http::Version::HTTP_11,
        parts.headers,
        velda_core::Body::Bytes(resp_body.freeze()),
    );
    conn.send_response(&server_resp).await?;

    Ok(())
}
