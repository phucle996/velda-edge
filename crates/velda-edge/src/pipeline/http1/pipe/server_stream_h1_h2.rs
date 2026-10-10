//! Layer 7 HTTP/1.1 to HTTP/2 Server-Stream Bridge Pipeline.
//!
//! Handles server-streaming RPCs (Server-Sent Events, progressive chunks, LLM tokens)
//! where downstream HTTP/1.1 requests are forwarded over HTTP/2, and response DATA chunks
//! are pumped progressively downstream as HTTP/1.1 chunked transfer encoding.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::server::response::Http1ServerResponseHead;
use velda_http1::wire::{send_chunk, send_chunked_end};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http2::Http2Config;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a server-streaming HTTP/1.1 downstream request bridged to an HTTP/2 backend.
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

    // 3. Read entire downstream body into RAM
    let body = match conn.read_body(framing).await {
        Ok(b) => b,
        Err(Http1Error::Timeout) => {
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::REQUEST_TIMEOUT,
                b"408 Request Timeout: client body read idle timeout exceeded\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::Timeout);
        }
        Err(e) => {
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(e);
        }
    };

    let body_bytes = match body {
        velda_core::Body::Bytes(b) => b,
        velda_core::Body::Empty => bytes::Bytes::new(),
    };

    // 4. Sanitize headers disallowed by RFC 9113 for HTTP/2
    velda_http2::client::sanitize_h2_headers(&mut head.headers);

    let h2_config = Http2Config::auto();

    // 5. Acquire upstream multiplexed connection
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

    // 6. Forward request over HTTP/2 stream
    let mut req = http::Request::builder()
        .method(head.method.clone())
        .uri(head.uri.clone())
        .version(http::Version::HTTP_2);
    *req.headers_mut().unwrap() = head.headers.clone();
    let req = req.body(()).unwrap();

    let has_body = !body_bytes.is_empty();
    let (response_future, mut send_stream) = match client.send_request(req, !has_body) {
        Ok(parts) => parts,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to initiate HTTP/2 stream to upstream"
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

    if has_body && let Err(e) = send_stream.send_data(body_bytes, true) {
        tracing::warn!(error = %e, "Failed to send request body data on HTTP/2 stream");
        let err_resp = Http1ServerResponse::from_bytes(
            StatusCode::BAD_GATEWAY,
            b"502 Bad Gateway: failed to transmit body\n".to_vec(),
        );
        conn.mark_close();
        let _ = conn.send_response(&err_resp).await;
        return Err(Http1Error::ConnectionClosed);
    }

    let response = match response_future.await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "Upstream HTTP/2 response header read failed");
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::ConnectionClosed);
        }
    };

    let (parts, mut body_stream) = response.into_parts();

    // 7. Send chunked response head downstream
    let resp_head =
        Http1ServerResponseHead::new(parts.status, http::Version::HTTP_11, parts.headers);
    conn.send_response_head_chunked(&resp_head).await?;

    // 8. Stream chunks downstream with backpressure & client disconnect detection
    while let Some(chunk) = body_stream.data().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "Error reading HTTP/2 response stream chunk");
                return Err(Http1Error::ConnectionClosed);
            }
        };
        let len = chunk.len();
        if let Err(e) = body_stream.flow_control().release_capacity(len) {
            tracing::debug!(error = %e, "Failed to release HTTP/2 flow control capacity");
        }

        if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
            tracing::debug!(error = %e, "Downstream client disconnected during streaming");
            return Err(Http1Error::ConnectionClosed);
        }
    }

    // 9. Send terminal chunk
    send_chunked_end(&mut conn.stream).await?;

    Ok(())
}
