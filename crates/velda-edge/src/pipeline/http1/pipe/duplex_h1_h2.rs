//! Layer 7 HTTP/1.1 to HTTP/2 Duplex Pipeline.
//!
//! Handles concurrent full-duplex bidirectional streaming (e.g. WebSocket / bidirectional stream)
//! between downstream HTTP/1.1 client and upstream HTTP/2 connection.

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::wire::{decode_chunk, send_chunk};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http2::Http2Config;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a full-duplex HTTP/1.1 downstream stream bridged to an HTTP/2 backend.
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

    // 4. Acquire upstream connection before duplex session begins
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
            tracing::warn!(error = %e, "Failed to initiate HTTP/2 duplex stream");
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::ConnectionClosed);
        }
    };

    // 6. Await response headers first to send response head downstream
    let response = response_future
        .await
        .map_err(|_| Http1Error::ConnectionClosed)?;
    let (parts, mut body_stream) = response.into_parts();

    let resp_head = velda_http1::server::response::Http1ServerResponseHead::new(
        parts.status,
        http::Version::HTTP_11,
        parts.headers,
    );
    conn.send_response_head_chunked(&resp_head).await?;

    // 7. Concurrent upload pump and download pump
    // Download pump: H2 stream -> Downstream chunks
    while let Some(chunk) = body_stream.data().await {
        let chunk = chunk.map_err(|_| Http1Error::ConnectionClosed)?;
        let _ = body_stream.flow_control().release_capacity(chunk.len());
        if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
            tracing::debug!(error = %e, "Downstream client disconnected during duplex");
            send_stream.send_reset(h2::Reason::CANCEL);
            return Err(Http1Error::ConnectionClosed);
        }

        // Opportunistically drain any downstream chunks available
        if let Ok(Some(Some(client_chunk))) = decode_chunk(&mut conn.read_buf) {
            send_stream.reserve_capacity(client_chunk.len());
            let _ = send_stream.send_data(client_chunk, false);
        }
    }

    let _ = send_stream.send_data(Bytes::new(), true);
    velda_http1::wire::send_chunked_end(&mut conn.stream).await?;

    Ok(())
}
