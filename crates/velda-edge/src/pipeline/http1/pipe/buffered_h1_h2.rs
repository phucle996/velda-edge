//! Layer 7 HTTP/1.1 to HTTP/2 Buffered Bridge Pipeline.
//!
//! Standard request-response REST API bridge where downstream HTTP/1.1 request body
//! is completely buffered into RAM, then forwarded over an upstream multiplexed HTTP/2
//! connection (RFC 9113) without Head-of-Line blocking.

use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http2::Http2Config;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a buffered HTTP/1.1 downstream request bridged to an HTTP/2 upstream multiplexed backend.
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

    // 6. Forward buffered request over HTTP/2 stream with self-healing retry
    let mut pipe_res = send_h2_buffered(&mut client, &head, &body_bytes).await;
    if let Err(ref e) = pipe_res
        && is_h2_refused_or_goaway(e)
    {
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/2 stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((mut fresh_client, _fresh_lease)) = upstream.acquire_fresh(&h2_config).await {
            pipe_res = send_h2_buffered(&mut fresh_client, &head, &body_bytes).await;
        }
    }

    match pipe_res {
        Ok(resp) => {
            conn.send_response(&resp).await?;
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/2 upstream pipe failed"
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
            Err(Http1Error::ConnectionClosed)
        }
    }
}

async fn send_h2_buffered(
    client: &mut h2::client::SendRequest<bytes::Bytes>,
    head: &Http1ServerRequestHead,
    body: &bytes::Bytes,
) -> Result<Http1ServerResponse, h2::Error> {
    let mut req = http::Request::builder()
        .method(head.method.clone())
        .uri(head.uri.clone())
        .version(http::Version::HTTP_2);
    *req.headers_mut().unwrap() = head.headers.clone();
    let req = req.body(()).unwrap();

    let has_body = !body.is_empty();
    let (response_future, mut send_stream) = client.send_request(req, !has_body)?;
    if has_body {
        send_stream.send_data(body.clone(), true)?;
    }

    let response = response_future.await?;
    let (parts, mut body_stream) = response.into_parts();

    let mut resp_body = BytesMut::new();
    while let Some(chunk) = body_stream.data().await {
        let chunk = chunk?;
        body_stream.flow_control().release_capacity(chunk.len())?;
        resp_body.extend_from_slice(&chunk);
    }

    Ok(Http1ServerResponse::new(
        parts.status,
        http::Version::HTTP_11,
        parts.headers,
        velda_core::Body::Bytes(resp_body.freeze()),
    ))
}

#[inline]
fn is_h2_refused_or_goaway(err: &h2::Error) -> bool {
    (err.is_reset() && err.reason() == Some(h2::Reason::REFUSED_STREAM)) || err.is_go_away()
}
