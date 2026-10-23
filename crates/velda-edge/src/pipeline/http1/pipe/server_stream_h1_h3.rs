//! Layer 7 HTTP/1.1 to HTTP/3 Server-Stream Bridge Pipeline.
//!
//! Handles server-streaming RPCs (e.g. Server-Sent Events, LLM token streaming)
//! where downstream HTTP/1.1 requests are forwarded over HTTP/3 / QUIC.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use velda_core::L7Request;
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http3::error::Http3Error;
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a server-streaming HTTP/1.1 downstream request bridged to an HTTP/3 QUIC backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut req_head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http3Upstream>,
    meta: &DownstreamMeta,
    plan: &velda_plugin::RoutePluginPlan,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 1. Enrich proxy forwarding headers
    velda_http1::server::header::enrich_headers(
        &mut req_head.headers,
        &req_head.uri,
        meta.peer,
        meta.local_addr,
        meta.is_tls,
    );

    // 2. Handle Expect: 100-continue unblocking
    if req_head.is_expect_100_continue() && framing != Http1BodyFraming::Empty {
        if let Err(e) = conn.send_100_continue().await {
            tracing::debug!(error = %e, "Failed to send 100 Continue downstream");
            return Err(e);
        }
        req_head.headers.remove(http::header::EXPECT);
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

    let req_method = req_head.method.clone();

    // 4. Sanitize connection-specific headers forbidden by RFC 9114 §4.2 before H3 encoding
    sanitize_headers(&mut req_head.headers);
    let req_id = req_head
        .headers
        .get(&crate::pipeline::X_REQUEST_ID)
        .cloned();

    // 5. Construct canonical L7Request
    let l7_req = L7Request::new(
        req_head.method,
        req_head.uri,
        http::Version::HTTP_3,
        req_head.headers,
        body,
    );

    let h3_config = Http3Config::auto();

    // 6. Acquire pooled HTTP/3 client
    let (client, _lease) = match upstream.acquire(&h3_config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 client"
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

    // 6. Forward request over HTTP/3 stream and get progressive streaming response
    let mut stream_res = client.send_stream_request(l7_req.clone()).await;
    if let Err(Http3Error::ConnectionClosed) = stream_res {
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection"
        );
        if let Ok((fresh_client, _fresh_lease)) = upstream.acquire_fresh(&h3_config).await {
            stream_res = fresh_client.send_stream_request(l7_req.clone()).await;
        }
    }

    let mut h3_resp = match stream_res {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream server-stream pipe failed"
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

    if let Some(ref req_id) = req_id {
        h3_resp
            .headers
            .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
    }

    // Fast-path guard: Bypasses post-response plugin chain if route has no hooks.
    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = velda_core::L7Response::new(
            h3_resp.status,
            http::Version::HTTP_11,
            std::mem::take(&mut h3_resp.headers),
            velda_core::Body::Empty,
        );
        velda_plugin::run_post_response_plugins(post_hooks, &l7_req.headers, &mut l7_resp);
        h3_resp.status = l7_resp.status;
        h3_resp.headers = l7_resp.headers.into();
    }
    meta.inject_response_headers(&mut h3_resp.headers);

    // 7. Check if response has no body (RFC 9110: HEAD, 1xx, 204, 304)
    let is_head = req_method == http::Method::HEAD;
    let is_no_body = h3_resp.status.is_informational()
        || h3_resp.status == StatusCode::NO_CONTENT
        || h3_resp.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body {
        let server_resp = Http1ServerResponse::new(
            h3_resp.status,
            http::Version::HTTP_11,
            h3_resp.headers,
            velda_core::Body::Empty,
        );
        conn.send_response(&server_resp).await?;
        return Ok(());
    }

    // 8. Immediately send chunked response head downstream!
    let resp_head = velda_http1::server::response::Http1ServerResponseHead::new(
        h3_resp.status,
        http::Version::HTTP_11,
        std::mem::take(&mut h3_resp.headers),
    );
    conn.send_response_head_chunked(&resp_head).await?;

    // 9. Stream response chunks downstream with batch coalescing and client disconnect propagation
    let mut chunk_batch: [bytes::Bytes; 16] = Default::default();

    loop {
        let first_chunk = match h3_resp.next_chunk().await {
            Some(Ok(c)) => c,
            Some(Err(e)) => {
                tracing::debug!(error = %e, "Error reading HTTP/3 response stream chunk");
                return Err(Http1Error::ConnectionClosed);
            }
            None => break,
        };

        chunk_batch[0] = first_chunk;
        let mut count = 1;
        let mut batch_bytes = chunk_batch[0].len();
        let mut reached_eof = false;

        // Drain up to 16 ready chunks already in the channel
        while count < 16 && batch_bytes < 131_072 {
            match h3_resp.chunk_rx.try_recv() {
                Ok(Ok(next_chunk)) => {
                    batch_bytes += next_chunk.len();
                    chunk_batch[count] = next_chunk;
                    count += 1;
                }
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "Error reading opportunistic HTTP/3 chunk");
                    return Err(Http1Error::ConnectionClosed);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    reached_eof = true;
                    break;
                }
            }
        }

        let send_res = if count == 1 {
            velda_http1::wire::send_chunk(&mut conn.stream, &chunk_batch[0]).await
        } else {
            velda_http1::wire::send_coalesced_chunks(&mut conn.stream, &chunk_batch[..count]).await
        };

        if let Err(e) = send_res {
            tracing::debug!(
                error = %e,
                "Downstream client disconnected during HTTP/3 server response streaming"
            );
            return Err(Http1Error::ConnectionClosed);
        }

        if reached_eof {
            break;
        }
    }

    // 10. Conclude chunked stream downstream
    velda_http1::wire::send_chunked_end(&mut conn.stream).await?;
    conn.stream.flush().await?;
    conn.compact_buffers();
    Ok(())
}
