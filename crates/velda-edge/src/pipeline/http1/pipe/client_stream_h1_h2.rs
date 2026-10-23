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
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse, ProgressiveChunkDecoder,
};
use velda_http2::Http2Config;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a client-streaming HTTP/1.1 downstream request bridged to an HTTP/2 backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut req_head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http2Upstream>,
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

    // 3. Sanitize headers disallowed by RFC 9113 for HTTP/2
    velda_http2::client::sanitize_h2_headers(&mut req_head.headers);

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
    let scheme = if upstream.is_tls() { "https" } else { "http" };
    let authority = req_head
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| req_head.uri.host())
        .unwrap_or(upstream.target_host());
    let path_and_query = req_head
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let uri = http::Uri::builder()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(path_and_query)
        .build()
        .unwrap_or_else(|_| req_head.uri.clone());

    let mut req = http::Request::builder()
        .method(req_head.method.clone())
        .uri(uri)
        .version(http::Version::HTTP_2);
    *req.headers_mut().unwrap() = req_head.headers.clone();
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
        Http1BodyFraming::Chunked => {
            let mut decoder = ProgressiveChunkDecoder::new();
            const TARGET_RESERVE: usize = 65536;
            loop {
                while let Some(chunk) = decoder.decode_step(&mut conn.read_buf)? {
                    send_stream.reserve_capacity(chunk.len());
                    send_stream
                        .send_data(chunk, false)
                        .map_err(|_| Http1Error::ConnectionClosed)?;
                }

                if decoder.is_done() {
                    break;
                }

                use tokio::io::AsyncReadExt;
                if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < TARGET_RESERVE {
                    conn.read_buf.reserve(TARGET_RESERVE);
                }
                let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                if n == 0 {
                    if decoder.is_done() {
                        break;
                    } else {
                        return Err(Http1Error::Parse(
                            "Unexpected EOF while reading chunked body".into(),
                        ));
                    }
                }
            }
        }
        Http1BodyFraming::ContentLength(len) => {
            let mut remaining = len;
            while remaining > 0 {
                let available = conn.read_buf.len();
                if available > 0 {
                    let chunk_len = available.min(remaining);
                    let chunk = conn.read_buf.split_to(chunk_len).freeze();
                    remaining = remaining.saturating_sub(chunk_len);
                    send_stream.reserve_capacity(chunk_len);
                    send_stream
                        .send_data(chunk, false)
                        .map_err(|_| Http1Error::ConnectionClosed)?;
                    continue;
                }
                let to_read = remaining.min(65536);
                if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < to_read {
                    conn.read_buf.reserve(to_read);
                }
                use tokio::io::AsyncReadExt;
                let n = conn.stream.read_buf(&mut conn.read_buf).await?;
                if n == 0 {
                    break;
                }
                let chunk_len = n.min(remaining);
                let chunk = conn.read_buf.split_to(chunk_len).freeze();
                remaining = remaining.saturating_sub(chunk_len);
                send_stream.reserve_capacity(chunk_len);
                send_stream
                    .send_data(chunk, false)
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

    let max_body_size = conn.config().max_body_size;
    let expected_len = parts
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| velda_http1::wire::parse_ascii_digits(v.as_bytes()).ok())
        .unwrap_or(0);
    if expected_len > max_body_size {
        let err_resp = Http1ServerResponse::from_bytes(
            StatusCode::PAYLOAD_TOO_LARGE,
            b"413 Payload Too Large\n".to_vec(),
        );
        conn.mark_close();
        conn.send_response(&err_resp).await?;
        return Ok(());
    }
    let mut resp_body = BytesMut::with_capacity(expected_len.min(max_body_size));
    while let Some(chunk) = body_stream.data().await {
        let chunk = chunk.map_err(|_| Http1Error::ConnectionClosed)?;
        if resp_body.len() + chunk.len() > max_body_size {
            let _ = body_stream.flow_control().release_capacity(chunk.len());
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::PAYLOAD_TOO_LARGE,
                b"413 Payload Too Large\n".to_vec(),
            );
            conn.mark_close();
            conn.send_response(&err_resp).await?;
            return Ok(());
        }
        let _ = body_stream.flow_control().release_capacity(chunk.len());
        resp_body.extend_from_slice(&chunk);
    }

    let mut headers = parts.headers;
    if let Some(req_id) = req_head.headers.get(&crate::pipeline::X_REQUEST_ID) {
        headers.insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
    }
    let mut status = parts.status;
    let frozen_body = resp_body.freeze();
    let initial_body = if plan.is_response_payload_passthrough() {
        velda_core::Body::Empty
    } else {
        velda_core::Body::Bytes(frozen_body.clone())
    };
    let mut final_body = velda_core::Body::Bytes(frozen_body);

    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = velda_core::L7Response::new(
            status,
            http::Version::HTTP_11,
            std::mem::take(&mut headers),
            initial_body,
        );
        velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut l7_resp);
        status = l7_resp.status;
        headers = l7_resp.headers.into();
        if !matches!(l7_resp.body, velda_core::Body::Empty) {
            final_body = l7_resp.body;
        }
    }
    meta.inject_response_headers(&mut headers);
    let server_resp = Http1ServerResponse::new(status, http::Version::HTTP_11, headers, final_body);
    conn.send_response(&server_resp).await?;

    Ok(())
}
