//! Layer 7 HTTP/2 to HTTP/2 Buffered Pipeline.
//!
//! Standard request-response strategy where downstream request body is buffered,
//! sent over an upstream multiplexed HTTP/2 connection, and response is returned
//! as HTTP/2 HEADERS + DATA frames, with automatic retry on GOAWAY / REFUSED_STREAM.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http2`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use h2::client::SendRequest;
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};
use velda_http2::Http2Error;
use velda_http2::config::Http2Config;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Forward a buffered request over an active H2 client stream and send response downstream.
async fn forward_buffered(
    head: &Http2ServerRequestHead,
    body: &Body,
    responder: &mut Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    let is_head = head.method == http::Method::HEAD;
    let mut http_req = http::Request::new(());
    *http_req.method_mut() = head.method.clone();
    *http_req.uri_mut() = head.uri.clone();
    *http_req.version_mut() = Version::HTTP_2;
    *http_req.headers_mut() = head.headers.clone();

    let has_body = !body.is_empty();
    let (response_fut, mut send_stream) = client.send_request(http_req, !has_body)?;
    if let Body::Bytes(b) = body
        && !b.is_empty()
    {
        send_stream.send_data(b.clone(), true)?;
    }

    let response = response_fut.await?;
    let (parts, mut body_stream) = response.into_parts();

    // Enforce non-streaming invariant: reject SSE if listener has server streaming disabled
    if let Some(content_type) = parts.headers.get(http::header::CONTENT_TYPE)
        && let Ok(ct_str) = content_type.to_str()
        && ct_str
            .as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    {
        return Err(Http2Error::StreamingViolation(
            "Upstream returned text/event-stream (SSE), but server streaming is disabled on this listener".into(),
        ));
    }

    let is_no_body_status = parts.status.is_informational()
        || parts.status == StatusCode::NO_CONTENT
        || parts.status == StatusCode::NOT_MODIFIED;

    let batch_threshold = (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);

    let resp_body = if is_head || is_no_body_status || body_stream.is_end_stream() {
        Body::Empty
    } else if let Some(first_chunk) = body_stream.data().await {
        let data = first_chunk?;
        let len = data.len();
        if len > config.max_body_size {
            let _ = body_stream.flow_control().release_capacity(len);
            return Err(Http2Error::PayloadTooLarge(len));
        }

        if body_stream.is_end_stream() {
            let _ = body_stream.flow_control().release_capacity(len);
            Body::Bytes(data)
        } else {
            let mut body_buf = BytesMut::with_capacity(len * 2);
            body_buf.extend_from_slice(&data);
            let mut unreleased_bytes = len;

            while let Some(chunk) = body_stream.data().await {
                let chunk_data = chunk?;
                let chunk_len = chunk_data.len();
                if body_buf.len() + chunk_len > config.max_body_size {
                    let _ = body_stream
                        .flow_control()
                        .release_capacity(unreleased_bytes + chunk_len);
                    return Err(Http2Error::PayloadTooLarge(body_buf.len() + chunk_len));
                }
                body_buf.extend_from_slice(&chunk_data);
                unreleased_bytes += chunk_len;

                if unreleased_bytes >= batch_threshold {
                    let _ = body_stream
                        .flow_control()
                        .release_capacity(unreleased_bytes);
                    unreleased_bytes = 0;
                }
            }

            if unreleased_bytes > 0 {
                let _ = body_stream
                    .flow_control()
                    .release_capacity(unreleased_bytes);
            }

            Body::Bytes(body_buf.freeze())
        }
    } else {
        Body::Empty
    };

    let resp = L7Response::new(parts.status, Version::HTTP_2, parts.headers, resp_body);
    responder.send_response(&resp)?;

    Ok(())
}

/// Serves a buffered HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Arc<Http2Config>,
    upstream: &Arc<Http2Upstream>,
) {
    // 1. Consume downstream body
    let body = match receiver.consume_all().await {
        Ok(b) => b,
        Err(e) => {
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 2. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 3. Acquire upstream multiplexed connection
    let (mut client, _lease) = match upstream.acquire(&config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 4. Pipe buffered request & response with automatic self-healing on connection reuse failure
    let mut res = forward_buffered(&head, &body, &mut responder, &mut client, &config).await;
    if let Err(ref e) = res
        && e.is_refused_or_goaway()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "HTTP/2 buffered stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((mut fresh_client, _fresh_lease)) = upstream.acquire_fresh(&config).await {
            res = forward_buffered(&head, &body, &mut responder, &mut fresh_client, &config).await;
        }
    }

    if let Err(e) = res {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "HTTP/2 upstream pipe failed"
        );
        let err_resp = L7Response::from_bytes(
            StatusCode::BAD_GATEWAY,
            format!("502 Bad Gateway: {e}\n").into_bytes(),
        )
        .with_header(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        let _ = responder.send_response(&err_resp);
    }
}
