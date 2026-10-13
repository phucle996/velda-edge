//! Layer 7 HTTP/2 to HTTP/2 Client-Stream Pipeline.
//!
//! Handles client-streaming uploads (large file uploads, chunked ingest) where downstream
//! request body DATA frames are continuously streamed to upstream HTTP/2 connection.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http2`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};
use velda_http2::Http2Error;
use velda_http2::config::Http2Config;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a client-streaming HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Arc<Http2Config>,
    upstream: &Arc<Http2Upstream>,
) {
    // 1. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 2. Fail-fast: Acquire upstream multiplexed connection before streaming
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
            if !receiver.is_end_stream() {
                let _ = responder.send_response_and_cancel_upload(&err_resp);
            } else {
                let _ = responder.send_response(&err_resp);
            }
            return;
        }
    };

    // 3. Pipe client streaming frames directly
    let pipe_res = async {
        let is_head = head.method == http::Method::HEAD;
        velda_http2::headers::sanitize_h2_headers(&mut head.headers);
        let http_req = head.into_http_request();
        let (response_fut, mut send_stream) = client.send_request(http_req, false)?;

        let upload_reserve =
            (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);
        let coalesce_limit = (upload_reserve / 2).clamp(16_384, 131_072);
        let low_watermark = (upload_reserve / 4).clamp(8_192, 65_536);
        send_stream.reserve_capacity(upload_reserve);

        let mut response_fut = std::pin::pin!(response_fut);
        let mut early_response = None;

        'upload: while let Some(mut chunk) = receiver.recv_coalesced_chunk(coalesce_limit).await? {
            while !chunk.is_empty() {
                let available = send_stream.capacity();
                if available == 0 {
                    send_stream.reserve_capacity(chunk.len().max(upload_reserve));
                    tokio::select! {
                        resp = &mut response_fut => {
                            early_response = Some(resp?);
                            break 'upload;
                        }
                        res = std::future::poll_fn(|cx| send_stream.poll_capacity(cx)) => {
                            match res {
                                Some(Ok(_)) => {}
                                Some(Err(err)) => return Err(err.into()),
                                None => break 'upload,
                            }
                        }
                    }
                    if send_stream.capacity() == 0 && early_response.is_none() {
                        break 'upload;
                    }
                    continue;
                }

                let to_send = chunk.len().min(available);
                let slice = chunk.split_to(to_send);
                send_stream.send_data(slice, false)?;

                if send_stream.capacity() < low_watermark {
                    send_stream.reserve_capacity(upload_reserve);
                }
            }
        }

        let response = match early_response {
            Some(resp) => resp,
            None => {
                let _ = send_stream.send_data(Bytes::new(), true);
                response_fut.await?
            }
        };
        let (parts, mut body_stream) = response.into_parts();

        if let Some(content_type) = parts.headers.get(http::header::CONTENT_TYPE)
            && let Ok(ct_str) = content_type.to_str()
            && ct_str
                .as_bytes()
                .windows(b"text/event-stream".len())
                .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
        {
            return Err(Http2Error::StreamingViolation(
                "Upstream returned text/event-stream (SSE) on client-only streaming route".into(),
            ));
        }

        let is_no_body_status = parts.status.is_informational()
            || parts.status == StatusCode::NO_CONTENT
            || parts.status == StatusCode::NOT_MODIFIED;

        let batch_threshold =
            (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);

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
                if let Some(content_length) = parts
                    .headers
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<usize>().ok())
                    && content_length > config.max_body_size
                {
                    let _ = body_stream.flow_control().release_capacity(len);
                    return Err(Http2Error::PayloadTooLarge(content_length));
                }

                let initial_cap = parts
                    .headers
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or_else(|| (len.saturating_mul(2)).clamp(16_384, config.max_body_size))
                    .min(config.max_body_size);
                let mut body_buf = BytesMut::with_capacity(initial_cap);
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

                    let waker = std::task::Waker::noop();
                    let mut cx = std::task::Context::from_waker(waker);
                    while let std::task::Poll::Ready(Some(Ok(extra))) =
                        body_stream.poll_data(&mut cx)
                    {
                        let extra_len = extra.len();
                        if body_buf.len() + extra_len > config.max_body_size {
                            let _ = body_stream
                                .flow_control()
                                .release_capacity(unreleased_bytes + extra_len);
                            return Err(Http2Error::PayloadTooLarge(body_buf.len() + extra_len));
                        }
                        body_buf.extend_from_slice(&extra);
                        unreleased_bytes += extra_len;
                    }

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
        Ok::<(), Http2Error>(())
    }
    .await;

    if let Err(e) = pipe_res {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "HTTP/2 upstream client-stream pipe failed"
        );
    }
}
