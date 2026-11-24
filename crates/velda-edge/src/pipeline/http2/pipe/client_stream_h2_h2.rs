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
use velda_http2::config::Http2Config;
use velda_http2::error::Http2Error;
use velda_http2::server::request::{Http2ServerRequestHead, Http2StreamReceiver};
use velda_http2::server::response::Http2Responder;

use crate::pipeline::{DownstreamMeta, PipeOutcome};
use crate::upstream::Http2Upstream;

/// Serves a client-streaming HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
pub async fn serve(
    req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Http2Config,
    upstream: &Arc<Http2Upstream>,
    plan: &velda_plugin::RoutePluginPlan,
) -> PipeOutcome {
    // 1. Fail-fast: Acquire upstream multiplexed connection before streaming
    let (client, lease) = match upstream.acquire(&config).await {
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
            return PipeOutcome::http(
                responder.last_status,
                receiver.bytes_received as u64,
                responder.last_bytes_tx,
                None,
                velda_core::CompletionReason::UpstreamError,
            );
        }
    };
    let upstream_addr = Some(lease.endpoint);

    // 3. Pipe client streaming frames directly
    let pipe_res = async {
        let is_head = req_head.method == http::Method::HEAD;
        let (response_fut, mut send_stream) = client
            .send_request(
                req_head.method.clone(),
                req_head.uri.clone(),
                req_head.headers.clone(),
                false,
            )
            .await?;

        let coalesce_limit =
            (config.initial_stream_window_size as usize / 2).clamp(16_384, 131_072);

        let mut response_fut = std::pin::pin!(response_fut);
        let mut early_response = None;
        let mut sent_end_stream = false;

        'upload: while !receiver.is_end_stream() {
            let chunk_opt = tokio::select! {
                resp = &mut response_fut => {
                    early_response = Some(resp?);
                    break 'upload;
                }
                res = receiver.recv_coalesced_chunk(coalesce_limit) => res?,
            };

            let Some(chunk) = chunk_opt else {
                break 'upload;
            };
            if chunk.is_empty() {
                if receiver.is_end_stream() {
                    break 'upload;
                }
                continue;
            }

            let is_last = receiver.is_end_stream();
            tokio::select! {
                resp = &mut response_fut => {
                    early_response = Some(resp?);
                    break 'upload;
                }
                res = send_stream.send_chunk(chunk, is_last) => {
                    res?;
                }
            }
            if is_last {
                sent_end_stream = true;
                break 'upload;
            }
        }

        let mut response = match early_response {
            Some(resp) => resp,
            None => {
                if !sent_end_stream {
                    let _ = send_stream.send_data(Bytes::new(), true);
                }
                response_fut.await?
            }
        };

        if let Some(content_type) = response.headers.get(http::header::CONTENT_TYPE)
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

        let is_no_body_status = response.status.is_informational()
            || response.status == StatusCode::NO_CONTENT
            || response.status == StatusCode::NOT_MODIFIED;

        let resp_body = if is_head || is_no_body_status || response.body.is_end_stream() {
            Body::Empty
        } else if let Some(first_chunk) = response.body.recv_chunk().await? {
            let len = first_chunk.len();
            if len > config.max_body_size {
                return Err(Http2Error::PayloadTooLarge(len));
            }

            if response.body.is_end_stream() {
                Body::Bytes(first_chunk)
            } else {
                if let Some(content_length) = response
                    .headers
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<usize>().ok())
                    && content_length > config.max_body_size
                {
                    return Err(Http2Error::PayloadTooLarge(content_length));
                }

                let initial_cap = response
                    .headers
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or_else(|| (len.saturating_mul(2)).clamp(16_384, config.max_body_size))
                    .min(config.max_body_size);
                let mut body_buf = BytesMut::with_capacity(initial_cap);
                body_buf.extend_from_slice(&first_chunk);

                while let Some(chunk_data) = response.body.recv_chunk().await? {
                    if body_buf.len() + chunk_data.len() > config.max_body_size {
                        return Err(Http2Error::PayloadTooLarge(
                            body_buf.len() + chunk_data.len(),
                        ));
                    }
                    body_buf.extend_from_slice(&chunk_data);
                }

                Body::Bytes(body_buf.freeze())
            }
        } else {
            Body::Empty
        };

        let mut resp = L7Response::new(
            response.status,
            Version::HTTP_2,
            response.headers,
            resp_body,
        );
        if let Some(post_hooks) = &plan.post_response_hooks {
            velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut resp);
        }
        meta.inject_response_headers(&mut resp.headers);
        responder.send_response(&resp)?;
        let rx_bytes = receiver.bytes_received as u64;
        if !receiver.is_end_stream() {
            tokio::spawn(async move { while let Ok(Some(_)) = receiver.recv_chunk().await {} });
        }
        Ok::<u64, Http2Error>(rx_bytes)
    }
    .await;

    let (bytes_rx, completion) = match pipe_res {
        Ok(rx) => (rx, velda_core::CompletionReason::Complete),
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/2 upstream client-stream pipe failed"
            );
            if responder.last_status == 0 {
                let mut err_resp = L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                )
                .with_header(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                meta.inject_response_headers(&mut err_resp.headers);
                let _ = responder.send_response(&err_resp);
            }
            (0, velda_core::CompletionReason::UpstreamError)
        }
    };
    PipeOutcome::http(
        responder.last_status,
        bytes_rx,
        responder.last_bytes_tx,
        upstream_addr,
        completion,
    )
}
