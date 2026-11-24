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

use bytes::BytesMut;
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};
use velda_http2::client::connection::Http2Client;
use velda_http2::config::Http2Config;
use velda_http2::error::Http2Error;
use velda_http2::server::request::{Http2ServerRequestHead, Http2StreamReceiver};
use velda_http2::server::response::Http2Responder;

use crate::pipeline::{DownstreamMeta, PipeOutcome};
use crate::upstream::Http2Upstream;

/// Forward a buffered request over an active H2 client stream and send response downstream.
async fn forward_buffered(
    req_head: &Http2ServerRequestHead,
    body: &Body,
    responder: &mut Http2Responder,
    client: &Http2Client,
    meta: &DownstreamMeta,
    config: &Http2Config,
    plan: &velda_plugin::RoutePluginPlan,
) -> Result<(), Http2Error> {
    let is_head = req_head.method == http::Method::HEAD;
    let has_body = !body.is_empty();
    let (response_fut, mut send_stream) = client
        .send_request(
            req_head.method.clone(),
            req_head.uri.clone(),
            req_head.headers.clone(),
            !has_body,
        )
        .await?;
    if has_body {
        send_stream.send_data(body.to_bytes(), true)?;
    }

    let mut response = response_fut.await?;

    // Enforce non-streaming invariant: reject SSE if listener has server streaming disabled
    if let Some(content_type) = response.headers.get(http::header::CONTENT_TYPE)
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
    responder.send_response_owned(resp)?;

    Ok(())
}

/// Serves a buffered HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
pub async fn serve(
    req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Http2Config,
    upstream: &Arc<Http2Upstream>,
    plan: &velda_plugin::RoutePluginPlan,
) -> PipeOutcome {
    // 1. Consume downstream body
    let body = match receiver.consume_all_into_slab(Some(&meta.slab_pool)).await {
        Ok(b) => b,
        Err(e) => {
            let mut err_resp = L7Response::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            meta.inject_response_headers(&mut err_resp.headers);
            let _ = responder.send_response(&err_resp);
            return PipeOutcome::http(
                responder.last_status,
                receiver.bytes_received as u64,
                responder.last_bytes_tx,
                None,
                velda_core::CompletionReason::Rejected,
            );
        }
    };
    let bytes_rx = receiver.bytes_received as u64;

    // 2. Acquire upstream multiplexed connection
    let (client, lease) = match upstream.acquire(&config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection"
            );
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
            return PipeOutcome::http(
                responder.last_status,
                bytes_rx,
                responder.last_bytes_tx,
                None,
                velda_core::CompletionReason::UpstreamError,
            );
        }
    };
    let mut upstream_addr = Some(lease.endpoint);

    // 4. Pipe buffered request & response with automatic self-healing on connection reuse failure
    let mut res = forward_buffered(
        &req_head,
        &body,
        &mut responder,
        &client,
        &meta,
        &config,
        plan,
    )
    .await;
    if let Err(ref e) = res
        && e.is_refused_or_goaway()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "HTTP/2 buffered stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((fresh_client, fresh_lease)) = upstream.acquire_fresh(&config).await {
            upstream_addr = Some(fresh_lease.endpoint);
            res = forward_buffered(
                &req_head,
                &body,
                &mut responder,
                &fresh_client,
                &meta,
                &config,
                plan,
            )
            .await;
        }
    }

    let completion = match res {
        Ok(()) => velda_core::CompletionReason::Complete,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/2 upstream pipe failed"
            );
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
            velda_core::CompletionReason::UpstreamError
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
