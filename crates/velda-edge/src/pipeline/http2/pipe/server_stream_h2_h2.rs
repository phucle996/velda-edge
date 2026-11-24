//! Layer 7 HTTP/2 to HTTP/2 Server-Stream Pipeline.
//!
//! Designed for Server-Sent Events (SSE), LLM token streaming, and chunked push,
//! streaming upstream response frames back to downstream as they arrive.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http2`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

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

/// Forward a request over active H2 client and stream response chunks downstream.
async fn forward_server_stream(
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
    let mut resp_ws = response.headers;
    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = L7Response::new(response.status, Version::HTTP_2, resp_ws, Body::Empty);
        velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut l7_resp);
        response.status = l7_resp.status;
        resp_ws = l7_resp.headers;
    }
    meta.inject_response_headers(&mut resp_ws);

    let is_no_body_status = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body_status {
        let resp = L7Response::new(response.status, Version::HTTP_2, resp_ws, Body::Empty);
        responder.send_response(&resp)?;
        return Ok(());
    }

    let mut sender = responder.send_stream_response(response.status, &resp_ws)?;

    let mut total_bytes = 0;
    let coalesce_limit = (config.initial_stream_window_size as usize / 8).clamp(16_384, 65_536);
    let zero_copy_threshold = (coalesce_limit / 4).clamp(8_192, 16_384);
    let mut pending_buf = bytes::BytesMut::with_capacity(coalesce_limit);

    while let Some(data) = response.body.recv_chunk().await? {
        let len = data.len();
        total_bytes += len;

        if total_bytes > config.max_body_size {
            return Err(Http2Error::PayloadTooLarge(total_bytes));
        }

        if pending_buf.is_empty() && len >= zero_copy_threshold {
            if sender.send_chunk(data).await.is_err() {
                tracing::debug!("Downstream client disconnected during H2 response streaming");
                responder.last_bytes_tx = sender.bytes_sent;
                return Ok(());
            }
        } else {
            pending_buf.extend_from_slice(&data);
            if pending_buf.len() >= coalesce_limit {
                let to_send = pending_buf.split().freeze();
                if sender.send_chunk(to_send).await.is_err() {
                    tracing::debug!("Downstream client disconnected during H2 response streaming");
                    responder.last_bytes_tx = sender.bytes_sent;
                    return Ok(());
                }
            }
        }
    }

    if !pending_buf.is_empty() {
        let to_send = pending_buf.freeze();
        if sender.send_chunk(to_send).await.is_err() {
            tracing::debug!("Downstream client disconnected during H2 response streaming tail");
            responder.last_bytes_tx = sender.bytes_sent;
            return Ok(());
        }
    }

    responder.last_bytes_tx = sender.bytes_sent;
    let _ = sender.finish();
    Ok(())
}

/// Serves a server-streaming HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
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
    let body = match receiver.consume_all().await {
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

    // 4. Pipe server-streaming response with automatic self-healing retry
    let mut res = forward_server_stream(
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
            "HTTP/2 server-stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((fresh_client, fresh_lease)) = upstream.acquire_fresh(&config).await {
            upstream_addr = Some(fresh_lease.endpoint);
            res = forward_server_stream(
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
