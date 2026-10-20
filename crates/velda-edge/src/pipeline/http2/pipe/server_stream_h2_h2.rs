//! Layer 7 HTTP/2 to HTTP/2 Server-Stream Pipeline.
//!
//! Designed for Server-Sent Events (SSE), LLM token streaming, and chunked push,
//! streaming upstream response frames back to downstream as they arrive.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http2`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use bytes::Bytes;
use h2::client::SendRequest;
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};
use velda_http2::Http2Error;
use velda_http2::config::Http2Config;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Forward a request over active H2 client and stream response chunks downstream.
async fn forward_server_stream(
    req_head: &Http2ServerRequestHead,
    body: &Body,
    responder: &mut Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
    plan: &velda_plugin::RoutePluginPlan,
) -> Result<(), Http2Error> {
    let is_head = req_head.method == http::Method::HEAD;
    let mut http_req = http::Request::new(());
    *http_req.method_mut() = req_head.method.clone();
    *http_req.uri_mut() = req_head.uri.clone();
    *http_req.version_mut() = Version::HTTP_2;
    *http_req.headers_mut() = req_head.headers.clone();

    let has_body = !body.is_empty();
    let (response_fut, mut send_stream) = client.send_request(http_req, !has_body)?;
    if let Body::Bytes(b) = body
        && !b.is_empty()
    {
        send_stream.send_data(b.clone(), true)?;
    }

    let response = response_fut.await?;
    let (mut parts, mut body_stream) = response.into_parts();

    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp =
            L7Response::new(parts.status, Version::HTTP_2, parts.headers, Body::Empty);
        velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut l7_resp);
        parts.status = l7_resp.status;
        parts.headers = l7_resp.headers;
    }

    let is_no_body_status = parts.status.is_informational()
        || parts.status == StatusCode::NO_CONTENT
        || parts.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body_status {
        let resp = L7Response::new(parts.status, Version::HTTP_2, parts.headers, Body::Empty);
        responder.send_response(&resp)?;
        return Ok(());
    }

    let mut sender = responder.send_stream_response(parts.status, &parts.headers)?;

    let batch_threshold = (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);
    let mut total_bytes = 0;
    let mut unreleased_bytes = 0;
    let coalesce_limit = (config.initial_stream_window_size as usize / 8).clamp(16_384, 65_536);
    let zero_copy_threshold = (coalesce_limit / 4).clamp(8_192, 16_384);
    let mut pending_buf = bytes::BytesMut::with_capacity(coalesce_limit);

    while let Some(chunk) = body_stream.data().await {
        let data = chunk?;
        let len = data.len();
        total_bytes += len;
        unreleased_bytes += len;

        if total_bytes > config.max_body_size {
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
            return Err(Http2Error::PayloadTooLarge(total_bytes));
        }

        if pending_buf.is_empty() && len >= zero_copy_threshold {
            if sender.send_chunk(data).await.is_err() {
                tracing::debug!("Downstream client disconnected during H2 response streaming");
                let _ = body_stream
                    .flow_control()
                    .release_capacity(unreleased_bytes);
                return Ok(());
            }
        } else {
            pending_buf.extend_from_slice(&data);
            if pending_buf.len() >= coalesce_limit {
                let to_send = pending_buf.split().freeze();
                if sender.send_chunk(to_send).await.is_err() {
                    tracing::debug!("Downstream client disconnected during H2 response streaming");
                    let _ = body_stream
                        .flow_control()
                        .release_capacity(unreleased_bytes);
                    return Ok(());
                }
            }
        }

        if unreleased_bytes >= batch_threshold {
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
            unreleased_bytes = 0;
        }
    }

    if !pending_buf.is_empty() {
        let to_send = pending_buf.freeze();
        if sender.send_chunk(to_send).await.is_err() {
            tracing::debug!("Downstream client disconnected during H2 response streaming tail");
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
            return Ok(());
        }
    }

    if unreleased_bytes > 0 {
        let _ = body_stream
            .flow_control()
            .release_capacity(unreleased_bytes);
    }

    let _ = sender.finish();
    Ok(())
}

/// Serves a server-streaming HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Http2Config,
    upstream: &Arc<Http2Upstream>,
    plan: &velda_plugin::RoutePluginPlan,
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
        &mut req_head.headers,
        &req_head.uri,
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

    // 4. Pipe server-streaming response with automatic self-healing retry
    let mut res =
        forward_server_stream(&req_head, &body, &mut responder, &mut client, &config, plan).await;
    if let Err(ref e) = res
        && e.is_refused_or_goaway()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "HTTP/2 server-stream refused or connection closed; self-healing with fresh connection"
        );
        if let Ok((mut fresh_client, _fresh_lease)) = upstream.acquire_fresh(&config).await {
            res = forward_server_stream(
                &req_head,
                &body,
                &mut responder,
                &mut fresh_client,
                &config,
                plan,
            )
            .await;
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
