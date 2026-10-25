//! Layer 7 HTTP/2 to HTTP/2 Duplex Bidirectional Pipeline.
//!
//! Handles full-duplex bidirectional streaming (e.g. gRPC bidi, real-time sync)
//! where downstream and upstream continuously exchange frames in both directions.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http2`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};
use velda_http2::Http2Error;
use velda_http2::config::Http2Config;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http2Upstream;

/// Serves a full-duplex bidirectional HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Http2Config,
    upstream: &Arc<Http2Upstream>,
    plan: &velda_plugin::RoutePluginPlan,
) {
    // 1. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut req_head.headers,
        &req_head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 2. Fail-fast: Acquire upstream multiplexed connection before duplex loop
    let (mut client, _lease) = match upstream.acquire(&config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection"
            );
            let mut responder = responder;
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

    // 3. Pipe duplex frames concurrently
    let duplex_res = async {
        let is_head = req_head.method == http::Method::HEAD;
        // Fast-path guard: Only snapshot request headers into a HeaderWorkspace if post-response
        // hooks are configured on this route. HeaderMap::clone() clones atomic pointer handles
        // (Bytes) in O(1) without copying string payloads.
        let req_headers_ref = if plan.post_response_hooks.is_some() {
            Some(velda_core::HeaderWorkspace::from_headers(
                req_head.headers.clone(),
            ))
        } else {
            None
        };
        velda_http2::headers::sanitize_h2_headers(&mut req_head.headers);
        let http_req = req_head.into_http_request();
        let is_empty_upload = receiver.is_end_stream();
        let (response_fut, mut send_stream) = client.send_request(http_req, is_empty_upload)?;

        let upload_coalesce = 65_536;

        let upload_task = async move {
            if is_empty_upload {
                return Ok::<(), Http2Error>(());
            }
            let mut sent_end_stream = false;
            while !receiver.is_end_stream() {
                let Some(mut chunk) = receiver.recv_coalesced_chunk(upload_coalesce).await? else {
                    break;
                };
                if chunk.is_empty() {
                    if receiver.is_end_stream() {
                        break;
                    }
                    continue;
                }
                let avail = send_stream.capacity();
                if avail < chunk.len() {
                    send_stream.reserve_capacity(chunk.len() - avail);
                }
                while !chunk.is_empty() {
                    let available = send_stream.capacity();
                    if available == 0 {
                        match std::future::poll_fn(|cx| send_stream.poll_capacity(cx)).await {
                            Some(Ok(_)) => {}
                            Some(Err(err)) => return Err(err.into()),
                            None => return Ok(()),
                        }
                        if send_stream.capacity() == 0 {
                            return Ok(());
                        }
                        continue;
                    }

                    let to_send = chunk.len().min(available);
                    let is_last = receiver.is_end_stream() && to_send == chunk.len();
                    let slice = chunk.split_to(to_send);
                    if send_stream.send_data(slice, is_last).is_err() {
                        return Ok(());
                    }
                    if is_last {
                        sent_end_stream = true;
                        break;
                    }
                }
                if sent_end_stream {
                    break;
                }
            }
            if !sent_end_stream {
                let _ = send_stream.send_data(Bytes::new(), true);
            }
            Ok::<(), Http2Error>(())
        };

        let config_max_body = config.max_body_size;
        let mut responder = responder;
        let download_task = async move {
            let response = response_fut.await?;
            let (mut parts, mut body_stream) = response.into_parts();

            if let Some(post_hooks) = &plan.post_response_hooks
                && let Some(ref req_headers) = req_headers_ref
            {
                let mut l7_resp =
                    L7Response::new(parts.status, Version::HTTP_2, parts.headers, Body::Empty);
                velda_plugin::run_post_response_plugins(post_hooks, req_headers, &mut l7_resp);
                parts.status = l7_resp.status;
                parts.headers = l7_resp.headers.into();
            }
            meta.inject_response_headers(&mut parts.headers);

            let is_no_body_status = parts.status.is_informational()
                || parts.status == StatusCode::NO_CONTENT
                || parts.status == StatusCode::NOT_MODIFIED;

            if is_head || is_no_body_status {
                let resp =
                    L7Response::new(parts.status, Version::HTTP_2, parts.headers, Body::Empty);
                responder.send_response_and_cancel_upload(&resp)?;
                return Ok(());
            }

            let mut sender = responder.send_stream_response(parts.status, &parts.headers)?;

            let batch_threshold = 65_536;
            let mut total_bytes = 0;
            let mut unreleased_bytes = 0;

            while let Some(chunk) = body_stream.data().await {
                let data = chunk?;
                let len = data.len();
                total_bytes += len;
                unreleased_bytes += len;

                if total_bytes > config_max_body {
                    let _ = body_stream
                        .flow_control()
                        .release_capacity(unreleased_bytes);
                    return Err(Http2Error::PayloadTooLarge(total_bytes));
                }

                if unreleased_bytes >= batch_threshold {
                    let _ = body_stream
                        .flow_control()
                        .release_capacity(unreleased_bytes);
                    unreleased_bytes = 0;
                }

                if !data.is_empty() && sender.send_chunk(data).await.is_err() {
                    tracing::debug!("Downstream client disconnected during H2 duplex streaming");
                    if unreleased_bytes > 0 {
                        let _ = body_stream
                            .flow_control()
                            .release_capacity(unreleased_bytes);
                    }
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
        };

        let (up_res, down_res) = tokio::join!(upload_task, download_task);
        up_res?;
        down_res?;
        Ok::<(), Http2Error>(())
    }
    .await;

    if let Err(e) = duplex_res {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "HTTP/2 upstream duplex pipe failed"
        );
    }
}
