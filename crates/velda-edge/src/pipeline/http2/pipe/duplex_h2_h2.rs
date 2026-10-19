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
        velda_http2::headers::sanitize_h2_headers(&mut req_head.headers);
        let http_req = req_head.into_http_request();
        let (response_fut, mut send_stream) = client.send_request(http_req, false)?;

        let upload_reserve = config.initial_stream_window_size as usize;
        let upload_coalesce = (upload_reserve / 2).max(16_384);
        let upload_low_wm = (upload_reserve / 4).max(8_192);

        let upload_task = async move {
            send_stream.reserve_capacity(upload_reserve);

            while let Some(mut chunk) = receiver.recv_coalesced_chunk(upload_coalesce).await? {
                while !chunk.is_empty() {
                    let available = send_stream.capacity();
                    if available == 0 {
                        send_stream.reserve_capacity(chunk.len().max(upload_reserve));
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
                    let slice = chunk.split_to(to_send);
                    if send_stream.send_data(slice, false).is_err() {
                        return Ok(());
                    }

                    if send_stream.capacity() < upload_low_wm {
                        send_stream.reserve_capacity(upload_reserve);
                    }
                }
            }
            let _ = send_stream.send_data(Bytes::new(), true);
            Ok::<(), Http2Error>(())
        };

        let config_max_body = config.max_body_size;
        let mut responder = responder;
        let download_task = async move {
            let response = response_fut.await?;
            let (parts, mut body_stream) = response.into_parts();

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

            let batch_threshold =
                (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);
            let mut total_bytes = 0;
            let mut unreleased_bytes = 0;
            let coalesce_limit =
                (config.initial_stream_window_size as usize / 8).clamp(16_384, 65_536);
            let zero_copy_threshold = (coalesce_limit / 4).clamp(8_192, 16_384);
            let mut pending_buf = bytes::BytesMut::with_capacity(coalesce_limit);

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

                if pending_buf.is_empty() && len >= zero_copy_threshold {
                    if sender.send_chunk(data).await.is_err() {
                        tracing::debug!(
                            "Downstream client disconnected during H2 duplex streaming"
                        );
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
                            tracing::debug!(
                                "Downstream client disconnected during H2 duplex streaming"
                            );
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
                    tracing::debug!(
                        "Downstream client disconnected during H2 duplex streaming tail"
                    );
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
        };

        let mut upload_pinned = std::pin::pin!(upload_task);
        let mut download_pinned = std::pin::pin!(download_task);

        tokio::select! {
            res = &mut download_pinned => {
                res?;
            }
            res = &mut upload_pinned => {
                res?;
                download_pinned.await?;
            }
        }
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
