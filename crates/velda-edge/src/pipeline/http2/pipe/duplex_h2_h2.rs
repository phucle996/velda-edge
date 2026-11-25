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
use velda_http2::config::Http2Config;
use velda_http2::error::Http2Error;
use velda_http2::server::request::{Http2ServerRequestHead, Http2StreamReceiver};
use velda_http2::server::response::Http2Responder;

use crate::pipeline::{DownstreamMeta, PipeOutcome};
use crate::upstream::Http2Upstream;

/// Serves a full-duplex bidirectional HTTP/2 downstream stream forwarded to an HTTP/2 upstream backend.
#[allow(clippy::too_many_arguments)]
pub async fn serve(
    req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    config: Http2Config,
    upstream: &Arc<Http2Upstream>,
    req_state: &velda_core::RequestState,
    plan: &velda_plugin::RoutePluginPlan,
) -> PipeOutcome {
    // 1. Fail-fast: Acquire upstream multiplexed connection before duplex loop
    let (client, lease) = match upstream.acquire(&config).await {
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

    // 3. Pipe duplex frames concurrently
    let duplex_res = async {
        let is_head = req_head.method == http::Method::HEAD;
        let is_empty_upload = receiver.is_end_stream();
        let (response_fut, mut send_stream) = client
            .send_request(
                req_head.method.clone(),
                req_head.uri.clone(),
                req_head.headers.clone(),
                is_empty_upload,
            )
            .await?;

        let upload_coalesce = 65_536;

        let upload_task = async move {
            if is_empty_upload {
                return Ok::<u64, Http2Error>(0);
            }
            let mut sent_end_stream = false;
            while !receiver.is_end_stream() {
                let Some(chunk) = receiver.recv_coalesced_chunk(upload_coalesce).await? else {
                    break;
                };
                if chunk.is_empty() {
                    if receiver.is_end_stream() {
                        break;
                    }
                    continue;
                }
                let is_last = receiver.is_end_stream();
                if send_stream.send_chunk(chunk, is_last).await.is_err() {
                    return Ok(receiver.bytes_received as u64);
                }
                if is_last {
                    sent_end_stream = true;
                    break;
                }
            }
            if !sent_end_stream {
                let _ = send_stream.send_data(Bytes::new(), true);
            }
            Ok::<u64, Http2Error>(receiver.bytes_received as u64)
        };

        let config_max_body = config.max_body_size;
        let mut responder = responder;
        let download_task = async move {
            let mut response = response_fut.await?;
            let mut resp_ws = response.headers;
            if let Some(post_hooks) = &plan.post_response_hooks {
                let mut l7_resp =
                    L7Response::new(response.status, Version::HTTP_2, resp_ws, Body::Empty);
                velda_plugin::run_post_response_plugins(
                    post_hooks,
                    req_state,
                    &req_head.headers,
                    &mut l7_resp,
                );
                response.status = l7_resp.status;
                resp_ws = l7_resp.headers;
            }
            meta.inject_response_headers(&mut resp_ws);

            let is_no_body_status = response.status.is_informational()
                || response.status == StatusCode::NO_CONTENT
                || response.status == StatusCode::NOT_MODIFIED;

            if is_head || is_no_body_status {
                let resp = L7Response::new(response.status, Version::HTTP_2, resp_ws, Body::Empty);
                responder.send_response_and_cancel_upload(&resp)?;
                return Ok::<(u16, u64), Http2Error>((responder.last_status, 0));
            }

            let mut sender = responder.send_stream_response(response.status, &resp_ws)?;
            let mut total_bytes = 0;

            while let Some(data) = response.body.recv_chunk().await? {
                total_bytes += data.len();
                if total_bytes > config_max_body {
                    return Err(Http2Error::PayloadTooLarge(total_bytes));
                }
                if !data.is_empty() && sender.send_chunk(data).await.is_err() {
                    tracing::debug!("Downstream client disconnected during H2 duplex streaming");
                    return Ok((responder.last_status, sender.bytes_sent));
                }
            }

            let tx = sender.bytes_sent;
            let _ = sender.finish();
            Ok((responder.last_status, tx))
        };

        let (up_res, down_res) = tokio::join!(upload_task, download_task);
        let rx = up_res?;
        let (status, tx) = down_res?;
        Ok::<(u16, u64, u64), Http2Error>((status, rx, tx))
    }
    .await;

    match duplex_res {
        Ok((status, rx, tx)) => PipeOutcome::http(
            status,
            rx,
            tx,
            upstream_addr,
            velda_core::CompletionReason::Complete,
        ),
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/2 upstream duplex pipe failed"
            );
            PipeOutcome::http(
                StatusCode::BAD_GATEWAY.as_u16(),
                0,
                0,
                upstream_addr,
                velda_core::CompletionReason::UpstreamError,
            )
        }
    }
}
