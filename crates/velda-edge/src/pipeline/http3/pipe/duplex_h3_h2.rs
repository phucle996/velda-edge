//! Layer 7 HTTP/3 to HTTP/2 Duplex Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming where downstream HTTP/3 requests are forwarded
//! to upstream HTTP/2 backends with true progressive streaming and zero whole-payload buffering.

use std::sync::Arc;

use http::header::{CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Request, L7Response};
use velda_plugin::RoutePluginPlan;

use crate::pipeline::http3::engine::Http3Responder;
use crate::upstream::Http2Upstream;

/// Minimum batching threshold for HTTP/2 WINDOW_UPDATE frame emission (32 KB).
/// Prevents micro-frame thrashing on small response chunk reads.
const MIN_H2_WINDOW_UPDATE_BATCH: usize = 32 * 1024;

/// Maximum batching threshold for HTTP/2 WINDOW_UPDATE frame emission (256 KB).
/// Ensures upstream HTTP/2 connection credit is returned in time to prevent pipe stall.
const MAX_H2_WINDOW_UPDATE_BATCH: usize = 256 * 1024;

/// Serves a full-duplex HTTP/3 downstream request bridged to an HTTP/2 backend.
pub async fn serve(
    req: &L7Request,
    responder: Http3Responder,
    upstream: &Arc<Http2Upstream>,
    plan: &Arc<RoutePluginPlan>,
) {
    let mut headers = req.headers.clone();
    velda_http2::client::sanitize_h2_headers(&mut headers);

    let (mut client, lease) = match upstream.acquire(&upstream.config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/2 connection for H3->H2 duplex bridge"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(err_resp).await;
            return;
        }
    };
    let mut lease = Some(lease);

    let mut h2_req = http::Request::builder()
        .method(req.method.clone())
        .uri(req.uri.clone())
        .version(Version::HTTP_2);
    *h2_req.headers_mut().unwrap() = headers.clone();
    let h2_req = h2_req.body(()).unwrap();

    let has_body = match &req.body {
        Body::Bytes(b) => !b.is_empty(),
        Body::Empty => false,
    };

    let (response_fut, mut send_stream) = match client.send_request(h2_req, !has_body) {
        Ok(res) => res,
        Err(e) if is_h2_refused_or_goaway(&e) => {
            if let Some(l) = lease.take() {
                l.mark_goaway();
            }
            tracing::debug!(
                upstream = %upstream.id(),
                "HTTP/2 stream refused; self-healing with fresh connection"
            );
            match upstream.acquire_fresh(&upstream.config).await {
                Ok((mut fresh_client, fresh_lease)) => {
                    lease = Some(fresh_lease);
                    let mut fresh_req = http::Request::builder()
                        .method(req.method.clone())
                        .uri(req.uri.clone())
                        .version(Version::HTTP_2);
                    *fresh_req.headers_mut().unwrap() = headers;
                    let fresh_req = fresh_req.body(()).unwrap();

                    match fresh_client.send_request(fresh_req, !has_body) {
                        Ok(res) => res,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                upstream = %upstream.id(),
                                "Failed to dispatch HTTP/2 request on fresh connection"
                            );
                            let err_resp = L7Response::from_bytes(
                                StatusCode::BAD_GATEWAY,
                                format!("502 Bad Gateway: {e}\n").into_bytes(),
                            )
                            .with_header(
                                CONTENT_TYPE,
                                HeaderValue::from_static("text/plain; charset=utf-8"),
                            );
                            let _ = responder.send_response(err_resp).await;
                            return;
                        }
                    }
                }
                Err(e) => {
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        format!("502 Bad Gateway: {e}\n").into_bytes(),
                    );
                    let _ = responder.send_response(err_resp).await;
                    return;
                }
            }
        }
        Err(e) => {
            if let Some(l) = lease.as_mut() {
                l.mark_goaway();
            }
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to dispatch HTTP/2 request to upstream for duplex bridge"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(err_resp).await;
            return;
        }
    };

    if let Body::Bytes(ref b) = req.body
        && !b.is_empty()
    {
        let _ = send_stream.send_data(b.clone(), true);
    }

    let response = match response_fut.await {
        Ok(r) => r,
        Err(e) => {
            if let Some(l) = lease.as_mut() {
                l.mark_goaway();
            }
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to await HTTP/2 response headers from upstream for duplex bridge"
            );
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = responder.send_response(err_resp).await;
            return;
        }
    };

    let (mut parts, mut body_stream) = response.into_parts();

    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp =
            L7Response::new(parts.status, Version::HTTP_3, parts.headers, Body::Empty);
        velda_plugin::run_post_response_plugins(post_hooks, &req.headers, &mut l7_resp);
        parts.status = l7_resp.status;
        parts.headers = l7_resp.headers;
    }

    let is_head = req.method == http::Method::HEAD;
    let is_no_body = parts.status.is_informational()
        || parts.status == StatusCode::NO_CONTENT
        || parts.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body {
        let resp = L7Response::new(parts.status, Version::HTTP_3, parts.headers, Body::Empty);
        let _ = responder.send_response(resp).await;
        return;
    }

    let mut stream_sender = match responder
        .send_stream_response(parts.status, &parts.headers)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to initiate downstream HTTP/3 streaming response for duplex bridge"
            );
            return;
        }
    };

    // RFC 9113 §5.2.2: Batch WINDOW_UPDATE frames at 1/4th of initial stream window size.
    let batch_threshold = (upstream.config.initial_stream_window_size as usize / 4)
        .clamp(MIN_H2_WINDOW_UPDATE_BATCH, MAX_H2_WINDOW_UPDATE_BATCH);
    let mut unreleased_bytes = 0;

    while let Some(chunk) = body_stream.data().await {
        let data = match chunk {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(error = %e, "HTTP/2 upstream body stream error in duplex bridge");
                let _ = body_stream
                    .flow_control()
                    .release_capacity(unreleased_bytes);
                return;
            }
        };

        let len = data.len();
        unreleased_bytes += len;

        if stream_sender.send_chunk(data).await.is_err() {
            tracing::debug!("Downstream client disconnected during H3 duplex streaming");
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
            return;
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

    let _ = stream_sender.finish().await;
}

#[inline]
fn is_h2_refused_or_goaway(err: &h2::Error) -> bool {
    (err.is_reset() && err.reason() == Some(h2::Reason::REFUSED_STREAM)) || err.is_go_away()
}
