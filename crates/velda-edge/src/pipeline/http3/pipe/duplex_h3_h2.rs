//! Layer 7 HTTP/3 to HTTP/2 Duplex Bridge Pipeline.
//!
//! Handles full-duplex bidirectional streaming where downstream HTTP/3 requests are forwarded
//! to upstream HTTP/2 backends with true progressive streaming and zero whole-payload buffering.

use std::sync::Arc;

use http::header::{CONTENT_TYPE, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Request, L7Response};
use velda_plugin::RoutePluginPlan;

use crate::pipeline::PipeOutcome;
use crate::pipeline::http3::engine::Http3Responder;
use crate::upstream::Http2Upstream;

/// Serves a full-duplex HTTP/3 downstream request bridged to an HTTP/2 backend.
pub async fn serve(
    req: &L7Request,
    responder: Http3Responder,
    upstream: &Arc<Http2Upstream>,
    plan: &Arc<RoutePluginPlan>,
) -> PipeOutcome {
    let (client, lease) = match upstream.acquire(&upstream.config).await {
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
            return PipeOutcome::http(
                req.body.len() as u64,
                0,
                None,
                velda_core::InternalStatus::UpstreamError,
            );
        }
    };
    let mut upstream_addr = Some(lease.endpoint);
    let mut lease = Some(lease);

    let has_body = !req.body.is_empty();

    let (response_fut, mut send_stream) = match client
        .send_request(
            req.method.clone(),
            req.uri.clone(),
            req.headers.clone(),
            !has_body,
        )
        .await
    {
        Ok(res) => res,
        Err(e) if e.is_refused_or_goaway() => {
            if let Some(l) = lease.take() {
                l.mark_goaway();
            }
            tracing::debug!(
                upstream = %upstream.id(),
                "HTTP/2 stream refused; self-healing with fresh connection"
            );
            match upstream.acquire_fresh(&upstream.config).await {
                Ok((fresh_client, fresh_lease)) => {
                    upstream_addr = Some(fresh_lease.endpoint);
                    lease = Some(fresh_lease);
                    match fresh_client
                        .send_request(
                            req.method.clone(),
                            req.uri.clone(),
                            req.headers.clone(),
                            !has_body,
                        )
                        .await
                    {
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
                            return PipeOutcome::http(
                                req.body.len() as u64,
                                0,
                                upstream_addr,
                                velda_core::InternalStatus::UpstreamError,
                            );
                        }
                    }
                }
                Err(e) => {
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        format!("502 Bad Gateway: {e}\n").into_bytes(),
                    );
                    let _ = responder.send_response(err_resp).await;
                    return PipeOutcome::http(
                        req.body.len() as u64,
                        0,
                        upstream_addr,
                        velda_core::InternalStatus::UpstreamError,
                    );
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
            return PipeOutcome::http(
                req.body.len() as u64,
                0,
                upstream_addr,
                velda_core::InternalStatus::UpstreamError,
            );
        }
    };

    if has_body {
        let _ = send_stream.send_data(req.body.to_bytes(), true);
    }

    let mut response = match response_fut.await {
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
            return PipeOutcome::http(
                req.body.len() as u64,
                0,
                upstream_addr,
                velda_core::InternalStatus::UpstreamError,
            );
        }
    };

    let mut resp_ws = response.headers;
    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = L7Response::new(response.status, Version::HTTP_3, resp_ws, Body::Empty);
        velda_plugin::run_post_response_plugins(post_hooks, &req.state, &req.headers, &mut l7_resp);
        response.status = l7_resp.status;
        resp_ws = l7_resp.headers;
    }

    let is_head = req.method == http::Method::HEAD;
    let is_no_body = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body {
        let resp = L7Response::new(response.status, Version::HTTP_3, resp_ws, Body::Empty);
        let _ = responder.send_response(resp).await;
        return PipeOutcome::http(
            req.body.len() as u64,
            0,
            upstream_addr,
            velda_core::InternalStatus::Completed,
        );
    }

    let mut stream_sender = match responder
        .send_stream_response(response.status, &resp_ws)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to initiate downstream HTTP/3 streaming response for duplex bridge"
            );
            return PipeOutcome::http(
                req.body.len() as u64,
                0,
                upstream_addr,
                velda_core::InternalStatus::ClientCanceled,
            );
        }
    };

    let mut failed = false;
    while let Some(chunk_res) = response.body.recv_chunk().await.transpose() {
        let data = match chunk_res {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(error = %e, "HTTP/2 upstream body stream error in duplex bridge");
                failed = true;
                break;
            }
        };

        if stream_sender.send_chunk(data).await.is_err() {
            tracing::debug!("Downstream client disconnected during H3 duplex streaming");
            failed = true;
            break;
        }
    }

    let bytes_tx = stream_sender.bytes_sent();
    let _ = stream_sender.finish().await;
    PipeOutcome::http(
        req.body.len() as u64,
        bytes_tx,
        upstream_addr,
        if failed {
            velda_core::InternalStatus::UpstreamError
        } else {
            velda_core::InternalStatus::Completed
        },
    )
}
