//! Layer 7 HTTP/2 to HTTP/3 Buffered Bridge Pipeline.
//!
//! Standard request-response RPC bridge where downstream HTTP/2 request body is buffered,
//! forwarded over persistent QUIC / HTTP/3 client multiplexing, and the response is returned
//! as HTTP/2 HEADERS + DATA frames to the downstream client.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};
use velda_http3::sanitize_headers;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a buffered HTTP/2 downstream stream forwarded to an HTTP/3 QUIC upstream backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http3Upstream>,
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

    // 2. Sanitize connection-specific headers forbidden by RFC 9114 §4.2
    sanitize_headers(&mut req_head.headers);

    // 4. Construct canonical L7Request
    let l7_req = L7Request::new(
        req_head.method,
        req_head.uri,
        http::Version::HTTP_3,
        req_head.headers,
        body,
    );

    // 5. Acquire pooled HTTP/3 client
    let (client, lease) = match upstream.acquire(&upstream.config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 client"
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

    // 5. Send request directly with self-healing on connection close
    let mut pipe_res = client.send_request_ref(&l7_req).await;
    let mut _fresh_lease_guard = None;
    if pipe_res
        .as_ref()
        .err()
        .is_some_and(|e| e.is_connection_closed())
    {
        lease.mark_goaway();
        drop(lease);
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection"
        );
        if let Ok((fresh_client, fresh_lease)) = upstream.acquire_fresh(&upstream.config).await {
            pipe_res = fresh_client.send_request(l7_req.clone()).await;
            _fresh_lease_guard = Some(fresh_lease);
        }
    } else {
        drop(lease);
    }

    match pipe_res {
        Ok(mut resp) => {
            // Fast-path guard: Bypasses post-response plugin chain if route has no hooks.
            if let Some(post_hooks) = &plan.post_response_hooks {
                velda_plugin::run_post_response_plugins(post_hooks, &l7_req.headers, &mut resp);
            }
            meta.inject_response_headers(&mut resp.headers);
            let _ = responder.send_response(&resp);
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream buffered pipe failed"
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
        }
    }
}
