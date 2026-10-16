//! Layer 7 HTTP/3 to HTTP/3 Duplex Bidirectional Pipeline.
//!
//! Handles full-duplex bidirectional streaming forwarded natively over upstream HTTP/3 QUIC connection
//! with true progressive chunk streaming and zero whole-payload buffering.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{Body, L7Request, L7Response};

use crate::pipeline::http3::engine::Http3Responder;
use crate::upstream::Http3Upstream;

/// Serves a full-duplex HTTP/3 downstream request forwarded to an HTTP/3 upstream backend.
pub async fn serve(req: &L7Request, responder: Http3Responder, upstream: &Arc<Http3Upstream>) {
    let (client, lease) = match upstream.acquire(&upstream.config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 connection for duplex bridge"
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

    let mut stream_res = client.send_stream_request_ref(req).await;
    let mut _fresh_lease_guard = None;
    if stream_res
        .as_ref()
        .err()
        .is_some_and(|e| e.is_connection_closed())
    {
        lease.mark_goaway();
        drop(lease);
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection for duplex bridge"
        );
        if let Ok((fresh_client, fresh_lease)) = upstream.acquire_fresh(&upstream.config).await {
            stream_res = fresh_client.send_stream_request_ref(req).await;
            if stream_res.is_err() {
                fresh_lease.mark_goaway();
            }
            _fresh_lease_guard = Some(fresh_lease);
        }
    }

    let mut stream_resp = match stream_res {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream duplex streaming request failed"
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

    let is_head = req.method == http::Method::HEAD;
    let is_no_body = stream_resp.status.is_informational()
        || stream_resp.status == StatusCode::NO_CONTENT
        || stream_resp.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body {
        let resp = L7Response::new(
            stream_resp.status,
            stream_resp.version,
            stream_resp.headers,
            Body::Empty,
        );
        let _ = responder.send_response(resp).await;
        return;
    }

    let mut stream_sender = match responder
        .send_stream_response(stream_resp.status, &stream_resp.headers)
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

    while let Some(chunk_res) = stream_resp.next_chunk().await {
        let chunk = match chunk_res {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "HTTP/3 upstream chunk read error in duplex bridge");
                return;
            }
        };

        if stream_sender.send_chunk(chunk).await.is_err() {
            tracing::debug!("Downstream client disconnected during H3 duplex streaming");
            return;
        }
    }

    let _ = stream_sender.finish().await;
}
