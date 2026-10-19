//! Layer 7 HTTP/2 to HTTP/3 Server-Stream Bridge Pipeline.
//!
//! Handles server-streaming RPCs (e.g. Server-Sent Events, LLM token streaming)
//! where downstream HTTP/2 requests are forwarded over HTTP/3 / QUIC, and responses
//! are streamed back downstream.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};
use velda_http3::error::Http3Error;
use velda_http3::sanitize_headers;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a server-streaming HTTP/2 downstream stream bridged to an HTTP/3 QUIC backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http3Upstream>,
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

    // 3. Sanitize connection-specific headers forbidden by RFC 9114 §4.2
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
    let (client, _lease) = match upstream.acquire(&upstream.config).await {
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

    // 5. Pipe server-streaming response with self-healing on connection close
    let mut stream_res = client.send_stream_request(l7_req.clone()).await;
    if let Err(Http3Error::ConnectionClosed) = stream_res {
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection"
        );
        if let Ok((fresh_client, _fresh_lease)) = upstream.acquire_fresh(&upstream.config).await {
            stream_res = fresh_client.send_stream_request(l7_req).await;
        }
    }

    let mut h3_resp = match stream_res {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream server-stream pipe failed"
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

    let is_no_body = h3_resp.status.is_informational()
        || h3_resp.status == StatusCode::NO_CONTENT
        || h3_resp.status == StatusCode::NOT_MODIFIED;

    if is_no_body {
        let resp = L7Response::new(
            h3_resp.status,
            http::Version::HTTP_2,
            h3_resp.headers,
            velda_core::Body::Empty,
        );
        let _ = responder.send_response(&resp);
        return;
    }

    // Immediately open streaming response downstream and transmit headers frame
    let mut stream_sender = match responder.send_stream_response(h3_resp.status, &h3_resp.headers) {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(error = %e, "Failed to initiate downstream HTTP/2 streaming response");
            return;
        }
    };

    while let Some(chunk_res) = h3_resp.next_chunk().await {
        match chunk_res {
            Ok(chunk) => {
                if let Err(e) = stream_sender.send_chunk(chunk).await {
                    tracing::debug!(error = %e, "Downstream HTTP/2 client disconnected");
                    return;
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "Error reading upstream HTTP/3 chunk");
                responder.send_reset(h2::Reason::INTERNAL_ERROR);
                return;
            }
        }
    }

    let _ = stream_sender.finish();
}
