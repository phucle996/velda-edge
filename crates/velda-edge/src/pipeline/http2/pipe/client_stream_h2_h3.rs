//! Layer 7 HTTP/2 to HTTP/3 Client-Stream Bridge Pipeline.
//!
//! Handles client streaming uploads (e.g. file uploads, log batch ingest) where
//! downstream HTTP/2 requests are forwarded over HTTP/3 / QUIC, returning a single response.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};
use velda_http3::Http3Config;
use velda_http3::error::Http3Error;
use velda_http3::pipe::pipe_client_stream;

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a client-streaming HTTP/2 downstream stream bridged to an HTTP/3 QUIC backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
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
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 3. Construct canonical L7Request
    let l7_req = L7Request::new(
        head.method,
        head.uri,
        http::Version::HTTP_3,
        head.headers,
        body,
    );

    let h3_config = Http3Config::auto();

    // 4. Acquire pooled HTTP/3 client
    let client = match upstream.acquire(&h3_config).await {
        Ok(c) => c,
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

    // 5. Pipe client-streaming upload with self-healing on connection close
    let mut pipe_res = pipe_client_stream(&client, &l7_req, &h3_config).await;
    if let Err(Http3Error::ConnectionClosed) = pipe_res {
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection"
        );
        if let Ok(fresh_client) = upstream.acquire_fresh(&h3_config).await {
            pipe_res = pipe_client_stream(&fresh_client, &l7_req, &h3_config).await;
        }
    }

    match pipe_res {
        Ok(resp) => {
            let _ = responder.send_response(&resp);
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream client-stream pipe failed"
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
}
