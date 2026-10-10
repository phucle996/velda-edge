//! Layer 7 HTTP/3 to HTTP/3 Server Streaming Pipeline.
//!
//! Handles server-streaming RPCs forwarded natively over upstream HTTP/3 QUIC connection.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::{L7Request, L7Response};
use velda_http3::Http3Config;
use velda_http3::error::Http3Error;
use velda_http3::pipe::pipe_server_stream;

use crate::upstream::Http3Upstream;

/// Serves a server-streaming HTTP/3 downstream request forwarded to an HTTP/3 upstream backend.
pub async fn serve(req: &L7Request, upstream: &Arc<Http3Upstream>) -> L7Response {
    let h3_config = Http3Config::auto();

    let client = match upstream.acquire(&h3_config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 connection"
            );
            return L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
        }
    };

    let mut pipe_res = pipe_server_stream(&client, req, &h3_config).await;
    if let Err(Http3Error::ConnectionClosed) = pipe_res {
        tracing::debug!(
            upstream = %upstream.id(),
            "HTTP/3 connection closed; self-healing with fresh QUIC connection"
        );
        if let Ok(fresh_client) = upstream.acquire_fresh(&h3_config).await {
            pipe_res = pipe_server_stream(&fresh_client, req, &h3_config).await;
        }
    }

    match pipe_res {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream server stream request failed"
            );
            L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )
        }
    }
}
