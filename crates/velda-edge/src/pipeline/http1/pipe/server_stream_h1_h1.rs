//! Layer 7 HTTP/1.1 to HTTP/1.1 Server-Stream Pipeline.
//!
//! Handles server-streaming RPCs (Server-Sent Events, progressive chunks, LLM tokens)
//! where downstream request body is completely read into RAM first, and response chunks
//! are pumped progressively downstream.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequest,
    Http1ServerRequestHead, Http1ServerResponse, pipe_server_stream,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a server-streaming HTTP/1.1 transaction forwarded to an HTTP/1.1 backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http1Upstream>,
    meta: &DownstreamMeta,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 1. Enrich proxy forwarding headers
    velda_http1::server::header::enrich_headers(
        &mut head.headers,
        &head.uri,
        meta.peer,
        meta.local_addr,
        meta.is_tls,
    );

    // 2. Handle Expect: 100-continue unblocking
    if head.is_expect_100_continue() && framing != Http1BodyFraming::Empty {
        if let Err(e) = conn.send_100_continue().await {
            tracing::debug!(error = %e, "Failed to send 100 Continue downstream");
            return Err(e);
        }
        head.headers.remove(http::header::EXPECT);
    }

    // 3. Read entire downstream body into RAM
    let body = match conn.read_body(framing).await {
        Ok(b) => b,
        Err(Http1Error::Timeout) => {
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::REQUEST_TIMEOUT,
                b"408 Request Timeout: client body read idle timeout exceeded\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::Timeout);
        }
        Err(e) => {
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(e);
        }
    };

    // 4. Acquire pooled HTTP/1.1 upstream connection
    let mut upstream_lease = match upstream.acquire().await {
        Ok(lease) => lease,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection"
            );
            let err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            return Err(Http1Error::ConnectionClosed);
        }
    };

    let cfg = *conn.config();
    let mut req = Http1ServerRequest::from_parts(head, body);

    // 5. Pipe server-streaming response with self-healing retry on stale pooled connection
    let mut res = pipe_server_stream(conn, &mut req, &mut *upstream_lease, &cfg).await;
    if let Err(ref e) = res
        && upstream_lease.is_reused()
        && e.is_stale_connection()
    {
        upstream_lease.mark_closed();
        drop(upstream_lease);

        tracing::debug!(
            upstream = %upstream.id(),
            "Stale pooled HTTP/1.1 connection detected; self-healing with fresh connection"
        );

        match upstream.acquire_fresh().await {
            Ok(mut fresh_lease) => {
                res = pipe_server_stream(conn, &mut req, &mut *fresh_lease, &cfg).await;
                if res.is_err() {
                    fresh_lease.mark_closed();
                }
            }
            Err(fresh_err) => {
                tracing::warn!(
                    error = %fresh_err,
                    upstream = %upstream.id(),
                    "Failed to acquire fresh connection for self-healing retry"
                );
            }
        }
    } else if res.is_err() {
        upstream_lease.mark_closed();
    }

    res
}
