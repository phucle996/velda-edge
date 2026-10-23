//! Layer 7 HTTP/1.1 to HTTP/3 Buffered Bridge Pipeline.
//!
//! Standard request-response REST API bridge where downstream HTTP/1.1 request body
//! is completely buffered into RAM, then forwarded over an upstream HTTP/3 / QUIC
//! client multiplexing session without Head-of-Line blocking.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::L7Request;
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse,
};
use velda_http3::{Http3Config, sanitize_headers};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http3Upstream;

/// Serves a buffered HTTP/1.1 downstream request bridged to an HTTP/3 QUIC backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut req_head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http3Upstream>,
    meta: &DownstreamMeta,
    plan: &velda_plugin::RoutePluginPlan,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 1. Enrich proxy forwarding headers
    velda_http1::server::header::enrich_headers(
        &mut req_head.headers,
        &req_head.uri,
        meta.peer,
        meta.local_addr,
        meta.is_tls,
    );

    // 2. Handle Expect: 100-continue unblocking
    if req_head.is_expect_100_continue() && framing != Http1BodyFraming::Empty {
        if let Err(e) = conn.send_100_continue().await {
            tracing::debug!(error = %e, "Failed to send 100 Continue downstream");
            return Err(e);
        }
        req_head.headers.remove(http::header::EXPECT);
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

    // 4. Sanitize connection-specific headers forbidden by RFC 9114 §4.2 before H3 encoding
    sanitize_headers(&mut req_head.headers);
    let req_id = req_head
        .headers
        .get(&crate::pipeline::X_REQUEST_ID)
        .cloned();

    // 5. Construct canonical L7Request
    let l7_req = L7Request::new(
        req_head.method,
        req_head.uri,
        http::Version::HTTP_3,
        req_head.headers,
        body,
    );

    let h3_config = Http3Config::auto();

    // 6. Acquire pooled HTTP/3 client
    let (client, lease) = match upstream.acquire(&h3_config).await {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/3 client"
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

    // 7. Send request directly with self-healing on connection close
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
        if let Ok((fresh_client, fresh_lease)) = upstream.acquire_fresh(&h3_config).await {
            pipe_res = fresh_client.send_request_ref(&l7_req).await;
            _fresh_lease_guard = Some(fresh_lease);
        }
    } else {
        drop(lease);
    }

    match pipe_res {
        Ok(mut l7_resp) => {
            if let Some(ref req_id) = req_id {
                l7_resp
                    .headers
                    .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
            }
            if let Some(post_hooks) = &plan.post_response_hooks {
                velda_plugin::run_post_response_plugins(post_hooks, &l7_req.headers, &mut l7_resp);
            }
            meta.inject_response_headers(&mut l7_resp.headers);
            let server_resp = Http1ServerResponse::new(
                l7_resp.status,
                http::Version::HTTP_11,
                l7_resp.headers.into(),
                l7_resp.body,
            );
            conn.send_response(&server_resp).await?;
            if conn.read_buf.is_empty() {
                conn.compact_buffers();
            }
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "HTTP/3 upstream buffered pipe failed"
            );
            let mut err_resp = Http1ServerResponse::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            meta.inject_response_headers(&mut err_resp.headers);
            conn.mark_close();
            let _ = conn.send_response(&err_resp).await;
            Err(Http1Error::ConnectionClosed)
        }
    }
}
