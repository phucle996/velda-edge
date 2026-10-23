//! Layer 7 HTTP/2 to HTTP/1.1 Server-Streaming Bridge Pipeline.
//!
//! Handles Server-Sent Events (SSE), AI LLM token generation, and progressive chunked
//! responses from HTTP/1.1 backends, streaming them immediately as HTTP/2 DATA frames
//! to downstream clients with flow-control backpressure.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use velda_core::L7Response;
use velda_http1::is_connection_close;
use velda_http1::server::request::Http1BodyFraming;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a server-streaming HTTP/2 downstream stream bridged to an HTTP/1.1 backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
    plan: &velda_plugin::RoutePluginPlan,
) {
    // 1. Consume downstream request body
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

    // 2. Ensure mandatory Host header (RFC 9112 §7.1)
    if !req_head.headers.contains_key(http::header::HOST)
        && let Some(host) =
            velda_http2::server::header::extract_host(&req_head.headers, &req_head.uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        req_head.headers.insert(http::header::HOST, hv);
    }

    // 3. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut req_head.headers,
        &req_head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 4. Acquire pooled HTTP/1.1 upstream connection
    let mut lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection for server-streaming bridge"
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

    // 5. Send HTTP/1.1 request head and body using pre-allocated pooled write_buf
    let (stream, write_buf) = lease.split_write();
    let send_res = velda_http1::client::send_request_parts(
        &req_head.method,
        &req_head.uri,
        &req_head.headers,
        &body,
        stream,
        write_buf,
    )
    .await;

    if let Err(e) = send_res {
        lease.mark_closed();
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to send request head to HTTP/1.1 upstream"
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

    // 6. Read HTTP/1.1 response head from upstream using pre-allocated pooled read_buf
    let (stream, read_buf) = lease.split_read();
    let (mut resp_head, framing) =
        match velda_http1::client::read_response_head(stream, read_buf, &upstream.config).await {
            Ok(parts) => parts,
            Err(e) => {
                lease.mark_closed();
                tracing::warn!(
                    error = %e,
                    upstream = %upstream.id(),
                    "Failed to read HTTP/1.1 response head from upstream"
                );
                let err_resp = L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                );
                let _ = responder.send_response(&err_resp);
                return;
            }
        };

    let conn_close = is_connection_close(&resp_head.headers);

    // 7. Strip hop-by-hop headers before sending over HTTP/2
    let mut clean_headers = resp_head.headers;
    velda_http1::client::sanitize_headers(&mut clean_headers);
    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = L7Response::new(
            resp_head.status,
            http::Version::HTTP_2,
            clean_headers,
            velda_core::Body::Empty,
        );
        velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut l7_resp);
        resp_head.status = l7_resp.status;
        clean_headers = l7_resp.headers.into();
    }
    meta.inject_response_headers(&mut clean_headers);

    // 8. Open downstream HTTP/2 streaming response
    let mut stream_sender =
        match responder.send_stream_response_owned(resp_head.status, clean_headers) {
            Ok(s) => s,
            Err(e) => {
                lease.mark_closed();
                tracing::warn!(
                    error = %e,
                    upstream = %upstream.id(),
                    "Failed to initiate downstream HTTP/2 streaming response"
                );
                return;
            }
        };

    // 9. Stream response chunks based on HTTP/1.1 framing
    let stream_res = match framing {
        Http1BodyFraming::Empty => Ok(()),
        Http1BodyFraming::Chunked => {
            let (stream, read_buf) = lease.split_read();
            loop {
                match velda_http1::client::read_next_chunk(stream, read_buf).await {
                    Ok(Some(chunk)) => {
                        if let Err(e) = stream_sender.send_chunk(chunk).await {
                            break Err(e);
                        }
                    }
                    Ok(None) => break Ok(()),
                    Err(_) => {
                        lease.mark_closed();
                        break Ok(());
                    }
                }
            }
        }
        Http1BodyFraming::ContentLength(total_len) => {
            let mut remaining = total_len;
            let mut failed = false;
            let chunk_limit = upstream.config.client_body_buffer_size;
            let (stream, read_buf) = lease.split_read();
            while remaining > 0 {
                let to_read = remaining.min(chunk_limit);
                match velda_http1::client::read_chunk_sized(stream, read_buf, to_read).await {
                    Ok(Some(chunk)) => {
                        remaining = remaining.saturating_sub(chunk.len());
                        if let Err(e) = stream_sender.send_chunk(chunk).await {
                            tracing::warn!(error = %e, "Client disconnected during H2 streaming");
                            failed = true;
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        lease.mark_closed();
                        failed = true;
                        break;
                    }
                }
            }
            if failed {
                lease.mark_closed();
            }
            Ok(())
        }
    };

    if conn_close || stream_res.is_err() {
        lease.mark_closed();
    }

    // Drop upstream lease immediately to return connection to the pool
    lease.read_buf.clear();
    drop(lease);

    let _ = stream_sender.finish();
}
