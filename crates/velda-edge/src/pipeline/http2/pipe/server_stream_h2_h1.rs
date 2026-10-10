//! Layer 7 HTTP/2 to HTTP/1.1 Server-Streaming Bridge Pipeline.
//!
//! Handles Server-Sent Events (SSE), AI LLM token generation, and progressive chunked
//! responses from HTTP/1.1 backends, streaming them immediately as HTTP/2 DATA frames
//! to downstream clients with flow-control backpressure.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use velda_core::L7Response;
use velda_http1::server::request::Http1BodyFraming;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a server-streaming HTTP/2 downstream stream bridged to an HTTP/1.1 backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
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
    if !head.headers.contains_key(http::header::HOST)
        && let Some(host) = velda_http2::server::header::extract_host(&head.headers, &head.uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        head.headers.insert(http::header::HOST, hv);
    }

    // 3. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
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

    let cfg = velda_http1::config::Http1Config::auto();
    let mut write_buf = bytes::BytesMut::with_capacity(cfg.upstream_write_base + body.len());

    // 5. Send HTTP/1.1 request head and body
    let send_res = velda_http1::client::send_request_parts(
        &head.method,
        &head.uri,
        &head.headers,
        &body,
        &mut *lease,
        &mut write_buf,
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

    // 6. Read HTTP/1.1 response head from upstream
    let mut read_buf = bytes::BytesMut::with_capacity(cfg.upstream_read_capacity);
    let (resp_head, framing) =
        match velda_http1::client::read_response_head(&mut *lease, &mut read_buf, &cfg).await {
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

    let conn_close = check_connection_close(&resp_head.headers);

    // 7. Strip hop-by-hop headers before sending over HTTP/2
    let mut clean_headers = resp_head.headers;
    velda_http1::client::sanitize_headers(&mut clean_headers);

    // 8. Open downstream HTTP/2 streaming response
    let mut stream_sender = match responder.send_stream_response(resp_head.status, &clean_headers) {
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
        Http1BodyFraming::Chunked => loop {
            match velda_http1::client::read_next_chunk(&mut *lease, &mut read_buf).await {
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
        },
        Http1BodyFraming::ContentLength(total_len) => {
            let mut remaining = total_len;
            let mut failed = false;
            while remaining > 0 {
                let to_read = remaining.min(65536);
                match velda_http1::client::read_chunk_sized(&mut *lease, &mut read_buf, to_read)
                    .await
                {
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

    let _ = stream_sender.finish();
}

#[inline]
fn check_connection_close(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|val| val.eq_ignore_ascii_case("close"))
}
