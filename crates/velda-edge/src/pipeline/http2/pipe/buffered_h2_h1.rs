//! Layer 7 HTTP/2 to HTTP/1.1 Buffered Bridge Pipeline.
//!
//! Handles standard request-response RPCs where the full downstream request body
//! is buffered before transmission to the HTTP/1.1 upstream, and the full backend
//! response is read before returning HTTP/2 HEADERS + DATA frames to the client.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use tokio::io::AsyncReadExt;
use velda_core::L7Response;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a buffered HTTP/2 downstream stream forwarded to an HTTP/1.1 upstream backend.
pub async fn serve(
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
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

    // 2. Ensure RFC 9112 §7.1 mandatory Host header for HTTP/1.1
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
                "Failed to acquire upstream HTTP/1.1 connection for buffered bridge"
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

    let mut write_buf = bytes::BytesMut::with_capacity(1024);

    // 5. Serialize & send HTTP/1.1 request
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
            "Failed to send buffered HTTP/1.1 request to upstream"
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

    // 6. Read and decode HTTP/1.1 response
    let read_cap = upstream.config.client_body_buffer_size;
    let mut read_buf = bytes::BytesMut::with_capacity(read_cap);
    let (head, framing) =
        match velda_http1::client::read_response_head(&mut *lease, &mut read_buf, &upstream.config)
            .await
        {
            Ok(res) => res,
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

    let conn_close = check_connection_close(&head.headers);

    let body = match framing {
        velda_http1::server::request::Http1BodyFraming::Empty => velda_core::Body::Empty,
        velda_http1::server::request::Http1BodyFraming::ContentLength(body_len) => {
            if body_len > upstream.config.max_body_size {
                lease.mark_closed();
                let err_resp = L7Response::from_bytes(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    b"413 Payload Too Large\n".to_vec(),
                );
                let _ = responder.send_response(&err_resp);
                return;
            }
            if body_len > 0 {
                while read_buf.len() < body_len {
                    let needed = body_len - read_buf.len();
                    if read_buf.capacity() - read_buf.len() < needed {
                        read_buf.reserve(needed);
                    }
                    match lease.read_buf(&mut read_buf).await {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => {
                            lease.mark_closed();
                            break;
                        }
                    }
                }
                velda_core::Body::Bytes(read_buf.split_to(read_buf.len().min(body_len)).freeze())
            } else {
                velda_core::Body::Empty
            }
        }
        velda_http1::server::request::Http1BodyFraming::Chunked => {
            let mut chunked_body = bytes::BytesMut::new();
            let mut failed = false;
            loop {
                match velda_http1::client::read_next_chunk(&mut *lease, &mut read_buf).await {
                    Ok(Some(chunk)) => {
                        if chunked_body.len() + chunk.len() > upstream.config.max_body_size {
                            failed = true;
                            break;
                        }
                        chunked_body.extend_from_slice(&chunk);
                    }
                    Ok(None) => break,
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
            if failed {
                lease.mark_closed();
                let err_resp = L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    b"502 Bad Gateway: upstream chunked read failed\n".to_vec(),
                );
                let _ = responder.send_response(&err_resp);
                return;
            }
            velda_core::Body::Bytes(chunked_body.freeze())
        }
    };

    if conn_close {
        lease.mark_closed();
    }

    let mut clean_headers = head.headers;
    velda_http1::client::sanitize_headers(&mut clean_headers);
    let l7_resp = L7Response::new(head.status, http::Version::HTTP_2, clean_headers, body);
    let _ = responder.send_response(&l7_resp);
}

/// Helper to check whether upstream backend signaled `Connection: close`.
#[inline]
fn check_connection_close(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|val| val.eq_ignore_ascii_case("close"))
}
