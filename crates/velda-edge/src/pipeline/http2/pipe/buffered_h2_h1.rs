//! Layer 7 HTTP/2 to HTTP/1.1 Buffered Bridge Pipeline.
//!
//! Handles standard request-response RPCs where the full downstream request body
//! is buffered before transmission to the HTTP/1.1 upstream, and the full backend
//! response is read before returning HTTP/2 HEADERS + DATA frames to the client.

use std::sync::Arc;

use bytes::Buf;

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::AsyncReadExt;
use velda_core::L7Response;
use velda_http1::is_connection_close;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a buffered HTTP/2 downstream stream forwarded to an HTTP/1.1 upstream backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
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

    // 2. Ensure RFC 9112 §7.1 mandatory Host header for HTTP/1.1
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

    // 5. Serialize & send HTTP/1.1 request using pre-allocated pooled lease.write_buf
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

    // 6. Read and decode HTTP/1.1 response using pre-allocated pooled lease.read_buf (zero heap allocations)
    let (stream, read_buf) = lease.split_read();
    let (resp_head, framing) =
        match velda_http1::client::read_response_head(stream, read_buf, &upstream.config).await {
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

    let conn_close = is_connection_close(&resp_head.headers);

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
                if lease.read_buf.len() < body_len {
                    let needed = body_len - lease.read_buf.len();
                    if lease.read_buf.capacity() - lease.read_buf.len() < needed {
                        lease.read_buf.reserve(needed);
                    }
                }
                let mut read_failed = false;
                {
                    let (stream, read_buf) = lease.split_read();
                    while read_buf.len() < body_len {
                        match stream.read_buf(read_buf).await {
                            Ok(0) => break,
                            Ok(_) => {}
                            Err(_) => {
                                read_failed = true;
                                break;
                            }
                        }
                    }
                }
                if read_failed {
                    lease.mark_closed();
                }
                let actual_len = lease.read_buf.len().min(body_len);
                let body_bytes = if actual_len <= 4096 && lease.read_buf.capacity() >= 16384 {
                    let b = bytes::Bytes::copy_from_slice(&lease.read_buf[..actual_len]);
                    lease.read_buf.advance(actual_len);
                    b
                } else {
                    lease.read_buf.split_to(actual_len).freeze()
                };
                velda_core::Body::Bytes(body_bytes)
            } else {
                velda_core::Body::Empty
            }
        }
        velda_http1::server::request::Http1BodyFraming::Chunked => {
            let mut chunked_body = bytes::BytesMut::new();
            let mut failed = false;
            {
                let (stream, read_buf) = lease.split_read();
                loop {
                    match velda_http1::client::read_next_chunk(stream, read_buf).await {
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

    // 7. Clear read buffer preserving pool allocation, then DROP LEASE IMMEDIATELY!
    // Returning the upstream connection to the pool BEFORE sending downstream frames
    // eliminates pool starvation under high stream concurrency.
    lease.read_buf.clear();
    drop(lease);

    // 8. Send HTTP/2 response downstream
    let mut l7_resp = L7Response::new(
        resp_head.status,
        http::Version::HTTP_2,
        resp_head.headers,
        body,
    );
    if let Some(post_hooks) = &plan.post_response_hooks {
        velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut l7_resp);
    }
    meta.inject_response_headers(&mut l7_resp.headers);
    let _ = responder.send_response_owned(l7_resp);
}
