//! Layer 7 HTTP/2 to HTTP/1.1 Client-Streaming Bridge Pipeline.
//!
//! Handles client-streaming uploads (large file uploads, multi-frame data ingestion)
//! by transforming incoming HTTP/2 DATA frames into RFC 9112 `Transfer-Encoding: chunked`
//! byte streams towards the HTTP/1.1 upstream backend.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, TRANSFER_ENCODING};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use velda_core::L7Response;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a client-streaming HTTP/2 downstream stream bridged to an HTTP/1.1 backend.
pub async fn serve(
    mut req_head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
    plan: &velda_plugin::RoutePluginPlan,
) {
    // 1. Ensure mandatory Host header (RFC 9112 §7.1)
    if !req_head.headers.contains_key(http::header::HOST)
        && let Some(host) =
            velda_http2::server::header::extract_host(&req_head.headers, &req_head.uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        req_head.headers.insert(http::header::HOST, hv);
    }

    // 2. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut req_head.headers,
        &req_head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 3. Opportunistically ingest first chunk (up to 262KB)
    let coalesce_limit = upstream.config.client_body_buffer_size;
    let (first_chunk, is_fin) = match receiver.recv_coalesced_chunk(coalesce_limit).await {
        Ok(Some(c)) => {
            let fin = receiver.is_end_stream();
            (Some(c), fin)
        }
        Ok(None) => (None, true),
        Err(e) => {
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {e}\n").into_bytes(),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    };

    // 4. Decide framing mode towards HTTP/1.1: Content-Length vs Chunked
    let is_chunked = if req_head.headers.contains_key(CONTENT_LENGTH) {
        req_head.headers.remove(TRANSFER_ENCODING);
        false
    } else if is_fin {
        let len = first_chunk.as_ref().map_or(0, |c| c.len());
        req_head
            .headers
            .insert(CONTENT_LENGTH, HeaderValue::from(len));
        req_head.headers.remove(TRANSFER_ENCODING);
        false
    } else {
        req_head
            .headers
            .insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        req_head.headers.remove(CONTENT_LENGTH);
        true
    };

    // 5. Acquire pooled HTTP/1.1 upstream connection
    let mut lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection for client-streaming bridge"
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

    // 6. Borrow pooled buffers from upstream lease
    let (upstream_stream, read_buf, write_buf) = lease.split_all();
    write_buf.clear();
    let initial_cap = 1024
        + if !is_chunked {
            first_chunk.as_ref().map_or(0, |c| c.len())
        } else {
            0
        };
    if write_buf.capacity() < initial_cap {
        write_buf.reserve(initial_cap - write_buf.capacity());
    }
    velda_http1::client::encode_request_head(
        &req_head.method,
        &req_head.uri,
        &req_head.headers,
        None,
        write_buf,
    );

    // 7. Split HTTP/1.1 stream into read and write halves for concurrent execution
    let (resp_res, should_close) = {
        let (mut read_half, mut write_half) = tokio::io::split(upstream_stream);

        let upload_fut = async {
            if !is_chunked {
                if let Some(chunk) = first_chunk {
                    write_buf.extend_from_slice(&chunk);
                }
                write_half.write_all(write_buf).await?;
                if !is_fin {
                    while let Some(chunk) = receiver
                        .recv_coalesced_chunk(coalesce_limit)
                        .await
                        .map_err(std::io::Error::other)?
                    {
                        if !chunk.is_empty() {
                            write_half.write_all(&chunk).await?;
                        }
                        if receiver.is_end_stream() {
                            break;
                        }
                    }
                }
            } else {
                write_half.write_all(write_buf).await?;
                if let Some(chunk) = first_chunk
                    && !chunk.is_empty()
                {
                    velda_http1::client::send_chunk_vectored(&mut write_half, &chunk)
                        .await
                        .map_err(std::io::Error::other)?;
                }
                if is_fin {
                    write_half.write_all(b"0\r\n\r\n").await?;
                } else {
                    while let Some(chunk) = receiver
                        .recv_coalesced_chunk(coalesce_limit)
                        .await
                        .map_err(std::io::Error::other)?
                    {
                        if !chunk.is_empty() {
                            velda_http1::client::send_chunk_vectored(&mut write_half, &chunk)
                                .await
                                .map_err(std::io::Error::other)?;
                        }
                        if receiver.is_end_stream() {
                            break;
                        }
                    }
                    write_half.write_all(b"0\r\n\r\n").await?;
                }
            }
            write_half.flush().await?;
            Ok::<(), std::io::Error>(())
        };

        let download_fut = async {
            read_buf.clear();
            loop {
                match read_half.read_buf(read_buf).await {
                    Ok(0) => {
                        if let Ok(Some(resp)) =
                            velda_http1::client::decode_response(read_buf, &upstream.config)
                        {
                            return Ok(resp);
                        } else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "upstream closed connection prematurely",
                            ));
                        }
                    }
                    Ok(_) => {
                        if let Ok(Some(resp)) =
                            velda_http1::client::decode_response(read_buf, &upstream.config)
                        {
                            return Ok(resp);
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        };

        let mut upload_pinned = std::pin::pin!(upload_fut);
        let mut download_pinned = std::pin::pin!(download_fut);

        let mut upload_done = false;
        let res = tokio::select! {
            resp = &mut download_pinned => resp,
            upload_res = &mut upload_pinned => {
                upload_done = upload_res.is_ok();
                if let Err(e) = upload_res {
                    tracing::warn!(error = %e, "Upload to HTTP/1.1 backend failed");
                }
                download_pinned.await
            }
        };

        match res {
            Ok(resp) => {
                let close = !upload_done || check_connection_close(&resp.headers);
                (Ok(resp.into_l7_response()), close)
            }
            Err(e) => (Err(e), true),
        }
    };

    if should_close {
        lease.mark_closed();
    }
    // Drop upstream lease immediately to return connection and pooled buffers to upstream pool
    drop(lease);

    match resp_res {
        Ok(mut l7_resp) => {
            if let Some(post_hooks) = &plan.post_response_hooks {
                velda_plugin::run_post_response_plugins(
                    post_hooks,
                    &req_head.headers,
                    &mut l7_resp,
                );
            }
            meta.inject_response_headers(&mut l7_resp.headers);
            let _ = responder.send_response(&l7_resp);
            if !receiver.is_end_stream() {
                tokio::spawn(async move { while let Ok(Some(_)) = receiver.recv_chunk().await {} });
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to read upstream HTTP/1.1 response");
            let mut err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            );
            meta.inject_response_headers(&mut err_resp.headers);
            let _ = responder.send_response(&err_resp);
            if !receiver.is_end_stream() {
                tokio::spawn(async move { while let Ok(Some(_)) = receiver.recv_chunk().await {} });
            }
        }
    }
}

#[inline]
fn check_connection_close(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|val| val.eq_ignore_ascii_case("close"))
}
