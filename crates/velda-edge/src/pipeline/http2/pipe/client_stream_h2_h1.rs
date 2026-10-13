//! Layer 7 HTTP/2 to HTTP/1.1 Client-Streaming Bridge Pipeline.
//!
//! Handles client-streaming uploads (large file uploads, multi-frame data ingestion)
//! by transforming incoming HTTP/2 DATA frames into RFC 9112 `Transfer-Encoding: chunked`
//! byte streams towards the HTTP/1.1 upstream backend.

use std::fmt::Write as _;
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
    mut head: Http2ServerRequestHead,
    mut receiver: Http2StreamReceiver,
    mut responder: Http2Responder,
    meta: Arc<DownstreamMeta>,
    upstream: &Arc<Http1Upstream>,
) {
    // 1. Ensure mandatory Host header (RFC 9112 §7.1)
    if !head.headers.contains_key(http::header::HOST)
        && let Some(host) = velda_http2::server::header::extract_host(&head.headers, &head.uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        head.headers.insert(http::header::HOST, hv);
    }

    // 2. Enrich proxy forwarding headers
    velda_http2::server::header::enrich_headers_precomputed(
        &mut head.headers,
        &head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
        meta.is_tls,
    );

    // 3. Mark request as Chunked transfer encoding towards HTTP/1.1
    head.headers
        .insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
    head.headers.remove(CONTENT_LENGTH);

    // 4. Acquire pooled HTTP/1.1 upstream connection
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

    // 5. Ingest first chunk (coalesced) and send HTTP/1.1 request head in a fused buffer
    let (first_chunk, is_fin) = match receiver.recv_coalesced_chunk(65_536).await {
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

    let first_len = first_chunk.as_ref().map(|c| c.len()).unwrap_or(0);
    let mut write_buf = bytes::BytesMut::with_capacity(1024 + first_len + 64);

    // Encode HTTP/1.1 request head
    velda_http1::client::encode_request_head(
        &head.method,
        &head.uri,
        &head.headers,
        None,
        &mut write_buf,
    );

    if let Some(chunk) = first_chunk
        && !chunk.is_empty()
    {
        let _ = write!(write_buf, "{:x}\r\n", chunk.len());
        write_buf.extend_from_slice(&chunk);
        write_buf.extend_from_slice(b"\r\n");
    }

    if is_fin {
        write_buf.extend_from_slice(b"0\r\n\r\n");
    }

    if let Err(e) = lease.write_all(&write_buf).await {
        lease.mark_closed();
        tracing::warn!(error = %e, upstream = %upstream.id(), "Failed to send chunked request head and initial payload");
        let err_resp = L7Response::from_bytes(
            StatusCode::BAD_GATEWAY,
            format!("502 Bad Gateway: {e}\n").into_bytes(),
        );
        let _ = responder.send_response(&err_resp);
        return;
    }

    // 6. Progressively stream any remaining client DATA frames to HTTP/1.1 chunked stream
    if !is_fin {
        let mut chunk_buf = bytes::BytesMut::with_capacity(65_536 + 64);
        loop {
            match receiver.recv_coalesced_chunk(65_536).await {
                Ok(Some(chunk)) => {
                    if !chunk.is_empty() {
                        chunk_buf.clear();
                        let _ = write!(chunk_buf, "{:x}\r\n", chunk.len());
                        chunk_buf.extend_from_slice(&chunk);
                        chunk_buf.extend_from_slice(b"\r\n");
                        if lease.write_all(&chunk_buf).await.is_err() {
                            lease.mark_closed();
                            tracing::warn!("Failed writing chunk to HTTP/1.1 backend");
                            let err_resp = L7Response::from_bytes(
                                StatusCode::BAD_GATEWAY,
                                b"502 Bad Gateway: upstream stream broken\n".to_vec(),
                            );
                            let _ = responder.send_response(&err_resp);
                            return;
                        }
                    }
                    if receiver.is_end_stream() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    lease.mark_closed();
                    tracing::warn!(error = %e, "Client disconnected during H2 upload");
                    return;
                }
            }
        }

        // Conclude chunked stream with terminal chunk 0\r\n\r\n
        if lease.write_all(b"0\r\n\r\n").await.is_err() {
            lease.mark_closed();
            tracing::warn!("Failed concluding HTTP/1.1 chunked stream");
            let err_resp = L7Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                b"502 Bad Gateway: failed to complete chunked upload\n".to_vec(),
            );
            let _ = responder.send_response(&err_resp);
            return;
        }
    }

    // 7. Read HTTP/1.1 response
    let mut read_buf = bytes::BytesMut::with_capacity(65_536);
    loop {
        match lease.read_buf(&mut read_buf).await {
            Ok(0) => {
                if let Ok(Some(resp)) =
                    velda_http1::client::decode_response(&mut read_buf, &upstream.config)
                {
                    if check_connection_close(&resp.headers) {
                        lease.mark_closed();
                    }
                    let l7_resp = resp.into_l7_response();
                    let _ = responder.send_response(&l7_resp);
                } else {
                    lease.mark_closed();
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        b"502 Bad Gateway: upstream closed connection prematurely\n".to_vec(),
                    );
                    let _ = responder.send_response(&err_resp);
                }
                break;
            }
            Ok(_) => match velda_http1::client::decode_response(&mut read_buf, &upstream.config) {
                Ok(Some(resp)) => {
                    if check_connection_close(&resp.headers) {
                        lease.mark_closed();
                    }
                    let l7_resp = resp.into_l7_response();
                    let _ = responder.send_response(&l7_resp);
                    break;
                }
                Ok(None) => continue,
                Err(e) => {
                    lease.mark_closed();
                    tracing::warn!(error = %e, "Failed to decode upstream HTTP/1.1 response");
                    let err_resp = L7Response::from_bytes(
                        StatusCode::BAD_GATEWAY,
                        format!("502 Bad Gateway: {e}\n").into_bytes(),
                    );
                    let _ = responder.send_response(&err_resp);
                    break;
                }
            },
            Err(e) => {
                lease.mark_closed();
                tracing::warn!(error = %e, "Error reading upstream HTTP/1.1 response");
                let err_resp = L7Response::from_bytes(
                    StatusCode::BAD_GATEWAY,
                    format!("502 Bad Gateway: {e}\n").into_bytes(),
                );
                let _ = responder.send_response(&err_resp);
                break;
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
