//! Layer 7 HTTP/2 to HTTP/1.1 Duplex Streaming Bridge Pipeline.
//!
//! Handles full bidirectional streaming (duplex) between a downstream HTTP/2 client
//! and an HTTP/1.1 backend, simultaneously uploading chunked data and streaming chunked
//! responses downstream with cancellation propagation.

use std::sync::Arc;

use http::StatusCode;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, TRANSFER_ENCODING};
use tokio::io::AsyncWriteExt;
use velda_core::L7Response;
use velda_http1::server::request::Http1BodyFraming;
use velda_http2::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a full-duplex HTTP/2 downstream stream bridged to an HTTP/1.1 backend.
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

    // 3. Mark request as Chunked transfer encoding towards HTTP/1.1
    req_head
        .headers
        .insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
    req_head.headers.remove(CONTENT_LENGTH);

    // 4. Acquire pooled HTTP/1.1 upstream connection
    let mut lease = match upstream.acquire().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                error = %e,
                upstream = %upstream.id(),
                "Failed to acquire upstream HTTP/1.1 connection for duplex bridge"
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

    let (upstream_stream, read_buf, write_buf) = lease.split_all();
    write_buf.clear();
    if write_buf.capacity() < 1024 {
        write_buf.reserve(1024 - write_buf.capacity());
    }

    // 5. Send HTTP/1.1 request head
    velda_http1::client::encode_request_head(
        &req_head.method,
        &req_head.uri,
        &req_head.headers,
        None,
        write_buf,
    );

    if let Err(e) = upstream_stream.write_all(write_buf).await {
        lease.mark_closed();
        tracing::warn!(error = %e, upstream = %upstream.id(), "Failed to send duplex request head");
        let err_resp = L7Response::from_bytes(
            StatusCode::BAD_GATEWAY,
            format!("502 Bad Gateway: {e}\n").into_bytes(),
        );
        let _ = responder.send_response(&err_resp);
        return;
    }

    // 6. Split HTTP/1.1 stream into read and write halves for concurrent duplex execution
    let (mut read_half, mut write_half) = tokio::io::split(upstream_stream);

    // Upload task: pipes downstream H2 DATA frames into upstream H1 chunked encoding
    let coalesce_limit = upstream.config.client_body_buffer_size;
    let upload_fut = async {
        loop {
            match receiver.recv_coalesced_chunk(coalesce_limit).await {
                Ok(Some(chunk)) => {
                    if !chunk.is_empty()
                        && velda_http1::client::send_chunk_vectored(&mut write_half, &chunk)
                            .await
                            .is_err()
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "failed to write chunk to upstream",
                        ));
                    }
                    if receiver.is_end_stream() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, e));
                }
            }
        }
        write_half.write_all(b"0\r\n\r\n").await?;
        write_half.flush().await?;
        Ok::<(), std::io::Error>(())
    };

    // Download task: reads upstream H1 response and streams H2 DATA frames downstream
    let download_fut = async {
        read_buf.clear();
        let (mut resp_head, framing) =
            velda_http1::client::read_response_head(&mut read_half, read_buf, &upstream.config)
                .await
                .map_err(std::io::Error::other)?;

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

        let mut stream_sender = responder
            .send_stream_response_owned(resp_head.status, clean_headers)
            .map_err(std::io::Error::other)?;

        match framing {
            Http1BodyFraming::Empty => {}
            Http1BodyFraming::Chunked => loop {
                match velda_http1::client::read_next_chunk(&mut read_half, read_buf).await {
                    Ok(Some(chunk)) => {
                        stream_sender
                            .send_chunk(chunk)
                            .await
                            .map_err(std::io::Error::other)?;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(std::io::Error::other(e));
                    }
                }
            },
            Http1BodyFraming::ContentLength(total_len) => {
                let mut remaining = total_len;
                let chunk_limit = upstream.config.client_body_buffer_size;
                while remaining > 0 {
                    let to_read = remaining.min(chunk_limit);
                    match velda_http1::client::read_chunk_sized(&mut read_half, read_buf, to_read)
                        .await
                    {
                        Ok(Some(chunk)) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            stream_sender
                                .send_chunk(chunk)
                                .await
                                .map_err(std::io::Error::other)?;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            return Err(std::io::Error::other(e));
                        }
                    }
                }
            }
        }

        let _ = stream_sender.finish();
        Ok::<(), std::io::Error>(())
    };

    // Concurrently drive upload and download, completing as soon as download terminates
    let failed = {
        let mut upload_pinned = std::pin::pin!(upload_fut);
        let mut download_pinned = std::pin::pin!(download_fut);

        tokio::select! {
            res = &mut download_pinned => res.is_err(),
            res = &mut upload_pinned => {
                if res.is_err() {
                    true
                } else {
                    download_pinned.await.is_err()
                }
            }
        }
    };

    if failed {
        lease.mark_closed();
        tracing::warn!(upstream = %upstream.id(), "Duplex H2 to H1 bridge encountered error");
    }
}
