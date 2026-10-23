//! Layer 7 HTTP/1.1 to HTTP/1.1 Server-Stream Pipeline.
//!
//! Handles server-streaming RPCs (Server-Sent Events, progressive chunks, LLM tokens)
//! where downstream request body is completely read into RAM first, and response chunks
//! are pumped progressively downstream.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http1`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;

use bytes::Buf;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::Body;
use velda_http1::{
    HeadEncodeOptions, Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequest,
    Http1ServerRequestHead, Http1ServerResponse, Http1ServerResponseHead, read_chunk_sized,
    read_next_chunk, read_response_head, sanitize_headers, send_chunk, send_chunked_end,
    send_request,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a server-streaming HTTP/1.1 transaction forwarded to an HTTP/1.1 backend.
pub async fn serve<IO>(
    conn: &mut Http1ServerConnection<IO>,
    mut req_head: Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<Http1Upstream>,
    meta: &DownstreamMeta,
    plan: &velda_plugin::RoutePluginPlan,
) -> Result<(), Http1Error>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 1. Enrich proxy forwarding headers using pre-computed connection metadata
    velda_http1::server::header::enrich_headers_cached(
        &mut req_head.headers,
        &req_head.uri,
        &meta.client_ip_header,
        &meta.client_port_header,
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
    let mut req = Http1ServerRequest::from_parts(req_head, body);
    sanitize_headers(&mut req.headers);

    // 5. Send request and read response head with self-healing retry on stale pooled connection
    let (mut resp_head, resp_framing) = {
        if conn
            .upstream_read_buf
            .capacity()
            .saturating_sub(conn.upstream_read_buf.len())
            < 4096
        {
            conn.upstream_read_buf.reserve(65536);
        }

        let exchange_attempt = async {
            send_request(&req, &mut *upstream_lease, &mut conn.upstream_write_buf).await?;
            read_response_head(&mut *upstream_lease, &mut conn.upstream_read_buf, &cfg).await
        };

        match exchange_attempt.await {
            Ok(head_and_framing) => head_and_framing,
            Err(e) if upstream_lease.is_reused && e.is_stale_connection() => {
                upstream_lease.mark_closed();
                drop(upstream_lease);

                tracing::debug!(
                    upstream = %upstream.id(),
                    "Stale pooled HTTP/1.1 connection detected; self-healing with fresh connection"
                );

                match upstream.acquire_fresh().await {
                    Ok(mut fresh_lease) => {
                        let retry_res = async {
                            send_request(&req, &mut *fresh_lease, &mut conn.upstream_write_buf)
                                .await?;
                            read_response_head(&mut *fresh_lease, &mut conn.upstream_read_buf, &cfg)
                                .await
                        }
                        .await;

                        match retry_res {
                            Ok(head_and_framing) => {
                                upstream_lease = fresh_lease;
                                head_and_framing
                            }
                            Err(retry_err) => {
                                fresh_lease.mark_closed();
                                return Err(retry_err);
                            }
                        }
                    }
                    Err(fresh_err) => {
                        tracing::warn!(
                            error = %fresh_err,
                            upstream = %upstream.id(),
                            "Failed to acquire fresh connection for self-healing retry"
                        );
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                upstream_lease.mark_closed();
                return Err(e);
            }
        }
    };

    sanitize_headers(&mut resp_head.headers);
    if let Some(req_id) = req.headers.get(&crate::pipeline::X_REQUEST_ID) {
        resp_head
            .headers
            .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
    }

    // Fast-path guard: Bypasses HeaderWorkspace instantiation & plugin dispatch if route has no hooks.
    // HeaderMap::clone() clones atomic pointer handles (Bytes) in O(1) without copying string payloads.
    if let Some(post_hooks) = &plan.post_response_hooks {
        let mut l7_resp = velda_core::L7Response::new(
            resp_head.status,
            resp_head.version,
            std::mem::take(&mut resp_head.headers),
            velda_core::Body::Empty,
        );
        let req_ws = velda_core::HeaderWorkspace::from_headers(req.headers.clone());
        velda_plugin::run_post_response_plugins(post_hooks, &req_ws, &mut l7_resp);
        resp_head.status = l7_resp.status;
        resp_head.headers = l7_resp.headers.into();
    }
    meta.inject_response_headers(&mut resp_head.headers);

    // 6. RFC 9110 §6.4.1 & §9.3.2: HEAD, 1xx, 204, 304 have no body
    let is_head = req.method == http::Method::HEAD;
    let is_no_body_status = resp_head.status.is_informational()
        || resp_head.status == StatusCode::NO_CONTENT
        || resp_head.status == StatusCode::NOT_MODIFIED;

    if is_head || is_no_body_status {
        let resp = Http1ServerResponse::new(
            resp_head.status,
            resp_head.version,
            resp_head.headers,
            Body::Empty,
        );
        conn.send_response(&resp).await?;
        return Ok(());
    }

    // 7. Downstream framing decision:
    // HTTP/1.0 does not support chunked transfer encoding!
    let is_http10 = req.version == http::Version::HTTP_10;
    if is_http10 {
        resp_head.version = http::Version::HTTP_10;
        resp_head.headers.remove(http::header::TRANSFER_ENCODING);
        conn.mark_close();
        let server_head =
            Http1ServerResponseHead::new(resp_head.status, resp_head.version, resp_head.headers);
        conn.send_response_parts(&server_head, &Body::Empty).await?;

        let mut client_disconnected = false;
        match resp_framing {
            Http1BodyFraming::Chunked => {
                while let Some(chunk) =
                    read_next_chunk(&mut *upstream_lease, &mut conn.upstream_read_buf).await?
                {
                    if let Err(e) = conn.send_raw_bytes(&chunk).await {
                        tracing::debug!(
                            error = %e,
                            "Downstream HTTP/1.0 client disconnected during server streaming"
                        );
                        client_disconnected = true;
                        break;
                    }
                }
            }
            Http1BodyFraming::ContentLength(total_len) => {
                let mut remaining = total_len;
                while remaining > 0 {
                    let to_read = remaining.min(65536);
                    match read_chunk_sized(
                        &mut *upstream_lease,
                        &mut conn.upstream_read_buf,
                        to_read,
                    )
                    .await?
                    {
                        Some(chunk) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            if let Err(e) = conn.send_raw_bytes(&chunk).await {
                                tracing::debug!(
                                    error = %e,
                                    "Downstream HTTP/1.0 client disconnected during sized server streaming"
                                );
                                client_disconnected = true;
                                break;
                            }
                        }
                        None => {
                            upstream_lease.mark_closed();
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body: premature connection close".into(),
                            ));
                        }
                    }
                }
            }
            Http1BodyFraming::Empty => {}
        }

        if client_disconnected {
            upstream_lease.mark_closed();
            return Err(Http1Error::ConnectionClosed);
        }
        return Ok(());
    }

    // 8. Pump response chunks downstream with single-write coalescing and client disconnect detection
    let server_head =
        Http1ServerResponseHead::new(resp_head.status, resp_head.version, resp_head.headers);
    let mut client_disconnected = false;

    match resp_framing {
        Http1BodyFraming::ContentLength(total_len) => {
            if total_len <= 65536 && conn.upstream_read_buf.len() < total_len {
                let needed = total_len - conn.upstream_read_buf.len();
                conn.upstream_read_buf.reserve(needed);
                while conn.upstream_read_buf.len() < total_len {
                    let n = (*upstream_lease)
                        .read_buf(&mut conn.upstream_read_buf)
                        .await?;
                    if n == 0 {
                        upstream_lease.mark_closed();
                        return Err(Http1Error::Parse(
                            "Unexpected EOF while reading upstream response body: premature connection close".into(),
                        ));
                    }
                }
            }

            let available = conn.upstream_read_buf.len().min(total_len);
            if available == total_len {
                // [FAST-PATH]: Entire response body is already present in memory.
                // Drop upstream lease immediately before downstream socket write to avoid pool starvation.
                drop(upstream_lease);

                conn.write_buf.clear();
                let force_close = conn.is_closed() || conn.close_requested;
                conn.encode_response_head(
                    server_head.version,
                    server_head.status,
                    &server_head.headers,
                    Some(total_len),
                    force_close,
                );

                if total_len > 0 {
                    let mut b1 = &conn.write_buf[..];
                    let mut b2 = &conn.upstream_read_buf[..total_len];
                    while !b1.is_empty() || !b2.is_empty() {
                        let bufs = [std::io::IoSlice::new(b1), std::io::IoSlice::new(b2)];
                        let bufs_slice = if b1.is_empty() { &bufs[1..] } else { &bufs[..] };
                        let n = conn.stream.write_vectored(bufs_slice).await?;
                        if n == 0 {
                            return Err(velda_http1::Http1Error::ConnectionClosed);
                        }
                        if n < b1.len() {
                            b1 = &b1[n..];
                        } else {
                            let rem = n - b1.len();
                            b1 = &[];
                            b2 = &b2[rem..];
                        }
                    }
                    conn.upstream_read_buf.advance(total_len);
                    if conn.upstream_read_buf.is_empty() {
                        conn.upstream_read_buf.clear();
                    }
                } else {
                    conn.stream.write_all(&conn.write_buf).await?;
                }
                conn.write_buf.clear();
                return Ok(());
            }

            // Normal chunked progressive stream
            conn.send_response_head_chunked(&server_head).await?;

            if available > 0 {
                let first_chunk = &conn.upstream_read_buf[..available];
                if let Err(e) = send_chunk(&mut conn.stream, first_chunk).await {
                    tracing::debug!(
                        error = %e,
                        "Downstream client disconnected while writing initial chunk"
                    );
                    client_disconnected = true;
                }
                conn.upstream_read_buf.advance(available);
                if conn.upstream_read_buf.is_empty() {
                    conn.upstream_read_buf.clear();
                }
            }

            if !client_disconnected {
                let mut remaining = total_len - available;
                while remaining > 0 {
                    let to_read = remaining.min(65536);
                    match read_chunk_sized(
                        &mut *upstream_lease,
                        &mut conn.upstream_read_buf,
                        to_read,
                    )
                    .await?
                    {
                        Some(chunk) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
                                tracing::debug!(
                                    error = %e,
                                    "Downstream client disconnected during Content-Length server streaming"
                                );
                                client_disconnected = true;
                                break;
                            }
                        }
                        None => {
                            upstream_lease.mark_closed();
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body: premature connection close".into(),
                            ));
                        }
                    }
                }
            }
        }
        Http1BodyFraming::Chunked => {
            // [OPPORTUNISTIC CHUNKED COALESCING]:
            // For micro-SSE ping/heartbeats and low-latency chunked streaming, check if the initial
            // chunk or the entire stream is already present in `upstream_read_buf`.
            let mut sent_via_coalesced = false;
            if let Some((wire1, payload1, is_term1)) =
                velda_http1::parse_single_chunk(&conn.upstream_read_buf)?
            {
                sent_via_coalesced = true;
                if is_term1 {
                    conn.upstream_read_buf.advance(wire1);
                    let _close = conn
                        .send_response_head_chunked_coalesced(&server_head, None, true)
                        .await?;
                    return Ok(());
                }

                let (is_terminated, total_wire) =
                    match velda_http1::parse_single_chunk(&conn.upstream_read_buf[wire1..])? {
                        Some((wire2, _, true)) => (true, wire1 + wire2),
                        _ => (false, wire1),
                    };

                let close = velda_http1::send_head_coalesced(
                    &mut conn.stream,
                    &mut conn.write_buf,
                    &server_head,
                    Some(payload1),
                    is_terminated,
                    HeadEncodeOptions {
                        body_len: None,
                        force_close: conn.close_requested,
                        request_id: conn.current_request_id.as_ref(),
                    },
                )
                .await?;
                if close {
                    conn.close_requested = true;
                }
                conn.upstream_read_buf.advance(total_wire);

                if is_terminated {
                    // Entire response was emitted in a single atomic socket write syscall.
                    drop(upstream_lease);
                    if conn.upstream_read_buf.is_empty() {
                        conn.upstream_read_buf.clear();
                    }
                    return Ok(());
                }

                // Stream continues: forward subsequent chunks
                while let Some(chunk) =
                    read_next_chunk(&mut *upstream_lease, &mut conn.upstream_read_buf).await?
                {
                    if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
                        tracing::debug!(
                            error = %e,
                            "Downstream client disconnected during chunked server streaming"
                        );
                        client_disconnected = true;
                        break;
                    }
                }
            }

            if !sent_via_coalesced {
                conn.send_response_head_chunked(&server_head).await?;

                while let Some(chunk) =
                    read_next_chunk(&mut *upstream_lease, &mut conn.upstream_read_buf).await?
                {
                    if let Err(e) = send_chunk(&mut conn.stream, &chunk).await {
                        tracing::debug!(
                            error = %e,
                            "Downstream client disconnected during chunked server streaming"
                        );
                        client_disconnected = true;
                        break;
                    }
                }
            }
        }
        Http1BodyFraming::Empty => {
            conn.send_response_head_chunked(&server_head).await?;
        }
    }

    if client_disconnected {
        upstream_lease.mark_closed();
        return Err(Http1Error::ConnectionClosed);
    }

    drop(upstream_lease);
    if conn.upstream_read_buf.is_empty() {
        conn.upstream_read_buf.clear();
    }

    send_chunked_end(&mut conn.stream).await?;
    Ok(())
}
