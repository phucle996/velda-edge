//! Layer 7 HTTP/1.1 to HTTP/1.1 Client-Stream Pipeline.
//!
//! Handles client streaming uploads (e.g. chunked file uploads, log batch ingest) where
//! downstream request chunks are streamed in real-time to the upstream backend.
//! Leases upstream connection prior to transmission.
//!
//! In accordance with Velda Edge Hourglass Architecture:
//! Protocol crates (`velda-http1`) strictly provide primitives ("WHAT"),
//! while this edge pipeline module owns the end-to-end execution workflow ("HOW").

use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::Body;
use velda_http1::{
    Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse, ProgressiveChunkDecoder, encode_chunk, read_response_head,
    sanitize_headers, send_chunk, send_chunked_end, send_request,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a client-streaming HTTP/1.1 transaction forwarded to an HTTP/1.1 backend.
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

    // 3. Acquire upstream connection before streaming upload begins
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
    sanitize_headers(&mut req_head.headers);
    let req_id = req_head
        .headers
        .get(&crate::pipeline::X_REQUEST_ID)
        .cloned();

    // 4. If request has no body (e.g. GET, HEAD), forward directly without chunked stream
    if framing == Http1BodyFraming::Empty {
        let req = velda_http1::Http1ServerRequest::new(
            req_head.method,
            req_head.uri,
            req_head.version,
            req_head.headers,
            Body::Empty,
        );

        let res = async {
            send_request(&req, &mut *upstream_lease, &mut conn.upstream_write_buf).await?;
            let (mut resp_head, resp_framing) = read_response_head(
                &mut *upstream_lease,
                &mut conn.upstream_read_buf,
                &cfg,
            )
            .await?;
            sanitize_headers(&mut resp_head.headers);
            if let Some(ref req_id) = req_id {
                resp_head
                    .headers
                    .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
            }

            if let Some(post_hooks) = &plan.post_response_hooks {
                let mut l7_resp = velda_core::L7Response::new(
                    resp_head.status,
                    resp_head.version,
                    std::mem::take(&mut resp_head.headers),
                    velda_core::Body::Empty,
                );
                velda_plugin::run_post_response_plugins(post_hooks, &req.headers, &mut l7_resp);
                resp_head.status = l7_resp.status;
                resp_head.headers = l7_resp.headers;
            }
            meta.inject_response_headers(&mut resp_head.headers);

            let is_head = req.method == http::Method::HEAD;
            let is_no_body_status = resp_head.status.is_informational()
                || resp_head.status == StatusCode::NO_CONTENT
                || resp_head.status == StatusCode::NOT_MODIFIED;

            if is_head || is_no_body_status {
                conn.write_buf.clear();
                conn.encode_response_head(
                    resp_head.version,
                    resp_head.status,
                    &resp_head.headers,
                    Some(0),
                    conn.close_requested,
                );
                conn.stream.write_all(&conn.write_buf).await?;
                if resp_head
                    .headers
                    .get(http::header::CONNECTION)
                    .and_then(|h| h.to_str().ok())
                    .is_some_and(|s| s.eq_ignore_ascii_case("close"))
                {
                    conn.close_requested = true;
                }
                return Ok::<(), Http1Error>(());
            }

            match resp_framing {
                Http1BodyFraming::Empty => {
                    conn.write_buf.clear();
                    conn.encode_response_head(
                        resp_head.version,
                        resp_head.status,
                        &resp_head.headers,
                        Some(0),
                        conn.close_requested,
                    );
                    conn.stream.write_all(&conn.write_buf).await?;
                    if resp_head
                        .headers
                        .get(http::header::CONNECTION)
                        .and_then(|h| h.to_str().ok())
                        .is_some_and(|s| s.eq_ignore_ascii_case("close"))
                    {
                        conn.close_requested = true;
                    }
                }
                Http1BodyFraming::ContentLength(len) => {
                    if len > cfg.max_body_size {
                        return Err(Http1Error::PayloadTooLarge(len));
                    }
                    if conn.upstream_read_buf.len() < len {
                        let needed = len - conn.upstream_read_buf.len();
                        conn.upstream_read_buf.reserve(needed);
                        while conn.upstream_read_buf.len() < len {
                            let n = upstream_lease
                                .read_buf(&mut conn.upstream_read_buf)
                                .await?;
                            if n == 0 {
                                return Err(Http1Error::Parse(
                                    "Unexpected EOF while reading upstream response body: premature connection close".into(),
                                ));
                            }
                        }
                    }
                    conn.write_buf.clear();
                    conn.encode_response_head(
                        resp_head.version,
                        resp_head.status,
                        &resp_head.headers,
                        Some(len),
                        conn.close_requested,
                    );
                    if len > 0 {
                        conn.write_buf.extend_from_slice(&conn.upstream_read_buf[..len]);
                        conn.upstream_read_buf.advance(len);
                    }
                    conn.stream.write_all(&conn.write_buf).await?;
                    if resp_head
                        .headers
                        .get(http::header::CONNECTION)
                        .and_then(|h| h.to_str().ok())
                        .is_some_and(|s| s.eq_ignore_ascii_case("close"))
                    {
                        conn.close_requested = true;
                    }
                }
                Http1BodyFraming::Chunked => {
                    return Err(Http1Error::StreamingViolation(
                        "Upstream returned chunked transfer-encoding on client-streaming route with server streaming disabled".into(),
                    ));
                }
            }
            Ok::<(), Http1Error>(())
        }
        .await;

        if res.is_err() {
            upstream_lease.mark_closed();
        }
        return res;
    }

    // 5. Pipe streaming upload chunks to upstream
    let pipe_res = async {
        // Encode chunked request head into upstream_write_buf
        velda_http1::encode_request_head_chunked(&req_head, &mut conn.upstream_write_buf);

        let mut total_body_bytes = 0usize;
        let idle_timeout = Duration::from_millis(cfg.idle_timeout_ms);
        let target_reserve = cfg.client_body_buffer_size.max(65536);
        let mut sent_terminal = false;
        let mut decoder = ProgressiveChunkDecoder::new();

        // Opportunistically encode any already-buffered chunk payload into the initial flight buffer
        while let Some(slice) = decoder.decode_step(&mut conn.read_buf)? {
            total_body_bytes += slice.len();
            if total_body_bytes > cfg.max_body_size {
                return Err(Http1Error::PayloadTooLarge(total_body_bytes));
            }
            encode_chunk(&slice, &mut conn.upstream_write_buf);
            if conn.upstream_write_buf.len() >= 65536 {
                break;
            }
        }

        if decoder.is_done() {
            velda_http1::encode_chunked_end(&mut conn.upstream_write_buf);
            sent_terminal = true;
        }

        // Single atomic write combining request head and initial chunks
        upstream_lease
            .write_all(&conn.upstream_write_buf)
            .await
            .map_err(Http1Error::Io)?;
        conn.upstream_write_buf.clear();

        // If terminal chunk was not in the initial buffer, progressively stream remaining chunks
        if !sent_terminal {
            loop {
                // Drain and dispatch all available chunk payload slices currently in memory
                while let Some(slice) = decoder.decode_step(&mut conn.read_buf)? {
                    total_body_bytes = total_body_bytes
                        .checked_add(slice.len())
                        .ok_or(Http1Error::PayloadTooLarge(usize::MAX))?;
                    if total_body_bytes > cfg.max_body_size {
                        return Err(Http1Error::PayloadTooLarge(total_body_bytes));
                    }
                    send_chunk(&mut *upstream_lease, &slice).await?;
                }

                if decoder.is_done() {
                    sent_terminal = true;
                    break;
                }

                if conn.read_buf.capacity().saturating_sub(conn.read_buf.len()) < target_reserve {
                    conn.read_buf.reserve(target_reserve);
                }

                let bytes_read = match tokio::time::timeout(
                    idle_timeout,
                    conn.stream.read_buf(&mut conn.read_buf),
                )
                .await
                {
                    Ok(res) => res?,
                    Err(_) => return Err(Http1Error::Timeout),
                };

                if bytes_read == 0 {
                    if decoder.is_done() {
                        sent_terminal = true;
                        break;
                    } else {
                        return Err(Http1Error::Parse(
                            "Unexpected EOF while reading chunked body".into(),
                        ));
                    }
                }
            }

            if sent_terminal {
                send_chunked_end(&mut *upstream_lease).await?;
            }
        }

        // Read upstream response head
        let (mut resp_head, resp_framing) = read_response_head(
            &mut *upstream_lease,
            &mut conn.upstream_read_buf,
            &cfg,
        )
        .await?;
        sanitize_headers(&mut resp_head.headers);
        if let Some(ref req_id) = req_id {
            resp_head
                .headers
                .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
        }

        if let Some(post_hooks) = &plan.post_response_hooks {
            let mut l7_resp = velda_core::L7Response::new(
                resp_head.status,
                resp_head.version,
                std::mem::take(&mut resp_head.headers),
                velda_core::Body::Empty,
            );
            velda_plugin::run_post_response_plugins(post_hooks, &req_head.headers, &mut l7_resp);
            resp_head.status = l7_resp.status;
            resp_head.headers = l7_resp.headers;
        }
        meta.inject_response_headers(&mut resp_head.headers);

        // Validate response framing (client streaming expects non-streaming response)
        if resp_framing == Http1BodyFraming::Chunked {
            return Err(Http1Error::StreamingViolation(
                "Upstream returned chunked transfer-encoding on client-streaming route with server streaming disabled".into(),
            ));
        }

        let is_head = req_head.method == http::Method::HEAD;
        let is_no_body_status = resp_head.status.is_informational()
            || resp_head.status == StatusCode::NO_CONTENT
            || resp_head.status == StatusCode::NOT_MODIFIED;

        if is_head || is_no_body_status {
            conn.write_buf.clear();
            conn.encode_response_head(
                resp_head.version,
                resp_head.status,
                &resp_head.headers,
                Some(0),
                conn.close_requested,
            );
            conn.stream.write_all(&conn.write_buf).await?;
            if resp_head
                .headers
                .get(http::header::CONNECTION)
                .and_then(|h| h.to_str().ok())
                .is_some_and(|s| s.eq_ignore_ascii_case("close"))
            {
                conn.close_requested = true;
            }
            return Ok::<(), Http1Error>(());
        }

        match resp_framing {
            Http1BodyFraming::Empty => {
                conn.write_buf.clear();
                conn.encode_response_head(
                    resp_head.version,
                    resp_head.status,
                    &resp_head.headers,
                    Some(0),
                    conn.close_requested,
                );
                conn.stream.write_all(&conn.write_buf).await?;
                if resp_head
                    .headers
                    .get(http::header::CONNECTION)
                    .and_then(|h| h.to_str().ok())
                    .is_some_and(|s| s.eq_ignore_ascii_case("close"))
                {
                    conn.close_requested = true;
                }
            }
            Http1BodyFraming::ContentLength(len) => {
                if len > cfg.max_body_size {
                    return Err(Http1Error::PayloadTooLarge(len));
                }
                if conn.upstream_read_buf.len() < len {
                    let needed = len - conn.upstream_read_buf.len();
                    conn.upstream_read_buf.reserve(needed);
                    while conn.upstream_read_buf.len() < len {
                        let n = upstream_lease
                            .read_buf(&mut conn.upstream_read_buf)
                            .await?;
                        if n == 0 {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body: premature connection close".into(),
                            ));
                        }
                    }
                }
                conn.write_buf.clear();
                conn.encode_response_head(
                    resp_head.version,
                    resp_head.status,
                    &resp_head.headers,
                    Some(len),
                    conn.close_requested,
                );
                if len > 0 {
                    conn.write_buf.extend_from_slice(&conn.upstream_read_buf[..len]);
                    conn.upstream_read_buf.advance(len);
                }
                conn.stream.write_all(&conn.write_buf).await?;
                if resp_head
                    .headers
                    .get(http::header::CONNECTION)
                    .and_then(|h| h.to_str().ok())
                    .is_some_and(|s| s.eq_ignore_ascii_case("close"))
                {
                    conn.close_requested = true;
                }
            }
            Http1BodyFraming::Chunked => unreachable!(),
        }
        Ok::<(), Http1Error>(())
    }
    .await;

    if pipe_res.is_err() {
        upstream_lease.mark_closed();
    }

    pipe_res
}
