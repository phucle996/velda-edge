//! Layer 7 HTTP/1.1 to HTTP/1.1 Duplex Pipeline.
//!
//! Handles concurrent full-duplex bidirectional streaming (WebSocket upgrade, tunnels)
//! between downstream HTTP/1.1 client and upstream HTTP/1.1 backend.
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
    HeadEncodeOptions, Http1BodyFraming, Http1Error, Http1ServerConnection, Http1ServerRequestHead,
    Http1ServerResponse, Http1ServerResponseHead, parse_single_chunk, read_chunk_sized,
    read_next_chunk, read_response_head, sanitize_headers, send_chunk, send_chunked_end,
};

use crate::pipeline::DownstreamMeta;
use crate::upstream::Http1Upstream;

/// Serves a full-duplex HTTP/1.1 transaction forwarded to an HTTP/1.1 backend.
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

    // 3. Acquire upstream connection before duplex streaming begins
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
    let req_method = req_head.method.clone();

    let idle_timeout = Duration::from_millis(cfg.idle_timeout_ms);
    let target_reserve = cfg.client_body_buffer_size.max(262144);
    let max_body_size = cfg.max_body_size;

    // Fast-path: If downstream request body is already fully buffered in memory,
    // dispatch request to upstream and stream response downstream directly on conn without tokio::io::split overhead.
    let mut chunked_coalesced_wire_len = 0usize;
    let is_fully_buffered_request = match framing {
        Http1BodyFraming::Empty => true,
        Http1BodyFraming::ContentLength(total_len) => conn.read_buf.len() >= total_len,
        Http1BodyFraming::Chunked => {
            let mut wire_len = 0usize;
            let mut has_term = false;
            while wire_len < conn.read_buf.len() {
                match parse_single_chunk(&conn.read_buf[wire_len..]) {
                    Ok(Some((needed, _, is_term))) => {
                        wire_len += needed;
                        if is_term {
                            has_term = true;
                            chunked_coalesced_wire_len = wire_len;
                            break;
                        }
                    }
                    _ => break,
                }
            }
            has_term
        }
    };

    if is_fully_buffered_request {
        match framing {
            Http1BodyFraming::Empty => {
                velda_http1::encode_request_head(
                    &req_head.method,
                    &req_head.uri,
                    &req_head.headers,
                    Some(0),
                    &mut conn.upstream_write_buf,
                );
            }
            Http1BodyFraming::ContentLength(total_len) => {
                velda_http1::encode_request_head(
                    &req_head.method,
                    &req_head.uri,
                    &req_head.headers,
                    Some(total_len),
                    &mut conn.upstream_write_buf,
                );
                if total_len > 0 {
                    conn.upstream_write_buf
                        .extend_from_slice(&conn.read_buf[..total_len]);
                    conn.read_buf.advance(total_len);
                }
            }
            Http1BodyFraming::Chunked => {
                velda_http1::encode_request_head_chunked(&req_head, &mut conn.upstream_write_buf);
                if chunked_coalesced_wire_len > 0 {
                    conn.upstream_write_buf
                        .extend_from_slice(&conn.read_buf[..chunked_coalesced_wire_len]);
                    conn.read_buf.advance(chunked_coalesced_wire_len);
                }
            }
        }

        upstream_lease.write_all(&conn.upstream_write_buf).await?;
        conn.upstream_write_buf.clear();

        let (mut resp_head, resp_framing) =
            read_response_head(&mut *upstream_lease, &mut conn.upstream_read_buf, &cfg).await?;
        sanitize_headers(&mut resp_head.headers);
        if let Some(req_id) = req_head.headers.get(&crate::pipeline::X_REQUEST_ID) {
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

        let is_head = req_method == http::Method::HEAD;
        let is_no_body = is_head
            || resp_head.status.is_informational()
            || resp_head.status == StatusCode::NO_CONTENT
            || resp_head.status == StatusCode::NOT_MODIFIED;

        let server_head =
            Http1ServerResponseHead::new(resp_head.status, resp_head.version, resp_head.headers);

        if is_no_body {
            let close = velda_http1::server::response::send_response_parts(
                &mut conn.stream,
                &mut conn.write_buf,
                &server_head,
                &Body::Empty,
                conn.close_requested,
            )
            .await?;
            if close {
                conn.close_requested = true;
            }
            return Ok(());
        }

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
                                "Unexpected EOF while reading upstream response body".into(),
                            ));
                        }
                    }
                }
                let available = conn.upstream_read_buf.len().min(total_len);
                if available == total_len {
                    // [FAST-PATH]: Entire response body is already present in memory.
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

                conn.send_response_head_chunked(&server_head).await?;
                if available > 0 {
                    let first_chunk = conn.upstream_read_buf.split_to(available).freeze();
                    send_chunk(&mut conn.stream, &first_chunk).await?;
                }
                let mut remaining = total_len - available;
                while remaining > 0 {
                    let to_read = remaining.min(262144);
                    match read_chunk_sized(
                        &mut *upstream_lease,
                        &mut conn.upstream_read_buf,
                        to_read,
                    )
                    .await?
                    {
                        Some(chunk) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            send_chunk(&mut conn.stream, &chunk).await?;
                        }
                        None => {
                            upstream_lease.mark_closed();
                            return Err(Http1Error::Parse("Unexpected EOF".into()));
                        }
                    }
                }
                send_chunked_end(&mut conn.stream).await?;
                return Ok(());
            }
            Http1BodyFraming::Chunked => {
                if let Some((wire1, payload1, is_term1)) =
                    velda_http1::parse_single_chunk(&conn.upstream_read_buf)?
                {
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
                        return Ok(());
                    }
                }

                conn.send_response_head_chunked(&server_head).await?;
                while let Some(chunk) =
                    read_next_chunk(&mut *upstream_lease, &mut conn.upstream_read_buf).await?
                {
                    send_chunk(&mut conn.stream, &chunk).await?;
                }
                send_chunked_end(&mut conn.stream).await?;
                return Ok(());
            }
            Http1BodyFraming::Empty => {
                let close = velda_http1::server::response::send_response_parts(
                    &mut conn.stream,
                    &mut conn.write_buf,
                    &server_head,
                    &Body::Empty,
                    conn.close_requested,
                )
                .await?;
                if close {
                    conn.close_requested = true;
                }
                return Ok(());
            }
        }
    }

    let (mut down_read, mut down_write) = tokio::io::split(&mut conn.stream);
    let (mut up_read, mut up_write) = tokio::io::split(&mut *upstream_lease);

    let Http1ServerConnection {
        ref mut read_buf,
        ref mut write_buf,
        ref mut upstream_read_buf,
        ref mut upstream_write_buf,
        ref mut close_requested,
        ..
    } = *conn;

    // Asymmetric coordination channel: Signals upload task when upstream response is fully received
    let (resp_done_tx, mut resp_done_rx) = tokio::sync::oneshot::channel::<()>();
    let upload_finished = std::sync::atomic::AtomicBool::new(framing == Http1BodyFraming::Empty);
    let req_id = req_head
        .headers
        .get(&crate::pipeline::X_REQUEST_ID)
        .cloned();

    // Concurrently pump client upload to upstream and upstream response downstream.
    let upload_task = async {
        let mut early_response = false;

        match framing {
            Http1BodyFraming::Chunked => {
                // Encode chunked request head into upstream_write_buf
                velda_http1::encode_request_head_chunked(&req_head, upstream_write_buf);

                let mut total_req_bytes = 0usize;
                let mut sent_terminal = false;

                // Opportunistically coalesce already-received chunks into initial write buffer
                let mut coalesced_wire_len = 0usize;
                let mut coalesced_payload_bytes = 0usize;
                while coalesced_wire_len < read_buf.len() {
                    match parse_single_chunk(&read_buf[coalesced_wire_len..])? {
                        Some((needed, payload, is_terminal)) => {
                            coalesced_wire_len += needed;
                            coalesced_payload_bytes += payload.len();
                            if is_terminal {
                                sent_terminal = true;
                                break;
                            }
                        }
                        None => break,
                    }
                }

                if coalesced_wire_len > 0 {
                    total_req_bytes += coalesced_payload_bytes;
                    if total_req_bytes > max_body_size {
                        return Err(Http1Error::PayloadTooLarge(total_req_bytes));
                    }
                    upstream_write_buf.extend_from_slice(&read_buf[..coalesced_wire_len]);
                    let remaining_len = read_buf.len() - coalesced_wire_len;
                    if remaining_len == 0 {
                        read_buf.clear();
                    } else {
                        read_buf.advance(coalesced_wire_len);
                    }
                }

                // Send request head + initial chunks in a single coalesced write
                up_write.write_all(upstream_write_buf).await?;
                upstream_write_buf.clear();

                if sent_terminal {
                    upload_finished.store(true, std::sync::atomic::Ordering::Release);
                } else {
                    loop {
                        if !early_response && resp_done_rx.try_recv().is_ok() {
                            early_response = true;
                        }

                        // Coalesce as many complete valid chunks as possible from read_buf
                        let mut wire_len = 0usize;
                        let mut payload_bytes = 0usize;
                        let mut reached_terminal = false;

                        while wire_len < read_buf.len() {
                            match parse_single_chunk(&read_buf[wire_len..])? {
                                Some((needed, payload, is_terminal)) => {
                                    wire_len += needed;
                                    payload_bytes += payload.len();
                                    if is_terminal {
                                        reached_terminal = true;
                                        break;
                                    }
                                }
                                None => break,
                            }
                        }

                        if wire_len > 0 {
                            total_req_bytes = total_req_bytes
                                .checked_add(payload_bytes)
                                .ok_or(Http1Error::PayloadTooLarge(usize::MAX))?;
                            if total_req_bytes > max_body_size {
                                return Err(Http1Error::PayloadTooLarge(total_req_bytes));
                            }

                            if !early_response {
                                tokio::select! {
                                    biased;
                                    _ = &mut resp_done_rx => {
                                        early_response = true;
                                    }
                                    write_res = up_write.write_all(&read_buf[..wire_len]) => {
                                        write_res?;
                                    }
                                }
                            }

                            let remaining_len = read_buf.len() - wire_len;
                            if remaining_len == 0 {
                                read_buf.clear();
                            } else {
                                read_buf.advance(wire_len);
                            }

                            if reached_terminal {
                                sent_terminal = true;
                                break;
                            }
                            continue;
                        }

                        if read_buf.capacity() - read_buf.len() < target_reserve {
                            read_buf.reserve(target_reserve);
                        }

                        let bytes_read = if !early_response {
                            tokio::select! {
                                biased;
                                _ = &mut resp_done_rx => {
                                    early_response = true;
                                    break;
                                }
                                res = tokio::time::timeout(idle_timeout, down_read.read_buf(read_buf)) => {
                                    match res {
                                        Ok(res) => res?,
                                        Err(_) => return Err(Http1Error::Timeout),
                                    }
                                }
                            }
                        } else {
                            break;
                        };

                        if bytes_read == 0 {
                            if read_buf.is_empty() {
                                break;
                            } else {
                                return Err(Http1Error::Parse(
                                    "Unexpected EOF while reading chunked body".into(),
                                ));
                            }
                        }
                    }

                    if sent_terminal {
                        upload_finished.store(true, std::sync::atomic::Ordering::Release);
                    }
                    if !early_response {
                        if !sent_terminal {
                            send_chunked_end(&mut up_write).await?;
                        }
                        up_write.flush().await?;
                    }
                }
            }
            Http1BodyFraming::ContentLength(total_len) => {
                velda_http1::encode_request_head(
                    &req_head.method,
                    &req_head.uri,
                    &req_head.headers,
                    Some(total_len),
                    upstream_write_buf,
                );

                let mut remaining = total_len;
                let initial_avail = read_buf.len().min(remaining);
                if initial_avail > 0 {
                    upstream_write_buf.extend_from_slice(&read_buf[..initial_avail]);
                    read_buf.advance(initial_avail);
                    remaining -= initial_avail;
                }

                up_write.write_all(upstream_write_buf).await?;
                upstream_write_buf.clear();

                while remaining > 0 {
                    if !early_response && resp_done_rx.try_recv().is_ok() {
                        early_response = true;
                    }

                    let available = read_buf.len().min(remaining);
                    if available > 0 {
                        if !early_response {
                            tokio::select! {
                                biased;
                                _ = &mut resp_done_rx => {
                                    early_response = true;
                                }
                                write_res = up_write.write_all(&read_buf[..available]) => {
                                    write_res?;
                                }
                            }
                        }
                        read_buf.advance(available);
                        remaining -= available;
                        if remaining == 0 {
                            break;
                        }
                        continue;
                    }

                    if read_buf.capacity() - read_buf.len() < target_reserve {
                        read_buf.reserve(target_reserve);
                    }

                    let bytes_read = if !early_response {
                        tokio::select! {
                            biased;
                            _ = &mut resp_done_rx => {
                                early_response = true;
                                break;
                            }
                            res = tokio::time::timeout(idle_timeout, down_read.read_buf(read_buf)) => {
                                match res {
                                    Ok(res) => res?,
                                    Err(_) => return Err(Http1Error::Timeout),
                                }
                            }
                        }
                    } else {
                        break;
                    };

                    if bytes_read == 0 {
                        break;
                    }
                }

                if remaining == 0 {
                    upload_finished.store(true, std::sync::atomic::Ordering::Release);
                }
                if !early_response {
                    up_write.flush().await?;
                }
            }
            Http1BodyFraming::Empty => {
                velda_http1::encode_request_head(
                    &req_head.method,
                    &req_head.uri,
                    &req_head.headers,
                    Some(0),
                    upstream_write_buf,
                );
                up_write.write_all(upstream_write_buf).await?;
                up_write.flush().await?;
                upstream_write_buf.clear();
            }
        }

        Ok::<bool, Http1Error>(early_response)
    };

    let download_task = async {
        // Read upstream response head reusing connection read buffer
        let (mut resp_head, resp_framing) =
            read_response_head(&mut up_read, upstream_read_buf, &cfg).await?;
        sanitize_headers(&mut resp_head.headers);
        if let Some(ref rid) = req_id {
            resp_head
                .headers
                .insert(crate::pipeline::X_REQUEST_ID, rid.clone());
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

        let is_head = req_method == http::Method::HEAD;
        let is_no_body_status = resp_head.status.is_informational()
            || resp_head.status == StatusCode::NO_CONTENT
            || resp_head.status == StatusCode::NOT_MODIFIED;

        if is_head || is_no_body_status {
            let server_head = Http1ServerResponseHead::new(
                resp_head.status,
                resp_head.version,
                resp_head.headers,
            );
            let close = velda_http1::server::response::send_response_parts(
                &mut down_write,
                write_buf,
                &server_head,
                &Body::Empty,
                *close_requested,
            )
            .await?;
            if close {
                *close_requested = true;
            }
            let _ = resp_done_tx.send(());
            return Ok::<(), Http1Error>(());
        }

        let server_head =
            Http1ServerResponseHead::new(resp_head.status, resp_head.version, resp_head.headers);

        match resp_framing {
            Http1BodyFraming::ContentLength(total_len) => {
                let available = upstream_read_buf.len().min(total_len);
                if available == total_len {
                    // [FAST-PATH]: Entire response body is already present in memory!
                    let body_slice = if total_len > 0 {
                        Some(&upstream_read_buf[..total_len])
                    } else {
                        None
                    };
                    let close =
                        velda_http1::server::response::send_response_head_chunked_coalesced(
                            &mut down_write,
                            write_buf,
                            &server_head,
                            body_slice,
                            true,
                            *close_requested,
                        )
                        .await?;
                    if close {
                        *close_requested = true;
                    }
                    if total_len > 0 {
                        upstream_read_buf.advance(total_len);
                    }
                    let _ = resp_done_tx.send(());
                    return Ok(());
                }

                let close = velda_http1::server::response::send_response_head_chunked(
                    &mut down_write,
                    write_buf,
                    server_head.version,
                    server_head.status,
                    &server_head.headers,
                    *close_requested,
                )
                .await?;
                if close {
                    *close_requested = true;
                }

                if available > 0 {
                    let first_chunk = upstream_read_buf.split_to(available).freeze();
                    send_chunk(&mut down_write, &first_chunk).await?;
                }

                let mut remaining = total_len - available;
                while remaining > 0 {
                    let to_read = remaining.min(262144);
                    match read_chunk_sized(&mut up_read, upstream_read_buf, to_read).await? {
                        Some(chunk) => {
                            remaining = remaining.saturating_sub(chunk.len());
                            send_chunk(&mut down_write, &chunk).await?;
                        }
                        None => {
                            return Err(Http1Error::Parse(
                                "Unexpected EOF while reading upstream response body".into(),
                            ));
                        }
                    }
                }
            }
            Http1BodyFraming::Chunked => {
                let mut sent_via_coalesced = false;
                if let Some((wire1, payload1, is_term1)) =
                    velda_http1::parse_single_chunk(upstream_read_buf)?
                {
                    sent_via_coalesced = true;
                    if is_term1 {
                        upstream_read_buf.advance(wire1);
                        let close =
                            velda_http1::server::response::send_response_head_chunked_coalesced(
                                &mut down_write,
                                write_buf,
                                &server_head,
                                None,
                                true,
                                *close_requested,
                            )
                            .await?;
                        if close {
                            *close_requested = true;
                        }
                        let _ = resp_done_tx.send(());
                        return Ok(());
                    }

                    let (is_terminated, total_wire) =
                        match velda_http1::parse_single_chunk(&upstream_read_buf[wire1..])? {
                            Some((wire2, _, true)) => (true, wire1 + wire2),
                            _ => (false, wire1),
                        };

                    let close =
                        velda_http1::server::response::send_response_head_chunked_coalesced(
                            &mut down_write,
                            write_buf,
                            &server_head,
                            Some(payload1),
                            is_terminated,
                            *close_requested,
                        )
                        .await?;
                    if close {
                        *close_requested = true;
                    }
                    upstream_read_buf.advance(total_wire);

                    if is_terminated {
                        let _ = resp_done_tx.send(());
                        return Ok(());
                    }
                }

                if !sent_via_coalesced {
                    let close = velda_http1::server::response::send_response_head_chunked(
                        &mut down_write,
                        write_buf,
                        server_head.version,
                        server_head.status,
                        &server_head.headers,
                        *close_requested,
                    )
                    .await?;
                    if close {
                        *close_requested = true;
                    }
                }

                while let Some(chunk) = read_next_chunk(&mut up_read, upstream_read_buf).await? {
                    send_chunk(&mut down_write, &chunk).await?;
                }
            }
            Http1BodyFraming::Empty => {
                let close = velda_http1::server::response::send_response_head_chunked(
                    &mut down_write,
                    write_buf,
                    server_head.version,
                    server_head.status,
                    &server_head.headers,
                    *close_requested,
                )
                .await?;
                if close {
                    *close_requested = true;
                }
            }
        }

        send_chunked_end(&mut down_write).await?;
        down_write.flush().await?;
        let _ = resp_done_tx.send(());
        Ok::<(), Http1Error>(())
    };

    let duplex_res = tokio::try_join!(upload_task, download_task);
    if !upload_finished.load(std::sync::atomic::Ordering::Acquire) {
        conn.mark_close();
    }
    match duplex_res {
        Ok((early_response, ())) => {
            if early_response {
                upstream_lease.mark_closed();
            }
            Ok(())
        }
        Err(e) => {
            upstream_lease.mark_closed();
            Err(e)
        }
    }
}
