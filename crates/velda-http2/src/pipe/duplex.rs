//! Concurrent full-duplex bidirectional HTTP/2 streaming.
//!
//! Concurrently streams downstream request DATA frames to upstream while
//! progressively streaming upstream response DATA frames back downstream.
//! Uses native HTTP/2 multiplexed streams without head-of-line blocking.

use bytes::Bytes;
use h2::client::SendRequest;
use http::Version;
use velda_core::{Body, L7Response};

use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2Responder, Http2ServerRequestHead, Http2StreamReceiver};

/// Concurrently pipes both request and response streams between downstream and upstream.
pub async fn pipe_duplex(
    mut head: Http2ServerRequestHead,
    mut body_rx: Http2StreamReceiver,
    mut responder: Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Build and sanitize outbound upstream H2 request with end_of_stream = false (zero-clone)
    let is_head = head.method == http::Method::HEAD;
    crate::headers::sanitize_h2_headers(&mut head.headers);
    let http_req = head.into_http_request();
    let (response_fut, mut send_stream) = client.send_request(http_req, false)?;

    // Task 1: Upload pump — stream downstream chunks to upstream with flow-control backpressure
    let upload_task = async move {
        const UPLOAD_BATCH_RESERVE: usize = 131_072; // 128 KB
        send_stream.reserve_capacity(UPLOAD_BATCH_RESERVE);

        while let Some(mut chunk) = body_rx.recv_chunk().await? {
            while !chunk.is_empty() {
                let available = send_stream.capacity();
                if available == 0 {
                    send_stream.reserve_capacity(chunk.len().max(UPLOAD_BATCH_RESERVE));
                    let res = std::future::poll_fn(|cx| send_stream.poll_capacity(cx)).await;
                    if let Some(err) = res {
                        err?;
                    }
                    continue;
                }

                let to_send = chunk.len().min(available);
                let slice = chunk.split_to(to_send);
                send_stream.send_data(slice, false)?;

                // Maintain continuous window headroom to avoid poll_capacity stalls
                if send_stream.capacity() < 32_768 {
                    send_stream.reserve_capacity(UPLOAD_BATCH_RESERVE);
                }
            }
        }
        send_stream.send_data(Bytes::new(), true)?;
        Ok::<(), Http2Error>(())
    };

    // Task 2: Download pump — await response headers and stream upstream chunks downstream
    let config_max_body = config.max_body_size;
    let download_task = async move {
        let response = response_fut.await?;
        let (parts, mut body_stream) = response.into_parts();

        let is_no_body_status = parts.status.is_informational()
            || parts.status == http::StatusCode::NO_CONTENT
            || parts.status == http::StatusCode::NOT_MODIFIED;

        if is_head || is_no_body_status {
            let resp = L7Response::new(parts.status, Version::HTTP_2, parts.headers, Body::Empty);
            responder.send_response(&resp)?;
            return Ok(());
        }

        let mut sender = responder.send_stream_response(parts.status, &parts.headers)?;

        let batch_threshold =
            (config.initial_stream_window_size as usize / 4).clamp(32_768, 262_144);
        let mut total_bytes = 0;
        let mut unreleased_bytes = 0;
        const COALESCE_LIMIT: usize = 65_536;
        let mut pending_buf = bytes::BytesMut::with_capacity(COALESCE_LIMIT);

        while let Some(chunk) = body_stream.data().await {
            let data = chunk?;
            let len = data.len();
            total_bytes += len;
            unreleased_bytes += len;

            if total_bytes > config_max_body {
                let _ = body_stream
                    .flow_control()
                    .release_capacity(unreleased_bytes);
                return Err(Http2Error::PayloadTooLarge(total_bytes));
            }

            if pending_buf.is_empty() && len >= COALESCE_LIMIT {
                if sender.send_chunk(data).await.is_err() {
                    tracing::debug!("Downstream client disconnected during H2 duplex streaming");
                    let _ = body_stream
                        .flow_control()
                        .release_capacity(unreleased_bytes);
                    return Ok(());
                }
            } else {
                pending_buf.extend_from_slice(&data);
                if pending_buf.len() >= COALESCE_LIMIT {
                    let to_send = pending_buf.split().freeze();
                    if sender.send_chunk(to_send).await.is_err() {
                        tracing::debug!(
                            "Downstream client disconnected during H2 duplex streaming"
                        );
                        let _ = body_stream
                            .flow_control()
                            .release_capacity(unreleased_bytes);
                        return Ok(());
                    }
                }
            }

            if unreleased_bytes >= batch_threshold {
                let _ = body_stream
                    .flow_control()
                    .release_capacity(unreleased_bytes);
                unreleased_bytes = 0;
            }
        }

        if !pending_buf.is_empty() {
            let to_send = pending_buf.freeze();
            if sender.send_chunk(to_send).await.is_err() {
                tracing::debug!("Downstream client disconnected during H2 duplex streaming tail");
                let _ = body_stream
                    .flow_control()
                    .release_capacity(unreleased_bytes);
                return Ok(());
            }
        }

        if unreleased_bytes > 0 {
            let _ = body_stream
                .flow_control()
                .release_capacity(unreleased_bytes);
        }

        let _ = sender.finish();
        Ok(())
    };

    tokio::try_join!(upload_task, download_task)?;
    Ok(())
}
