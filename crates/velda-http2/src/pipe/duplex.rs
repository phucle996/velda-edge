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
use crate::server::{Http2RequestHead, Http2Responder, Http2StreamReceiver};

/// Concurrently pipes both request and response streams between downstream and upstream.
pub async fn pipe_duplex(
    mut head: Http2RequestHead,
    mut body_rx: Http2StreamReceiver,
    responder: Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Build and sanitize outbound upstream H2 request with end_of_stream = false (zero-clone)
    let is_head = head.method == http::Method::HEAD;
    crate::headers::sanitize_h2_headers(&mut head.headers);
    let mut builder = http::Request::builder()
        .method(head.method)
        .uri(head.uri)
        .version(Version::HTTP_2);

    for (k, v) in head.headers.drain() {
        if let Some(k) = k {
            builder = builder.header(k, v);
        }
    }
    let http_req = builder
        .body(())
        .map_err(|e| Http2Error::Parse(e.to_string()))?;
    let (response_fut, mut send_stream) = client.send_request(http_req, false)?;

    // Task 1: Upload pump — stream downstream chunks to upstream
    let upload_task = async move {
        while let Some(chunk) = body_rx.recv_chunk().await? {
            send_stream.send_data(chunk, false)?;
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

        let mut total_bytes = 0;
        while let Some(chunk) = body_stream.data().await {
            let data = chunk?;
            let len = data.len();
            total_bytes += len;
            if total_bytes > config_max_body {
                let _ = body_stream.flow_control().release_capacity(len);
                return Err(Http2Error::PayloadTooLarge(total_bytes));
            }

            if sender.send_chunk(data).await.is_err() {
                // Downstream client disconnected mid-stream
                tracing::debug!("Downstream client disconnected during H2 duplex streaming");
                return Ok(());
            }

            let _ = body_stream.flow_control().release_capacity(len);
        }

        let _ = sender.finish();
        Ok(())
    };

    tokio::try_join!(upload_task, download_task)?;
    Ok(())
}
