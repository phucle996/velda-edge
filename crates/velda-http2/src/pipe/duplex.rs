//! Concurrent full-duplex bidirectional HTTP/2 streaming.
//!
//! Concurrently streams downstream request DATA frames to upstream while
//! progressively streaming upstream response DATA frames back downstream.
//! Uses native HTTP/2 multiplexed streams without head-of-line blocking.

use bytes::Bytes;
use h2::client::SendRequest;

use super::{build_outbound_request, pump_response_stream};
use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::{Http2RequestHead, Http2Responder, Http2StreamReceiver};

/// Concurrently pipes both request and response streams between downstream and upstream.
pub async fn pipe_duplex(
    head: Http2RequestHead,
    mut body_rx: Http2StreamReceiver,
    responder: Http2Responder,
    client: &mut SendRequest<Bytes>,
    config: &Http2Config,
) -> Result<(), Http2Error> {
    // 1. Build and sanitize outbound upstream H2 request with end_of_stream = false (zero-clone)
    let http_req = build_outbound_request(head)?;
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
        let (parts, body_stream) = response.into_parts();
        let sender = responder.send_stream_response(parts.status, &parts.headers)?;
        pump_response_stream(body_stream, sender, config_max_body).await
    };

    tokio::try_join!(upload_task, download_task)?;
    Ok(())
}
