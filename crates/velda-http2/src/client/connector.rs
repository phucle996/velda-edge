//! Upstream HTTP/2 Client Connector (RFC 9113).
//!
//! Handles TCP connection establishment, H2 client handshake, background connection driver
//! execution, and multiplexed request dispatching to upstream endpoints.

use std::net::SocketAddr;
use tokio::net::TcpStream;
use velda_core::{IngressLimits, L7Request, L7Response};

use super::decode::{decode_l7_response, decode_response};
use super::encode::{send_h2_request, send_l7_request};
use super::response::Http2Response;
use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::server::request::Http2Request;

/// Active HTTP/2 upstream connector for dispatching requests to microservice backends.
pub struct Http2UpstreamConnector;

impl Http2UpstreamConnector {
    /// Connects to the upstream target and performs the HTTP/2 client handshake.
    ///
    /// Spawns the H2 connection driver onto a background Tokio task.
    pub async fn connect(
        target: SocketAddr,
        config: &Http2Config,
    ) -> Result<h2::client::SendRequest<bytes::Bytes>, Http2Error> {
        let stream = TcpStream::connect(target).await?;
        let mut builder = h2::client::Builder::default();
        builder
            .initial_connection_window_size(config.initial_connection_window_size)
            .initial_window_size(config.initial_stream_window_size)
            .max_concurrent_streams(config.max_concurrent_streams)
            .max_frame_size(config.max_frame_size)
            .max_header_list_size(config.max_header_list_size)
            .enable_push(config.enable_push);

        let (client, h2_conn) = builder.handshake(stream).await?;
        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        Ok(client)
    }

    /// Forwards an HTTP/2 [`L7Request`] to the target backend endpoint over cleartext TCP / H2,
    /// returning the parsed [`L7Response`].
    pub async fn forward_request(
        req: &L7Request,
        target: SocketAddr,
        limits: &IngressLimits,
    ) -> Result<L7Response, Http2Error> {
        Self::forward_request_with_config(req, target, limits, &Http2Config::default()).await
    }

    /// Forwards an HTTP/2 [`L7Request`] with explicit [`Http2Config`].
    pub async fn forward_request_with_config(
        req: &L7Request,
        target: SocketAddr,
        limits: &IngressLimits,
        config: &Http2Config,
    ) -> Result<L7Response, Http2Error> {
        let mut client = Self::connect(target, config).await?;
        let resp_fut = send_l7_request(&mut client, req)?;
        decode_l7_response(resp_fut, limits).await
    }

    /// Forwards an [`Http2Request`] to the target backend, returning an [`Http2Response`].
    pub async fn forward_h2_request(
        req: &Http2Request,
        target: SocketAddr,
        limits: &IngressLimits,
    ) -> Result<Http2Response, Http2Error> {
        Self::forward_h2_request_with_config(req, target, limits, &Http2Config::default()).await
    }

    /// Forwards an [`Http2Request`] with explicit [`Http2Config`].
    pub async fn forward_h2_request_with_config(
        req: &Http2Request,
        target: SocketAddr,
        limits: &IngressLimits,
        config: &Http2Config,
    ) -> Result<Http2Response, Http2Error> {
        let mut client = Self::connect(target, config).await?;
        let resp_fut = send_h2_request(&mut client, req)?;
        decode_response(resp_fut, limits).await
    }

    /// Forwards a request to the upstream target and returns the response head and a progressive stream receiver.
    ///
    /// Ideal for Server-Sent Events (SSE) and LLM token streaming pass-through without buffering.
    pub async fn forward_streaming_request(
        req: &L7Request,
        target: SocketAddr,
        limits: &IngressLimits,
    ) -> Result<
        (
            super::response::Http2ResponseHead,
            crate::server::decode::Http2StreamReceiver,
        ),
        Http2Error,
    > {
        let mut client = Self::connect(target, &Http2Config::default()).await?;
        let resp_fut = send_l7_request(&mut client, req)?;
        super::decode::decode_streaming_response(resp_fut, limits).await
    }
}
