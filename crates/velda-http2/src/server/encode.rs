//! Layer 7 HTTP/2 Response Serialization and Multiplexed Stream Responder.
//!
//! Encodes outgoing HTTP/2 HEADERS and DATA frames back to the client over an
//! isolated logical stream handle (`SendResponse`). Supports both one-shot
//! responses and progressive chunk streaming (e.g. Server-Sent Events, NDJSON,
//! large file downloads).

use bytes::Bytes;
use h2::SendStream;
use h2::server::SendResponse;
use http::{HeaderMap, Response, StatusCode, Version};
use velda_core::{Body, L7Response};

use crate::client::response::Http2Response;
use crate::error::Http2Error;

/// Responder handle for an active HTTP/2 multiplexed stream.
///
/// Each stream in HTTP/2 possesses an independent responder handle, allowing
/// concurrent tasks to stream responses without serialization head-of-line blocking.
#[derive(Debug)]
pub struct Http2Responder {
    respond: SendResponse<Bytes>,
}

impl Http2Responder {
    /// Creates a new [`Http2Responder`] from an underlying H2 stream send handle.
    #[inline]
    pub fn new(respond: SendResponse<Bytes>) -> Self {
        Self { respond }
    }

    /// Returns the logical HTTP/2 stream ID associated with this responder.
    #[inline]
    pub fn stream_id(&self) -> h2::StreamId {
        self.respond.stream_id()
    }

    /// Sends a response using the canonical [`L7Response`] type.
    pub fn send_response(self, response: &L7Response) -> Result<(), Http2Error> {
        self.send_parts(response.status, &response.headers, &response.body)
    }

    /// Sends a response using the protocol-owned [`Http2Response`] type.
    pub fn send_h2_response(self, response: &Http2Response) -> Result<(), Http2Error> {
        self.send_parts(response.head.status, &response.head.headers, &response.body)
    }

    /// Initiates a progressive streaming HTTP/2 response without closing the stream.
    ///
    /// Sends the HTTP/2 HEADERS frame with `end_of_stream = false` and returns
    /// an [`Http2StreamSender`] handle for progressively transmitting DATA chunks
    /// (e.g. Server-Sent Events (SSE), NDJSON streaming, or chunked file downloads).
    pub fn send_stream_response(
        mut self,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Result<Http2StreamSender, Http2Error> {
        let mut builder = Response::builder().status(status).version(Version::HTTP_2);

        for (name, val) in headers {
            builder = builder.header(name, val);
        }

        let http_response = builder
            .body(())
            .map_err(|e| Http2Error::Parse(e.to_string()))?;

        let send_stream = self.respond.send_response(http_response, false)?;
        Ok(Http2StreamSender::new(send_stream))
    }

    /// Sends response headers and body over the HTTP/2 stream in one shot.
    pub fn send_parts(
        mut self,
        status: StatusCode,
        headers: &HeaderMap,
        body: &Body,
    ) -> Result<(), Http2Error> {
        let mut builder = Response::builder().status(status).version(Version::HTTP_2);

        for (name, val) in headers {
            builder = builder.header(name, val);
        }

        let has_body = !body.is_empty();
        let is_end_of_stream = !has_body;
        let http_response = builder
            .body(())
            .map_err(|e| Http2Error::Parse(e.to_string()))?;

        let mut send_stream = self
            .respond
            .send_response(http_response, is_end_of_stream)?;

        if let Body::Bytes(b) = body
            && !b.is_empty()
        {
            send_stream.send_data(b.clone(), true)?;
        }

        Ok(())
    }
}

/// Progressive stream sender for an active HTTP/2 stream (e.g. SSE, NDJSON, chunked downloads).
#[derive(Debug)]
pub struct Http2StreamSender {
    send_stream: SendStream<Bytes>,
}

impl Http2StreamSender {
    /// Creates a new [`Http2StreamSender`].
    #[inline]
    pub fn new(send_stream: SendStream<Bytes>) -> Self {
        Self { send_stream }
    }

    /// Returns the logical HTTP/2 stream identifier.
    #[inline]
    pub fn stream_id(&self) -> h2::StreamId {
        self.send_stream.stream_id()
    }

    /// Progressively sends a DATA frame chunk across the HTTP/2 stream without closing it.
    ///
    /// Manages stream flow-control backpressure: if the client's window capacity is
    /// temporarily exhausted, reserves capacity asynchronously.
    pub async fn send_chunk(&mut self, data: Bytes) -> Result<(), Http2Error> {
        if data.is_empty() {
            return Ok(());
        }
        self.send_stream.reserve_capacity(data.len());
        while self.send_stream.capacity() < data.len() {
            match std::future::poll_fn(|cx| self.send_stream.poll_capacity(cx)).await {
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(Http2Error::H2(e)),
                None => return Err(Http2Error::ConnectionClosed),
            }
        }
        self.send_stream.send_data(data, false)?;
        Ok(())
    }

    /// Attempts to send a DATA chunk immediately if flow-control window capacity permits.
    pub fn try_send_chunk(&mut self, data: Bytes) -> Result<(), Http2Error> {
        if data.is_empty() {
            return Ok(());
        }
        self.send_stream.reserve_capacity(data.len());
        self.send_stream.send_data(data, false)?;
        Ok(())
    }

    /// Completes the HTTP/2 stream by sending an empty DATA frame with `end_of_stream = true`.
    pub fn finish(mut self) -> Result<(), Http2Error> {
        self.send_stream.send_data(Bytes::new(), true)?;
        Ok(())
    }

    /// Completes the HTTP/2 stream with a final DATA chunk.
    pub async fn finish_with_data(mut self, data: Bytes) -> Result<(), Http2Error> {
        if data.is_empty() {
            return self.finish();
        }
        self.send_stream.reserve_capacity(data.len());
        while self.send_stream.capacity() < data.len() {
            match std::future::poll_fn(|cx| self.send_stream.poll_capacity(cx)).await {
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(Http2Error::H2(e)),
                None => return Err(Http2Error::ConnectionClosed),
            }
        }
        self.send_stream.send_data(data, true)?;
        Ok(())
    }

    /// Aborts the stream with an explicit HTTP/2 RST_STREAM frame.
    pub fn send_reset(&mut self, reason: h2::Reason) {
        self.send_stream.send_reset(reason);
    }
}
