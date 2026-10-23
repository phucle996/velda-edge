//! Downstream HTTP/2 Server Response Entity and Multiplexed Stream Responder (RFC 9113).
//!
//! Encodes outgoing HTTP/2 HEADERS and DATA frames back to the client over an
//! isolated logical stream handle (`SendResponse`). Supports both one-shot
//! responses and progressive chunk streaming.

use bytes::Bytes;
use h2::SendStream;
use h2::server::SendResponse;
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Response, StatusCode, Version};
use velda_core::{Body, L7Response};

use crate::error::Http2Error;
use crate::headers::X_REQUEST_ID;

/// Constructs an HTTP/2 response head frame builder with sanitized RFC 9113 headers.
#[inline]
pub fn build_h2_response(
    status: StatusCode,
    headers: &HeaderMap,
) -> Result<Response<()>, Http2Error> {
    let mut http_response = Response::new(());
    *http_response.status_mut() = status;
    *http_response.version_mut() = Version::HTTP_2;
    http_response.headers_mut().reserve(headers.len());
    for (name, val) in headers {
        if !crate::headers::is_disallowed_h2_header(name, val) {
            http_response
                .headers_mut()
                .append(name.clone(), val.clone());
        }
    }
    Ok(http_response)
}

/// Downstream HTTP/2 response head metadata.
#[derive(Debug, Clone)]
pub struct Http2ServerResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// Protocol version (HTTP/2.0).
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
}

impl Http2ServerResponseHead {
    /// Creates a new HTTP/2 server response head.
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap) -> Self {
        Self {
            status,
            version: Version::HTTP_2,
            headers,
        }
    }
}

/// Downstream protocol-owned HTTP/2 response.
#[derive(Debug, Clone)]
pub struct Http2ServerResponse {
    /// Response status code.
    pub status: StatusCode,
    /// Protocol version (HTTP/2.0).
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response payload body.
    pub body: Body,
}

impl Http2ServerResponse {
    /// Creates a new HTTP/2 server response.
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap, body: Body) -> Self {
        Self {
            status,
            version: Version::HTTP_2,
            headers,
            body,
        }
    }

    /// Fast constructor from raw status code and byte payload.
    #[inline]
    pub fn from_bytes(status: StatusCode, bytes: Vec<u8>) -> Self {
        let body = if bytes.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(bytes.into())
        };
        Self::new(status, HeaderMap::new(), body)
    }

    /// Appends a header using fluent builder pattern.
    #[inline]
    pub fn with_header(mut self, name: HeaderName, val: HeaderValue) -> Self {
        self.headers.insert(name, val);
        self
    }
}

/// Responder handle for an active HTTP/2 multiplexed stream.
#[derive(Debug)]
pub struct Http2Responder {
    respond: SendResponse<Bytes>,
    request_id: Option<HeaderValue>,
}

impl Http2Responder {
    /// Creates a new [`Http2Responder`] from an underlying H2 stream send handle.
    #[inline]
    pub fn new(respond: SendResponse<Bytes>) -> Self {
        Self {
            respond,
            request_id: None,
        }
    }

    /// Sets an optional W3C Request ID header value to automatically stamp onto downstream responses.
    #[inline]
    pub fn with_request_id(mut self, request_id: Option<HeaderValue>) -> Self {
        self.request_id = request_id;
        self
    }

    #[inline]
    fn prepare_h2_response(
        &self,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Result<Response<()>, Http2Error> {
        let mut response = build_h2_response(status, headers)?;
        if let Some(ref req_id) = self.request_id
            && !response.headers().contains_key(&X_REQUEST_ID)
        {
            response.headers_mut().insert(X_REQUEST_ID, req_id.clone());
        }
        Ok(response)
    }

    #[inline]
    fn prepare_h2_response_owned(
        &self,
        status: StatusCode,
        mut headers: HeaderMap,
    ) -> Result<Response<()>, Http2Error> {
        crate::headers::sanitize_h2_headers(&mut headers);
        if let Some(ref req_id) = self.request_id
            && !headers.contains_key(&X_REQUEST_ID)
        {
            headers.insert(X_REQUEST_ID, req_id.clone());
        }
        let mut response = Response::new(());
        *response.status_mut() = status;
        *response.version_mut() = Version::HTTP_2;
        *response.headers_mut() = headers;
        Ok(response)
    }

    /// Returns the logical HTTP/2 stream ID associated with this responder.
    #[inline]
    pub fn stream_id(&self) -> h2::StreamId {
        self.respond.stream_id()
    }

    /// Sends a response using the canonical [`L7Response`] type.
    pub fn send_response(&mut self, response: &L7Response) -> Result<(), Http2Error> {
        self.send_parts(response.status, &response.headers, &response.body)
    }

    /// Sends a response using an owned [`L7Response`], avoiding body buffer cloning and header copying.
    pub fn send_response_owned(&mut self, response: L7Response) -> Result<(), Http2Error> {
        let is_no_body = response.status.is_informational()
            || response.status == StatusCode::NO_CONTENT
            || response.status == StatusCode::NOT_MODIFIED;

        let has_body = !is_no_body && !response.body.is_empty();
        let http_response =
            self.prepare_h2_response_owned(response.status, response.headers.into())?;
        let mut send_stream = self.respond.send_response(http_response, !has_body)?;

        if has_body && !response.body.is_empty() {
            send_stream.send_data(response.body.to_bytes(), true)?;
        }

        Ok(())
    }

    /// Sends a response and immediately resets the incoming stream to cancel any pending peer upload.
    pub fn send_response_and_cancel_upload(
        &mut self,
        response: &L7Response,
    ) -> Result<(), Http2Error> {
        let is_no_body = response.status.is_informational()
            || response.status == StatusCode::NO_CONTENT
            || response.status == StatusCode::NOT_MODIFIED;
        let has_body = !is_no_body && !response.body.is_empty();
        let http_response = self.prepare_h2_response(response.status, &response.headers)?;
        let mut send_stream = self.respond.send_response(http_response, !has_body)?;

        if has_body && !response.body.is_empty() {
            let _ = send_stream.send_data(response.body.to_bytes(), true);
        }
        send_stream.send_reset(h2::Reason::NO_ERROR);
        Ok(())
    }

    /// Aborts the stream immediately with an HTTP/2 RST_STREAM frame.
    #[inline]
    pub fn send_reset(&mut self, reason: h2::Reason) {
        self.respond.send_reset(reason);
    }

    /// Sends an HTTP/2 response given discrete parts.
    pub fn send_parts(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
        body: &Body,
    ) -> Result<(), Http2Error> {
        let is_no_body = status.is_informational()
            || status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_MODIFIED;

        let has_body = !is_no_body && !body.is_empty();
        let http_response = self.prepare_h2_response(status, headers)?;

        let mut send_stream = self.respond.send_response(http_response, !has_body)?;

        if has_body && !body.is_empty() {
            send_stream.send_data(body.to_bytes(), true)?;
        }

        Ok(())
    }

    /// Sends an HTTP/2 response given owned discrete parts, eliminating `Bytes` cloning.
    pub fn send_parts_owned(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
        body: Body,
    ) -> Result<(), Http2Error> {
        let is_no_body = status.is_informational()
            || status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_MODIFIED;

        let has_body = !is_no_body && !body.is_empty();
        let http_response = self.prepare_h2_response(status, headers)?;

        let mut send_stream = self.respond.send_response(http_response, !has_body)?;

        if has_body && !body.is_empty() {
            send_stream.send_data(body.to_bytes(), true)?;
        }

        Ok(())
    }

    /// Opens a progressive send stream by sending only the HEADERS frame.
    pub fn send_head(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
        end_stream: bool,
    ) -> Result<SendStream<Bytes>, Http2Error> {
        let http_response = self.prepare_h2_response(status, headers)?;
        let send_stream = self.respond.send_response(http_response, end_stream)?;
        Ok(send_stream)
    }

    /// Opens a progressive send stream with owned headers, eliminating HeaderMap cloning.
    pub fn send_head_owned(
        &mut self,
        status: StatusCode,
        headers: HeaderMap,
        end_stream: bool,
    ) -> Result<SendStream<Bytes>, Http2Error> {
        let http_response = self.prepare_h2_response_owned(status, headers)?;
        let send_stream = self.respond.send_response(http_response, end_stream)?;
        Ok(send_stream)
    }

    /// Sends the response head and returns a progressive stream sender for streaming response chunks.
    pub fn send_stream_response(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Result<Http2StreamSender, Http2Error> {
        let send_stream = self.send_head(status, headers, false)?;
        Ok(Http2StreamSender::new(send_stream))
    }

    /// Sends the response head with owned headers and returns a progressive stream sender.
    pub fn send_stream_response_owned(
        &mut self,
        status: StatusCode,
        headers: HeaderMap,
    ) -> Result<Http2StreamSender, Http2Error> {
        let send_stream = self.send_head_owned(status, headers, false)?;
        Ok(Http2StreamSender::new(send_stream))
    }
}

/// Progressive stream sender for transmitting HTTP/2 response DATA chunks downstream.
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

    /// Sends a DATA frame chunk downstream with RFC 9113 flow-control backpressure.
    pub async fn send_chunk(&mut self, mut chunk: Bytes) -> Result<(), Http2Error> {
        while !chunk.is_empty() {
            let available = self.send_stream.capacity();
            if available == 0 {
                self.send_stream.reserve_capacity(chunk.len().max(65_536));
                let res = std::future::poll_fn(|cx| self.send_stream.poll_capacity(cx)).await;
                match res {
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => return Err(Http2Error::ConnectionClosed),
                }
                if self.send_stream.capacity() == 0 {
                    return Err(Http2Error::ConnectionClosed);
                }
                continue;
            }

            let to_send = chunk.len().min(available);
            let slice = chunk.split_to(to_send);
            self.send_stream.send_data(slice, false)?;

            if self.send_stream.capacity() < 32_768 && !chunk.is_empty() {
                self.send_stream.reserve_capacity(chunk.len().max(65_536));
            }
        }
        Ok(())
    }

    /// Sends trailing headers to conclude the stream.
    pub fn send_trailers(&mut self, mut trailers: HeaderMap) -> Result<(), Http2Error> {
        crate::headers::sanitize_h2_headers(&mut trailers);
        self.send_stream.send_trailers(trailers)?;
        Ok(())
    }

    /// Explicitly closes the stream by sending an empty DATA frame with END_STREAM flag.
    pub fn finish(&mut self) -> Result<(), Http2Error> {
        self.send_stream.send_data(Bytes::new(), true)?;
        Ok(())
    }

    /// Explicitly resets the stream (e.g. to cancel ongoing client upload after response sent).
    #[inline]
    pub fn reset(&mut self, reason: h2::Reason) {
        self.send_stream.send_reset(reason);
    }
}
