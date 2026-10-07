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

use crate::client::response::Http2ClientResponse;
use crate::error::Http2Error;

#[cold]
#[inline(never)]
fn cold_parse_error(e: impl std::fmt::Display) -> Http2Error {
    Http2Error::Parse(e.to_string())
}

/// Constructs an HTTP/2 response head frame builder with sanitized RFC 9113 headers.
#[inline]
pub fn build_h2_response(
    status: StatusCode,
    headers: &HeaderMap,
) -> Result<Response<()>, Http2Error> {
    let mut builder = Response::builder().status(status).version(Version::HTTP_2);
    for (name, val) in headers {
        if !crate::headers::is_disallowed_h2_header(name, val) {
            builder = builder.header(name, val);
        }
    }
    builder.body(()).map_err(cold_parse_error)
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

    /// Sends this response through the given responder.
    pub fn send_to(&self, responder: &mut Http2Responder) -> Result<(), Http2Error> {
        responder.send_parts(self.status, &self.headers, &self.body)
    }
}

/// Responder handle for an active HTTP/2 multiplexed stream.
#[derive(Debug)]
pub struct Http2Responder {
    respond: SendResponse<Bytes>,
    alt_svc: Option<HeaderValue>,
}

impl Http2Responder {
    /// Creates a new [`Http2Responder`] from an underlying H2 stream send handle.
    #[inline]
    pub fn new(respond: SendResponse<Bytes>) -> Self {
        Self {
            respond,
            alt_svc: None,
        }
    }

    /// Sets an optional Alt-Svc header value to automatically advertise on downstream responses.
    #[inline]
    pub fn with_alt_svc(mut self, alt_svc: Option<HeaderValue>) -> Self {
        self.alt_svc = alt_svc;
        self
    }

    #[inline]
    fn prepare_h2_response(
        &self,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Result<Response<()>, Http2Error> {
        let mut response = build_h2_response(status, headers)?;
        if let Some(ref alt_svc) = self.alt_svc
            && !response.headers().contains_key(http::header::ALT_SVC)
        {
            response
                .headers_mut()
                .insert(http::header::ALT_SVC, alt_svc.clone());
        }
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

    /// Sends a downstream server response.
    pub fn send_server_response(
        &mut self,
        response: &Http2ServerResponse,
    ) -> Result<(), Http2Error> {
        self.send_parts(response.status, &response.headers, &response.body)
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

        if has_body
            && let Body::Bytes(b) = &response.body
            && !b.is_empty()
        {
            let _ = send_stream.send_data(b.clone(), true);
        }
        send_stream.send_reset(h2::Reason::NO_ERROR);
        Ok(())
    }

    /// Aborts the stream immediately with an HTTP/2 RST_STREAM frame.
    #[inline]
    pub fn send_reset(&mut self, reason: h2::Reason) {
        self.respond.send_reset(reason);
    }

    /// Sends an upstream response using the protocol-owned [`Http2ClientResponse`] type.
    pub fn send_client_response(
        &mut self,
        response: &Http2ClientResponse,
    ) -> Result<(), Http2Error> {
        self.send_parts(response.head.status, &response.head.headers, &response.body)
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

        if has_body
            && let Body::Bytes(b) = body
            && !b.is_empty()
        {
            send_stream.send_data(b.clone(), true)?;
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

    /// Sends the response head and returns a progressive stream sender for streaming response chunks.
    pub fn send_stream_response(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Result<Http2StreamSender, Http2Error> {
        let send_stream = self.send_head(status, headers, false)?;
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

    /// Sends a DATA frame chunk downstream.
    pub async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), Http2Error> {
        self.send_stream.reserve_capacity(chunk.len());
        self.send_stream.send_data(chunk, false)?;
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
}
