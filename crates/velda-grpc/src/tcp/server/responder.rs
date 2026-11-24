//! Downstream gRPC response serialization and streaming responder backed by `velda-http2`.
//!
//! Encodes canonical gRPC status codes, Length-Prefixed Message (LPM) data frames,
//! and HTTP/2 response headers/trailers directly over [`HeaderWorkspace`] without `HeaderMap`.

use bytes::{Bytes, BytesMut};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, StatusCode};
use velda_core::{Body, GrpcStatus, HeaderWorkspace};
use velda_http2::server::response::{Http2Responder, Http2StreamSender};

use crate::error::GrpcError;
use crate::frame::encode_grpc_frame;
use crate::wire::{GrpcWire, X_REQUEST_ID};

/// Downstream gRPC server response head metadata over TCP.
#[derive(Debug, Clone)]
pub struct GrpcServerResponseHead {
    /// HTTP status code (typically 200 OK).
    pub status: StatusCode,
    /// Canonical gRPC status.
    pub grpc_status: GrpcStatus,
    /// Response headers.
    pub headers: HeaderWorkspace,
    /// Response trailers.
    pub trailers: HeaderWorkspace,
}

impl GrpcServerResponseHead {
    /// Creates a new gRPC server response head.
    #[inline]
    pub fn new(
        grpc_status: GrpcStatus,
        headers: HeaderWorkspace,
        trailers: HeaderWorkspace,
    ) -> Self {
        Self {
            status: StatusCode::OK,
            grpc_status,
            headers,
            trailers,
        }
    }
}

/// Downstream protocol-owned gRPC server response over TCP.
#[derive(Debug, Clone)]
pub struct GrpcServerResponse {
    /// Response head metadata.
    pub head: GrpcServerResponseHead,
    /// Response payload (LPM or raw bytes).
    pub body: Body,
}

impl GrpcServerResponse {
    /// Creates a new gRPC server response.
    #[inline]
    pub fn new(
        grpc_status: GrpcStatus,
        headers: HeaderWorkspace,
        trailers: HeaderWorkspace,
        body: Body,
    ) -> Self {
        Self {
            head: GrpcServerResponseHead::new(grpc_status, headers, trailers),
            body,
        }
    }

    /// Creates a standard Unary response.
    pub fn unary(
        status: GrpcStatus,
        payload: Option<Bytes>,
        extra_headers: Option<HeaderWorkspace>,
    ) -> Self {
        let headers = extra_headers.unwrap_or_else(|| HeaderWorkspace::empty(4));
        let trailers = status.to_trailer_workspace(None);
        let body = match payload {
            Some(b) => Body::Bytes(b),
            None => Body::Empty,
        };
        Self::new(status, headers, trailers, body)
    }

    /// Creates a trailers-only gRPC response for errors or immediate completions.
    pub fn trailers_only(status: GrpcStatus, message: Option<&str>) -> Self {
        let trailers = status.to_trailer_workspace(message);
        Self::new(status, HeaderWorkspace::empty(0), trailers, Body::Empty)
    }

    /// Converts into canonical [`velda_core::L7Response`].
    pub fn into_l7_response(self) -> velda_core::L7Response {
        let mut headers = self.head.headers;
        for (name, val) in self.head.trailers.iter() {
            headers.append(name, val.clone());
        }
        velda_core::L7Response::new(self.head.status, http::Version::HTTP_2, headers, self.body)
    }
}

/// Streaming responder for an active downstream gRPC call.
#[derive(Debug)]
pub struct GrpcResponder {
    pub inner: Http2Responder,
}

impl GrpcResponder {
    /// Creates a new [`GrpcResponder`] wrapping an active [`Http2Responder`].
    #[inline]
    pub fn new(inner: Http2Responder) -> Self {
        Self { inner }
    }

    /// Sets an optional W3C Request ID header value stamped onto downstream responses.
    #[inline]
    pub fn with_request_id(mut self, request_id: Option<HeaderValue>) -> Self {
        self.inner.request_id = request_id;
        self
    }

    /// Sends a standard Unary response: HTTP 200 + optional LPM frame + trailers with `grpc-status`.
    pub async fn send_unary_response(
        &mut self,
        status: GrpcStatus,
        payload: Option<&[u8]>,
        extra_headers: Option<HeaderWorkspace>,
    ) -> Result<(), GrpcError> {
        let mut headers = extra_headers.unwrap_or_else(|| HeaderWorkspace::empty(4));
        headers.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);
        if let Some(ref req_id) = self.inner.request_id {
            headers.insert_bypass(X_REQUEST_ID, req_id.clone());
        }

        let is_trailers_only = payload.is_none() && status != GrpcStatus::Ok;
        if is_trailers_only {
            headers.insert(GrpcWire::STATUS_NAME, status.code_header_value());
            self.inner
                .send_parts(StatusCode::OK, &headers, &Body::Empty)?;
            return Ok(());
        }

        let mut sender = self.inner.send_stream_response(StatusCode::OK, &headers)?;

        if let Some(data) = payload {
            let mut buf = BytesMut::new();
            encode_grpc_frame(data, false, &mut buf);
            sender.send_chunk(buf.freeze()).await?;
        }

        sender.send_trailers(status.to_trailer_workspace(None))?;
        Ok(())
    }

    /// Responds immediately with a Trailers-Only gRPC response (e.g. for routing errors).
    #[cold]
    pub fn send_trailers_only(
        &mut self,
        status: GrpcStatus,
        message: Option<&str>,
    ) -> Result<(), GrpcError> {
        let mut headers = HeaderWorkspace::empty(4);
        headers.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);
        if let Some(ref req_id) = self.inner.request_id {
            headers.insert_bypass(X_REQUEST_ID, req_id.clone());
        }
        headers.insert(GrpcWire::STATUS_NAME, status.code_header_value());
        if let Some(val) = message.and_then(|m| HeaderValue::from_str(m).ok()) {
            headers.insert(GrpcWire::MESSAGE_NAME, val);
        }

        self.inner
            .send_parts(StatusCode::OK, &headers, &Body::Empty)?;
        Ok(())
    }

    /// Sends an HTTP/2 response header frame from [`HeaderWorkspace`].
    ///
    /// When `end_of_stream` is `true`, concludes the stream in a single `HEADERS` frame (`Ok(None)`).
    /// When `end_of_stream` is `false`, returns an active [`Http2StreamSender`] for chunks/trailers.
    pub fn send_response_headers(
        &mut self,
        status: StatusCode,
        headers: &HeaderWorkspace,
        end_of_stream: bool,
    ) -> Result<Option<Http2StreamSender>, GrpcError> {
        if end_of_stream {
            self.inner.send_parts(status, headers, &Body::Empty)?;
            Ok(None)
        } else {
            let sender = self.inner.send_stream_response(status, headers)?;
            Ok(Some(sender))
        }
    }
}
