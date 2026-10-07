//! HTTP/2 Upstream Outbound Request Entity and Wire Serializer (RFC 9113).
//!
//! Encodes outgoing HTTP/2 requests towards upstream backends across multiplexed streams.

use bytes::Bytes;
use h2::SendStream;
use h2::client::{ResponseFuture, SendRequest};
use http::{HeaderMap, Method, Request, Uri, Version};
use velda_core::{Body, L7Request};

use crate::error::Http2Error;
use crate::server::request::Http2ServerRequest;

/// Outbound HTTP/2 client request head metadata.
#[derive(Debug, Clone)]
pub struct Http2ClientRequestHead {
    /// HTTP method.
    pub method: Method,
    /// Target URI / path and query.
    pub uri: Uri,
    /// Protocol version (HTTP/2.0).
    pub version: Version,
    /// Outbound request headers.
    pub headers: HeaderMap,
}

impl Http2ClientRequestHead {
    /// Creates a new [`Http2ClientRequestHead`].
    #[inline]
    pub fn new(method: Method, uri: Uri, headers: HeaderMap) -> Self {
        Self {
            method,
            uri,
            version: Version::HTTP_2,
            headers,
        }
    }
}

/// Protocol-owned HTTP/2 client request sent to upstream backends.
#[derive(Debug, Clone)]
pub struct Http2ClientRequest {
    /// Request head (method, URI, headers, version).
    pub head: Http2ClientRequestHead,
    /// Request payload body.
    pub body: Body,
}

impl Http2ClientRequest {
    /// Creates a new [`Http2ClientRequest`].
    #[inline]
    pub fn new(head: Http2ClientRequestHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Constructs an upstream client request from a downstream server request.
    #[inline]
    pub fn from_server_request(req: &Http2ServerRequest) -> Self {
        let head = Http2ClientRequestHead::new(
            req.head.method.clone(),
            req.head.uri.clone(),
            req.head.headers.clone(),
        );
        Self::new(head, req.body.clone())
    }

    /// Builds the standard `http::Request<()>` for H2 client submission.
    pub fn to_http_request(&self) -> Result<Request<()>, Http2Error> {
        let mut builder = Request::builder()
            .method(&self.head.method)
            .uri(&self.head.uri)
            .version(Version::HTTP_2);

        for (k, v) in &self.head.headers {
            builder = builder.header(k, v);
        }

        builder
            .body(())
            .map_err(|e| Http2Error::Parse(e.to_string()))
    }

    /// Submits this request to the upstream client connection over a multiplexed stream.
    pub fn send_to(
        &self,
        client: &mut SendRequest<Bytes>,
    ) -> Result<(ResponseFuture, SendStream<Bytes>), Http2Error> {
        let has_body = !self.body.is_empty();
        let http_req = self.to_http_request()?;
        let (response_fut, mut send_stream) = client.send_request(http_req, !has_body)?;

        if has_body
            && let Body::Bytes(b) = &self.body
            && !b.is_empty()
        {
            send_stream.send_data(b.clone(), true)?;
        }

        Ok((response_fut, send_stream))
    }

    /// Converts into canonical [`L7Request`].
    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(
            self.head.method,
            self.head.uri,
            Version::HTTP_2,
            self.head.headers,
            self.body,
        )
    }

    /// Constructs from canonical [`L7Request`].
    pub fn from_l7_request(req: L7Request) -> Self {
        let head = Http2ClientRequestHead::new(req.method, req.uri, req.headers);
        Self::new(head, req.body)
    }
}
