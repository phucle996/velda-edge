//! Downstream gRPC stream representation over UDP.

use quinn_proto::{ConnectionHandle, StreamId};
use std::net::SocketAddr;
use velda_core::L7Request;

/// Downstream gRPC server request head metadata over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpServerRequestHead {
    /// Request URI.
    pub uri: http::Uri,
    /// Target authority.
    pub authority: Option<String>,
    /// Request headers / metadata.
    pub headers: http::HeaderMap,
}

impl GrpcUdpServerRequestHead {
    /// Creates a new request head.
    #[inline]
    pub fn new(uri: http::Uri, authority: Option<String>, headers: http::HeaderMap) -> Self {
        Self {
            uri,
            authority,
            headers,
        }
    }

    /// Fast-path lookup for request path.
    #[inline]
    pub fn path(&self) -> &str {
        self.uri.path()
    }
}

/// Downstream protocol-owned gRPC server request over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpServerRequest {
    /// Request head metadata.
    pub head: GrpcUdpServerRequestHead,
    /// Request payload.
    pub body: velda_core::Body,
}

impl GrpcUdpServerRequest {
    /// Creates a new request.
    #[inline]
    pub fn new(head: GrpcUdpServerRequestHead, body: velda_core::Body) -> Self {
        Self { head, body }
    }

    /// Constructs from canonical [`L7Request`].
    pub fn from_l7_request(req: &L7Request) -> Self {
        let authority = req
            .headers
            .get(":authority")
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| {
                req.headers
                    .get(http::header::HOST)
                    .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            })
            .or_else(|| req.uri.authority().map(|a| a.as_str().to_string()));

        let head = GrpcUdpServerRequestHead::new(req.uri.clone(), authority, req.headers.clone());
        Self::new(head, req.body.clone())
    }

    /// Converts into canonical [`L7Request`].
    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(
            http::Method::POST,
            self.head.uri,
            http::Version::HTTP_3,
            self.head.headers,
            self.body,
        )
    }
}

/// Represents an active downstream gRPC request stream received over UDP.
#[derive(Debug)]
pub struct GrpcUdpServerStream {
    /// QUIC connection handle.
    pub handle: ConnectionHandle,
    /// Stream identifier.
    pub stream_id: StreamId,
    /// Associated L7 request.
    pub request: L7Request,
    /// Remote client address.
    pub peer: SocketAddr,
}

impl GrpcUdpServerStream {
    /// Converts to protocol-owned [`GrpcUdpServerRequest`].
    #[inline]
    pub fn to_server_request(&self) -> GrpcUdpServerRequest {
        GrpcUdpServerRequest::from_l7_request(&self.request)
    }

    /// Creates a new downstream stream wrapper.
    pub fn new(
        handle: ConnectionHandle,
        stream_id: StreamId,
        request: L7Request,
        peer: SocketAddr,
    ) -> Self {
        Self {
            handle,
            stream_id,
            request,
            peer,
        }
    }

    /// Extracts the effective authority from `:authority`, Host header, or URI authority.
    #[inline]
    pub fn authority(&self) -> Option<&str> {
        self.request
            .headers
            .get(":authority")
            .and_then(|v| v.to_str().ok())
            .or_else(|| {
                self.request
                    .headers
                    .get(http::header::HOST)
                    .and_then(|h| h.to_str().ok())
            })
            .or_else(|| self.request.uri.authority().map(|a| a.as_str()))
    }

    /// Strips untrusted client forwarding headers and RFC 9114/9113 hop-by-hop headers,
    /// and injects authoritative proxy forwarding headers (RFC 7239 + `X-Forwarded-*`).
    pub fn enrich_forwarded_headers(&mut self, local_addr: SocketAddr) {
        let auth_hdr = self.request.headers.get(":authority").cloned();
        let host_hdr = self.request.headers.get(http::header::HOST).cloned();
        let authority_str = auth_hdr
            .as_ref()
            .and_then(|v| v.to_str().ok())
            .or_else(|| host_hdr.as_ref().and_then(|h| h.to_str().ok()))
            .or_else(|| self.request.uri.authority().map(|a| a.as_str()));

        super::header::enrich_headers(
            &mut self.request.headers,
            self.peer,
            local_addr,
            authority_str,
        );
    }
}
