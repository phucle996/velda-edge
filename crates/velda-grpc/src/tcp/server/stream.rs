//! Downstream active gRPC stream representation.

use super::responder::GrpcResponder;
use crate::error::GrpcError;
use crate::frame::{GrpcFrame, decode_grpc_frame};
use bytes::{Bytes, BytesMut};

/// Downstream gRPC server request head metadata over TCP.
#[derive(Debug, Clone)]
pub struct GrpcServerRequestHead {
    /// Request URI.
    pub uri: http::Uri,
    /// Target authority (:authority or Host).
    pub authority: Option<String>,
    /// Request metadata / headers.
    pub headers: http::HeaderMap,
}

impl GrpcServerRequestHead {
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

/// Downstream protocol-owned gRPC server request over TCP.
#[derive(Debug, Clone)]
pub struct GrpcServerRequest {
    /// Request head metadata.
    pub head: GrpcServerRequestHead,
    /// Request payload (LPM or raw bytes).
    pub body: velda_core::Body,
}

impl GrpcServerRequest {
    /// Creates a new gRPC server request.
    #[inline]
    pub fn new(head: GrpcServerRequestHead, body: velda_core::Body) -> Self {
        Self { head, body }
    }

    /// Constructs from HTTP request parts and body.
    pub fn from_parts(parts: &http::request::Parts, body: velda_core::Body) -> Self {
        let authority = parts
            .headers
            .get(":authority")
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| parts.uri.authority().map(|a| a.as_str().to_string()));

        let head = GrpcServerRequestHead::new(parts.uri.clone(), authority, parts.headers.clone());
        Self::new(head, body)
    }

    /// Converts into canonical [`velda_core::L7Request`].
    pub fn into_l7_request(self) -> velda_core::L7Request {
        velda_core::L7Request::new(
            http::Method::POST,
            self.head.uri,
            http::Version::HTTP_2,
            self.head.headers,
            self.body,
        )
    }
}

/// An accepted, active downstream gRPC stream.
pub struct GrpcServerStream {
    /// Initial HTTP/2 headers / parts (:path, :authority, metadata).
    pub parts: http::request::Parts,
    /// Streaming body reader receiving raw gRPC message frames.
    pub recv_stream: h2::RecvStream,
    /// Streaming response sender responding to client.
    pub respond: GrpcResponder,
}

impl GrpcServerStream {
    /// Reads and parses the full unary request into a [`GrpcServerRequest`].
    pub async fn read_server_request(
        &mut self,
        max_body_size: usize,
    ) -> Result<GrpcServerRequest, GrpcError> {
        let body = match self.read_unary_message(max_body_size).await? {
            Some(payload) => velda_core::Body::Bytes(payload),
            None => velda_core::Body::Empty,
        };
        Ok(GrpcServerRequest::from_parts(&self.parts, body))
    }
    /// Reads exactly one Length-Prefixed Message from the downstream stream (Unary).
    ///
    /// Fast-paths single complete frames to return `Ok(Some(payload))` with zero heap allocation and zero copying.
    /// Buffers incoming data chunks when fragmented across frames until full LPM payload is received.
    /// Enforces `max_body_size` — rejects with [`GrpcError::PayloadTooLarge`] if exceeded.
    /// Returns `Ok(Some(payload))` or `Ok(None)` if stream was empty.
    pub async fn read_unary_message(
        &mut self,
        max_body_size: usize,
    ) -> Result<Option<Bytes>, GrpcError> {
        let Some(first_chunk_res) = self.recv_stream.data().await else {
            return Ok(None);
        };
        let first_chunk = first_chunk_res.map_err(GrpcError::H2)?;
        let first_len = first_chunk.len();
        let _ = self.recv_stream.flow_control().release_capacity(first_len);

        // Fast-path: single complete LPM frame (typical for Unary RPCs)
        if first_len >= GrpcFrame::HEADER_SIZE {
            let msg_len = u32::from_be_bytes([
                first_chunk[1],
                first_chunk[2],
                first_chunk[3],
                first_chunk[4],
            ]) as usize;
            if msg_len > max_body_size {
                return Err(GrpcError::PayloadTooLarge(msg_len));
            }
            if first_len == GrpcFrame::HEADER_SIZE + msg_len {
                let flag = first_chunk[0];
                if flag != GrpcFrame::FLAG_UNCOMPRESSED && flag != GrpcFrame::FLAG_COMPRESSED {
                    return Err(GrpcError::Protocol(format!(
                        "invalid gRPC compression flag: {flag}; must be 0 or 1 per RFC"
                    )));
                }
                return Ok(Some(first_chunk.slice(GrpcFrame::HEADER_SIZE..first_len)));
            }
        }

        // Multi-chunk fallback
        let mut buf = BytesMut::with_capacity(first_len * 2);
        buf.extend_from_slice(&first_chunk);

        if let Some((_compressed, payload)) = decode_grpc_frame(&mut buf)? {
            return Ok(Some(payload));
        }

        while let Some(chunk_res) = self.recv_stream.data().await {
            let chunk = chunk_res.map_err(GrpcError::H2)?;
            let len = chunk.len();
            buf.extend_from_slice(&chunk);
            let _ = self.recv_stream.flow_control().release_capacity(len);

            if buf.len() >= GrpcFrame::HEADER_SIZE {
                let msg_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                if msg_len > max_body_size {
                    return Err(GrpcError::PayloadTooLarge(msg_len));
                }
            }

            if buf.len() > max_body_size + GrpcFrame::HEADER_SIZE {
                return Err(GrpcError::PayloadTooLarge(buf.len()));
            }

            if let Some((_compressed, payload)) = decode_grpc_frame(&mut buf)? {
                return Ok(Some(payload));
            }
        }

        if buf.is_empty() {
            Ok(None)
        } else {
            Err(GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Incomplete gRPC message frame received",
            )))
        }
    }

    /// Reads and buffers the complete wire Length-Prefixed Message from downstream stream.
    ///
    /// Preserves the 5-byte LPM frame header for zero-copy forwarding to upstream backends.
    pub async fn read_raw_message(&mut self, max_body_size: usize) -> Result<Bytes, GrpcError> {
        if self.recv_stream.is_end_stream() {
            return Ok(Bytes::new());
        }

        let Some(first_chunk_res) = self.recv_stream.data().await else {
            return Ok(Bytes::new());
        };
        let chunk = first_chunk_res.map_err(GrpcError::H2)?;
        let len = chunk.len();
        let _ = self.recv_stream.flow_control().release_capacity(len);

        if len >= 5 {
            let declared_len =
                u32::from_be_bytes([chunk[1], chunk[2], chunk[3], chunk[4]]) as usize;
            if declared_len > max_body_size {
                let _ = self.respond.send_trailers_only(
                    crate::status::GrpcStatus::ResourceExhausted,
                    Some("request message length exceeds limit"),
                );
                return Err(GrpcError::PayloadTooLarge(declared_len));
            }
        }

        if self.recv_stream.is_end_stream() {
            Ok(chunk)
        } else {
            let mut buf = BytesMut::with_capacity(len * 2);
            buf.extend_from_slice(&chunk);
            let mut checked_lpm = len >= 5;

            while let Some(chunk_res) = self.recv_stream.data().await {
                let chunk = chunk_res.map_err(GrpcError::H2)?;
                let len = chunk.len();
                buf.extend_from_slice(&chunk);
                let _ = self.recv_stream.flow_control().release_capacity(len);

                if !checked_lpm && buf.len() >= 5 {
                    let declared_len =
                        u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                    if declared_len > max_body_size {
                        let _ = self.respond.send_trailers_only(
                            crate::status::GrpcStatus::ResourceExhausted,
                            Some("request message length exceeds limit"),
                        );
                        return Err(GrpcError::PayloadTooLarge(declared_len));
                    }
                    checked_lpm = true;
                }

                if buf.len() > max_body_size + 5 {
                    let _ = self.respond.send_trailers_only(
                        crate::status::GrpcStatus::ResourceExhausted,
                        Some("request message size exceeds limit"),
                    );
                    return Err(GrpcError::PayloadTooLarge(buf.len()));
                }
            }
            Ok(buf.freeze())
        }
    }

    /// Extracts the effective authority from `:authority` or URI authority.
    #[inline]
    pub fn authority(&self) -> Option<&str> {
        self.parts
            .headers
            .get(":authority")
            .and_then(|v| v.to_str().ok())
            .or_else(|| self.parts.uri.authority().map(|a| a.as_str()))
    }

    /// Strips untrusted client forwarding headers and RFC 9113 hop-by-hop headers,
    /// and injects authoritative proxy forwarding headers (RFC 7239 + `X-Forwarded-*`).
    pub fn enrich_forwarded_headers(
        &mut self,
        peer: std::net::SocketAddr,
        local_addr: std::net::SocketAddr,
        is_tls: bool,
    ) {
        let auth_hdr = self.parts.headers.get(":authority").cloned();
        let authority_str = auth_hdr
            .as_ref()
            .and_then(|v| v.to_str().ok())
            .or_else(|| self.parts.uri.authority().map(|a| a.as_str()));

        super::header::enrich_headers(
            &mut self.parts.headers,
            peer,
            local_addr,
            is_tls,
            authority_str,
        );
    }
}
