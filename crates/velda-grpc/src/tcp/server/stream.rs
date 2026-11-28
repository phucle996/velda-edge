//! Downstream active gRPC stream representation backed by `velda-http2` and [`HeaderWorkspace`].

use bytes::{Bytes, BytesMut};
use velda_core::HeaderWorkspace;
use velda_http2::server::request::{Http2ServerRequestHead, Http2StreamReceiver};

use super::responder::GrpcResponder;
use crate::frame::{GrpcFrame, decode_grpc_frame};
use crate::outcome::GrpcOutcome;

/// Downstream gRPC server request head metadata over TCP.
#[derive(Debug, Clone)]
pub struct GrpcServerRequestHead {
    /// Request URI.
    pub uri: http::Uri,
    /// Target authority (:authority or Host).
    pub authority: Option<String>,
    /// Request metadata / headers.
    pub headers: HeaderWorkspace,
}

impl GrpcServerRequestHead {
    /// Creates a new request head.
    #[inline]
    pub fn new(uri: http::Uri, authority: Option<String>, headers: HeaderWorkspace) -> Self {
        Self {
            uri,
            authority,
            headers,
        }
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

    /// Constructs from [`Http2ServerRequestHead`] and body.
    pub fn from_head(head: &Http2ServerRequestHead, body: velda_core::Body) -> Self {
        let authority = head
            .headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| head.uri.authority().map(|a| a.as_str().to_string()));

        let grpc_head =
            GrpcServerRequestHead::new(head.uri.clone(), authority, head.headers.clone());
        Self::new(grpc_head, body)
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
    /// Initial HTTP/2 request head (`:path`, `:authority`, enriched [`HeaderWorkspace`]).
    pub head: Http2ServerRequestHead,
    /// Streaming body reader receiving raw gRPC message frames.
    pub receiver: Http2StreamReceiver,
    /// Streaming response sender responding to client.
    pub respond: GrpcResponder,
}

impl GrpcServerStream {
    /// Reads and parses the full unary request into a [`GrpcServerRequest`].
    pub async fn read_server_request(
        &mut self,
        max_body_size: usize,
    ) -> Result<GrpcServerRequest, GrpcOutcome> {
        let body = match self.read_unary_message(max_body_size).await? {
            Some(payload) => velda_core::Body::Bytes(payload),
            None => velda_core::Body::Empty,
        };
        Ok(GrpcServerRequest::from_head(&self.head, body))
    }

    /// Reads exactly one Length-Prefixed Message from the downstream stream (Unary).
    pub async fn read_unary_message(
        &mut self,
        max_body_size: usize,
    ) -> Result<Option<Bytes>, GrpcOutcome> {
        let Some(first_chunk) = self
            .receiver
            .recv_chunk()
            .await
            .map_err(GrpcOutcome::from_http2)?
        else {
            return Ok(None);
        };
        let first_len = first_chunk.len();

        // Fast-path: single complete LPM frame (typical for Unary RPCs)
        if first_len >= GrpcFrame::HEADER_SIZE {
            let msg_len = u32::from_be_bytes([
                first_chunk[1],
                first_chunk[2],
                first_chunk[3],
                first_chunk[4],
            ]) as usize;
            if msg_len > max_body_size {
                return Err(GrpcOutcome::PayloadTooLarge(msg_len));
            }
            if GrpcFrame::HEADER_SIZE.checked_add(msg_len) == Some(first_len) {
                let flag = first_chunk[0];
                if flag != GrpcFrame::FLAG_UNCOMPRESSED && flag != GrpcFrame::FLAG_COMPRESSED {
                    return Err(GrpcOutcome::Protocol(format!(
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

        while let Some(chunk) = self
            .receiver
            .recv_chunk()
            .await
            .map_err(GrpcOutcome::from_http2)?
        {
            buf.extend_from_slice(&chunk);

            if buf.len() >= GrpcFrame::HEADER_SIZE {
                let msg_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                if msg_len > max_body_size {
                    return Err(GrpcOutcome::PayloadTooLarge(msg_len));
                }
            }

            if buf.len() > max_body_size.saturating_add(GrpcFrame::HEADER_SIZE) {
                return Err(GrpcOutcome::PayloadTooLarge(buf.len()));
            }

            if let Some((_compressed, payload)) = decode_grpc_frame(&mut buf)? {
                return Ok(Some(payload));
            }
        }

        if buf.is_empty() {
            Ok(None)
        } else {
            Err(GrpcOutcome::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Incomplete gRPC message frame received",
            )))
        }
    }

    /// Reads and buffers the complete wire Length-Prefixed Message from downstream stream.
    ///
    /// Preserves the 5-byte LPM frame header for zero-copy forwarding to upstream backends.
    pub async fn read_raw_message(&mut self, max_body_size: usize) -> Result<Bytes, GrpcOutcome> {
        if self.receiver.is_end_stream() {
            return Ok(Bytes::new());
        }

        let Some(chunk) = self
            .receiver
            .recv_chunk()
            .await
            .map_err(GrpcOutcome::from_http2)?
        else {
            return Ok(Bytes::new());
        };
        let len = chunk.len();

        if len >= 5 {
            let declared_len =
                u32::from_be_bytes([chunk[1], chunk[2], chunk[3], chunk[4]]) as usize;
            if declared_len > max_body_size {
                return Err(GrpcOutcome::PayloadTooLarge(declared_len));
            }
        }

        if len > max_body_size.saturating_add(GrpcFrame::HEADER_SIZE) {
            return Err(GrpcOutcome::PayloadTooLarge(len));
        }

        if self.receiver.is_end_stream() {
            Ok(chunk)
        } else {
            let mut buf = BytesMut::with_capacity(len * 2);
            buf.extend_from_slice(&chunk);
            let mut checked_lpm = len >= 5;

            while let Some(chunk) = self
                .receiver
                .recv_chunk()
                .await
                .map_err(GrpcOutcome::from_http2)?
            {
                buf.extend_from_slice(&chunk);

                if !checked_lpm && buf.len() >= 5 {
                    let declared_len =
                        u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                    if declared_len > max_body_size {
                        return Err(GrpcOutcome::PayloadTooLarge(declared_len));
                    }
                    checked_lpm = true;
                }

                if buf.len() > max_body_size.saturating_add(GrpcFrame::HEADER_SIZE) {
                    return Err(GrpcOutcome::PayloadTooLarge(buf.len()));
                }
            }
            Ok(buf.freeze())
        }
    }
}
