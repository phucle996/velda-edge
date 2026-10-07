//! Layer 7 HTTP/3 response serialization and egress packet generation.
//!
//! Serializes [`L7Response`] into RFC 9114 HEADERS (QPACK-encoded) and DATA frames,
//! writes them to the designated QUIC send stream, and drains outgoing datagrams.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use quinn_proto::{Connection, StreamId};
use velda_core::{Body, L7Response};

use crate::error::Http3Error;
use crate::frame::{Http3Frame, encode_frame};
use crate::qpack::encode_qpack_response;
use crate::server::connection::OutgoingDatagram;

/// Downstream HTTP/3 server response head metadata.
#[derive(Debug, Clone)]
pub struct Http3ServerResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// Protocol version (HTTP/3.0).
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
}

impl Http3ServerResponseHead {
    /// Creates a new HTTP/3 server response head.
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap) -> Self {
        Self {
            status,
            version: Version::HTTP_3,
            headers,
        }
    }
}

/// Downstream protocol-owned HTTP/3 server response.
#[derive(Debug, Clone)]
pub struct Http3ServerResponse {
    /// Response status code.
    pub status: StatusCode,
    /// Protocol version (HTTP/3.0).
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response payload body.
    pub body: Body,
}

impl Http3ServerResponse {
    /// Creates a new HTTP/3 server response.
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap, body: Body) -> Self {
        Self {
            status,
            version: Version::HTTP_3,
            headers,
            body,
        }
    }

    /// Fast constructor from status code and byte payload.
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

    /// Converts from canonical [`L7Response`].
    #[inline]
    pub fn from_l7_response(resp: L7Response) -> Self {
        Self {
            status: resp.status,
            version: Version::HTTP_3,
            headers: resp.headers,
            body: resp.body,
        }
    }

    /// Converts into canonical [`L7Response`].
    #[inline]
    pub fn into_l7_response(self) -> L7Response {
        L7Response::new(self.status, self.version, self.headers, self.body)
    }

    /// Converts from upstream [`Http3ClientResponse`].
    pub fn from_client_response(resp: crate::client::response::Http3ClientResponse) -> Self {
        Self {
            status: resp.status,
            version: Version::HTTP_3,
            headers: resp.headers,
            body: resp.body,
        }
    }

    /// Returns response head metadata.
    #[inline]
    pub fn head(&self) -> Http3ServerResponseHead {
        Http3ServerResponseHead::new(self.status, self.headers.clone())
    }

    /// Encodes this server response into HTTP/3 QPACK HEADERS and DATA frames buffer.
    pub fn encode_frames(&self) -> BytesMut {
        let mut header_payload = BytesMut::new();
        encode_qpack_response(self.status, &self.headers, &mut header_payload);
        let header_frame = Http3Frame::Headers(header_payload.freeze());

        let mut write_buf = BytesMut::new();
        encode_frame(&header_frame, &mut write_buf);

        let is_no_body = self.status.is_informational()
            || self.status == StatusCode::NO_CONTENT
            || self.status == StatusCode::NOT_MODIFIED;

        if !is_no_body
            && let Body::Bytes(ref b) = self.body
            && !b.is_empty()
        {
            let data_frame = Http3Frame::Data(b.clone());
            encode_frame(&data_frame, &mut write_buf);
        }

        write_buf
    }

    /// Serializes this response into HTTP/3 frames, writes to QUIC send stream,
    /// and drains newly generated outgoing datagrams.
    pub fn send(
        &self,
        conn: &mut Connection,
        stream_id: StreamId,
        transmit_buf: &mut Vec<u8>,
        now: Instant,
    ) -> Result<Vec<OutgoingDatagram>, Http3Error> {
        let write_buf = self.encode_frames();

        let mut written = 0;
        while written < write_buf.len() {
            match conn.send_stream(stream_id).write(&write_buf[written..]) {
                Ok(n) => {
                    if n == 0 {
                        break;
                    }
                    written += n;
                }
                Err(e) => return Err(Http3Error::H3(e.to_string())),
            }
        }

        if written < write_buf.len() {
            return Err(Http3Error::H3(format!(
                "Failed to write complete response: stream flow control window exhausted ({written}/{} bytes written)",
                write_buf.len()
            )));
        }

        let _ = conn.send_stream(stream_id).finish();

        let mut outgoing = Vec::new();
        transmit_buf.clear();
        while let Some(transmit) = conn.poll_transmit(now, 1, transmit_buf) {
            if transmit.size > 0 {
                let packet = Bytes::copy_from_slice(&transmit_buf[..transmit.size]);
                outgoing.push(OutgoingDatagram {
                    peer: transmit.destination,
                    payload: packet,
                });
            }
            transmit_buf.clear();
        }

        Ok(outgoing)
    }
}

/// Serializes status and body into HTTP/3 HEADERS and DATA frames in a buffer.
pub fn build_edge_response_frames(status: StatusCode, body: &[u8]) -> BytesMut {
    let mut header_payload = BytesMut::new();
    encode_qpack_response(status, &HeaderMap::new(), &mut header_payload);
    let header_frame = Http3Frame::Headers(header_payload.freeze());

    let mut write_buf = BytesMut::new();
    encode_frame(&header_frame, &mut write_buf);

    let is_no_body = status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;

    if !is_no_body && !body.is_empty() {
        let data_frame = Http3Frame::Data(Bytes::copy_from_slice(body));
        encode_frame(&data_frame, &mut write_buf);
    }
    write_buf
}

/// Serializes an [`L7Response`] into HTTP/3 HEADERS and DATA frames, writes them
/// to the QUIC stream, and drains newly generated outgoing UDP packets.
///
/// Pure behavior: caller provides the connection and response reference;
/// the protocol processes and returns outgoing datagrams immediately.
pub fn send_response(
    conn: &mut Connection,
    stream_id: StreamId,
    response: &L7Response,
    transmit_buf: &mut Vec<u8>,
    now: Instant,
) -> Result<Vec<OutgoingDatagram>, Http3Error> {
    // 1. Encode QPACK HEADERS frame
    let mut header_payload = BytesMut::new();
    encode_qpack_response(response.status, &response.headers, &mut header_payload);
    let header_frame = Http3Frame::Headers(header_payload.freeze());

    let mut write_buf = BytesMut::new();
    encode_frame(&header_frame, &mut write_buf);

    // 2. Encode DATA frame if body is present and status permits body (RFC 9114 §4.1)
    let is_no_body = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    if !is_no_body
        && let Body::Bytes(ref b) = response.body
        && !b.is_empty()
    {
        let data_frame = Http3Frame::Data(b.clone());
        encode_frame(&data_frame, &mut write_buf);
    }

    // 3. Write frames to send stream with full buffer coverage
    let mut written = 0;
    while written < write_buf.len() {
        match conn.send_stream(stream_id).write(&write_buf[written..]) {
            Ok(n) => {
                if n == 0 {
                    break;
                }
                written += n;
            }
            Err(e) => return Err(Http3Error::H3(e.to_string())),
        }
    }

    if written < write_buf.len() {
        return Err(Http3Error::H3(format!(
            "Failed to write complete response: stream flow control window exhausted ({written}/{} bytes written)",
            write_buf.len()
        )));
    }

    // Finish stream cleanly
    let _ = conn.send_stream(stream_id).finish();

    // 4. Drain newly generated packets
    let mut outgoing = Vec::new();
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(now, 1, transmit_buf) {
        if transmit.size > 0 {
            let packet = Bytes::copy_from_slice(&transmit_buf[..transmit.size]);
            outgoing.push(OutgoingDatagram {
                peer: transmit.destination,
                payload: packet,
            });
        }
        transmit_buf.clear();
    }

    Ok(outgoing)
}
