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
use crate::frame::{Http3Frame, encode_frame, encode_varint, encode_varint_slice, frame_id};
use crate::qpack::encode_qpack_response;
use crate::server::connection::OutgoingDatagram;

/// Initial buffer capacity for serializing HTTP/3 response HEADERS frames (512 bytes).
/// Accommodates status code, server, date, and common content headers without resizing.
pub const INITIAL_RESPONSE_HEADER_CAPACITY: usize = 512;

/// Maximum number of datagrams drained per `poll_transmit` invocation (64 datagrams).
/// Aligns with the Linux kernel GSO (`UDP_SEGMENT`) batching limit.
pub const GSO_TRANSMIT_BATCH_SIZE: usize = 64;

/// Maximum bytes required to encode an RFC 9000 VarInt frame type and payload length header (8 bytes).
pub const MAX_VARINT_FRAME_HEADER_SIZE: usize = 8;

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
        // 1. Encode HEADERS frame into small write_buf
        let mut header_payload = BytesMut::new();
        encode_qpack_response(self.status, &self.headers, &mut header_payload);
        let header_frame = Http3Frame::Headers(header_payload.freeze());

        let mut write_buf = BytesMut::with_capacity(512);
        encode_frame(&header_frame, &mut write_buf);

        let mut written = 0;
        while written < write_buf.len() {
            match conn.send_stream(stream_id).write(&write_buf[written..]) {
                Ok(n) if n > 0 => written += n,
                Ok(_) => break,
                Err(e) => return Err(Http3Error::H3(e.to_string())),
            }
        }
        if written < write_buf.len() {
            return Err(Http3Error::H3(format!(
                "Failed to write complete response headers: stream flow control window exhausted ({written}/{} bytes written)",
                write_buf.len()
            )));
        }

        // 2. Stream DATA frame directly without copying payload
        let is_no_body = self.status.is_informational()
            || self.status == StatusCode::NO_CONTENT
            || self.status == StatusCode::NOT_MODIFIED;

        if !is_no_body
            && let Body::Bytes(ref b) = self.body
            && !b.is_empty()
        {
            let mut hdr_buf = BytesMut::with_capacity(16);
            encode_varint(frame_id::DATA, &mut hdr_buf);
            encode_varint(b.len() as u64, &mut hdr_buf);

            let mut hdr_written = 0;
            while hdr_written < hdr_buf.len() {
                match conn.send_stream(stream_id).write(&hdr_buf[hdr_written..]) {
                    Ok(n) if n > 0 => hdr_written += n,
                    Ok(_) => break,
                    Err(e) => return Err(Http3Error::H3(e.to_string())),
                }
            }
            if hdr_written < hdr_buf.len() {
                return Err(Http3Error::H3(
                    "Flow control window exhausted for DATA frame header".into(),
                ));
            }

            let mut body_written = 0;
            while body_written < b.len() {
                match conn.send_stream(stream_id).write(&b[body_written..]) {
                    Ok(n) if n > 0 => body_written += n,
                    Ok(_) => break,
                    Err(e) => return Err(Http3Error::H3(e.to_string())),
                }
            }
            if body_written < b.len() {
                return Err(Http3Error::H3(format!(
                    "Failed to write complete response body: stream flow control window exhausted ({body_written}/{} bytes written)",
                    b.len()
                )));
            }
        }

        let _ = conn.send_stream(stream_id).finish();

        let mut outgoing = Vec::new();
        transmit_buf.clear();
        while let Some(transmit) = conn.poll_transmit(now, 64, transmit_buf) {
            if transmit.size > 0 {
                outgoing.push(OutgoingDatagram {
                    peer: transmit.destination,
                    payload: Bytes::copy_from_slice(&transmit_buf[..transmit.size]),
                    segment_size: transmit.segment_size.map(|s| s as u16),
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
/// If the stream flow control window is partially filled, writes as much payload as possible
/// and returns the remaining unwritten [`Bytes`] slice for subsequent draining on flow-control credit.
/// If the entire payload is written, closes the stream with `.finish()` and returns `None` for remaining.
pub fn send_response(
    conn: &mut Connection,
    stream_id: StreamId,
    response: &L7Response,
    transmit_buf: &mut Vec<u8>,
    now: Instant,
) -> Result<(Vec<OutgoingDatagram>, Option<Bytes>), Http3Error> {
    // 1. Encode QPACK HEADERS frame with pre-allocated buffer
    let mut header_payload = BytesMut::with_capacity(256);
    encode_qpack_response(response.status, &response.headers, &mut header_payload);
    let mut write_buf = BytesMut::with_capacity(16 + header_payload.len());
    encode_varint(frame_id::HEADERS, &mut write_buf);
    encode_varint(header_payload.len() as u64, &mut write_buf);
    write_buf.extend_from_slice(&header_payload);

    let mut written = 0;
    while written < write_buf.len() {
        match conn.send_stream(stream_id).write(&write_buf[written..]) {
            Ok(n) if n > 0 => written += n,
            Ok(_) => break,
            Err(e) => return Err(Http3Error::H3(e.to_string())),
        }
    }
    if written < write_buf.len() {
        return Err(Http3Error::H3(format!(
            "Failed to write complete response headers: stream flow control window exhausted ({written}/{} bytes written)",
            write_buf.len()
        )));
    }

    // 2. Stream DATA frame directly without copying payload
    let is_no_body = response.status.is_informational()
        || response.status == StatusCode::NO_CONTENT
        || response.status == StatusCode::NOT_MODIFIED;

    let mut remaining_body: Option<Bytes> = None;

    if !is_no_body
        && let Body::Bytes(ref b) = response.body
        && !b.is_empty()
    {
        let mut hdr_buf = [0u8; 16];
        let mut hdr_len = encode_varint_slice(frame_id::DATA, &mut hdr_buf);
        hdr_len += encode_varint_slice(b.len() as u64, &mut hdr_buf[hdr_len..]);

        let mut hdr_written = 0;
        while hdr_written < hdr_len {
            match conn
                .send_stream(stream_id)
                .write(&hdr_buf[hdr_written..hdr_len])
            {
                Ok(n) if n > 0 => hdr_written += n,
                Ok(_) => break,
                Err(e) => return Err(Http3Error::H3(e.to_string())),
            }
        }
        if hdr_written < hdr_len {
            return Err(Http3Error::H3(
                "Flow control window exhausted for DATA frame header".into(),
            ));
        }

        let mut body_written = 0;
        while body_written < b.len() {
            match conn.send_stream(stream_id).write(&b[body_written..]) {
                Ok(n) if n > 0 => body_written += n,
                Ok(_) => break,
                Err(quinn_proto::WriteError::Blocked) => break,
                Err(e) => return Err(Http3Error::H3(e.to_string())),
            }
        }
        if body_written < b.len() {
            remaining_body = Some(b.slice(body_written..));
        }
    }

    // Finish stream cleanly only if all body bytes have been completely written
    if remaining_body.is_none() {
        let _ = conn.send_stream(stream_id).finish();
    }

    // 4. Drain newly generated packets with GSO batching
    let mut outgoing = Vec::new();
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(now, 64, transmit_buf) {
        if transmit.size > 0 {
            outgoing.push(OutgoingDatagram {
                peer: transmit.destination,
                payload: Bytes::copy_from_slice(&transmit_buf[..transmit.size]),
                segment_size: transmit.segment_size.map(|s| s as u16),
            });
        }
        transmit_buf.clear();
    }

    Ok((outgoing, remaining_body))
}

/// Sends only the response HEADERS frame (QPACK-encoded) to start a progressive streaming response.
pub fn send_response_head(
    conn: &mut Connection,
    stream_id: StreamId,
    status: StatusCode,
    headers: &HeaderMap,
    transmit_buf: &mut Vec<u8>,
    now: Instant,
) -> Result<Vec<OutgoingDatagram>, Http3Error> {
    let mut header_payload = BytesMut::new();
    encode_qpack_response(status, headers, &mut header_payload);
    let header_frame = Http3Frame::Headers(header_payload.freeze());

    let mut write_buf = BytesMut::with_capacity(INITIAL_RESPONSE_HEADER_CAPACITY);
    encode_frame(&header_frame, &mut write_buf);

    let mut written = 0;
    while written < write_buf.len() {
        match conn.send_stream(stream_id).write(&write_buf[written..]) {
            Ok(n) if n > 0 => written += n,
            Ok(_) => break,
            Err(e) => return Err(Http3Error::H3(e.to_string())),
        }
    }
    if written < write_buf.len() {
        return Err(Http3Error::H3(format!(
            "Failed to write complete response headers: stream flow control window exhausted ({written}/{} bytes written)",
            write_buf.len()
        )));
    }

    let mut outgoing = Vec::new();
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(now, GSO_TRANSMIT_BATCH_SIZE, transmit_buf) {
        if transmit.size > 0 {
            outgoing.push(OutgoingDatagram {
                peer: transmit.destination,
                payload: Bytes::copy_from_slice(&transmit_buf[..transmit.size]),
                segment_size: transmit.segment_size.map(|s| s as u16),
            });
        }
        transmit_buf.clear();
    }

    Ok(outgoing)
}

/// Encodes an RFC 9114 DATA frame header with varint length prefix for the given chunk.
#[inline]
pub fn encode_data_frame(chunk: &Bytes) -> Bytes {
    if chunk.is_empty() {
        return Bytes::new();
    }
    let mut frame_buf = BytesMut::with_capacity(MAX_VARINT_FRAME_HEADER_SIZE + chunk.len());
    encode_varint(frame_id::DATA, &mut frame_buf);
    encode_varint(chunk.len() as u64, &mut frame_buf);
    frame_buf.extend_from_slice(chunk);
    frame_buf.freeze()
}

/// Sends a progressive response DATA frame chunk to the designated QUIC send stream.
///
/// Encodes an RFC 9114 DATA frame header with the chunk length and writes the payload.
/// If `finish` is true and all bytes are written, closes the stream with `.finish()`.
pub fn send_data_chunk(
    conn: &mut Connection,
    stream_id: StreamId,
    chunk: &Bytes,
    finish: bool,
    transmit_buf: &mut Vec<u8>,
    now: Instant,
) -> Result<(Vec<OutgoingDatagram>, Option<Bytes>), Http3Error> {
    let mut remaining_frame: Option<Bytes> = None;

    if !chunk.is_empty() {
        let mut hdr_buf = [0u8; 16];
        let mut hdr_len = encode_varint_slice(frame_id::DATA, &mut hdr_buf);
        hdr_len += encode_varint_slice(chunk.len() as u64, &mut hdr_buf[hdr_len..]);

        let mut hdr_written = 0;
        while hdr_written < hdr_len {
            match conn
                .send_stream(stream_id)
                .write(&hdr_buf[hdr_written..hdr_len])
            {
                Ok(n) if n > 0 => hdr_written += n,
                Ok(_) => break,
                Err(quinn_proto::WriteError::Blocked) => break,
                Err(e) => return Err(Http3Error::H3(e.to_string())),
            }
        }

        if hdr_written < hdr_len {
            let mut fallback = BytesMut::with_capacity((hdr_len - hdr_written) + chunk.len());
            fallback.extend_from_slice(&hdr_buf[hdr_written..hdr_len]);
            fallback.extend_from_slice(chunk);
            remaining_frame = Some(fallback.freeze());
        } else {
            let mut body_written = 0;
            while body_written < chunk.len() {
                match conn.send_stream(stream_id).write(&chunk[body_written..]) {
                    Ok(n) if n > 0 => body_written += n,
                    Ok(_) => break,
                    Err(quinn_proto::WriteError::Blocked) => break,
                    Err(e) => return Err(Http3Error::H3(e.to_string())),
                }
            }
            if body_written < chunk.len() {
                remaining_frame = Some(chunk.slice(body_written..));
            }
        }
    }

    if finish && remaining_frame.is_none() {
        let _ = conn.send_stream(stream_id).finish();
    }

    let mut outgoing = Vec::new();
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(now, GSO_TRANSMIT_BATCH_SIZE, transmit_buf) {
        if transmit.size > 0 {
            outgoing.push(OutgoingDatagram {
                peer: transmit.destination,
                payload: Bytes::copy_from_slice(&transmit_buf[..transmit.size]),
                segment_size: transmit.segment_size.map(|s| s as u16),
            });
        }
        transmit_buf.clear();
    }

    Ok((outgoing, remaining_frame))
}
