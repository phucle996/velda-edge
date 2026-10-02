//! HTTP/3 (RFC 9114) frame parsing and variable-length integer decoding.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::Http3Error;

/// Canonical RFC 9114 HTTP/3 error codes.
pub mod error_code {
    /// No error or graceful shutdown.
    pub const H3_NO_ERROR: u64 = 0x0100;
    /// General protocol violation.
    pub const H3_GENERAL_PROTOCOL_ERROR: u64 = 0x0101;
    /// Internal edge engine error.
    pub const H3_INTERNAL_ERROR: u64 = 0x0102;
    /// Stream creation error.
    pub const H3_STREAM_CREATION_ERROR: u64 = 0x0103;
    /// Critical stream closed unexpectedly.
    pub const H3_CLOSED_CRITICAL_STREAM: u64 = 0x0104;
    /// Frame received unexpectedly for stream state.
    pub const H3_FRAME_UNEXPECTED: u64 = 0x0105;
    /// Frame syntax or length error.
    pub const H3_FRAME_ERROR: u64 = 0x0106;
    /// Excessive load or resource exhaustion.
    pub const H3_EXCESSIVE_LOAD: u64 = 0x0107;
    /// Frame or stream ID exceeds permissible maximum.
    pub const H3_ID_ERROR: u64 = 0x0108;
    /// Invalid SETTINGS parameter or value.
    pub const H3_SETTINGS_ERROR: u64 = 0x0109;
    /// Missing mandatory SETTINGS frame.
    pub const H3_MISSING_SETTINGS: u64 = 0x010a;
    /// Request rejected before processing started.
    pub const H3_REQUEST_REJECTED: u64 = 0x010b;
    /// Request cancelled by client or stream abort.
    pub const H3_REQUEST_CANCELLED: u64 = 0x010c;
    /// Incomplete request stream before FIN.
    pub const H3_REQUEST_INCOMPLETE: u64 = 0x010d;
    /// Malformed HTTP message framing.
    pub const H3_MESSAGE_ERROR: u64 = 0x010e;
    /// CONNECT request tunnel negotiation failure.
    pub const H3_CONNECT_ERROR: u64 = 0x010f;
    /// Peer requires ALPN or HTTP version fallback.
    pub const H3_VERSION_FALLBACK: u64 = 0x0110;
}

/// Canonical RFC 9114 unidirectional stream types.
pub mod stream_type {
    /// Control stream carrying SETTINGS and connection-level frames (RFC 9114 Section 6.2.1).
    pub const CONTROL: u64 = 0x00;
    /// Push stream carrying server push responses (RFC 9114 Section 6.2.2).
    pub const PUSH: u64 = 0x01;
    /// QPACK encoder stream carrying dynamic table insertions (RFC 9204 Section 4.2).
    pub const QPACK_ENCODER: u64 = 0x02;
    /// QPACK decoder stream carrying table acknowledgments (RFC 9204 Section 4.2).
    pub const QPACK_DECODER: u64 = 0x03;
}

/// Canonical RFC 9114 and RFC 9204 SETTINGS identifiers.
pub mod settings_id {
    /// Maximum capacity of QPACK dynamic table (RFC 9204 Section 5).
    pub const QPACK_MAX_TABLE_CAPACITY: u64 = 0x01;
    /// Maximum size of field section in bytes (RFC 9114 Section 7.2.4.1).
    pub const MAX_FIELD_SECTION_SIZE: u64 = 0x06;
    /// Maximum number of streams blocked on dynamic QPACK insertions (RFC 9204 Section 5).
    pub const QPACK_BLOCKED_STREAMS: u64 = 0x07;
    /// Extended CONNECT protocol support (RFC 9220).
    pub const ENABLE_CONNECT_PROTOCOL: u64 = 0x08;
    /// HTTP/3 Datagram support (RFC 9297).
    pub const H3_DATAGRAM: u64 = 0x0033_3877;
}

/// Canonical RFC 9114 HTTP/3 Frame type identifiers.
pub mod frame_id {
    /// DATA frame (RFC 9114 Section 7.2.1).
    pub const DATA: u64 = 0x00;
    /// HEADERS frame (RFC 9114 Section 7.2.2).
    pub const HEADERS: u64 = 0x01;
    /// CANCEL_PUSH frame (RFC 9114 Section 7.2.3).
    pub const CANCEL_PUSH: u64 = 0x02;
    /// SETTINGS frame (RFC 9114 Section 7.2.4).
    pub const SETTINGS: u64 = 0x04;
    /// PUSH_PROMISE frame (RFC 9114 Section 7.2.5).
    pub const PUSH_PROMISE: u64 = 0x05;
    /// GOAWAY frame (RFC 9114 Section 7.2.6).
    pub const GOAWAY: u64 = 0x07;
    /// MAX_PUSH_ID frame (RFC 9114 Section 7.2.7).
    pub const MAX_PUSH_ID: u64 = 0x0d;
}

/// HTTP/3 Frame Types defined in RFC 9114.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Data,
    Headers,
    CancelPush,
    Settings,
    PushPromise,
    GoAway,
    MaxPushId,
    Unknown(u64),
}

impl From<u64> for FrameType {
    fn from(val: u64) -> Self {
        match val {
            frame_id::DATA => Self::Data,
            frame_id::HEADERS => Self::Headers,
            frame_id::CANCEL_PUSH => Self::CancelPush,
            frame_id::SETTINGS => Self::Settings,
            frame_id::PUSH_PROMISE => Self::PushPromise,
            frame_id::GOAWAY => Self::GoAway,
            frame_id::MAX_PUSH_ID => Self::MaxPushId,
            other => Self::Unknown(other),
        }
    }
}

/// A parsed HTTP/3 frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Http3Frame {
    /// DATA frame carrying request/response body payload.
    Data(Bytes),
    /// HEADERS frame carrying QPACK-encoded headers.
    Headers(Bytes),
    /// CANCEL_PUSH frame signaling push stream cancellation.
    CancelPush(u64),
    /// SETTINGS frame carrying connection configuration pairs.
    Settings(Vec<(u64, u64)>),
    /// GOAWAY frame signaling graceful connection termination.
    GoAway(u64),
    /// Other reserved or extension frames.
    Other { frame_type: u64, payload: Bytes },
}

/// Decodes an RFC 9000 variable-length integer from a borrowed byte slice.
///
/// Returns `Some((value, bytes_consumed))` without mutating or cloning the slice.
#[inline]
pub fn decode_varint_slice(slice: &[u8]) -> Option<(u64, usize)> {
    if slice.is_empty() {
        return None;
    }

    let first = slice[0];
    let prefix = first >> 6;
    let length = match prefix {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => unreachable!(),
    };

    if slice.len() < length {
        return None;
    }

    let mut val = (first & 0x3f) as u64;
    for &b in slice.iter().take(length).skip(1) {
        val = (val << 8) | (b as u64);
    }

    Some((val, length))
}

/// Decodes an RFC 9000 variable-length integer from a byte buffer.
pub fn decode_varint(buf: &mut BytesMut) -> Option<u64> {
    let (val, len) = decode_varint_slice(buf)?;
    buf.advance(len);
    Some(val)
}

/// Encodes an RFC 9000 variable-length integer into a byte buffer.
pub fn encode_varint(val: u64, dst: &mut BytesMut) {
    if val <= 63 {
        dst.put_u8(val as u8);
    } else if val <= 16383 {
        dst.put_u8((0x40 | (val >> 8)) as u8);
        dst.put_u8((val & 0xff) as u8);
    } else if val <= 1073741823 {
        dst.put_u8((0x80 | (val >> 24)) as u8);
        dst.put_u8(((val >> 16) & 0xff) as u8);
        dst.put_u8(((val >> 8) & 0xff) as u8);
        dst.put_u8((val & 0xff) as u8);
    } else {
        dst.put_u8((0xc0 | (val >> 56)) as u8);
        dst.put_u8(((val >> 48) & 0xff) as u8);
        dst.put_u8(((val >> 40) & 0xff) as u8);
        dst.put_u8(((val >> 32) & 0xff) as u8);
        dst.put_u8(((val >> 24) & 0xff) as u8);
        dst.put_u8(((val >> 16) & 0xff) as u8);
        dst.put_u8(((val >> 8) & 0xff) as u8);
        dst.put_u8((val & 0xff) as u8);
    }
}

/// Decodes the next HTTP/3 frame from the buffer.
///
/// Inspects framing headers zero-copy via [`decode_varint_slice`] before consuming bytes.
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<Http3Frame>, Http3Error> {
    if buf.is_empty() {
        return Ok(None);
    }

    let Some((frame_type, type_len)) = decode_varint_slice(buf) else {
        return Ok(None);
    };

    let Some((length, len_len)) = decode_varint_slice(&buf[type_len..]) else {
        return Ok(None);
    };

    let header_bytes = type_len + len_len;
    let payload_len = length as usize;

    if buf.len() < header_bytes + payload_len {
        return Ok(None); // Need more payload bytes
    }

    // Consume header from real buffer
    buf.advance(header_bytes);
    let payload = buf.split_to(payload_len).freeze();

    let frame = match FrameType::from(frame_type) {
        FrameType::Data => Http3Frame::Data(payload),
        FrameType::Headers => Http3Frame::Headers(payload),
        FrameType::CancelPush => {
            let id = decode_varint_slice(&payload).map(|(v, _)| v).unwrap_or(0);
            Http3Frame::CancelPush(id)
        }
        FrameType::Settings => {
            let mut settings = Vec::new();
            let mut rem = &payload[..];
            while !rem.is_empty() {
                if let Some((id, id_len)) = decode_varint_slice(rem) {
                    rem = &rem[id_len..];
                    if let Some((val, val_len)) = decode_varint_slice(rem) {
                        rem = &rem[val_len..];
                        settings.push((id, val));
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            Http3Frame::Settings(settings)
        }
        FrameType::GoAway => {
            let id = decode_varint_slice(&payload).map(|(v, _)| v).unwrap_or(0);
            Http3Frame::GoAway(id)
        }
        _ => Http3Frame::Other {
            frame_type,
            payload,
        },
    };

    Ok(Some(frame))
}

/// Encodes an HTTP/3 frame into the buffer.
pub fn encode_frame(frame: &Http3Frame, dst: &mut BytesMut) {
    match frame {
        Http3Frame::Data(data) => {
            encode_varint(frame_id::DATA, dst);
            encode_varint(data.len() as u64, dst);
            dst.put_slice(data);
        }
        Http3Frame::Headers(headers) => {
            encode_varint(frame_id::HEADERS, dst);
            encode_varint(headers.len() as u64, dst);
            dst.put_slice(headers);
        }
        Http3Frame::CancelPush(id) => {
            let mut id_buf = BytesMut::new();
            encode_varint(*id, &mut id_buf);
            encode_varint(frame_id::CANCEL_PUSH, dst);
            encode_varint(id_buf.len() as u64, dst);
            dst.put_slice(&id_buf);
        }
        Http3Frame::GoAway(id) => {
            let mut id_buf = BytesMut::new();
            encode_varint(*id, &mut id_buf);
            encode_varint(frame_id::GOAWAY, dst);
            encode_varint(id_buf.len() as u64, dst);
            dst.put_slice(&id_buf);
        }
        Http3Frame::Settings(settings) => {
            let mut s_buf = BytesMut::new();
            for (k, v) in settings {
                encode_varint(*k, &mut s_buf);
                encode_varint(*v, &mut s_buf);
            }
            encode_varint(frame_id::SETTINGS, dst);
            encode_varint(s_buf.len() as u64, dst);
            dst.put_slice(&s_buf);
        }
        Http3Frame::Other {
            frame_type,
            payload,
        } => {
            encode_varint(*frame_type, dst);
            encode_varint(payload.len() as u64, dst);
            dst.put_slice(payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        let values = [
            0,
            42,
            63,
            64,
            16383,
            16384,
            1073741823,
            1073741824,
            4611686018427387903,
        ];
        for val in values {
            let mut buf = BytesMut::new();
            encode_varint(val, &mut buf);
            let (decoded_slice, len) = decode_varint_slice(&buf).unwrap();
            assert_eq!(decoded_slice, val);
            assert_eq!(len, buf.len());

            let decoded = decode_varint(&mut buf).unwrap();
            assert_eq!(decoded, val);
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn test_data_frame_roundtrip() {
        let frame = Http3Frame::Data(Bytes::from_static(b"quic payload"));
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);

        let decoded = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_headers_frame_roundtrip() {
        let frame = Http3Frame::Headers(Bytes::from_static(b"qpack headers block"));
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);

        let decoded = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_settings_frame_roundtrip() {
        let settings = vec![
            (settings_id::QPACK_MAX_TABLE_CAPACITY, 4096),
            (settings_id::MAX_FIELD_SECTION_SIZE, 65536),
            (settings_id::QPACK_BLOCKED_STREAMS, 100),
        ];
        let frame = Http3Frame::Settings(settings);
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);

        let decoded = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_cancel_push_roundtrip() {
        let frame = Http3Frame::CancelPush(42);
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);

        let decoded = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert!(buf.is_empty());
    }
}
