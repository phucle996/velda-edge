//! HTTP/3 (RFC 9114) frame parsing and variable-length integer decoding.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::Http3Error;

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
            0x00 => Self::Data,
            0x01 => Self::Headers,
            0x03 => Self::CancelPush,
            0x04 => Self::Settings,
            0x05 => Self::PushPromise,
            0x07 => Self::GoAway,
            0x0d => Self::MaxPushId,
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
    /// SETTINGS frame carrying connection configuration pairs.
    Settings(Vec<(u64, u64)>),
    /// GOAWAY frame signaling graceful connection termination.
    GoAway(u64),
    /// Other reserved or extension frames.
    Other { frame_type: u64, payload: Bytes },
}

/// Decodes an RFC 9000 variable-length integer from a byte buffer.
pub fn decode_varint(buf: &mut BytesMut) -> Option<u64> {
    if buf.is_empty() {
        return None;
    }

    let first = buf[0];
    let prefix = first >> 6;
    let length = match prefix {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => unreachable!(),
    };

    if buf.len() < length {
        return None;
    }

    let mut val = (first & 0x3f) as u64;
    buf.advance(1);

    for _ in 1..length {
        val = (val << 8) | (buf.get_u8() as u64);
    }

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
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<Http3Frame>, Http3Error> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut peek_buf = buf.clone();
    let Some(frame_type) = decode_varint(&mut peek_buf) else {
        return Ok(None);
    };

    let Some(length) = decode_varint(&mut peek_buf) else {
        return Ok(None);
    };

    let header_bytes = buf.len() - peek_buf.len();
    if peek_buf.len() < length as usize {
        return Ok(None); // Need more payload bytes
    }

    // Consume header from real buffer
    buf.advance(header_bytes);
    let payload = buf.split_to(length as usize).freeze();

    let frame = match FrameType::from(frame_type) {
        FrameType::Data => Http3Frame::Data(payload),
        FrameType::Headers => Http3Frame::Headers(payload),
        FrameType::GoAway => {
            let mut p = BytesMut::from(&payload[..]);
            let id = decode_varint(&mut p).unwrap_or(0);
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
            encode_varint(0x00, dst);
            encode_varint(data.len() as u64, dst);
            dst.put_slice(data);
        }
        Http3Frame::Headers(headers) => {
            encode_varint(0x01, dst);
            encode_varint(headers.len() as u64, dst);
            dst.put_slice(headers);
        }
        Http3Frame::GoAway(id) => {
            let mut id_buf = BytesMut::new();
            encode_varint(*id, &mut id_buf);
            encode_varint(0x07, dst);
            encode_varint(id_buf.len() as u64, dst);
            dst.put_slice(&id_buf);
        }
        Http3Frame::Settings(settings) => {
            let mut s_buf = BytesMut::new();
            for (k, v) in settings {
                encode_varint(*k, &mut s_buf);
                encode_varint(*v, &mut s_buf);
            }
            encode_varint(0x04, dst);
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
}
