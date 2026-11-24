//! RFC 9113 HTTP/2 Binary Wire Framing Codec.
//!
//! ```text
//! RFC 9113 §4.1 HTTP/2 Frame Format (9-byte Header + Variable Payload):
//!  +-----------------------------------------------+
//!  |                 Length (24)                   |  [0..3] max_frame_size (16KB..16MB-1)
//!  +---------------+---------------+---------------+
//!  |   Type (8)    |   Flags (8)   |
//!  +-+-------------+---------------+---------------+
//!  |R|                 Stream Identifier (31)      |  [5..9] R bit masked out (& 0x7FFF_FFFF)
//!  +=+=============================================+
//!  |                 Frame Payload (0...)          |
//!  +-----------------------------------------------+
//!
//! Frame Kinds:
//!  0x0 DATA          (stream != 0, END_STREAM=0x1, PADDED=0x8)
//!  0x1 HEADERS       (stream != 0, END_STREAM=0x1, END_HEADERS=0x4, PADDED=0x8, PRIORITY=0x20)
//!  0x2 PRIORITY      (stream != 0, len == 5)
//!  0x3 RST_STREAM    (stream != 0, len == 4 -> error_code)
//!  0x4 SETTINGS      (stream == 0, ACK=0x1 -> len==0 | pairs*6)
//!  0x5 PUSH_PROMISE  (stream != 0, END_HEADERS=0x4, PADDED=0x8)
//!  0x6 PING          (stream == 0, ACK=0x1, len == 8)
//!  0x7 GOAWAY        (stream == 0, len >= 8 -> last_stream_id, error_code, debug_data)
//!  0x8 WINDOW_UPDATE (stream >= 0, len == 4 -> increment 1..=0x7FFF_FFFF)
//!  0x9 CONTINUATION  (stream != 0, END_HEADERS=0x4)
//! ```

use bytes::{BufMut, Bytes, BytesMut};

/// HTTP/2 client connection preface sent before the initial SETTINGS frame (RFC 9113 §3.4).
pub const CONNECTION_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Minimum permitted `SETTINGS_MAX_FRAME_SIZE` per RFC 9113 §6.5.2 (16,384 bytes).
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;

/// Maximum permitted `SETTINGS_MAX_FRAME_SIZE` per RFC 9113 §6.5.2 (16,777,215 bytes).
pub const MAX_ALLOWED_FRAME_SIZE: u32 = 16_777_215;

/// Default RFC 9113 initial flow-control window size (65,535 bytes).
pub const DEFAULT_INITIAL_WINDOW_SIZE: u32 = 65_535;

/// Maximum legal RFC 9113 flow-control window size (`2^31 - 1`).
pub const MAX_WINDOW_SIZE: u32 = 0x7FFF_FFFF;

/// Wire frame header size in bytes (RFC 9113 §4.1).
pub const FRAME_HEADER_LEN: usize = 9;

/// HTTP/2 Frame Type IDs (RFC 9113 §6).
pub mod frame_type {
    pub const DATA: u8 = 0x0;
    pub const HEADERS: u8 = 0x1;
    pub const PRIORITY: u8 = 0x2;
    pub const RST_STREAM: u8 = 0x3;
    pub const SETTINGS: u8 = 0x4;
    pub const PUSH_PROMISE: u8 = 0x5;
    pub const PING: u8 = 0x6;
    pub const GOAWAY: u8 = 0x7;
    pub const WINDOW_UPDATE: u8 = 0x8;
    pub const CONTINUATION: u8 = 0x9;
}

/// HTTP/2 Frame Flags (RFC 9113 §6).
pub mod flags {
    pub const END_STREAM: u8 = 0x1;
    pub const ACK: u8 = 0x1;
    pub const END_HEADERS: u8 = 0x4;
    pub const PADDED: u8 = 0x8;
    pub const PRIORITY: u8 = 0x20;
}

/// HTTP/2 SETTINGS Parameter Identifiers (RFC 9113 §6.5.2).
pub mod setting_id {
    pub const HEADER_TABLE_SIZE: u16 = 0x1;
    pub const ENABLE_PUSH: u16 = 0x2;
    pub const MAX_CONCURRENT_STREAMS: u16 = 0x3;
    pub const INITIAL_WINDOW_SIZE: u16 = 0x4;
    pub const MAX_FRAME_SIZE: u16 = 0x5;
    pub const MAX_HEADER_LIST_SIZE: u16 = 0x6;
    pub const NO_RFC7540_PRIORITIES: u16 = 0x9;
}

/// Canonical 32-bit HTTP/2 Error Codes (RFC 9113 §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct H2ErrorCode(pub u32);

impl H2ErrorCode {
    pub const NO_ERROR: Self = Self(0x0);
    pub const PROTOCOL_ERROR: Self = Self(0x1);
    pub const INTERNAL_ERROR: Self = Self(0x2);
    pub const FLOW_CONTROL_ERROR: Self = Self(0x3);
    pub const SETTINGS_TIMEOUT: Self = Self(0x4);
    pub const STREAM_CLOSED: Self = Self(0x5);
    pub const FRAME_SIZE_ERROR: Self = Self(0x6);
    pub const REFUSED_STREAM: Self = Self(0x7);
    pub const CANCEL: Self = Self(0x8);
    pub const COMPRESSION_ERROR: Self = Self(0x9);
    pub const CONNECT_ERROR: Self = Self(0xa);
    pub const ENHANCE_YOUR_CALM: Self = Self(0xb);
    pub const INADEQUATE_SECURITY: Self = Self(0xc);
    pub const HTTP_1_1_REQUIRED: Self = Self(0xd);
}

/// Parsed 9-byte HTTP/2 frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub length: u32,
    pub kind: u8,
    pub flags: u8,
    pub stream_id: u32,
}

/// Frame decode violation mapped to RFC 9113 connection/stream error semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameError {
    pub code: H2ErrorCode,
    pub stream_id: u32,
    pub msg: &'static str,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HTTP/2 frame error (code={:#x}, stream={}): {}",
            self.code.0, self.stream_id, self.msg
        )
    }
}

impl std::error::Error for FrameError {}

/// Single `SETTINGS` identifier/value pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettingPair {
    pub id: u16,
    pub value: u32,
}

/// Decoded RFC 9113 HTTP/2 frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum H2Frame {
    Data {
        stream_id: u32,
        end_stream: bool,
        payload: Bytes,
    },
    Headers {
        stream_id: u32,
        end_stream: bool,
        end_headers: bool,
        header_block: Bytes,
    },
    Priority {
        stream_id: u32,
        exclusive: bool,
        dependency: u32,
        weight: u8,
    },
    RstStream {
        stream_id: u32,
        error_code: H2ErrorCode,
    },
    Settings {
        ack: bool,
        entries: Vec<SettingPair>,
    },
    PushPromise {
        stream_id: u32,
        end_headers: bool,
        promised_stream_id: u32,
        header_block: Bytes,
    },
    Ping {
        ack: bool,
        opaque_data: [u8; 8],
    },
    GoAway {
        last_stream_id: u32,
        error_code: H2ErrorCode,
        debug_data: Bytes,
    },
    WindowUpdate {
        stream_id: u32,
        increment: u32,
    },
    Continuation {
        stream_id: u32,
        end_headers: bool,
        header_block: Bytes,
    },
    /// Unknown extension frame (MUST be ignored per RFC 9113 §4.1).
    Unknown {
        header: FrameHeader,
        payload: Bytes,
    },
}

/// Decodes a 9-byte frame header from `buf`.
/// Returns `Ok(None)` when fewer than 9 bytes are available.
#[inline]
pub fn parse_frame_header(buf: &[u8]) -> Result<Option<FrameHeader>, FrameError> {
    let Some(hdr) = buf.get(..FRAME_HEADER_LEN) else {
        return Ok(None);
    };
    let length = ((hdr[0] as u32) << 16) | ((hdr[1] as u32) << 8) | (hdr[2] as u32);
    let kind = hdr[3];
    let flags = hdr[4];
    let stream_id = u32::from_be_bytes([hdr[5], hdr[6], hdr[7], hdr[8]]) & 0x7FFF_FFFF;
    Ok(Some(FrameHeader {
        length,
        kind,
        flags,
        stream_id,
    }))
}

/// Parses one complete HTTP/2 frame from `buf` enforcing `max_frame_size`.
/// Returns `Ok(None)` if `buf` does not yet hold the full frame.
/// On `Ok(Some((frame, total_consumed)))`, `total_consumed == 9 + header.length`.
pub fn decode_frame(
    buf: &[u8],
    max_frame_size: u32,
) -> Result<Option<(H2Frame, usize)>, FrameError> {
    let Some(header) = parse_frame_header(buf)? else {
        return Ok(None);
    };
    if header.length > max_frame_size {
        return Err(FrameError {
            code: H2ErrorCode::FRAME_SIZE_ERROR,
            stream_id: 0,
            msg: "frame payload exceeds SETTINGS_MAX_FRAME_SIZE",
        });
    }

    let total_len = FRAME_HEADER_LEN + header.length as usize;
    let Some(raw_frame) = buf.get(..total_len) else {
        return Ok(None);
    };
    let payload = &raw_frame[FRAME_HEADER_LEN..];
    let frame = parse_frame_payload(header, payload)?;
    Ok(Some((frame, total_len)))
}

#[inline]
fn strip_padding(payload: &[u8], is_padded: bool) -> Result<&[u8], FrameError> {
    if !is_padded {
        return Ok(payload);
    }
    let Some((&pad_len_byte, rest)) = payload.split_first() else {
        return Err(FrameError {
            code: H2ErrorCode::FRAME_SIZE_ERROR,
            stream_id: 0,
            msg: "PADDED frame missing pad length octet",
        });
    };
    let pad_len = pad_len_byte as usize;
    let Some(unpadded_len) = rest.len().checked_sub(pad_len) else {
        return Err(FrameError {
            code: H2ErrorCode::PROTOCOL_ERROR,
            stream_id: 0,
            msg: "padding length exceeds frame payload",
        });
    };
    Ok(&rest[..unpadded_len])
}

fn parse_frame_payload(header: FrameHeader, payload: &[u8]) -> Result<H2Frame, FrameError> {
    let sid = header.stream_id;
    match header.kind {
        frame_type::DATA => {
            if sid == 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "DATA frame on stream 0",
                });
            }
            let unpadded = strip_padding(payload, (header.flags & flags::PADDED) != 0)?;
            Ok(H2Frame::Data {
                stream_id: sid,
                end_stream: (header.flags & flags::END_STREAM) != 0,
                payload: Bytes::copy_from_slice(unpadded),
            })
        }
        frame_type::HEADERS => {
            if sid == 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "HEADERS frame on stream 0",
                });
            }
            let mut body = strip_padding(payload, (header.flags & flags::PADDED) != 0)?;
            if (header.flags & flags::PRIORITY) != 0 {
                let Some((_prio, rest)) = body.split_at_checked(5) else {
                    return Err(FrameError {
                        code: H2ErrorCode::FRAME_SIZE_ERROR,
                        stream_id: 0,
                        msg: "HEADERS with PRIORITY truncated",
                    });
                };
                body = rest;
            }
            Ok(H2Frame::Headers {
                stream_id: sid,
                end_stream: (header.flags & flags::END_STREAM) != 0,
                end_headers: (header.flags & flags::END_HEADERS) != 0,
                header_block: Bytes::copy_from_slice(body),
            })
        }
        frame_type::PRIORITY => {
            if sid == 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "PRIORITY frame on stream 0",
                });
            }
            if payload.len() != 5 {
                return Err(FrameError {
                    code: H2ErrorCode::FRAME_SIZE_ERROR,
                    stream_id: sid,
                    msg: "PRIORITY frame length must be 5",
                });
            }
            let raw_dep = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Ok(H2Frame::Priority {
                stream_id: sid,
                exclusive: (raw_dep & 0x8000_0000) != 0,
                dependency: raw_dep & 0x7FFF_FFFF,
                weight: payload[4],
            })
        }
        frame_type::RST_STREAM => {
            if sid == 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "RST_STREAM frame on stream 0",
                });
            }
            if payload.len() != 4 {
                return Err(FrameError {
                    code: H2ErrorCode::FRAME_SIZE_ERROR,
                    stream_id: 0,
                    msg: "RST_STREAM frame length must be 4",
                });
            }
            let code = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Ok(H2Frame::RstStream {
                stream_id: sid,
                error_code: H2ErrorCode(code),
            })
        }
        frame_type::SETTINGS => {
            if sid != 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "SETTINGS frame on non-zero stream",
                });
            }
            let ack = (header.flags & flags::ACK) != 0;
            if ack {
                if !payload.is_empty() {
                    return Err(FrameError {
                        code: H2ErrorCode::FRAME_SIZE_ERROR,
                        stream_id: 0,
                        msg: "SETTINGS ACK must have empty payload",
                    });
                }
                return Ok(H2Frame::Settings {
                    ack: true,
                    entries: Vec::new(),
                });
            }
            if !payload.len().is_multiple_of(6) {
                return Err(FrameError {
                    code: H2ErrorCode::FRAME_SIZE_ERROR,
                    stream_id: 0,
                    msg: "SETTINGS payload length must be a multiple of 6",
                });
            }
            let mut entries = Vec::with_capacity(payload.len() / 6);
            for chunk in payload.as_chunks::<6>().0 {
                entries.push(SettingPair {
                    id: u16::from_be_bytes([chunk[0], chunk[1]]),
                    value: u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]),
                });
            }
            Ok(H2Frame::Settings {
                ack: false,
                entries,
            })
        }
        frame_type::PUSH_PROMISE => {
            if sid == 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "PUSH_PROMISE frame on stream 0",
                });
            }
            let unpadded = strip_padding(payload, (header.flags & flags::PADDED) != 0)?;
            let Some((pid_bytes, block)) = unpadded.split_at_checked(4) else {
                return Err(FrameError {
                    code: H2ErrorCode::FRAME_SIZE_ERROR,
                    stream_id: 0,
                    msg: "PUSH_PROMISE missing promised stream id",
                });
            };
            let promised =
                u32::from_be_bytes([pid_bytes[0], pid_bytes[1], pid_bytes[2], pid_bytes[3]])
                    & 0x7FFF_FFFF;
            Ok(H2Frame::PushPromise {
                stream_id: sid,
                end_headers: (header.flags & flags::END_HEADERS) != 0,
                promised_stream_id: promised,
                header_block: Bytes::copy_from_slice(block),
            })
        }
        frame_type::PING => {
            if sid != 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "PING frame on non-zero stream",
                });
            }
            if payload.len() != 8 {
                return Err(FrameError {
                    code: H2ErrorCode::FRAME_SIZE_ERROR,
                    stream_id: 0,
                    msg: "PING frame length must be 8",
                });
            }
            let mut opaque_data = [0u8; 8];
            opaque_data.copy_from_slice(payload);
            Ok(H2Frame::Ping {
                ack: (header.flags & flags::ACK) != 0,
                opaque_data,
            })
        }
        frame_type::GOAWAY => {
            if sid != 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "GOAWAY frame on non-zero stream",
                });
            }
            let Some((head, debug)) = payload.split_at_checked(8) else {
                return Err(FrameError {
                    code: H2ErrorCode::FRAME_SIZE_ERROR,
                    stream_id: 0,
                    msg: "GOAWAY frame shorter than 8 bytes",
                });
            };
            let last_stream_id =
                u32::from_be_bytes([head[0], head[1], head[2], head[3]]) & 0x7FFF_FFFF;
            let err_code = u32::from_be_bytes([head[4], head[5], head[6], head[7]]);
            Ok(H2Frame::GoAway {
                last_stream_id,
                error_code: H2ErrorCode(err_code),
                debug_data: Bytes::copy_from_slice(debug),
            })
        }
        frame_type::WINDOW_UPDATE => {
            if payload.len() != 4 {
                return Err(FrameError {
                    code: H2ErrorCode::FRAME_SIZE_ERROR,
                    stream_id: 0,
                    msg: "WINDOW_UPDATE frame length must be 4",
                });
            }
            let increment =
                u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7FFF_FFFF;
            if increment == 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: sid,
                    msg: "WINDOW_UPDATE increment must be non-zero",
                });
            }
            Ok(H2Frame::WindowUpdate {
                stream_id: sid,
                increment,
            })
        }
        frame_type::CONTINUATION => {
            if sid == 0 {
                return Err(FrameError {
                    code: H2ErrorCode::PROTOCOL_ERROR,
                    stream_id: 0,
                    msg: "CONTINUATION frame on stream 0",
                });
            }
            Ok(H2Frame::Continuation {
                stream_id: sid,
                end_headers: (header.flags & flags::END_HEADERS) != 0,
                header_block: Bytes::copy_from_slice(payload),
            })
        }
        _ => Ok(H2Frame::Unknown {
            header,
            payload: Bytes::copy_from_slice(payload),
        }),
    }
}

/// Writes a 9-byte HTTP/2 frame header directly into `dst`.
#[inline]
pub fn write_frame_header(length: u32, kind: u8, flags: u8, stream_id: u32, dst: &mut BytesMut) {
    dst.put_u8(((length >> 16) & 0xff) as u8);
    dst.put_u8(((length >> 8) & 0xff) as u8);
    dst.put_u8((length & 0xff) as u8);
    dst.put_u8(kind);
    dst.put_u8(flags);
    dst.put_u32(stream_id & 0x7FFF_FFFF);
}

/// Writes a `DATA` frame (splitting into multiple frames if `payload.len() > max_frame_size`).
#[inline]
pub fn write_data_frames(
    stream_id: u32,
    mut payload: &[u8],
    end_stream: bool,
    max_frame_size: usize,
    dst: &mut BytesMut,
) {
    let chunk_limit = max_frame_size.max(DEFAULT_MAX_FRAME_SIZE as usize);
    if payload.is_empty() {
        let f = if end_stream { flags::END_STREAM } else { 0 };
        write_frame_header(0, frame_type::DATA, f, stream_id, dst);
        return;
    }
    while !payload.is_empty() {
        let take = payload.len().min(chunk_limit);
        let (chunk, rest) = payload.split_at(take);
        let is_last = rest.is_empty();
        let f = if end_stream && is_last {
            flags::END_STREAM
        } else {
            0
        };
        write_frame_header(chunk.len() as u32, frame_type::DATA, f, stream_id, dst);
        dst.put_slice(chunk);
        payload = rest;
    }
}

/// Writes a `HEADERS` frame followed by `CONTINUATION` frames if `header_block.len() > max_frame_size`.
#[inline]
pub fn write_headers_frames(
    stream_id: u32,
    header_block: &[u8],
    end_stream: bool,
    max_frame_size: usize,
    dst: &mut BytesMut,
) {
    let chunk_limit = max_frame_size.max(DEFAULT_MAX_FRAME_SIZE as usize);
    let end_stream_flag = if end_stream { flags::END_STREAM } else { 0 };
    if header_block.len() <= chunk_limit {
        write_frame_header(
            header_block.len() as u32,
            frame_type::HEADERS,
            end_stream_flag | flags::END_HEADERS,
            stream_id,
            dst,
        );
        dst.put_slice(header_block);
        return;
    }

    let (first, mut rest) = header_block.split_at(chunk_limit);
    write_frame_header(
        first.len() as u32,
        frame_type::HEADERS,
        end_stream_flag,
        stream_id,
        dst,
    );
    dst.put_slice(first);

    while !rest.is_empty() {
        let take = rest.len().min(chunk_limit);
        let (chunk, next) = rest.split_at(take);
        let f = if next.is_empty() {
            flags::END_HEADERS
        } else {
            0
        };
        write_frame_header(
            chunk.len() as u32,
            frame_type::CONTINUATION,
            f,
            stream_id,
            dst,
        );
        dst.put_slice(chunk);
        rest = next;
    }
}

/// Writes a non-ACK `SETTINGS` frame.
#[inline]
pub fn write_settings(entries: &[SettingPair], dst: &mut BytesMut) {
    write_frame_header((entries.len() * 6) as u32, frame_type::SETTINGS, 0, 0, dst);
    for pair in entries {
        dst.put_u16(pair.id);
        dst.put_u32(pair.value);
    }
}

/// Writes an empty `SETTINGS` ACK frame.
#[inline]
pub fn write_settings_ack(dst: &mut BytesMut) {
    write_frame_header(0, frame_type::SETTINGS, flags::ACK, 0, dst);
}

/// Writes a `RST_STREAM` frame.
#[inline]
pub fn write_rst_stream(stream_id: u32, code: H2ErrorCode, dst: &mut BytesMut) {
    write_frame_header(4, frame_type::RST_STREAM, 0, stream_id, dst);
    dst.put_u32(code.0);
}

/// Writes a `PING` or `PING ACK` frame.
#[inline]
pub fn write_ping(ack: bool, opaque_data: &[u8; 8], dst: &mut BytesMut) {
    let f = if ack { flags::ACK } else { 0 };
    write_frame_header(8, frame_type::PING, f, 0, dst);
    dst.put_slice(opaque_data);
}

/// Writes a `GOAWAY` frame.
#[inline]
pub fn write_goaway(last_stream_id: u32, code: H2ErrorCode, debug_data: &[u8], dst: &mut BytesMut) {
    write_frame_header((8 + debug_data.len()) as u32, frame_type::GOAWAY, 0, 0, dst);
    dst.put_u32(last_stream_id & 0x7FFF_FFFF);
    dst.put_u32(code.0);
    dst.put_slice(debug_data);
}

/// Writes a `WINDOW_UPDATE` frame.
#[inline]
pub fn write_window_update(stream_id: u32, increment: u32, dst: &mut BytesMut) {
    write_frame_header(4, frame_type::WINDOW_UPDATE, 0, stream_id, dst);
    dst.put_u32(increment & 0x7FFF_FFFF);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_roundtrip_all_core_types() {
        let mut buf = BytesMut::new();

        write_settings(
            &[
                SettingPair {
                    id: setting_id::MAX_CONCURRENT_STREAMS,
                    value: 256,
                },
                SettingPair {
                    id: setting_id::INITIAL_WINDOW_SIZE,
                    value: 1_048_576,
                },
            ],
            &mut buf,
        );
        write_settings_ack(&mut buf);
        write_window_update(0, 65536, &mut buf);
        write_headers_frames(
            1,
            b"\x82\x86\x84",
            false,
            DEFAULT_MAX_FRAME_SIZE as usize,
            &mut buf,
        );
        write_data_frames(
            1,
            b"hello http2",
            true,
            DEFAULT_MAX_FRAME_SIZE as usize,
            &mut buf,
        );
        write_ping(false, b"12345678", &mut buf);
        write_rst_stream(3, H2ErrorCode::CANCEL, &mut buf);
        write_goaway(3, H2ErrorCode::NO_ERROR, b"drain", &mut buf);

        let mut slice = &buf[..];
        let mut frames = Vec::new();
        while let Some((frame, n)) = decode_frame(slice, DEFAULT_MAX_FRAME_SIZE).unwrap() {
            frames.push(frame);
            slice = &slice[n..];
        }
        assert!(slice.is_empty());
        assert_eq!(frames.len(), 8);
        assert!(
            matches!(frames[0], H2Frame::Settings { ack: false, ref entries } if entries.len() == 2)
        );
        assert_eq!(
            frames[1],
            H2Frame::Settings {
                ack: true,
                entries: Vec::new()
            }
        );
        assert_eq!(
            frames[2],
            H2Frame::WindowUpdate {
                stream_id: 0,
                increment: 65536
            }
        );
        assert!(matches!(
            frames[3],
            H2Frame::Headers {
                stream_id: 1,
                end_stream: false,
                end_headers: true,
                ..
            }
        ));
        assert!(matches!(
            frames[4],
            H2Frame::Data {
                stream_id: 1,
                end_stream: true,
                ref payload
            } if &payload[..] == b"hello http2"
        ));
        assert_eq!(
            frames[5],
            H2Frame::Ping {
                ack: false,
                opaque_data: *b"12345678"
            }
        );
        assert_eq!(
            frames[6],
            H2Frame::RstStream {
                stream_id: 3,
                error_code: H2ErrorCode::CANCEL
            }
        );
        assert!(matches!(
            frames[7],
            H2Frame::GoAway {
                last_stream_id: 3,
                error_code: H2ErrorCode::NO_ERROR,
                ref debug_data
            } if &debug_data[..] == b"drain"
        ));
    }

    #[test]
    fn test_headers_fragmented_into_continuation_frames() {
        let mut buf = BytesMut::new();
        let big_block = vec![0xaa; DEFAULT_MAX_FRAME_SIZE as usize + 512];
        write_headers_frames(
            5,
            &big_block,
            true,
            DEFAULT_MAX_FRAME_SIZE as usize,
            &mut buf,
        );

        let (f1, n1) = decode_frame(&buf, DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
        let (f2, n2) = decode_frame(&buf[n1..], DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(n1 + n2, buf.len());
        assert!(matches!(
            f1,
            H2Frame::Headers {
                stream_id: 5,
                end_stream: true,
                end_headers: false,
                ref header_block
            } if header_block.len() == DEFAULT_MAX_FRAME_SIZE as usize
        ));
        assert!(matches!(
            f2,
            H2Frame::Continuation {
                stream_id: 5,
                end_headers: true,
                ref header_block
            } if header_block.len() == 512
        ));
    }

    #[test]
    fn test_frame_validation_errors() {
        // DATA on stream 0 -> PROTOCOL_ERROR
        let mut buf = BytesMut::new();
        write_frame_header(0, frame_type::DATA, 0, 0, &mut buf);
        assert_eq!(
            decode_frame(&buf, DEFAULT_MAX_FRAME_SIZE).unwrap_err().code,
            H2ErrorCode::PROTOCOL_ERROR
        );

        // WINDOW_UPDATE with increment 0 -> PROTOCOL_ERROR
        buf.clear();
        write_window_update(1, 0, &mut buf);
        let err = decode_frame(&buf, DEFAULT_MAX_FRAME_SIZE).unwrap_err();
        assert_eq!(err.code, H2ErrorCode::PROTOCOL_ERROR);
        assert_eq!(err.stream_id, 1);

        // SETTINGS ACK with non-zero payload -> FRAME_SIZE_ERROR
        buf.clear();
        write_frame_header(6, frame_type::SETTINGS, flags::ACK, 0, &mut buf);
        buf.put_slice(&[0u8; 6]);
        assert_eq!(
            decode_frame(&buf, DEFAULT_MAX_FRAME_SIZE).unwrap_err().code,
            H2ErrorCode::FRAME_SIZE_ERROR
        );

        // Payload exceeding max_frame_size -> FRAME_SIZE_ERROR
        buf.clear();
        write_frame_header(DEFAULT_MAX_FRAME_SIZE + 1, frame_type::DATA, 0, 1, &mut buf);
        assert_eq!(
            decode_frame(&buf, DEFAULT_MAX_FRAME_SIZE).unwrap_err().code,
            H2ErrorCode::FRAME_SIZE_ERROR
        );
    }
}
