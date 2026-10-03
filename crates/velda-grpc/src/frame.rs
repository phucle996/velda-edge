//! gRPC Length-Prefixed Message (LPM) framing utilities.
//!
//! A gRPC message frame consists of:
//! - 1 byte compressed flag (0 = uncompressed, 1 = compressed)
//! - 4 bytes big-endian unsigned integer (message length)
//! - N bytes binary serialized protobuf payload

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::GrpcError;

/// Canonical gRPC Length-Prefixed Message (LPM) framing constants.
pub struct GrpcFrame;

impl GrpcFrame {
    /// Size of the 1-byte compression flag in the LPM header.
    pub const FLAG_SIZE: usize = 1;

    /// Size of the 4-byte big-endian message length in the LPM header.
    pub const LENGTH_SIZE: usize = 4;

    /// Total header size of a standard gRPC Length-Prefixed Message frame (5 bytes).
    pub const HEADER_SIZE: usize = Self::FLAG_SIZE + Self::LENGTH_SIZE;

    /// Flag byte indicating uncompressed payload (0).
    pub const FLAG_UNCOMPRESSED: u8 = 0;

    /// Flag byte indicating compressed payload (1).
    pub const FLAG_COMPRESSED: u8 = 1;
}

/// Encodes a payload into a gRPC Length-Prefixed Message (LPM).
pub fn encode_grpc_frame(data: &[u8], compressed: bool, dst: &mut BytesMut) {
    dst.reserve(GrpcFrame::HEADER_SIZE + data.len());
    dst.put_u8(if compressed {
        GrpcFrame::FLAG_COMPRESSED
    } else {
        GrpcFrame::FLAG_UNCOMPRESSED
    });
    dst.put_u32(data.len() as u32);
    dst.put_slice(data);
}

/// Attempts to decode a single gRPC Length-Prefixed Message from the buffer.
///
/// Returns `Ok(Some((compressed, payload)))` if a complete frame is available,
/// or `Ok(None)` if more data is required.
///
/// Enforces the gRPC RFC invariant: returns [`GrpcError::Protocol`] if the compression
/// flag byte is neither `0` nor `1`.
pub fn decode_grpc_frame(src: &mut BytesMut) -> Result<Option<(bool, Bytes)>, GrpcError> {
    if src.len() < GrpcFrame::HEADER_SIZE {
        return Ok(None);
    }

    let flag = src[0];
    let compressed = match flag {
        GrpcFrame::FLAG_UNCOMPRESSED => false,
        GrpcFrame::FLAG_COMPRESSED => true,
        _ => {
            return Err(GrpcError::Protocol(format!(
                "invalid gRPC compression flag: {flag}; must be 0 or 1 per RFC"
            )));
        }
    };

    let length = u32::from_be_bytes([
        src[GrpcFrame::FLAG_SIZE],
        src[GrpcFrame::FLAG_SIZE + 1],
        src[GrpcFrame::FLAG_SIZE + 2],
        src[GrpcFrame::FLAG_SIZE + 3],
    ]) as usize;

    if src.len() < GrpcFrame::HEADER_SIZE + length {
        return Ok(None);
    }

    src.advance(GrpcFrame::HEADER_SIZE);
    let payload = src.split_to(length).freeze();

    Ok(Some((compressed, payload)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_frame_roundtrip() {
        let mut buf = BytesMut::new();
        let payload = b"hello grpc world";
        encode_grpc_frame(payload, false, &mut buf);

        assert_eq!(buf.len(), GrpcFrame::HEADER_SIZE + payload.len());

        let decoded = decode_grpc_frame(&mut buf).unwrap();
        assert!(decoded.is_some());
        let (compressed, data) = decoded.unwrap();
        assert!(!compressed);
        assert_eq!(&data[..], payload);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_grpc_frame_partial_read() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0, 0, 0, 0, 10, 1, 2]); // Only 2 bytes of 10-byte payload
        assert_eq!(decode_grpc_frame(&mut buf).unwrap(), None);
    }

    #[test]
    fn test_grpc_frame_invalid_compression_flag() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[2, 0, 0, 0, 4, 1, 2, 3, 4]); // Flag 2 is invalid per RFC
        let err = decode_grpc_frame(&mut buf).unwrap_err();
        match err {
            GrpcError::Protocol(msg) => assert!(msg.contains("invalid gRPC compression flag: 2")),
            _ => panic!("Expected GrpcError::Protocol"),
        }
    }
}
