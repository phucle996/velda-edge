//! gRPC Length-Prefixed Message (LPM) framing utilities.
//!
//! A gRPC message frame consists of:
//! - 1 byte compressed flag (0 = uncompressed, 1 = compressed)
//! - 4 bytes big-endian unsigned integer (message length)
//! - N bytes binary serialized protobuf payload

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::GrpcError;

/// Header size of a standard gRPC Length-Prefixed Message frame.
pub const GRPC_FRAME_HEADER_SIZE: usize = 5;

/// Encodes a payload into a gRPC Length-Prefixed Message (LPM).
pub fn encode_grpc_frame(data: &[u8], compressed: bool, dst: &mut BytesMut) {
    dst.reserve(GRPC_FRAME_HEADER_SIZE + data.len());
    dst.put_u8(if compressed { 1 } else { 0 });
    dst.put_u32(data.len() as u32);
    dst.put_slice(data);
}

/// Attempts to decode a single gRPC Length-Prefixed Message from the buffer.
///
/// Returns `Ok(Some((compressed, payload)))` if a complete frame is available,
/// or `Ok(None)` if more data is required.
pub fn decode_grpc_frame(src: &mut BytesMut) -> Result<Option<(bool, Bytes)>, GrpcError> {
    if src.len() < GRPC_FRAME_HEADER_SIZE {
        return Ok(None);
    }

    let compressed = src[0] != 0;
    let length = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;

    if src.len() < GRPC_FRAME_HEADER_SIZE + length {
        return Ok(None);
    }

    src.advance(GRPC_FRAME_HEADER_SIZE);
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

        assert_eq!(buf.len(), GRPC_FRAME_HEADER_SIZE + payload.len());

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
}
