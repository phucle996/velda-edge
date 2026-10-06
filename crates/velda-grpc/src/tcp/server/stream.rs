//! Downstream active gRPC stream representation.

use super::responder::GrpcResponder;
use crate::error::GrpcError;
use crate::frame::{GrpcFrame, decode_grpc_frame};
use bytes::{Bytes, BytesMut};

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
}
