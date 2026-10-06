//! Downstream gRPC response encoder and trailers serializer over UDP.

use bytes::BytesMut;
use velda_core::{Body, L7Response};

use crate::status::GrpcStatus;
use crate::udp::wire::{UdpFrame, encode_qpack_response, encode_udp_frame};

/// Responder for encoding gRPC response packets over UDP.
#[derive(Debug, Default)]
pub struct GrpcUdpResponder;

impl GrpcUdpResponder {
    /// Builds raw wire frame buffers for a gRPC response over UDP.
    pub fn build_response_frames(response: &L7Response) -> BytesMut {
        let mut header_payload = BytesMut::new();
        encode_qpack_response(response.status, &response.headers, &mut header_payload);

        let mut write_buf = BytesMut::new();
        encode_udp_frame(&UdpFrame::Headers(header_payload.freeze()), &mut write_buf);

        if let Body::Bytes(ref b) = response.body
            && !b.is_empty()
        {
            encode_udp_frame(&UdpFrame::Data(b.clone()), &mut write_buf);
        }

        write_buf
    }

    /// Formats a trailers-only gRPC response for rapid rejection or error signaling.
    pub fn build_trailers_only(status: GrpcStatus, message: Option<&str>) -> BytesMut {
        let resp = status.to_l7_response(message);
        Self::build_response_frames(&resp)
    }
}
