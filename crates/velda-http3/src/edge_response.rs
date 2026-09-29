//! Layer 7 HTTP/3 response serialization and egress packet generation.
//!
//! Operates on borrowed `&L7Response` references without holding or retaining
//! response business data inside the protocol engine.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use http::StatusCode;
use quinn_proto::{Connection, StreamId};
use velda_core::{Body, L7Response};

use crate::composer_parse::OutgoingDatagram;
use crate::error::Http3Error;
use crate::frame::{Http3Frame, encode_frame};

/// Helper encoding minimal HTTP/3 response headers (:status pseudo-header).
pub fn encode_minimal_headers(status: StatusCode, dst: &mut BytesMut) {
    let status_str = format!(":status {}", status.as_u16());
    dst.extend_from_slice(status_str.as_bytes());
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
    // 1. Encode HEADERS frame
    let mut header_payload = BytesMut::new();
    encode_minimal_headers(response.status, &mut header_payload);
    let header_frame = Http3Frame::Headers(header_payload.freeze());

    let mut write_buf = BytesMut::new();
    encode_frame(&header_frame, &mut write_buf);

    // 2. Encode DATA frame if body is present
    if let Body::Bytes(ref b) = response.body {
        let data_frame = Http3Frame::Data(b.clone());
        encode_frame(&data_frame, &mut write_buf);
    }

    // 3. Write frames to send stream
    conn.send_stream(stream_id)
        .write(&write_buf)
        .map_err(|e| Http3Error::H3(e.to_string()))?;

    // Finish stream
    let _ = conn.send_stream(stream_id).finish();

    // 4. Drain newly generated packets
    let mut outgoing = Vec::new();
    while let Some(transmit) = conn.poll_transmit(now, 1, transmit_buf) {
        if transmit.size > 0 {
            let packet = Bytes::copy_from_slice(&transmit_buf[..transmit.size]);
            outgoing.push(OutgoingDatagram {
                peer: transmit.destination,
                payload: packet,
            });
        }
    }

    Ok(outgoing)
}
