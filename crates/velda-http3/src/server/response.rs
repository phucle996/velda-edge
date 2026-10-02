//! Layer 7 HTTP/3 response serialization and egress packet generation.
//!
//! Serializes [`L7Response`] into RFC 9114 HEADERS (QPACK-encoded) and DATA frames,
//! writes them to the designated QUIC send stream, and drains outgoing datagrams.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use http::{HeaderMap, StatusCode};
use quinn_proto::{Connection, StreamId};
use velda_core::{Body, L7Response};

use crate::error::Http3Error;
use crate::frame::{Http3Frame, encode_frame};
use crate::qpack::encode_qpack_response;
use crate::server::connection::OutgoingDatagram;

/// Serializes status and body into HTTP/3 HEADERS and DATA frames in a buffer.
pub fn build_edge_response_frames(status: StatusCode, body: &[u8]) -> BytesMut {
    let mut header_payload = BytesMut::new();
    encode_qpack_response(status, &HeaderMap::new(), &mut header_payload);
    let header_frame = Http3Frame::Headers(header_payload.freeze());

    let mut write_buf = BytesMut::new();
    encode_frame(&header_frame, &mut write_buf);

    if !body.is_empty() {
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

    // 2. Encode DATA frame if body is present
    if let Body::Bytes(ref b) = response.body {
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
