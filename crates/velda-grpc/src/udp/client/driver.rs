//! Background QUIC connection driver loop for gRPC UDP upstream client.
//!
//! Multiplexes concurrent bidirectional RPC streams onto a single QUIC connection,
//! driving stream reads, writes, timeouts, cancellation, and UDP I/O.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use http::{HeaderMap, StatusCode, Version};
use quinn_proto::{Dir, Endpoint, Event, StreamEvent, StreamId};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use velda_core::{Body, L7Request, L7Response};

use crate::error::GrpcError;
use crate::udp::wire::{
    UdpFrame, decode_qpack_headers, decode_udp_frame, encode_qpack_request, encode_udp_frame,
};

/// Internal command dispatched to the background QUIC connection driver loop.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ClientCommand {
    /// Dispatches an L7 request over a newly opened multiplexed bidirectional stream.
    SendRequest {
        request: L7Request,
        reply_tx: oneshot::Sender<Result<L7Response, GrpcError>>,
    },
    /// Gracefully closes the QUIC connection.
    Close,
}

struct ActiveStream {
    reply_tx: oneshot::Sender<Result<L7Response, GrpcError>>,
}

async fn flush_conn_transmits(
    conn: &mut quinn_proto::Connection,
    socket: &UdpSocket,
    transmit_buf: &mut Vec<u8>,
) {
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(Instant::now(), 1, transmit_buf) {
        if transmit.size > 0 {
            let _ = socket
                .send_to(&transmit_buf[..transmit.size], transmit.destination)
                .await;
        }
        transmit_buf.clear();
    }
}

/// Background QUIC connection driver loop.
///
/// Multiplexes concurrent bidirectional RPC streams onto a single QUIC connection,
/// driving stream reads, writes, timeouts, and datagram ingestion.
pub(crate) async fn run_client_driver(
    socket: Arc<UdpSocket>,
    mut endpoint: Endpoint,
    mut conn: quinn_proto::Connection,
    _handle: quinn_proto::ConnectionHandle,
    mut command_rx: mpsc::Receiver<ClientCommand>,
    max_message_size: usize,
) {
    let mut active_streams: HashMap<StreamId, ActiveStream> = HashMap::new();
    let mut stream_headers: HashMap<StreamId, (Option<StatusCode>, HeaderMap)> = HashMap::new();
    let mut stream_raw_bufs: HashMap<StreamId, BytesMut> = HashMap::new();
    let mut stream_data_payloads: HashMap<StreamId, BytesMut> = HashMap::new();
    let mut transmit_buf = Vec::with_capacity(65535);
    let mut socket_recv_buf = vec![0u8; 65535];
    let mut chunk_scratch_buf = Vec::with_capacity(65536);

    loop {
        // Step 1: Drain pending transmits immediately
        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;

        // Step 2: Determine next timeout deadline
        let now = Instant::now();
        let timeout_deadline = conn.poll_timeout();
        let sleep_duration = match timeout_deadline {
            Some(deadline) if deadline <= now => Duration::from_millis(0),
            Some(deadline) => deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(100)),
            None => Duration::from_millis(100),
        };

        // Step 3: Wait for events, commands, or timeouts
        tokio::select! {
            cmd_opt = command_rx.recv() => {
                match cmd_opt {
                    Some(ClientCommand::SendRequest { request, reply_tx }) => {
                        let stream_id = match conn.streams().open(Dir::Bi) {
                            Some(id) => id,
                            None => {
                                let _ = reply_tx.send(Err(GrpcError::Protocol(
                                    "No bidirectional streams available on QUIC connection".into(),
                                )));
                                continue;
                            }
                        };

                        // 1. Encode request headers into QPACK buffer
                        let mut header_buf = BytesMut::new();
                        encode_qpack_request(&request.method, &request.uri, &request.headers, &mut header_buf);

                        let mut send_buf = BytesMut::new();
                        encode_udp_frame(&UdpFrame::Headers(header_buf.freeze()), &mut send_buf);

                        // 2. Wrap body frame if present
                        if let Body::Bytes(ref b) = request.body && !b.is_empty() {
                            encode_udp_frame(&UdpFrame::Data(b.clone()), &mut send_buf);
                        }

                        // 3. Write all encoded frames to the send stream
                        let mut written = 0;
                        let mut write_err = None;
                        while written < send_buf.len() {
                            match conn.send_stream(stream_id).write(&send_buf[written..]) {
                                Ok(n) => {
                                    if n == 0 {
                                        break;
                                    }
                                    written += n;
                                }
                                Err(e) => {
                                    write_err = Some(e);
                                    break;
                                }
                            }
                        }

                        if let Some(e) = write_err {
                            let _ = reply_tx.send(Err(GrpcError::Internal(format!("Failed to write to stream: {e}"))));
                            continue;
                        }

                        // 4. Finish sending side of stream
                        let _ = conn.send_stream(stream_id).finish();
                        active_streams.insert(stream_id, ActiveStream { reply_tx });
                        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;
                    }
                    Some(ClientCommand::Close) | None => {
                        conn.close(
                            Instant::now(),
                            quinn_proto::VarInt::from_u32(0),
                            bytes::Bytes::from_static(b"client closed"),
                        );
                        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;
                        break;
                    }
                }
            }

            recv_res = socket.recv_from(&mut socket_recv_buf) => {
                match recv_res {
                    Ok((len, from_addr)) => {
                        let now = Instant::now();
                        let payload = BytesMut::from(&socket_recv_buf[..len]);
                        let mut resp_buf = Vec::new();
                        if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ce)) =
                            endpoint.handle(now, from_addr, None, None, payload, &mut resp_buf)
                        {
                            conn.handle_event(ce);
                        }
                        if !resp_buf.is_empty() {
                            let _ = socket.send_to(&resp_buf, from_addr).await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "QUIC client UDP socket read error");
                    }
                }
            }

            _ = tokio::time::sleep(sleep_duration) => {
                let now = Instant::now();
                if let Some(deadline) = conn.poll_timeout() && deadline <= now {
                    conn.handle_timeout(now);
                }
            }
        }

        // Step 4: Cancelled stream cleanup
        let cancelled_streams: Vec<StreamId> = active_streams
            .iter()
            .filter_map(|(&id, active)| {
                if active.reply_tx.is_closed() {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        for id in cancelled_streams {
            active_streams.remove(&id);
            stream_headers.remove(&id);
            stream_raw_bufs.remove(&id);
            stream_data_payloads.remove(&id);
            let _ = conn
                .send_stream(id)
                .reset(quinn_proto::VarInt::from_u32(0x10c));
            let _ = conn
                .recv_stream(id)
                .stop(quinn_proto::VarInt::from_u32(0x10c));
        }

        // Step 5: Poll and process internal QUIC connection events
        while let Some(event) = conn.poll() {
            match event {
                Event::Stream(StreamEvent::Readable { id }) => {
                    if id.dir() == Dir::Uni {
                        if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                            while let Some(_chunk) = chunks.next(65535).ok().flatten() {}
                        }
                        continue;
                    }

                    if !active_streams.contains_key(&id) {
                        if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                            while let Some(_chunk) = chunks.next(65535).ok().flatten() {}
                            let _ = chunks.finalize();
                        }
                        stream_headers.remove(&id);
                        stream_raw_bufs.remove(&id);
                        stream_data_payloads.remove(&id);
                        continue;
                    }

                    let mut payload_too_large = false;
                    let mut stream_reset_err = None;
                    chunk_scratch_buf.clear();

                    let current_raw_len = stream_raw_bufs.get(&id).map(|b| b.len()).unwrap_or(0);
                    if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                        loop {
                            match chunks.next(65535) {
                                Ok(Some(chunk)) => {
                                    if current_raw_len + chunk_scratch_buf.len() + chunk.bytes.len()
                                        > max_message_size + 65536
                                    {
                                        payload_too_large = true;
                                        break;
                                    }
                                    chunk_scratch_buf.extend_from_slice(&chunk.bytes);
                                }
                                Ok(None) => break,
                                Err(quinn_proto::ReadError::Blocked) => break,
                                Err(quinn_proto::ReadError::Reset(err)) => {
                                    stream_reset_err = Some(err.to_string());
                                    break;
                                }
                            }
                        }
                        let _ = chunks.finalize();
                    }

                    if let Some(err) = stream_reset_err {
                        if let Some(active) = active_streams.remove(&id) {
                            let _ = active.reply_tx.send(Err(GrpcError::Internal(format!(
                                "Stream reset by peer: {err}"
                            ))));
                        }
                        stream_headers.remove(&id);
                        stream_raw_bufs.remove(&id);
                        stream_data_payloads.remove(&id);
                        continue;
                    }

                    if payload_too_large {
                        let _ = conn
                            .recv_stream(id)
                            .stop(quinn_proto::VarInt::from_u32(0x10e));
                        if let Some(active) = active_streams.remove(&id) {
                            let _ = active
                                .reply_tx
                                .send(Err(GrpcError::PayloadTooLarge(max_message_size + 1)));
                        }
                        stream_headers.remove(&id);
                        stream_raw_bufs.remove(&id);
                        stream_data_payloads.remove(&id);
                        continue;
                    }

                    let raw_buf = stream_raw_bufs.entry(id).or_default();
                    raw_buf.extend_from_slice(&chunk_scratch_buf);

                    while let Ok(Some(frame)) = decode_udp_frame(raw_buf) {
                        match frame {
                            UdpFrame::Headers(header_bytes) => {
                                if let Ok(decoded) = decode_qpack_headers(&header_bytes) {
                                    stream_headers.insert(id, (decoded.status, decoded.headers));
                                }
                            }
                            UdpFrame::Data(data_bytes) => {
                                let data_buf = stream_data_payloads.entry(id).or_default();
                                data_buf.extend_from_slice(&data_bytes);
                            }
                            _ => {}
                        }
                    }
                }

                Event::Stream(StreamEvent::Finished { id }) => {
                    if let Some(stream) = active_streams.remove(&id) {
                        let (status, headers) = stream_headers
                            .remove(&id)
                            .unwrap_or_else(|| (Some(StatusCode::OK), HeaderMap::new()));
                        let body_bytes = stream_data_payloads
                            .remove(&id)
                            .map(|b| b.freeze())
                            .unwrap_or_default();
                        stream_raw_bufs.remove(&id);

                        let resp = L7Response::new(
                            status.unwrap_or(StatusCode::OK),
                            Version::HTTP_3,
                            headers,
                            Body::Bytes(body_bytes),
                        );
                        let _ = stream.reply_tx.send(Ok(resp));
                    }
                }

                Event::ConnectionLost { reason } => {
                    tracing::warn!(reason = %reason, "Upstream gRPC QUIC connection terminated");
                    for (_, stream) in active_streams.drain() {
                        let _ = stream.reply_tx.send(Err(GrpcError::Protocol(format!(
                            "QUIC connection lost: {reason}"
                        ))));
                    }
                    return;
                }

                _ => {}
            }
        }

        if conn.is_drained() {
            break;
        }
    }
}
