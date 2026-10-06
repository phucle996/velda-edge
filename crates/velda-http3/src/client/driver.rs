//! Background QUIC connection driver loop for HTTP/3 upstream client (RFC 9114).
//!
//! Manages multiplexed bidirectional request-response streams over a persistent QUIC
//! connection, driving stream reads, writes, timeouts, cancellation, and UDP I/O.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use http::{HeaderMap, StatusCode, Version};
use quinn_proto::{Dir, Endpoint, Event, StreamEvent, StreamId};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use velda_core::{Body, L7Request, L7Response};

use crate::error::Http3Error;
use crate::frame::{Http3Frame, decode_frame, encode_frame, error_code};
use crate::qpack::{decode_qpack, encode_qpack_request};

/// Internal command dispatched to the background QUIC connection driver loop.
///
/// Note: `clippy::large_enum_variant` is deliberately permitted here to preserve zero heap
/// allocation on the request dispatch hot path. `SendRequest` represents 99.999% of channel traffic,
/// and keeping `L7Request` unboxed avoids `malloc`/`free` overhead per request.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ClientCommand {
    /// Dispatches an L7 request over a newly opened multiplexed bidirectional stream.
    SendRequest {
        request: L7Request,
        reply_tx: oneshot::Sender<Result<L7Response, Http3Error>>,
    },
    /// Gracefully closes the QUIC connection.
    Close,
}

struct ActiveStream {
    method: http::Method,
    reply_tx: oneshot::Sender<Result<L7Response, Http3Error>>,
}

/// Background QUIC connection driver loop.
///
/// Multiplexes concurrent bidirectional request streams onto a single QUIC connection,
/// driving stream reads, writes, timeouts, and packet ingestion.
pub(crate) async fn run_client_driver(
    socket: Arc<UdpSocket>,
    mut endpoint: Endpoint,
    mut conn: quinn_proto::Connection,
    _handle: quinn_proto::ConnectionHandle,
    mut command_rx: mpsc::Receiver<ClientCommand>,
    max_body_size: usize,
) {
    let mut active_streams: HashMap<StreamId, ActiveStream> = HashMap::new();
    let mut stream_headers: HashMap<StreamId, (Option<StatusCode>, HeaderMap)> = HashMap::new();
    let mut stream_raw_bufs: HashMap<StreamId, BytesMut> = HashMap::new();
    let mut stream_data_payloads: HashMap<StreamId, BytesMut> = HashMap::new();
    let mut transmit_buf = Vec::with_capacity(65535);
    let mut socket_recv_buf = vec![0u8; 65535];
    let mut chunk_scratch_buf = Vec::with_capacity(65536);

    loop {
        // Flush connection outgoing packets
        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;

        // Determine next timeout deadline
        let timeout_sleep = match conn.poll_timeout() {
            Some(t) => {
                let now = Instant::now();
                if t <= now {
                    Duration::from_millis(0)
                } else {
                    t - now
                }
            }
            None => Duration::from_millis(100),
        };

        tokio::select! {
            // Ingest new command from client handles
            cmd = command_rx.recv() => {
                match cmd {
                    Some(ClientCommand::SendRequest { request, reply_tx }) => {
                        let stream_id = match conn.streams().open(Dir::Bi) {
                            Some(id) => id,
                            None => {
                                let _ = reply_tx.send(Err(Http3Error::H3("No concurrent bidirectional streams available".into())));
                                continue;
                            }
                        };

                        let req_method = request.method.clone();

                        // Encode QPACK request HEADERS frame
                        let mut header_buf = BytesMut::new();
                        encode_qpack_request(&request.method, &request.uri, &request.headers, &mut header_buf);
                        let header_frame = Http3Frame::Headers(header_buf.freeze());

                        let mut send_buf = BytesMut::new();
                        encode_frame(&header_frame, &mut send_buf);

                        // Encode optional DATA frame using move semantics (zero clone)
                        if let Body::Bytes(b) = request.body
                            && !b.is_empty()
                        {
                            let data_frame = Http3Frame::Data(b);
                            encode_frame(&data_frame, &mut send_buf);
                        }

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
                            let _ = reply_tx.send(Err(Http3Error::H3(e.to_string())));
                            continue;
                        }
                        if written < send_buf.len() {
                            let _ = reply_tx.send(Err(Http3Error::H3(format!(
                                "Client send stream window exhausted: {written}/{} bytes written",
                                send_buf.len()
                            ))));
                            continue;
                        }
                        let _ = conn.send_stream(stream_id).finish();

                        active_streams.insert(stream_id, ActiveStream { method: req_method, reply_tx });
                        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;
                    }
                    Some(ClientCommand::Close) | None => {
                        conn.close(
                            Instant::now(),
                            quinn_proto::VarInt::from_u32(error_code::H3_NO_ERROR as u32),
                            bytes::Bytes::from_static(b"done"),
                        );
                        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;
                        break;
                    }
                }
            }

            // Ingest incoming UDP packets from network
            res = socket.recv_from(&mut socket_recv_buf) => {
                let (len, from_addr) = match res {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let now = Instant::now();
                let payload = BytesMut::from(&socket_recv_buf[..len]);
                let mut resp_buf = Vec::new();
                match endpoint.handle(now, from_addr, None, None, payload, &mut resp_buf) {
                    Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ce)) => {
                        conn.handle_event(ce);
                    }
                    Some(quinn_proto::DatagramEvent::Response(transmit))
                        if transmit.size > 0 =>
                    {
                        let _ = socket
                            .send_to(&transmit_buf[..transmit.size], transmit.destination)
                            .await;
                    }
                    _ => {}
                }
                if !resp_buf.is_empty() {
                    let _ = socket.send_to(&resp_buf, from_addr).await;
                }
            }

            // Timeout expiration
            _ = tokio::time::sleep(timeout_sleep) => {
                let now = Instant::now();
                if let Some(timeout) = conn.poll_timeout()
                    && timeout <= now
                {
                    conn.handle_timeout(now);
                }
            }
        }

        // RFC 9114 Section 4.1: Sweep cancelled streams where caller dropped receiver
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
            // H3_REQUEST_CANCELLED (RFC 9114 Section 4.1 & 8.1)
            let _ = conn.send_stream(id).reset(quinn_proto::VarInt::from_u32(
                error_code::H3_REQUEST_CANCELLED as u32,
            ));
            let _ = conn.recv_stream(id).stop(quinn_proto::VarInt::from_u32(
                error_code::H3_REQUEST_CANCELLED as u32,
            ));
        }

        // Poll QUIC stream events
        while let Some(event) = conn.poll() {
            match event {
                Event::Stream(StreamEvent::Readable { id }) => {
                    if id.dir() == Dir::Uni {
                        // Server unidirectional stream (Control Stream, QPACK encoder/decoder).
                        // Drain incoming bytes to avoid blocking flow control.
                        if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                            while let Some(_chunk) = chunks.next(65535).ok().flatten() {}
                        }
                        continue;
                    }

                    if !active_streams.contains_key(&id) {
                        // Stream was cancelled or already completed. Drain bytes to advance flow control.
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
                    let mut stream_finished = false;
                    let mut stream_reset_err = None;
                    chunk_scratch_buf.clear();

                    let current_raw_len = stream_raw_bufs.get(&id).map(|b| b.len()).unwrap_or(0);
                    if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                        loop {
                            match chunks.next(65535) {
                                Ok(Some(chunk)) => {
                                    if current_raw_len + chunk_scratch_buf.len() + chunk.bytes.len()
                                        > max_body_size + 65536
                                    {
                                        payload_too_large = true;
                                        break;
                                    }
                                    chunk_scratch_buf.extend_from_slice(&chunk.bytes);
                                }
                                Ok(None) => {
                                    stream_finished = true;
                                    break;
                                }
                                Err(quinn_proto::ReadError::Blocked) => {
                                    break;
                                }
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
                            let _ = active
                                .reply_tx
                                .send(Err(Http3Error::H3(format!("Stream reset by peer: {err}"))));
                        }
                        stream_headers.remove(&id);
                        stream_raw_bufs.remove(&id);
                        stream_data_payloads.remove(&id);
                        continue;
                    }

                    if payload_too_large {
                        let _ = conn.recv_stream(id).stop(quinn_proto::VarInt::from_u32(
                            error_code::H3_MESSAGE_ERROR as u32,
                        ));
                        if let Some(active) = active_streams.remove(&id) {
                            let _ = active
                                .reply_tx
                                .send(Err(Http3Error::PayloadTooLarge(max_body_size + 1)));
                        }
                        stream_headers.remove(&id);
                        stream_raw_bufs.remove(&id);
                        stream_data_payloads.remove(&id);
                        continue;
                    }

                    let raw_buf = stream_raw_bufs.entry(id).or_default();
                    raw_buf.extend_from_slice(&chunk_scratch_buf);

                    // Decode incoming frames
                    if let Some(buf) = stream_raw_bufs.get_mut(&id) {
                        while let Ok(Some(frame)) = decode_frame(buf) {
                            match frame {
                                Http3Frame::Headers(payload) => {
                                    if let Ok(decoded) = decode_qpack(&payload) {
                                        stream_headers
                                            .insert(id, (decoded.status, decoded.headers));
                                    }
                                }
                                Http3Frame::Data(payload) => {
                                    let data_buf = stream_data_payloads.entry(id).or_default();
                                    data_buf.extend_from_slice(&payload);
                                }
                                _ => {}
                            }
                        }
                    }

                    // Only complete and emit the response when peer has finished sending stream data (FIN reached)
                    if stream_finished {
                        stream_raw_bufs.remove(&id);
                        let headers_opt = stream_headers.remove(&id);
                        let body_opt = stream_data_payloads.remove(&id);

                        if let Some(active) = active_streams.remove(&id) {
                            let (status, headers) =
                                headers_opt.unwrap_or((Some(StatusCode::OK), HeaderMap::new()));
                            let status_code = status.unwrap_or(StatusCode::OK);
                            let is_no_body = active.method == http::Method::HEAD
                                || status_code.is_informational()
                                || status_code == StatusCode::NO_CONTENT
                                || status_code == StatusCode::NOT_MODIFIED;

                            let body = if is_no_body {
                                Body::Empty
                            } else {
                                body_opt
                                    .map(|b| {
                                        if b.is_empty() {
                                            Body::Empty
                                        } else {
                                            Body::Bytes(b.freeze())
                                        }
                                    })
                                    .unwrap_or(Body::Empty)
                            };

                            let response =
                                L7Response::new(status_code, Version::HTTP_3, headers, body);
                            let _ = active.reply_tx.send(Ok(response));
                        }
                    }
                }
                Event::Stream(StreamEvent::Finished { .. }) => {
                    // Quinn's StreamEvent::Finished signals that our outgoing send stream has been
                    // acknowledged by peer, not that the incoming response stream has finished.
                }
                Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                    if id.dir() == Dir::Uni {
                        continue;
                    }
                    if let Some(active) = active_streams.remove(&id) {
                        let _ = active.reply_tx.send(Err(Http3Error::H3(format!(
                            "Stream reset by peer: {error_code}"
                        ))));
                    }
                    stream_headers.remove(&id);
                    stream_raw_bufs.remove(&id);
                    stream_data_payloads.remove(&id);
                }
                Event::ConnectionLost { reason } => {
                    for (_, active) in active_streams.drain() {
                        let _ = active
                            .reply_tx
                            .send(Err(Http3Error::H3(format!("Connection lost: {reason}"))));
                    }
                    return;
                }
                _ => {}
            }
        }

        // Flush any outgoing packets (ACKs, stream data, flow control) generated by event processing
        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;

        if conn.is_drained() {
            break;
        }
    }

    // Notify any remaining in-flight requests
    for (_, active) in active_streams.drain() {
        let _ = active.reply_tx.send(Err(Http3Error::ConnectionClosed));
    }
}

/// Helper to drain and transmit newly generated QUIC packets onto the UDP socket.
pub(crate) async fn flush_conn_transmits(
    conn: &mut quinn_proto::Connection,
    socket: &UdpSocket,
    transmit_buf: &mut Vec<u8>,
) {
    let now = Instant::now();
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(now, 1, transmit_buf) {
        if transmit.size > 0 {
            let _ = socket
                .send_to(&transmit_buf[..transmit.size], transmit.destination)
                .await;
        }
        transmit_buf.clear();
    }
}
