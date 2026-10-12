//! Background QUIC connection driver loop for HTTP/3 upstream client (RFC 9114).
//!
//! Manages multiplexed bidirectional request-response streams over a persistent QUIC
//! connection, driving stream reads, writes, timeouts, cancellation, and UDP I/O.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use http::{HeaderMap, StatusCode, Version};
use quinn_proto::{Dir, Endpoint, Event, StreamEvent, StreamId};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use velda_core::{Body, L7Request, L7Response};

use super::response::Http3StreamResponse;
use crate::error::Http3Error;
use crate::frame::{Http3Frame, decode_frame, encode_frame, error_code};
use crate::qpack::{decode_qpack, encode_qpack_request};

/// Internal command dispatched to the background QUIC connection driver loop.
///
/// Note: `clippy::large_enum_variant` is deliberately permitted here to preserve zero heap
/// allocation on the request dispatch hot path. Keeping `L7Request` unboxed avoids `malloc`/`free` overhead per request.
pub(crate) type OpenStreamReply = Result<
    (
        super::connector::Http3StreamSender,
        oneshot::Receiver<Result<Http3StreamResponse, Http3Error>>,
    ),
    Http3Error,
>;

#[allow(clippy::large_enum_variant)]
pub(crate) enum ClientCommand {
    /// Dispatches an L7 request over a newly opened multiplexed bidirectional stream (buffered response).
    SendRequest {
        request: L7Request,
        reply_tx: oneshot::Sender<Result<L7Response, Http3Error>>,
    },
    /// Dispatches an L7 request over a newly opened multiplexed bidirectional stream (progressive streaming response).
    SendStreamRequest {
        request: L7Request,
        reply_tx: oneshot::Sender<Result<Http3StreamResponse, Http3Error>>,
    },
    /// Opens a progressive bidirectional stream, returning an Http3StreamSender and a receiver for Http3StreamResponse.
    OpenStream {
        method: http::Method,
        uri: http::Uri,
        headers: HeaderMap,
        reply_tx: oneshot::Sender<OpenStreamReply>,
    },
    /// Sends a chunk of request DATA frame or finishes the request stream.
    SendChunk {
        stream_id: StreamId,
        chunk: bytes::Bytes,
        is_fin: bool,
    },
    /// Gracefully closes the QUIC connection.
    Close,
}

type StreamHeadChannel = (
    oneshot::Sender<Result<Http3StreamResponse, Http3Error>>,
    mpsc::Receiver<Result<bytes::Bytes, Http3Error>>,
);

enum StreamReply {
    Buffered {
        reply_tx: oneshot::Sender<Result<L7Response, Http3Error>>,
        data_payload: BytesMut,
    },
    Streaming {
        head_tx: Option<StreamHeadChannel>,
        chunk_tx: mpsc::Sender<Result<bytes::Bytes, Http3Error>>,
    },
}

struct ActiveStreamSlot {
    method: http::Method,
    reply: StreamReply,
    headers: Option<(Option<StatusCode>, HeaderMap)>,
    raw_buf: BytesMut,
    outbound_buf: BytesMut,
    outbound_finished: bool,
}

#[inline]
fn flush_stream_outbound(
    conn: &mut quinn_proto::Connection,
    stream_id: StreamId,
    slot: &mut ActiveStreamSlot,
) {
    use bytes::Buf;
    while !slot.outbound_buf.is_empty() {
        match conn.send_stream(stream_id).write(&slot.outbound_buf) {
            Ok(n) if n > 0 => {
                slot.outbound_buf.advance(n);
            }
            _ => break,
        }
    }
    if slot.outbound_buf.is_empty() {
        slot.outbound_buf.clear();
        if slot.outbound_finished {
            let _ = conn.send_stream(stream_id).finish();
        }
    }
}

const MAX_PENDING_STREAMS: usize = 4096;
const PENDING_STREAM_TIMEOUT: Duration = Duration::from_secs(10);

#[inline]
fn is_open_command_cancelled(cmd: &ClientCommand) -> bool {
    match cmd {
        ClientCommand::SendRequest { reply_tx, .. } => reply_tx.is_closed(),
        ClientCommand::SendStreamRequest { reply_tx, .. } => reply_tx.is_closed(),
        ClientCommand::OpenStream { reply_tx, .. } => reply_tx.is_closed(),
        _ => false,
    }
}

#[inline]
fn reject_open_command(cmd: ClientCommand, err: Http3Error) {
    match cmd {
        ClientCommand::SendRequest { reply_tx, .. } => {
            let _ = reply_tx.send(Err(err));
        }
        ClientCommand::SendStreamRequest { reply_tx, .. } => {
            let _ = reply_tx.send(Err(err));
        }
        ClientCommand::OpenStream { reply_tx, .. } => {
            let _ = reply_tx.send(Err(err));
        }
        _ => {}
    }
}

#[inline]
fn dispatch_open_command(
    cmd: ClientCommand,
    stream_id: StreamId,
    conn: &mut quinn_proto::Connection,
    streams: &mut HashMap<StreamId, ActiveStreamSlot>,
    sender_command_tx: &mpsc::Sender<ClientCommand>,
) {
    match cmd {
        ClientCommand::SendRequest { request, reply_tx } => {
            let req_method = request.method.clone();

            // Encode QPACK request HEADERS frame
            let mut header_buf = BytesMut::new();
            encode_qpack_request(
                &request.method,
                &request.uri,
                &request.headers,
                &mut header_buf,
            );
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

            let mut slot = ActiveStreamSlot {
                method: req_method,
                reply: StreamReply::Buffered {
                    reply_tx,
                    data_payload: BytesMut::new(),
                },
                headers: None,
                raw_buf: BytesMut::new(),
                outbound_buf: send_buf,
                outbound_finished: true,
            };
            flush_stream_outbound(conn, stream_id, &mut slot);
            streams.insert(stream_id, slot);
        }
        ClientCommand::SendStreamRequest { request, reply_tx } => {
            let req_method = request.method.clone();

            // Encode QPACK request HEADERS frame
            let mut header_buf = BytesMut::new();
            encode_qpack_request(
                &request.method,
                &request.uri,
                &request.headers,
                &mut header_buf,
            );
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

            let (chunk_tx, chunk_rx) = mpsc::channel(32);
            let mut slot = ActiveStreamSlot {
                method: req_method,
                reply: StreamReply::Streaming {
                    head_tx: Some((reply_tx, chunk_rx)),
                    chunk_tx,
                },
                headers: None,
                raw_buf: BytesMut::new(),
                outbound_buf: send_buf,
                outbound_finished: true,
            };
            flush_stream_outbound(conn, stream_id, &mut slot);
            streams.insert(stream_id, slot);
        }
        ClientCommand::OpenStream {
            method,
            uri,
            headers,
            reply_tx,
        } => {
            let req_method = method.clone();

            // Encode QPACK request HEADERS frame
            let mut header_buf = BytesMut::new();
            encode_qpack_request(&method, &uri, &headers, &mut header_buf);
            let header_frame = Http3Frame::Headers(header_buf.freeze());

            let mut send_buf = BytesMut::new();
            encode_frame(&header_frame, &mut send_buf);

            let (resp_tx, resp_rx) = oneshot::channel();
            let (chunk_tx, chunk_rx) = mpsc::channel(32);
            let stream_sender = super::connector::Http3StreamSender {
                stream_id,
                command_tx: sender_command_tx.clone(),
            };

            let mut slot = ActiveStreamSlot {
                method: req_method,
                reply: StreamReply::Streaming {
                    head_tx: Some((resp_tx, chunk_rx)),
                    chunk_tx,
                },
                headers: None,
                raw_buf: BytesMut::new(),
                outbound_buf: send_buf,
                outbound_finished: false,
            };
            flush_stream_outbound(conn, stream_id, &mut slot);
            streams.insert(stream_id, slot);
            let _ = reply_tx.send(Ok((stream_sender, resp_rx)));
        }
        _ => {}
    }
}

#[inline]
fn drain_pending_commands(
    pending_commands: &mut VecDeque<(Instant, ClientCommand)>,
    conn: &mut quinn_proto::Connection,
    streams: &mut HashMap<StreamId, ActiveStreamSlot>,
    sender_command_tx: &mpsc::Sender<ClientCommand>,
) {
    while let Some(front) = pending_commands.front() {
        if front.0.elapsed() > PENDING_STREAM_TIMEOUT {
            let (_, cmd) = pending_commands.pop_front().unwrap();
            reject_open_command(
                cmd,
                Http3Error::H3("Pending stream request timed out".into()),
            );
            continue;
        }
        if is_open_command_cancelled(&front.1) {
            pending_commands.pop_front();
            continue;
        }
        match conn.streams().open(Dir::Bi) {
            Some(stream_id) => {
                let (_, cmd) = pending_commands.pop_front().unwrap();
                dispatch_open_command(cmd, stream_id, conn, streams, sender_command_tx);
            }
            None => break,
        }
    }
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
    sender_command_tx: mpsc::Sender<ClientCommand>,
    max_body_size: usize,
) {
    let mut streams: HashMap<StreamId, ActiveStreamSlot> = HashMap::with_capacity(256);
    let mut pending_commands: VecDeque<(Instant, ClientCommand)> = VecDeque::with_capacity(128);
    let mut transmit_buf = Vec::with_capacity(98304);
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
            // Ingest new command from client handles with adaptive batch draining
            cmd = command_rx.recv() => {
                drain_pending_commands(
                    &mut pending_commands,
                    &mut conn,
                    &mut streams,
                    &sender_command_tx,
                );

                let mut current_cmd = cmd;
                let mut batch_count = 0;
                let mut should_exit = false;

                while let Some(c) = current_cmd {
                    match c {
                        ClientCommand::SendRequest { .. }
                        | ClientCommand::SendStreamRequest { .. }
                        | ClientCommand::OpenStream { .. } => {
                            if !pending_commands.is_empty() {
                                if pending_commands.len() >= MAX_PENDING_STREAMS {
                                    reject_open_command(
                                        c,
                                        Http3Error::H3("No concurrent bidirectional streams available (queue full)".into()),
                                    );
                                } else {
                                    pending_commands.push_back((Instant::now(), c));
                                }
                            } else {
                                match conn.streams().open(Dir::Bi) {
                                    Some(stream_id) => {
                                        dispatch_open_command(
                                            c,
                                            stream_id,
                                            &mut conn,
                                            &mut streams,
                                            &sender_command_tx,
                                        );
                                    }
                                    None => {
                                        pending_commands.push_back((Instant::now(), c));
                                    }
                                }
                            }
                        }
                        ClientCommand::SendChunk {
                            stream_id,
                            chunk,
                            is_fin,
                        } => {
                            if let Some(slot) = streams.get_mut(&stream_id) {
                                if !chunk.is_empty() {
                                    let data_frame = Http3Frame::Data(chunk);
                                    encode_frame(&data_frame, &mut slot.outbound_buf);
                                }
                                if is_fin {
                                    slot.outbound_finished = true;
                                }
                                flush_stream_outbound(&mut conn, stream_id, slot);
                            }
                        }
                        ClientCommand::Close => {
                            conn.close(
                                Instant::now(),
                                quinn_proto::VarInt::from_u32(error_code::H3_NO_ERROR as u32),
                                bytes::Bytes::from_static(b"done"),
                            );
                            should_exit = true;
                            break;
                        }
                    }

                    batch_count += 1;
                    if batch_count >= 32 {
                        break;
                    }
                    current_cmd = command_rx.try_recv().ok();
                }

                flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;
                if should_exit {
                    break;
                }
            }

            // Ingest incoming UDP packets from network with opportunistic batch draining
            res = socket.recv_from(&mut socket_recv_buf) => {
                let mut current_res = Some(res);
                let mut drain_count = 0;

                while let Some(r) = current_res {
                    let (len, from_addr) = match r {
                        Ok(pair) => pair,
                        Err(_) => return,
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
                                .send_to(&resp_buf[..transmit.size], transmit.destination)
                                .await;
                        }
                        _ => {}
                    }
                    if !resp_buf.is_empty() {
                        let _ = socket.send_to(&resp_buf, from_addr).await;
                    }

                    drain_count += 1;
                    if drain_count >= 32 {
                        break;
                    }
                    current_res = socket.try_recv_from(&mut socket_recv_buf).ok().map(Ok);
                }

                drain_pending_commands(
                    &mut pending_commands,
                    &mut conn,
                    &mut streams,
                    &sender_command_tx,
                );
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

        // Flush streams with pending outbound data
        for (&id, slot) in streams.iter_mut() {
            if !slot.outbound_buf.is_empty() {
                flush_stream_outbound(&mut conn, id, slot);
            }
        }

        // RFC 9114 Section 4.1: Sweep cancelled streams where caller dropped receiver
        let cancelled_streams: Vec<StreamId> = streams
            .iter()
            .filter_map(|(&id, slot)| match &slot.reply {
                StreamReply::Buffered { reply_tx, .. } => {
                    if reply_tx.is_closed() {
                        Some(id)
                    } else {
                        None
                    }
                }
                StreamReply::Streaming { head_tx, chunk_tx } => {
                    let head_closed = head_tx
                        .as_ref()
                        .map(|(tx, _)| tx.is_closed())
                        .unwrap_or(false);
                    let chunk_closed = chunk_tx.is_closed();
                    if head_closed || chunk_closed {
                        Some(id)
                    } else {
                        None
                    }
                }
            })
            .collect();

        for id in cancelled_streams {
            streams.remove(&id);
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

                    if !streams.contains_key(&id) {
                        // Stream was cancelled or already completed. Drain bytes to advance flow control.
                        if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                            while let Some(_chunk) = chunks.next(65535).ok().flatten() {}
                            let _ = chunks.finalize();
                        }
                        continue;
                    }

                    // RFC 9114 Flow control backpressure: If stream is active and progressive chunk buffer is full,
                    // do not read from Quinn yet to withhold receive window credit from peer.
                    if let Some(slot) = streams.get(&id)
                        && let StreamReply::Streaming { chunk_tx, .. } = &slot.reply
                        && chunk_tx.capacity() == 0
                    {
                        continue;
                    }

                    let mut payload_too_large = false;
                    let mut stream_finished = false;
                    let mut stream_reset_err = None;
                    chunk_scratch_buf.clear();

                    let is_buffered = streams
                        .get(&id)
                        .map(|s| matches!(s.reply, StreamReply::Buffered { .. }))
                        .unwrap_or(false);
                    let current_raw_len = streams.get(&id).map(|s| s.raw_buf.len()).unwrap_or(0);
                    if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                        loop {
                            match chunks.next(65535) {
                                Ok(Some(chunk)) => {
                                    // Security: Enforce max_body_size buffer limits strictly for buffered
                                    // responses that accumulate into RAM. Progressive Streaming streams are
                                    // forwarded chunk-by-chunk with constant memory footprint and flow control.
                                    if is_buffered
                                        && current_raw_len
                                            + chunk_scratch_buf.len()
                                            + chunk.bytes.len()
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
                        if let Some(slot) = streams.remove(&id) {
                            match slot.reply {
                                StreamReply::Buffered { reply_tx, .. } => {
                                    let _ = reply_tx.send(Err(Http3Error::H3(format!(
                                        "Stream reset by peer: {err}"
                                    ))));
                                }
                                StreamReply::Streaming {
                                    mut head_tx,
                                    chunk_tx,
                                } => {
                                    if let Some((tx, _)) = head_tx.take() {
                                        let _ = tx.send(Err(Http3Error::H3(format!(
                                            "Stream reset by peer: {err}"
                                        ))));
                                    } else {
                                        let _ = chunk_tx.try_send(Err(Http3Error::H3(format!(
                                            "Stream reset by peer: {err}"
                                        ))));
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    if payload_too_large {
                        let _ = conn.recv_stream(id).stop(quinn_proto::VarInt::from_u32(
                            error_code::H3_MESSAGE_ERROR as u32,
                        ));
                        if let Some(slot) = streams.remove(&id) {
                            match slot.reply {
                                StreamReply::Buffered { reply_tx, .. } => {
                                    let _ = reply_tx
                                        .send(Err(Http3Error::PayloadTooLarge(max_body_size + 1)));
                                }
                                StreamReply::Streaming {
                                    mut head_tx,
                                    chunk_tx,
                                } => {
                                    if let Some((tx, _)) = head_tx.take() {
                                        let _ = tx.send(Err(Http3Error::PayloadTooLarge(
                                            max_body_size + 1,
                                        )));
                                    } else {
                                        let _ = chunk_tx.try_send(Err(
                                            Http3Error::PayloadTooLarge(max_body_size + 1),
                                        ));
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    if let Some(slot) = streams.get_mut(&id) {
                        slot.raw_buf.extend_from_slice(&chunk_scratch_buf);

                        // Decode incoming frames
                        while let Ok(Some(frame)) = decode_frame(&mut slot.raw_buf) {
                            match frame {
                                Http3Frame::Headers(payload) => {
                                    if let Ok(decoded) = decode_qpack(&payload) {
                                        slot.headers =
                                            Some((decoded.status, decoded.headers.clone()));
                                        if let StreamReply::Streaming {
                                            ref mut head_tx, ..
                                        } = slot.reply
                                            && let Some((tx, chunk_rx)) = head_tx.take()
                                        {
                                            let status = decoded.status.unwrap_or(StatusCode::OK);
                                            let _ = tx.send(Ok(Http3StreamResponse::new(
                                                status,
                                                decoded.headers,
                                                chunk_rx,
                                            )));
                                        }
                                    }
                                }
                                Http3Frame::Data(payload) => match &mut slot.reply {
                                    StreamReply::Buffered { data_payload, .. } => {
                                        data_payload.extend_from_slice(&payload);
                                    }
                                    StreamReply::Streaming { chunk_tx, .. } => {
                                        let _ = chunk_tx.try_send(Ok(payload));
                                    }
                                },
                                _ => {}
                            }
                        }
                    }

                    // Complete stream handling upon FIN
                    let slot = if stream_finished {
                        streams.remove(&id)
                    } else {
                        None
                    };
                    if let Some(slot) = slot {
                        let (status, headers) = slot
                            .headers
                            .unwrap_or((Some(StatusCode::OK), HeaderMap::new()));
                        let status_code = status.unwrap_or(StatusCode::OK);

                        match slot.reply {
                            StreamReply::Buffered {
                                reply_tx,
                                data_payload,
                            } => {
                                let is_no_body = slot.method == http::Method::HEAD
                                    || status_code.is_informational()
                                    || status_code == StatusCode::NO_CONTENT
                                    || status_code == StatusCode::NOT_MODIFIED;

                                let body = if is_no_body || data_payload.is_empty() {
                                    Body::Empty
                                } else {
                                    Body::Bytes(data_payload.freeze())
                                };

                                let response =
                                    L7Response::new(status_code, Version::HTTP_3, headers, body);
                                let _ = reply_tx.send(Ok(response));
                            }
                            StreamReply::Streaming {
                                mut head_tx,
                                chunk_tx,
                            } => {
                                if let Some((tx, chunk_rx)) = head_tx.take() {
                                    let _ = tx.send(Ok(Http3StreamResponse::new(
                                        status_code,
                                        headers,
                                        chunk_rx,
                                    )));
                                }
                                drop(chunk_tx);
                            }
                        }
                    }
                }
                Event::Stream(StreamEvent::Finished { .. }) => {
                    // Quinn's StreamEvent::Finished signals that our outgoing send stream has been
                    // acknowledged by peer, not that the incoming response stream has finished.
                }
                Event::Stream(StreamEvent::Writable { id }) => {
                    if let Some(slot) = streams.get_mut(&id) {
                        flush_stream_outbound(&mut conn, id, slot);
                    }
                }
                Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                    if id.dir() == Dir::Uni {
                        continue;
                    }
                    if let Some(slot) = streams.remove(&id) {
                        match slot.reply {
                            StreamReply::Buffered { reply_tx, .. } => {
                                let _ = reply_tx.send(Err(Http3Error::H3(format!(
                                    "Stream reset by peer: {error_code}"
                                ))));
                            }
                            StreamReply::Streaming {
                                mut head_tx,
                                chunk_tx,
                            } => {
                                if let Some((tx, _)) = head_tx.take() {
                                    let _ = tx.send(Err(Http3Error::H3(format!(
                                        "Stream reset by peer: {error_code}"
                                    ))));
                                } else {
                                    let _ = chunk_tx.try_send(Err(Http3Error::H3(format!(
                                        "Stream reset by peer: {error_code}"
                                    ))));
                                }
                            }
                        }
                    }
                }
                Event::ConnectionLost { reason } => {
                    for (_, cmd) in pending_commands.drain(..) {
                        reject_open_command(
                            cmd,
                            Http3Error::H3(format!("Connection lost: {reason}")),
                        );
                    }
                    for (_, slot) in streams.drain() {
                        match slot.reply {
                            StreamReply::Buffered { reply_tx, .. } => {
                                let _ = reply_tx.send(Err(Http3Error::H3(format!(
                                    "Connection lost: {reason}"
                                ))));
                            }
                            StreamReply::Streaming {
                                mut head_tx,
                                chunk_tx,
                            } => {
                                if let Some((tx, _)) = head_tx.take() {
                                    let _ = tx.send(Err(Http3Error::H3(format!(
                                        "Connection lost: {reason}"
                                    ))));
                                } else {
                                    let _ = chunk_tx.try_send(Err(Http3Error::H3(format!(
                                        "Connection lost: {reason}"
                                    ))));
                                }
                            }
                        }
                    }
                    return;
                }
                _ => {}
            }
        }

        drain_pending_commands(
            &mut pending_commands,
            &mut conn,
            &mut streams,
            &sender_command_tx,
        );

        // Flush any outgoing packets (ACKs, stream data, flow control) generated by event processing
        flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;

        if conn.is_drained() {
            break;
        }
    }

    // Notify any remaining pending requests
    for (_, cmd) in pending_commands.drain(..) {
        reject_open_command(cmd, Http3Error::ConnectionClosed);
    }

    // Notify any remaining in-flight requests
    for (_, slot) in streams.drain() {
        match slot.reply {
            StreamReply::Buffered { reply_tx, .. } => {
                let _ = reply_tx.send(Err(Http3Error::ConnectionClosed));
            }
            StreamReply::Streaming {
                mut head_tx,
                chunk_tx,
            } => {
                if let Some((tx, _)) = head_tx.take() {
                    let _ = tx.send(Err(Http3Error::ConnectionClosed));
                } else {
                    let _ = chunk_tx.try_send(Err(Http3Error::ConnectionClosed));
                }
            }
        }
    }
}

/// Maximum datagrams to batch into a single GSO transmission (64 datagrams ~= 76-96KB).
const MAX_GSO_SEGMENTS: usize = 64;

#[cfg(target_os = "linux")]
static GSO_AVAILABLE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

#[cfg(target_os = "linux")]
fn send_gso_datagrams(
    fd: std::os::unix::io::RawFd,
    data: &[u8],
    destination: std::net::SocketAddr,
    segment_size: u16,
) -> std::io::Result<usize> {
    use std::mem;

    let sock_addr = socket2::SockAddr::from(destination);

    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };

    const UDP_SEGMENT: libc::c_int = 103;
    let mut ctrl_buf = [0u8; 64];

    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_name = sock_addr.as_ptr() as *mut libc::c_void;
    msg.msg_namelen = sock_addr.len();
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = ctrl_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen =
        unsafe { libc::CMSG_SPACE(mem::size_of::<libc::c_ushort>() as libc::c_uint) } as usize;

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if !cmsg.is_null() {
            (*cmsg).cmsg_level = libc::IPPROTO_UDP;
            (*cmsg).cmsg_type = UDP_SEGMENT;
            (*cmsg).cmsg_len =
                libc::CMSG_LEN(mem::size_of::<libc::c_ushort>() as libc::c_uint) as usize;
            let data_ptr = libc::CMSG_DATA(cmsg) as *mut libc::c_ushort;
            *data_ptr = segment_size;
        }
    }

    let ret = unsafe { libc::sendmsg(fd, &msg, 0) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        // If kernel denies UDP_SEGMENT (e.g. EINVAL or ENOPROTOOPT), disable permanently
        if let Some(code) = err.raw_os_error()
            && (code == libc::EINVAL || code == libc::ENOPROTOOPT)
        {
            GSO_AVAILABLE.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        Err(err)
    } else {
        Ok(ret as usize)
    }
}

/// Helper to drain and transmit newly generated QUIC packets onto the UDP socket.
/// Supports generic UDP segmentation offload (GSO) on Linux to batch multiple packets
/// in a single syscall regardless of request/stream payload size.
pub(crate) async fn flush_conn_transmits(
    conn: &mut quinn_proto::Connection,
    socket: &UdpSocket,
    transmit_buf: &mut Vec<u8>,
) {
    #[cfg(target_os = "linux")]
    use std::os::unix::io::AsRawFd;

    let now = Instant::now();
    transmit_buf.clear();
    while let Some(transmit) = conn.poll_transmit(now, MAX_GSO_SEGMENTS, transmit_buf) {
        if transmit.size > 0 {
            let data = &transmit_buf[..transmit.size];
            let mut sent_via_gso = false;

            if let Some(seg_size) = transmit.segment_size {
                #[cfg(target_os = "linux")]
                if GSO_AVAILABLE.load(std::sync::atomic::Ordering::Relaxed) {
                    let seg_u16 = seg_size as u16;
                    let fd = socket.as_raw_fd();
                    let res = socket.try_io(tokio::io::Interest::WRITABLE, || {
                        send_gso_datagrams(fd, data, transmit.destination, seg_u16)
                    });
                    match res {
                        Ok(_) => {
                            sent_via_gso = true;
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            let async_res = socket
                                .async_io(tokio::io::Interest::WRITABLE, || {
                                    send_gso_datagrams(fd, data, transmit.destination, seg_u16)
                                })
                                .await;
                            if async_res.is_ok() {
                                sent_via_gso = true;
                            }
                        }
                        Err(_) => {} // fallback to uncoalesced chunks below
                    }
                }
            }

            if !sent_via_gso {
                let seg = transmit.segment_size.unwrap_or(data.len());
                for chunk in data.chunks(seg) {
                    match socket.try_send_to(chunk, transmit.destination) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            let _ = socket.send_to(chunk, transmit.destination).await;
                        }
                        Err(_) => {}
                    }
                }
            }
        }
        transmit_buf.clear();
    }
}
