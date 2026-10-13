//! Background QUIC connection driver loop for HTTP/3 upstream client (RFC 9114).
//!
//! # Concurrency Architecture
//! Unlike HTTP/1.1 and HTTP/2, QUIC manages encryption keys, congestion control, loss
//! recovery timers, packet numbering, and flow control across all multiplexed streams in a
//! single unified connection state machine ([`quinn_proto::Connection`]).
//!
//! Because `quinn_proto` is strictly non-I/O and non-thread-safe:
//! - A dedicated Tokio task ("Driver") exclusively owns the `Connection` and `UdpSocket`.
//! - Edge pipelines interact with this driver by passing [`ClientCommand`]s over an async channel.
//! - This actor pattern eliminates mutex contention on the hot path, ensures lock-free packet
//!   processing, and keeps request execution fully asynchronous across concurrent streams.

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

/// Return type for [`ClientCommand::OpenStream`].
///
/// Contains an [`Http3StreamSender`] handle (allowing callers to push request chunks concurrently)
/// and a oneshot receiver for incoming [`Http3StreamResponse`] headers and chunks.
pub(crate) type OpenStreamReply = Result<
    (
        super::connector::Http3StreamSender,
        oneshot::Receiver<Result<Http3StreamResponse, Http3Error>>,
    ),
    Http3Error,
>;

/// Internal commands dispatched from proxy pipelines to the background QUIC connection driver loop.
///
/// Permitting large enum variants keeps `L7Request` unboxed on the stack, eliminating extra heap
/// allocations (`malloc`/`free`) on the upstream request dispatch path.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ClientCommand {
    /// Dispatches an L7 request over a newly opened stream expecting a fully buffered response.
    /// Suited for typical REST/JSON RPC where payload sizes are small and known in advance.
    SendRequest {
        request: L7Request,
        reply_tx: oneshot::Sender<Result<L7Response, Http3Error>>,
    },
    /// Dispatches an L7 request over a newly opened stream expecting progressive response streaming.
    /// Emits headers immediately and forwards chunks through `mpsc` with flow control (e.g. SSE, large downloads).
    SendStreamRequest {
        request: L7Request,
        reply_tx: oneshot::Sender<Result<Http3StreamResponse, Http3Error>>,
    },
    /// Opens a progressive bidirectional (duplex) stream.
    /// Used for gRPC-Web over H3, chunked uploads, and bidirectional tunneling where request and response stream concurrently.
    OpenStream {
        method: http::Method,
        uri: http::Uri,
        headers: HeaderMap,
        initial_chunk: Option<bytes::Bytes>,
        is_fin: bool,
        reply_tx: oneshot::Sender<OpenStreamReply>,
    },
    /// Sends a progressive request DATA chunk or signals stream completion (FIN).
    /// Used by client-streaming and duplex pipelines to transmit chunks incrementally without buffering the full request in RAM.
    SendChunk {
        stream_id: StreamId,
        chunk: bytes::Bytes,
        is_fin: bool,
    },
    /// Gracefully closes the QUIC connection.
    /// Sends an immediate `H3_NO_ERROR` CONNECTION_CLOSE frame to the peer and drains pending packets.
    Close,
}

type StreamHeadChannel = (
    oneshot::Sender<Result<Http3StreamResponse, Http3Error>>,
    mpsc::Receiver<Result<bytes::Bytes, Http3Error>>,
);

/// Response dispatch mechanism tailored to the proxy pipeline strategy:
/// - `Buffered`: Accumulates payload in a single `BytesMut` buffer, emitting `L7Response` on FIN.
/// - `Streaming`: Hands off `Http3StreamResponse` headers immediately and feeds incoming chunks into `chunk_tx`.
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

/// State slot tracking an active in-flight bidirectional HTTP/3 stream.
/// Consolidated into a single slot for O(1) lookups without multi-map fragmentation.
struct ActiveStreamSlot {
    method: http::Method,
    reply: StreamReply,
    headers: Option<(Option<StatusCode>, HeaderMap)>,
    /// Accumulates incoming wire bytes until complete HTTP/3 frames can be parsed.
    /// QUIC delivers stream fragments across arbitrary UDP datagram boundaries.
    raw_buf: BytesMut,
    /// Retains outbound encoded frames when QUIC flow control limits write capacity.
    /// Flushed whenever Quinn signals `StreamEvent::Writable`.
    outbound_buf: BytesMut,
    /// Indicates all request data has been provided and Quinn's send stream should be finalized.
    outbound_finished: bool,
}

/// Flushes pending outbound bytes into Quinn's send stream buffer.
///
/// QUIC enforces stream-level and connection-level flow control windows (RFC 9000 Section 4).
/// If the peer's receive window is partially filled, `write()` consumes fewer bytes than available.
/// Unwritten bytes remain in `outbound_buf` until Quinn emits `StreamEvent::Writable`.
/// Once `outbound_buf` is empty and `outbound_finished` is set, `.finish()` transmits the FIN flag.
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

/// Maximum number of pending stream requests to queue when upstream reaches `MAX_STREAMS` limit.
/// Caps memory growth when an upstream server throttles stream concurrency.
const MAX_PENDING_STREAMS: usize = 4096;

/// Timeout after which a queued pending stream request is dropped with an error.
/// Prevents head-of-line blocking if upstream concurrency credits never arrive.
const PENDING_STREAM_TIMEOUT: Duration = Duration::from_secs(10);

/// Checks whether the caller has abandoned a queued request before it was assigned a stream ID.
/// Avoids opening an upstream QUIC stream if the downstream client already disconnected or timed out.
#[inline]
fn is_open_command_cancelled(cmd: &ClientCommand) -> bool {
    match cmd {
        ClientCommand::SendRequest { reply_tx, .. } => reply_tx.is_closed(),
        ClientCommand::SendStreamRequest { reply_tx, .. } => reply_tx.is_closed(),
        ClientCommand::OpenStream { reply_tx, .. } => reply_tx.is_closed(),
        _ => false,
    }
}

/// Rejects an unassigned open command by notifying the waiting caller channel.
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

/// Dispatches an open command onto an allocated bidirectional QUIC stream.
///
/// Coalescing the QPACK-encoded HEADERS frame and initial request DATA into the outbound buffer
/// allows Quinn to pack both into the first flight of QUIC packets (often a single 1-RTT UDP datagram),
/// cutting upstream round-trip latency to the bare minimum.
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

            // Move bytes directly into Http3Frame::Data to avoid copying buffer memory
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

            // Move bytes directly into Http3Frame::Data to avoid copying buffer memory
            if let Body::Bytes(b) = request.body
                && !b.is_empty()
            {
                let data_frame = Http3Frame::Data(b);
                encode_frame(&data_frame, &mut send_buf);
            }

            // Channel capacity of 32 chunks provides sufficient pipeline credit while bounding memory under slow consumers
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
            initial_chunk,
            is_fin,
            reply_tx,
        } => {
            let req_method = method.clone();

            // Encode QPACK request HEADERS frame
            let mut header_buf = BytesMut::new();
            encode_qpack_request(&method, &uri, &headers, &mut header_buf);
            let header_frame = Http3Frame::Headers(header_buf.freeze());

            let mut send_buf = BytesMut::new();
            encode_frame(&header_frame, &mut send_buf);

            // Encode optional initial DATA frame with zero-copy move semantics
            if let Some(chunk) = initial_chunk
                && !chunk.is_empty()
            {
                let data_frame = Http3Frame::Data(chunk);
                encode_frame(&data_frame, &mut send_buf);
            }

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
                outbound_finished: is_fin,
            };
            flush_stream_outbound(conn, stream_id, &mut slot);
            streams.insert(stream_id, slot);
            let _ = reply_tx.send(Ok((stream_sender, resp_rx)));
        }
        _ => {}
    }
}

/// Drains pending queued stream requests in FIFO order when QUIC stream concurrency credits arrive.
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
    let mut cancelled_streams_buf = Vec::with_capacity(64);
    let mut resp_buf = Vec::with_capacity(2048);

    // Transmit ClientHello immediately to initiate the QUIC handshake
    flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;

    loop {
        // Query timer deadline from Quinn for QUIC loss detection (PTO), ACK delay, congestion pacing, and keep-alive
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
            // Branch 1: Ingest new commands from proxy pipelines.
            // Draining up to 128 commands in a batch amortizes channel wakeup and context switch overhead under high concurrency.
            cmd = command_rx.recv() => {
                // First attempt to drain any queued commands waiting for peer stream credits
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
                                // Maintain strict FIFO order if commands are already queued
                                if pending_commands.len() >= MAX_PENDING_STREAMS {
                                    // Fast-reject when queue is full (4096) to shed upstream overload and prevent unbounded memory growth
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
                                        // Upstream reached `MAX_STREAMS` concurrency limit; queue briefly for peer credit grant
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
                            // Encode DATA frame dynamically for progressive streaming uploads
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
                            // Graceful shutdown: RFC 9114 Section 8.1 requires H3_NO_ERROR CONNECTION_CLOSE frame to let peer close cleanly
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
                    if batch_count >= 128 {
                        break;
                    }
                    current_cmd = command_rx.try_recv().ok();
                }

                if should_exit {
                    flush_conn_transmits(&mut conn, &socket, &mut transmit_buf).await;
                    break;
                }
            }

            // Branch 2: Ingest incoming UDP datagrams from network with opportunistic batch draining.
            // Non-blocking try_recv_from loop drains up to 64 packets in bulk to feed Quinn with minimal epoll wakeups.
            res = socket.recv_from(&mut socket_recv_buf) => {
                let mut current_res = Some(res);
                let mut drain_count = 0;

                while let Some(r) = current_res {
                    let (len, from_addr) = match r {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let now = Instant::now();
                    let payload = BytesMut::from(&socket_recv_buf[..len]);
                    resp_buf.clear();
                    match endpoint.handle(now, from_addr, None, None, payload, &mut resp_buf) {
                        Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ce)) => {
                            conn.handle_event(ce);
                        }
                        Some(quinn_proto::DatagramEvent::Response(transmit))
                            if transmit.size > 0 =>
                        {
                            // Stateless resets or version negotiation packets produced during handling must be bounced back immediately
                            let _ = socket
                                .send_to(&resp_buf[..transmit.size], transmit.destination)
                                .await;
                        }
                        _ => {}
                    }

                    drain_count += 1;
                    if drain_count >= 64 {
                        break;
                    }
                    current_res = socket.try_recv_from(&mut socket_recv_buf).ok().map(Ok);
                }

                // Check if incoming packets opened new streams or updated peer stream credits
                drain_pending_commands(
                    &mut pending_commands,
                    &mut conn,
                    &mut streams,
                    &sender_command_tx,
                );
            }

            // Branch 3: Timer tick for QUIC loss recovery, pacing, and keepalive probes
            _ = tokio::time::sleep(timeout_sleep) => {
                let now = Instant::now();
                if let Some(timeout) = conn.poll_timeout()
                    && timeout <= now
                {
                    conn.handle_timeout(now);
                }
            }
        }

        // Flush any streams that have outbound data pending due to prior flow control blocking
        for (&id, slot) in streams.iter_mut() {
            if !slot.outbound_buf.is_empty() {
                flush_stream_outbound(&mut conn, id, slot);
            }
        }

        // RFC 9114 Section 4.1 & 8.1: sweep cancelled streams where caller dropped receiver to release upstream server resources
        cancelled_streams_buf.clear();
        for (&id, slot) in streams.iter() {
            let is_cancelled = match &slot.reply {
                StreamReply::Buffered { reply_tx, .. } => reply_tx.is_closed(),
                StreamReply::Streaming { head_tx, chunk_tx } => {
                    let head_closed = head_tx
                        .as_ref()
                        .map(|(tx, _)| tx.is_closed())
                        .unwrap_or(false);
                    head_closed || chunk_tx.is_closed()
                }
            };
            if is_cancelled {
                cancelled_streams_buf.push(id);
            }
        }

        for id in cancelled_streams_buf.drain(..) {
            streams.remove(&id);
            // H3_REQUEST_CANCELLED (0x010c) informs peer that request was cancelled before completion
            let _ = conn.send_stream(id).reset(quinn_proto::VarInt::from_u32(
                error_code::H3_REQUEST_CANCELLED as u32,
            ));
            let _ = conn.recv_stream(id).stop(quinn_proto::VarInt::from_u32(
                error_code::H3_REQUEST_CANCELLED as u32,
            ));
        }

        // Poll QUIC stream events emitted by the state machine after datagram ingestion
        while let Some(event) = conn.poll() {
            match event {
                Event::Stream(StreamEvent::Readable { id }) => {
                    if id.dir() == Dir::Uni {
                        // Server unidirectional stream (Control Stream, QPACK encoder/decoder); drain to advance flow control windows (RFC 9114 Section 6.2)
                        if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                            while let Some(_chunk) = chunks.next(65535).ok().flatten() {}
                        }
                        continue;
                    }

                    if !streams.contains_key(&id) {
                        // Stream cancelled or completed previously; drain late arrivals to advance connection-level receive window
                        if let Ok(mut chunks) = conn.recv_stream(id).read(true) {
                            while let Some(_chunk) = chunks.next(65535).ok().flatten() {}
                            let _ = chunks.finalize();
                        }
                        continue;
                    }

                    // Backpressure propagation: if downstream chunk buffer is full, pause reading from Quinn to withhold window credit from upstream
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
                                    // responses that accumulate into RAM. Progressive streaming responses are
                                    // forwarded chunk-by-chunk with constant memory footprint and flow control,
                                    // so they do not risk unbounded memory accumulation here.
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
                                    // FIN received: peer finished transmitting this stream.
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
                        // RFC 9114 Section 4.1: Message too large error (0x010e).
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

                        // Ingest stream bytes and parse complete HTTP/3 frames.
                        // A single QUIC read may yield partial frames or multiple concatenated frames.
                        while let Ok(Some(frame)) = decode_frame(&mut slot.raw_buf) {
                            match frame {
                                Http3Frame::Headers(payload) => {
                                    if let Ok(decoded) = decode_qpack(&payload) {
                                        slot.headers =
                                            Some((decoded.status, decoded.headers.clone()));
                                        // For streaming responses, dispatch headers immediately to unblock downstream TTFB
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
                                // RFC 9110 Section 6.4.1: HEAD responses, informational (1xx), 204 No Content,
                                // and 304 Not Modified must not carry a body even if Content-Length is present.
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

            // Fallback for systems without UDP_SEGMENT support or unsegmented single datagrams
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
