//! Self-Contained HTTP/2 Protocol Engine (RFC 9113 & RFC 7541, Sans-IO).
//!
//! ```text
//! crates/velda-http2/src/proto/
//!   ├── frame.rs         ──▶ 9B binary frame parser/writer + RFC 9113 §4/§6 checks
//!   ├── hpack.rs         ──▶ RFC 7541 static + dynamic table -> HeaderWorkspace
//!   ├── huffman.rs       ──▶ RFC 7541 Appendix B canonical Huffman coding
//!   ├── flow_control.rs  ──▶ 2-level ConnFlowControl / StreamFlowControl accounting
//!   ├── guard.rs         ──▶ DoS / CVE guards (Rapid Reset, Control Flood, CONTINUATION)
//!   └── mod.rs           ──▶ H2Connection state machine (Server & Client roles)
//! ```

pub mod flow_control;
pub mod frame;
pub mod guard;
pub mod hpack;
pub mod huffman;

use std::collections::{BTreeMap, HashMap, VecDeque};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{Method, StatusCode, Uri};
use velda_core::buffer::header::HeaderWorkspace;

use crate::config::Http2Config;

use self::flow_control::{ConnFlowControl, StreamFlowControl};
use self::frame::{
    CONNECTION_PREFACE, DEFAULT_INITIAL_WINDOW_SIZE, DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN,
    H2ErrorCode, H2Frame, MAX_ALLOWED_FRAME_SIZE, MAX_WINDOW_SIZE, SettingPair, decode_frame,
    flags, frame_type, setting_id, write_data_frames, write_goaway, write_headers_frames,
    write_ping, write_rst_stream, write_settings, write_settings_ack, write_window_update,
};
use self::guard::{ControlFloodGuard, PendingHeaderAssembly, RapidResetGuard};
use self::hpack::{
    DEFAULT_HEADER_TABLE_SIZE, DecodedH2Headers, H2IngressEnrich, HpackDecoder, HpackEncoder,
    encode_stateless_literal, encode_stateless_status,
};

/// Endpoint role of an [`H2Connection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum H2Role {
    Server,
    Client,
}

/// Logical lifecycle state of an HTTP/2 stream (RFC 9113 §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
}

/// Fatal connection-level protocol/flow-control/compression failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct H2ProtocolError {
    pub code: H2ErrorCode,
    pub msg: &'static str,
}

impl std::fmt::Display for H2ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HTTP/2 connection error ({:#x}): {}",
            self.code.0, self.msg
        )
    }
}

impl std::error::Error for H2ProtocolError {}

/// Events produced by [`H2Connection`] when processing inbound bytes.
#[derive(Debug)]
pub enum H2Event {
    /// Server received an initial request `HEADERS` block (decoded in 1 pass).
    RequestHeaders {
        stream_id: u32,
        decoded: DecodedH2Headers,
        end_stream: bool,
    },
    /// Client received an initial response `HEADERS` block.
    ResponseHeaders {
        stream_id: u32,
        decoded: DecodedH2Headers,
        end_stream: bool,
    },
    /// Inbound `DATA` payload chunk on `stream_id`.
    Data {
        stream_id: u32,
        payload: Bytes,
        end_stream: bool,
    },
    /// Trailing `HEADERS` frame with `END_STREAM` on an open stream.
    Trailers {
        stream_id: u32,
        trailers: HeaderWorkspace,
    },
    /// Stream aborted via `RST_STREAM`.
    StreamReset {
        stream_id: u32,
        error_code: H2ErrorCode,
    },
    /// Peer `WINDOW_UPDATE` freed additional send capacity for `stream_id` (`0` = connection).
    WindowUpdated { stream_id: u32 },
    /// Peer acknowledged or initiated graceful connection shutdown via `GOAWAY`.
    GoAway {
        last_stream_id: u32,
        error_code: H2ErrorCode,
        debug_data: Bytes,
    },
}

struct StreamEntry {
    state: StreamState,
    received_initial_headers: bool,
    flow: StreamFlowControl,
    send_queue: VecDeque<Bytes>,
    send_end_stream_pending: bool,
}

impl StreamEntry {
    fn new(recv_window: u32, send_window: u32) -> Self {
        Self {
            state: StreamState::Open,
            received_initial_headers: false,
            flow: StreamFlowControl::new(recv_window, send_window),
            send_queue: VecDeque::new(),
            send_end_stream_pending: false,
        }
    }

    #[inline]
    fn remote_open(&self) -> bool {
        matches!(self.state, StreamState::Open | StreamState::HalfClosedLocal)
    }

    #[inline]
    fn local_open(&self) -> bool {
        matches!(
            self.state,
            StreamState::Open | StreamState::HalfClosedRemote
        )
    }

    #[inline]
    fn close_remote(&mut self) {
        self.state = match self.state {
            StreamState::Open => StreamState::HalfClosedRemote,
            StreamState::HalfClosedLocal | StreamState::HalfClosedRemote | StreamState::Closed => {
                StreamState::Closed
            }
        };
    }

    #[inline]
    fn close_local(&mut self) {
        self.state = match self.state {
            StreamState::Open => StreamState::HalfClosedLocal,
            StreamState::HalfClosedRemote | StreamState::HalfClosedLocal | StreamState::Closed => {
                StreamState::Closed
            }
        };
    }
}

/// Sans-IO RFC 9113 HTTP/2 Connection Engine managing handshake, HPACK dynamic state,
/// two-level flow control, stream multiplexing, `GOAWAY` draining, and flood/Rapid-Reset guards.
pub struct H2Connection {
    role: H2Role,
    config: Http2Config,

    preface_verified: bool,
    recv_buf: BytesMut,
    outbound_buf: BytesMut,

    decoder: HpackDecoder,
    encoder: HpackEncoder,

    local_max_frame_size: u32,
    peer_max_frame_size: u32,
    peer_max_concurrent_streams: u32,
    peer_initial_window_size: u32,

    conn_flow: ConnFlowControl,
    rapid_reset_guard: RapidResetGuard,
    control_flood_guard: ControlFloodGuard,

    next_local_stream_id: u32,
    last_peer_stream_id: u32,
    active_streams: BTreeMap<u32, StreamEntry>,
    closed_streams: HashMap<u32, ()>,
    closed_stream_order: VecDeque<u32>,

    pending_headers: Option<PendingHeaderAssembly>,
    local_goaway_sent: bool,
    peer_goaway_last_stream_id: Option<u32>,
}

impl H2Connection {
    /// Initializes a new HTTP/2 connection engine and queues the opening preface/`SETTINGS` frames.
    pub fn new(role: H2Role, config: Http2Config) -> Self {
        let local_max_frame = config
            .max_frame_size
            .clamp(DEFAULT_MAX_FRAME_SIZE, MAX_ALLOWED_FRAME_SIZE);
        let initial_stream_win = config.initial_stream_window_size.min(MAX_WINDOW_SIZE);
        let initial_conn_win = config
            .initial_connection_window_size
            .clamp(DEFAULT_INITIAL_WINDOW_SIZE, MAX_WINDOW_SIZE);

        let mut conn = Self {
            role,
            config,
            preface_verified: role == H2Role::Client,
            recv_buf: BytesMut::with_capacity(16 * 1024),
            outbound_buf: BytesMut::with_capacity(16 * 1024),
            decoder: HpackDecoder::new(
                DEFAULT_HEADER_TABLE_SIZE,
                config.max_header_list_size as usize,
            ),
            encoder: HpackEncoder::new(DEFAULT_HEADER_TABLE_SIZE),
            local_max_frame_size: local_max_frame,
            peer_max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            peer_max_concurrent_streams: u32::MAX,
            peer_initial_window_size: DEFAULT_INITIAL_WINDOW_SIZE,
            conn_flow: ConnFlowControl::new(initial_conn_win),
            rapid_reset_guard: RapidResetGuard::new(config.max_consecutive_resets),
            control_flood_guard: ControlFloodGuard::new(config.max_pending_control_frames),
            next_local_stream_id: if role == H2Role::Client { 1 } else { 2 },
            last_peer_stream_id: 0,
            active_streams: BTreeMap::new(),
            closed_streams: HashMap::new(),
            closed_stream_order: VecDeque::new(),
            pending_headers: None,
            local_goaway_sent: false,
            peer_goaway_last_stream_id: None,
        };

        if role == H2Role::Client {
            conn.outbound_buf.extend_from_slice(CONNECTION_PREFACE);
        }

        let settings = [
            SettingPair {
                id: setting_id::HEADER_TABLE_SIZE,
                value: DEFAULT_HEADER_TABLE_SIZE as u32,
            },
            SettingPair {
                id: setting_id::ENABLE_PUSH,
                value: 0,
            },
            SettingPair {
                id: setting_id::MAX_CONCURRENT_STREAMS,
                value: config.max_concurrent_streams,
            },
            SettingPair {
                id: setting_id::INITIAL_WINDOW_SIZE,
                value: initial_stream_win,
            },
            SettingPair {
                id: setting_id::MAX_FRAME_SIZE,
                value: local_max_frame,
            },
            SettingPair {
                id: setting_id::MAX_HEADER_LIST_SIZE,
                value: config.max_header_list_size,
            },
        ];
        write_settings(&settings, &mut conn.outbound_buf);

        if initial_conn_win > DEFAULT_INITIAL_WINDOW_SIZE {
            let delta = initial_conn_win - DEFAULT_INITIAL_WINDOW_SIZE;
            write_window_update(0, delta, &mut conn.outbound_buf);
        }

        conn
    }

    #[inline]
    pub fn role(&self) -> H2Role {
        self.role
    }

    #[inline]
    pub fn active_stream_count(&self) -> usize {
        self.active_streams.len()
    }

    #[inline]
    pub fn peer_goaway_last_stream_id(&self) -> Option<u32> {
        self.peer_goaway_last_stream_id
    }

    #[inline]
    pub fn is_draining(&self) -> bool {
        self.local_goaway_sent || self.peer_goaway_last_stream_id.is_some()
    }

    #[inline]
    pub fn has_outbound(&self) -> bool {
        !self.outbound_buf.is_empty()
    }

    #[inline]
    pub fn conn_send_window(&self) -> i64 {
        self.conn_flow.send_window
    }

    #[inline]
    pub fn stream_send_window(&self, stream_id: u32) -> Option<i64> {
        self.active_streams
            .get(&stream_id)
            .map(|s| s.flow.send_window.min(self.conn_flow.send_window))
    }

    /// Drains all pending serialized wire frames ready to flush to the transport socket.
    #[inline]
    pub fn take_outbound(&mut self) -> Bytes {
        self.outbound_buf.split().freeze()
    }

    /// Feeds raw transport bytes without ingress enrichment (client role or generic decode).
    pub fn feed_bytes(&mut self, data: &[u8]) -> Result<Vec<H2Event>, H2ProtocolError> {
        self.feed_bytes_internal(data, &mut |_| None)
    }

    /// Feeds raw transport bytes on a downstream server connection, running 1-pass HPACK
    /// `decode_ingress` with per-stream [`H2IngressEnrich`].
    pub fn feed_bytes_ingress(
        &mut self,
        data: &[u8],
        mut make_enrich: impl FnMut(u32) -> H2IngressEnrich,
    ) -> Result<Vec<H2Event>, H2ProtocolError> {
        self.feed_bytes_internal(data, &mut |sid| Some(make_enrich(sid)))
    }

    fn feed_bytes_internal(
        &mut self,
        data: &[u8],
        make_enrich: &mut dyn FnMut(u32) -> Option<H2IngressEnrich>,
    ) -> Result<Vec<H2Event>, H2ProtocolError> {
        self.recv_buf.extend_from_slice(data);

        if !self.preface_verified {
            if self.recv_buf.len() < CONNECTION_PREFACE.len() {
                return Ok(Vec::new());
            }
            if &self.recv_buf[..CONNECTION_PREFACE.len()] != CONNECTION_PREFACE {
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "invalid HTTP/2 client connection preface",
                ));
            }
            self.recv_buf.advance(CONNECTION_PREFACE.len());
            self.preface_verified = true;
        }

        let mut events = Vec::new();
        loop {
            let parsed = match decode_frame(&self.recv_buf, self.local_max_frame_size) {
                Ok(Some(v)) => v,
                Ok(None) => break,
                Err(fe) => return Err(self.fail_connection(fe.code, fe.msg)),
            };
            let (frame, consumed) = parsed;
            let wire_payload_len = consumed.saturating_sub(FRAME_HEADER_LEN);
            self.recv_buf.advance(consumed);

            if let Some(ref mut pending) = self.pending_headers {
                match frame {
                    H2Frame::Continuation {
                        stream_id,
                        end_headers,
                        header_block,
                    } if stream_id == pending.stream_id => {
                        if let Err((code, msg)) = pending.push_continuation(
                            &header_block,
                            self.config.max_continuation_frames,
                            self.config.max_header_list_size,
                        ) {
                            return Err(self.fail_connection(code, msg));
                        }
                        if end_headers {
                            let finished = self.pending_headers.take().unwrap();
                            self.finish_header_block(
                                finished.stream_id,
                                finished.end_stream,
                                &finished.buf,
                                make_enrich,
                                &mut events,
                            )?;
                        }
                        continue;
                    }
                    _ => {
                        return Err(self.fail_connection(
                            H2ErrorCode::PROTOCOL_ERROR,
                            "expected CONTINUATION frame for in-flight header block",
                        ));
                    }
                }
            }

            self.handle_frame(frame, wire_payload_len, make_enrich, &mut events)?;
        }

        Ok(events)
    }

    /// Allocates the next client stream ID, enforces concurrency/`GOAWAY`, encodes headers, and
    /// queues `HEADERS` (and optional `CONTINUATION`) frames.
    pub fn send_request(
        &mut self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderWorkspace,
        end_stream: bool,
    ) -> Result<u32, H2ProtocolError> {
        if self.role != H2Role::Client {
            return Err(H2ProtocolError {
                code: H2ErrorCode::INTERNAL_ERROR,
                msg: "send_request requires client role",
            });
        }
        if let Some(last) = self.peer_goaway_last_stream_id
            && self.next_local_stream_id > last
        {
            return Err(H2ProtocolError {
                code: H2ErrorCode::REFUSED_STREAM,
                msg: "peer GOAWAY forbids opening new streams",
            });
        }
        if self.active_streams.len() >= self.peer_max_concurrent_streams as usize {
            return Err(H2ProtocolError {
                code: H2ErrorCode::REFUSED_STREAM,
                msg: "peer MAX_CONCURRENT_STREAMS reached",
            });
        }

        let stream_id = self.next_local_stream_id;
        self.next_local_stream_id = self.next_local_stream_id.saturating_add(2);

        let mut entry = StreamEntry::new(
            self.config.initial_stream_window_size.min(MAX_WINDOW_SIZE),
            self.peer_initial_window_size,
        );
        if end_stream {
            entry.close_local();
        }
        self.active_streams.insert(stream_id, entry);

        let mut block = BytesMut::with_capacity(512);
        self.encoder
            .encode_request(method, uri, headers, &mut block);
        write_headers_frames(
            stream_id,
            &block,
            end_stream,
            self.peer_max_frame_size as usize,
            &mut self.outbound_buf,
        );
        Ok(stream_id)
    }

    /// Encodes and queues a response head on an active server stream.
    pub fn send_response(
        &mut self,
        stream_id: u32,
        status: StatusCode,
        headers: &HeaderWorkspace,
        end_stream: bool,
    ) -> Result<(), H2ProtocolError> {
        let Some(stream) = self.active_streams.get_mut(&stream_id) else {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "stream does not exist or already closed",
            });
        };
        if !stream.local_open() {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "local stream side is closed",
            });
        }

        let mut block = BytesMut::with_capacity(512);
        self.encoder.encode_response(status, headers, &mut block);
        write_headers_frames(
            stream_id,
            &block,
            end_stream,
            self.peer_max_frame_size as usize,
            &mut self.outbound_buf,
        );

        if end_stream {
            stream.close_local();
            if stream.state == StreamState::Closed {
                self.retire_stream(stream_id);
                self.rapid_reset_guard.on_clean_stream_complete();
            }
        }
        Ok(())
    }

    /// Queues a zero-body `HEADERS` (`END_STREAM | END_HEADERS`) early response directly into
    /// `outbound_buf` from pre-encoded stateless HPACK bytes plus optional per-request metadata
    /// (`x-request-id`, `alt-svc`).
    ///
    /// Uses RFC 7541 Static Indexing and Literal Without Indexing exclusively so the connection's
    /// HPACK dynamic table remains completely untouched for parallel and subsequent streams.
    pub fn send_stateless_early_response(
        &mut self,
        stream_id: u32,
        status: StatusCode,
        preencoded_hpack: Option<&[u8]>,
        request_id: Option<&[u8]>,
        alt_svc: Option<&[u8]>,
    ) -> Result<(), H2ProtocolError> {
        let Some(stream) = self.active_streams.get_mut(&stream_id) else {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "stream does not exist or already closed",
            });
        };
        if !stream.local_open() {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "local stream side is closed",
            });
        }

        let hdr_pos = self.outbound_buf.len();
        self.outbound_buf.resize(hdr_pos + FRAME_HEADER_LEN, 0);

        if let Some(static_block) = preencoded_hpack {
            self.outbound_buf.put_slice(static_block);
        } else {
            encode_stateless_status(status, &mut self.outbound_buf);
            encode_stateless_literal(
                b"content-type",
                b"text/plain; charset=utf-8",
                &mut self.outbound_buf,
            );
            encode_stateless_literal(b"cache-control", b"no-store", &mut self.outbound_buf);
            if status == StatusCode::TOO_MANY_REQUESTS {
                encode_stateless_literal(b"retry-after", b"1", &mut self.outbound_buf);
            }
        }

        if let Some(req_id) = request_id {
            encode_stateless_literal(b"x-request-id", req_id, &mut self.outbound_buf);
        }
        if let Some(alt) = alt_svc {
            encode_stateless_literal(b"alt-svc", alt, &mut self.outbound_buf);
        }

        let block_len = self.outbound_buf.len() - hdr_pos - FRAME_HEADER_LEN;
        let max_frame = self.peer_max_frame_size.max(DEFAULT_MAX_FRAME_SIZE) as usize;
        if block_len <= max_frame {
            let sid_masked = stream_id & 0x7FFF_FFFF;
            let f = flags::END_STREAM | flags::END_HEADERS;
            let hdr = &mut self.outbound_buf[hdr_pos..hdr_pos + FRAME_HEADER_LEN];
            hdr[0] = ((block_len >> 16) & 0xff) as u8;
            hdr[1] = ((block_len >> 8) & 0xff) as u8;
            hdr[2] = (block_len & 0xff) as u8;
            hdr[3] = frame_type::HEADERS;
            hdr[4] = f;
            hdr[5..9].copy_from_slice(&sid_masked.to_be_bytes());
        } else {
            let overflow_block = self.outbound_buf.split_off(hdr_pos + FRAME_HEADER_LEN);
            self.outbound_buf.truncate(hdr_pos);
            write_headers_frames(
                stream_id,
                &overflow_block,
                true,
                max_frame,
                &mut self.outbound_buf,
            );
        }

        stream.close_local();
        if stream.state == StreamState::Closed {
            self.retire_stream(stream_id);
            self.rapid_reset_guard.on_clean_stream_complete();
        }
        Ok(())
    }

    /// Queues outbound `DATA` payload for `stream_id` and flushes up to two-level windows.
    pub fn send_data(
        &mut self,
        stream_id: u32,
        payload: Bytes,
        end_stream: bool,
    ) -> Result<(), H2ProtocolError> {
        let Some(stream) = self.active_streams.get_mut(&stream_id) else {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "stream closed before send_data",
            });
        };
        if !stream.local_open() {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "local stream side closed",
            });
        }
        if !payload.is_empty() {
            stream.send_queue.push_back(payload);
        }
        if end_stream {
            stream.send_end_stream_pending = true;
        }
        self.flush_stream_data(stream_id);
        Ok(())
    }

    /// Sends trailing `HEADERS` (`END_STREAM`) on `stream_id`.
    pub fn send_trailers(
        &mut self,
        stream_id: u32,
        trailers: &HeaderWorkspace,
    ) -> Result<(), H2ProtocolError> {
        let Some(stream) = self.active_streams.get_mut(&stream_id) else {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "stream closed before send_trailers",
            });
        };
        if !stream.local_open() {
            return Err(H2ProtocolError {
                code: H2ErrorCode::STREAM_CLOSED,
                msg: "local stream side closed",
            });
        }
        let mut block = BytesMut::with_capacity(256);
        self.encoder.encode_trailers(trailers, &mut block);
        write_headers_frames(
            stream_id,
            &block,
            true,
            self.peer_max_frame_size as usize,
            &mut self.outbound_buf,
        );
        stream.close_local();
        if stream.state == StreamState::Closed {
            self.retire_stream(stream_id);
            self.rapid_reset_guard.on_clean_stream_complete();
        }
        Ok(())
    }

    /// Records `bytes` consumed by the caller, emitting batched `WINDOW_UPDATE` frames.
    pub fn release_recv_capacity(&mut self, stream_id: u32, bytes: u32) {
        if let Some(inc) = self.conn_flow.release_recv(bytes) {
            write_window_update(0, inc, &mut self.outbound_buf);
        }
        if let Some(stream) = self.active_streams.get_mut(&stream_id)
            && stream.remote_open()
            && let Some(inc) = stream.flow.release_recv(bytes)
        {
            write_window_update(stream_id, inc, &mut self.outbound_buf);
        }
    }

    /// Sends `RST_STREAM` for `stream_id` and removes it from active state.
    pub fn send_reset(&mut self, stream_id: u32, code: H2ErrorCode) {
        if self.active_streams.contains_key(&stream_id) {
            write_rst_stream(stream_id, code, &mut self.outbound_buf);
            self.retire_stream(stream_id);
        }
    }

    /// Sends a `PING` frame with 8-byte `opaque_data`.
    pub fn send_ping(&mut self, opaque_data: [u8; 8]) {
        write_ping(false, &opaque_data, &mut self.outbound_buf);
    }

    /// Initiates graceful shutdown by emitting `GOAWAY(last_peer_stream_id, NO_ERROR)`.
    pub fn initiate_graceful_shutdown(&mut self) {
        if !self.local_goaway_sent {
            self.local_goaway_sent = true;
            write_goaway(
                self.last_peer_stream_id,
                H2ErrorCode::NO_ERROR,
                b"",
                &mut self.outbound_buf,
            );
        }
    }

    fn flush_stream_data(&mut self, stream_id: u32) {
        let Some(stream) = self.active_streams.get_mut(&stream_id) else {
            return;
        };

        while let Some(mut front) = stream.send_queue.pop_front() {
            if front.is_empty() {
                continue;
            }
            let allowed = self.conn_flow.send_window.min(stream.flow.send_window);
            if allowed <= 0 {
                stream.send_queue.push_front(front);
                return;
            }
            let send_len = front.len().min(allowed as usize);
            let to_send = front.split_to(send_len);
            if !front.is_empty() {
                stream.send_queue.push_front(front);
            }

            let is_final = stream.send_end_stream_pending && stream.send_queue.is_empty();
            if is_final {
                stream.send_end_stream_pending = false;
            }
            self.conn_flow.send_window -= send_len as i64;
            stream.flow.send_window -= send_len as i64;

            write_data_frames(
                stream_id,
                &to_send,
                is_final,
                self.peer_max_frame_size as usize,
                &mut self.outbound_buf,
            );

            if is_final {
                stream.close_local();
                if stream.state == StreamState::Closed {
                    self.retire_stream(stream_id);
                    self.rapid_reset_guard.on_clean_stream_complete();
                    return;
                }
            }
        }

        if stream.send_end_stream_pending {
            stream.send_end_stream_pending = false;
            write_data_frames(
                stream_id,
                &[],
                true,
                self.peer_max_frame_size as usize,
                &mut self.outbound_buf,
            );
            stream.close_local();
            if stream.state == StreamState::Closed {
                self.retire_stream(stream_id);
                self.rapid_reset_guard.on_clean_stream_complete();
            }
        }
    }

    fn flush_all_send_queues(&mut self) {
        let sids: Vec<u32> = self.active_streams.keys().copied().collect();
        for sid in sids {
            if self.conn_flow.send_window <= 0 {
                break;
            }
            self.flush_stream_data(sid);
        }
    }

    fn handle_frame(
        &mut self,
        frame: H2Frame,
        wire_payload_len: usize,
        make_enrich: &mut dyn FnMut(u32) -> Option<H2IngressEnrich>,
        events: &mut Vec<H2Event>,
    ) -> Result<(), H2ProtocolError> {
        match frame {
            H2Frame::Settings { ack, entries } => {
                self.control_flood_guard
                    .on_control_frame()
                    .map_err(|(c, m)| self.fail_connection(c, m))?;
                if !ack {
                    self.apply_peer_settings(&entries)?;
                    write_settings_ack(&mut self.outbound_buf);
                }
            }
            H2Frame::Ping { ack, opaque_data } => {
                self.control_flood_guard
                    .on_control_frame()
                    .map_err(|(c, m)| self.fail_connection(c, m))?;
                if !ack {
                    write_ping(true, &opaque_data, &mut self.outbound_buf);
                }
            }
            H2Frame::GoAway {
                last_stream_id,
                error_code,
                debug_data,
            } => {
                self.peer_goaway_last_stream_id = Some(last_stream_id);
                events.push(H2Event::GoAway {
                    last_stream_id,
                    error_code,
                    debug_data,
                });
            }
            H2Frame::Priority { .. } => {
                self.control_flood_guard
                    .on_control_frame()
                    .map_err(|(c, m)| self.fail_connection(c, m))?;
            }
            H2Frame::PushPromise { .. } => {
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "PUSH_PROMISE forbidden when ENABLE_PUSH = 0",
                ));
            }
            H2Frame::Continuation { .. } => {
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "unexpected CONTINUATION frame without open HEADERS block",
                ));
            }
            H2Frame::WindowUpdate {
                stream_id,
                increment,
            } => {
                self.control_flood_guard
                    .on_control_frame()
                    .map_err(|(c, m)| self.fail_connection(c, m))?;
                if stream_id == 0 {
                    if let Err(code) = self.conn_flow.apply_peer_window_update(increment) {
                        return Err(
                            self.fail_connection(code, "connection send window exceeded 2^31 - 1")
                        );
                    }
                    self.flush_all_send_queues();
                    events.push(H2Event::WindowUpdated { stream_id: 0 });
                } else if let Some(stream) = self.active_streams.get_mut(&stream_id) {
                    if let Err(code) = stream.flow.apply_peer_window_update(increment) {
                        write_rst_stream(stream_id, code, &mut self.outbound_buf);
                        self.retire_stream(stream_id);
                        events.push(H2Event::StreamReset {
                            stream_id,
                            error_code: code,
                        });
                    } else {
                        self.flush_stream_data(stream_id);
                        events.push(H2Event::WindowUpdated { stream_id });
                    }
                } else if !self.closed_streams.contains_key(&stream_id)
                    && self.is_idle_stream(stream_id)
                {
                    return Err(self.fail_connection(
                        H2ErrorCode::PROTOCOL_ERROR,
                        "WINDOW_UPDATE on idle stream",
                    ));
                }
            }
            H2Frame::RstStream {
                stream_id,
                error_code,
            } => {
                if !self.closed_streams.contains_key(&stream_id) && self.is_idle_stream(stream_id) {
                    return Err(self.fail_connection(
                        H2ErrorCode::PROTOCOL_ERROR,
                        "RST_STREAM on idle stream",
                    ));
                }
                self.rapid_reset_guard
                    .on_rst_stream()
                    .map_err(|(c, m)| self.fail_connection(c, m))?;
                if self.active_streams.contains_key(&stream_id) {
                    self.retire_stream(stream_id);
                    events.push(H2Event::StreamReset {
                        stream_id,
                        error_code,
                    });
                }
            }
            H2Frame::Headers {
                stream_id,
                end_stream,
                end_headers,
                header_block,
            } => {
                self.control_flood_guard.on_productive_frame();
                if end_headers {
                    self.finish_header_block(
                        stream_id,
                        end_stream,
                        &header_block,
                        make_enrich,
                        events,
                    )?;
                } else {
                    self.pending_headers = Some(PendingHeaderAssembly::start(
                        stream_id,
                        end_stream,
                        &header_block,
                    ));
                }
            }
            H2Frame::Data {
                stream_id,
                end_stream,
                payload,
            } => {
                self.control_flood_guard.on_productive_frame();
                let wire_len = wire_payload_len as i64;
                if let Err(code) = self.conn_flow.consume_recv(wire_len) {
                    return Err(self.fail_connection(code, "connection receive window exhausted"));
                }

                let Some(stream) = self.active_streams.get_mut(&stream_id) else {
                    if self.closed_streams.contains_key(&stream_id) {
                        write_rst_stream(
                            stream_id,
                            H2ErrorCode::STREAM_CLOSED,
                            &mut self.outbound_buf,
                        );
                        self.release_recv_capacity(stream_id, wire_payload_len as u32);
                        return Ok(());
                    }
                    return Err(self.fail_connection(
                        H2ErrorCode::PROTOCOL_ERROR,
                        "DATA frame on idle stream",
                    ));
                };

                if !stream.remote_open() {
                    write_rst_stream(
                        stream_id,
                        H2ErrorCode::STREAM_CLOSED,
                        &mut self.outbound_buf,
                    );
                    self.release_recv_capacity(stream_id, wire_payload_len as u32);
                    return Ok(());
                }

                if let Err(code) = stream.flow.consume_recv(wire_len) {
                    write_rst_stream(stream_id, code, &mut self.outbound_buf);
                    self.retire_stream(stream_id);
                    events.push(H2Event::StreamReset {
                        stream_id,
                        error_code: code,
                    });
                    return Ok(());
                }

                if end_stream {
                    stream.close_remote();
                    if stream.state == StreamState::Closed {
                        self.retire_stream(stream_id);
                        self.rapid_reset_guard.on_clean_stream_complete();
                    }
                }

                events.push(H2Event::Data {
                    stream_id,
                    payload,
                    end_stream,
                });
            }
            H2Frame::Unknown { .. } => {}
        }

        Ok(())
    }

    fn finish_header_block(
        &mut self,
        stream_id: u32,
        end_stream: bool,
        header_block: &[u8],
        make_enrich: &mut dyn FnMut(u32) -> Option<H2IngressEnrich>,
        events: &mut Vec<H2Event>,
    ) -> Result<(), H2ProtocolError> {
        let is_existing = self.active_streams.contains_key(&stream_id);
        let is_trailing = if is_existing {
            self.active_streams
                .get(&stream_id)
                .is_some_and(|s| s.received_initial_headers && self.role == H2Role::Server)
                || self.active_streams.get(&stream_id).is_some_and(|s| {
                    self.role == H2Role::Client && s.received_initial_headers && end_stream
                })
        } else {
            false
        };

        if !is_existing {
            if self.role != H2Role::Server {
                if self.closed_streams.contains_key(&stream_id) {
                    return Ok(());
                }
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "client received HEADERS on unknown stream",
                ));
            }
            if stream_id == 0
                || stream_id.is_multiple_of(2)
                || stream_id <= self.last_peer_stream_id
            {
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "invalid client-initiated HTTP/2 stream identifier",
                ));
            }
            if self.local_goaway_sent {
                write_rst_stream(
                    stream_id,
                    H2ErrorCode::REFUSED_STREAM,
                    &mut self.outbound_buf,
                );
                return Ok(());
            }
            if self.active_streams.len() >= self.config.max_concurrent_streams as usize {
                write_rst_stream(
                    stream_id,
                    H2ErrorCode::REFUSED_STREAM,
                    &mut self.outbound_buf,
                );
                return Ok(());
            }
            self.last_peer_stream_id = stream_id;
            let entry = StreamEntry::new(
                self.config.initial_stream_window_size.min(MAX_WINDOW_SIZE),
                self.peer_initial_window_size,
            );
            self.active_streams.insert(stream_id, entry);
        }

        let decoded = if self.role == H2Role::Server && !is_trailing {
            match make_enrich(stream_id) {
                Some(enrich) => self.decoder.decode_ingress(header_block, enrich),
                None => self.decoder.decode(header_block),
            }
        } else {
            self.decoder.decode(header_block)
        }
        .map_err(|he| self.fail_connection(he.code, he.msg))?;

        let Some(stream) = self.active_streams.get_mut(&stream_id) else {
            return Ok(());
        };
        if !stream.remote_open() {
            return Err(self.fail_connection(
                H2ErrorCode::STREAM_CLOSED,
                "HEADERS received on half-closed remote stream",
            ));
        }

        if is_trailing {
            if !end_stream
                || decoded.method.is_some()
                || decoded.uri.is_some()
                || decoded.status.is_some()
            {
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "invalid trailing HEADERS (missing END_STREAM or contains pseudo-header)",
                ));
            }
            stream.close_remote();
            if stream.state == StreamState::Closed {
                self.retire_stream(stream_id);
                self.rapid_reset_guard.on_clean_stream_complete();
            }
            events.push(H2Event::Trailers {
                stream_id,
                trailers: decoded.headers,
            });
            return Ok(());
        }

        if self.role == H2Role::Client {
            let Some(status) = decoded.status else {
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "response HEADERS missing :status pseudo-header",
                ));
            };
            if !status.is_informational() {
                stream.received_initial_headers = true;
            }
            if end_stream {
                stream.close_remote();
                if stream.state == StreamState::Closed {
                    self.retire_stream(stream_id);
                    self.rapid_reset_guard.on_clean_stream_complete();
                }
            }
            events.push(H2Event::ResponseHeaders {
                stream_id,
                decoded,
                end_stream,
            });
        } else {
            if decoded.method.is_none()
                || (decoded.method != Some(Method::CONNECT) && decoded.uri.is_none())
            {
                return Err(self.fail_connection(
                    H2ErrorCode::PROTOCOL_ERROR,
                    "request HEADERS missing required :method or :path pseudo-header",
                ));
            }
            stream.received_initial_headers = true;
            if end_stream {
                stream.close_remote();
                if stream.state == StreamState::Closed {
                    self.retire_stream(stream_id);
                    self.rapid_reset_guard.on_clean_stream_complete();
                }
            }
            events.push(H2Event::RequestHeaders {
                stream_id,
                decoded,
                end_stream,
            });
        }

        Ok(())
    }

    fn apply_peer_settings(&mut self, entries: &[SettingPair]) -> Result<(), H2ProtocolError> {
        for pair in entries {
            match pair.id {
                setting_id::HEADER_TABLE_SIZE => {
                    let capped = (pair.value as usize).min(64 * 1024);
                    self.encoder.set_peer_max_table_size(capped);
                }
                setting_id::ENABLE_PUSH => {
                    if pair.value > 1 {
                        return Err(self.fail_connection(
                            H2ErrorCode::PROTOCOL_ERROR,
                            "SETTINGS_ENABLE_PUSH must be 0 or 1",
                        ));
                    }
                }
                setting_id::MAX_CONCURRENT_STREAMS => {
                    self.peer_max_concurrent_streams = pair.value;
                }
                setting_id::INITIAL_WINDOW_SIZE => {
                    if pair.value > MAX_WINDOW_SIZE {
                        return Err(self.fail_connection(
                            H2ErrorCode::FLOW_CONTROL_ERROR,
                            "SETTINGS_INITIAL_WINDOW_SIZE exceeded 2^31 - 1",
                        ));
                    }
                    let delta = pair.value as i64 - self.peer_initial_window_size as i64;
                    self.peer_initial_window_size = pair.value;
                    for stream in self.active_streams.values_mut() {
                        if let Err(code) = stream.flow.apply_initial_window_delta(delta) {
                            return Err(self.fail_connection(
                                code,
                                "stream send window overflowed after SETTINGS_INITIAL_WINDOW_SIZE",
                            ));
                        }
                    }
                    self.flush_all_send_queues();
                }
                setting_id::MAX_FRAME_SIZE => {
                    if !(DEFAULT_MAX_FRAME_SIZE..=MAX_ALLOWED_FRAME_SIZE).contains(&pair.value) {
                        return Err(self.fail_connection(
                            H2ErrorCode::PROTOCOL_ERROR,
                            "SETTINGS_MAX_FRAME_SIZE out of bounds",
                        ));
                    }
                    self.peer_max_frame_size = pair.value;
                }
                setting_id::MAX_HEADER_LIST_SIZE | setting_id::NO_RFC7540_PRIORITIES => {}
                _ => {}
            }
        }
        Ok(())
    }

    #[inline]
    fn is_idle_stream(&self, stream_id: u32) -> bool {
        if stream_id.is_multiple_of(2) {
            self.role == H2Role::Client && stream_id > self.last_peer_stream_id
        } else if self.role == H2Role::Server {
            stream_id > self.last_peer_stream_id
        } else {
            stream_id >= self.next_local_stream_id
        }
    }

    #[inline]
    fn retire_stream(&mut self, stream_id: u32) {
        self.active_streams.remove(&stream_id);
        if self.closed_streams.insert(stream_id, ()).is_none() {
            self.closed_stream_order.push_back(stream_id);
            if self.closed_stream_order.len() > 2048
                && let Some(old) = self.closed_stream_order.pop_front()
            {
                self.closed_streams.remove(&old);
            }
        }
    }

    #[inline]
    fn fail_connection(&mut self, code: H2ErrorCode, msg: &'static str) -> H2ProtocolError {
        if !self.local_goaway_sent {
            self.local_goaway_sent = true;
            write_goaway(
                self.last_peer_stream_id,
                code,
                msg.as_bytes(),
                &mut self.outbound_buf,
            );
        }
        H2ProtocolError { code, msg }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::frame::decode_frame;
    use crate::proto::hpack::H2ForwardingAddrs;
    use http::header::{HeaderName, HeaderValue};

    #[test]
    fn test_proto_client_server_full_roundtrip_with_trailers_and_flow_control() {
        let mut cfg = Http2Config::from_hardware(2, 4 * 1024 * 1024 * 1024);
        cfg.initial_stream_window_size = 64 * 1024;
        cfg.initial_connection_window_size = 128 * 1024;

        let mut client = H2Connection::new(H2Role::Client, cfg);
        let mut server = H2Connection::new(H2Role::Server, cfg);

        let c_out = client.take_outbound();
        assert!(matches!(
            server.feed_bytes(&c_out).unwrap().as_slice(),
            [H2Event::WindowUpdated { stream_id: 0 }]
        ));
        let s_out = server.take_outbound();
        assert!(matches!(
            client.feed_bytes(&s_out).unwrap().as_slice(),
            [H2Event::WindowUpdated { stream_id: 0 }]
        ));
        let c_ack = client.take_outbound();
        let s_ack = server.take_outbound();
        let _ = server.feed_bytes(&c_ack).unwrap();
        let _ = client.feed_bytes(&s_ack).unwrap();

        let mut req_headers = HeaderWorkspace::empty(4);
        req_headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let uri: Uri = "https://velda.test/upload".parse().unwrap();
        let sid = client
            .send_request(&Method::POST, &uri, &req_headers, false)
            .unwrap();
        assert_eq!(sid, 1);

        let body_96k = Bytes::from(vec![0x5a; 96 * 1024]);
        client.send_data(sid, body_96k.clone(), true).unwrap();

        let mut received_body = BytesMut::new();
        let mut got_req = false;
        let mut req_done = false;
        for _ in 0..4 {
            let chunk = client.take_outbound();
            if chunk.is_empty() {
                break;
            }
            for ev in server.feed_bytes(&chunk).unwrap() {
                match ev {
                    H2Event::RequestHeaders { stream_id, .. } => {
                        assert_eq!(stream_id, sid);
                        got_req = true;
                    }
                    H2Event::Data {
                        stream_id,
                        payload,
                        end_stream,
                    } => {
                        let plen = payload.len() as u32;
                        received_body.extend_from_slice(&payload);
                        server.release_recv_capacity(stream_id, plen);
                        if end_stream {
                            req_done = true;
                        }
                    }
                    _ => {}
                }
            }
            let s_wu = server.take_outbound();
            if !s_wu.is_empty() {
                client.feed_bytes(&s_wu).unwrap();
            }
        }
        assert!(got_req && req_done);
        assert_eq!(&received_body[..], &body_96k[..]);

        let mut resp_ws = HeaderWorkspace::empty(2);
        resp_ws.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/grpc"),
        );
        server
            .send_response(sid, StatusCode::OK, &resp_ws, false)
            .unwrap();
        server
            .send_data(sid, Bytes::from_static(b"grpc-frame"), false)
            .unwrap();
        let mut trailers = HeaderWorkspace::empty(2);
        trailers.insert(
            HeaderName::from_static("grpc-status"),
            HeaderValue::from_static("0"),
        );
        server.send_trailers(sid, &trailers).unwrap();
        assert_eq!(server.active_stream_count(), 0);

        let s_resp = server.take_outbound();
        let client_events = client.feed_bytes(&s_resp).unwrap();
        assert!(matches!(
            client_events[0],
            H2Event::ResponseHeaders { stream_id: 1, .. }
        ));
        assert!(matches!(
            &client_events[1],
            H2Event::Data { stream_id: 1, payload, end_stream: false } if &payload[..] == b"grpc-frame"
        ));
        assert!(matches!(
            &client_events[2],
            H2Event::Trailers { stream_id: 1, trailers } if trailers.get("grpc-status").unwrap() == "0"
        ));
        assert_eq!(client.active_stream_count(), 0);
    }

    #[test]
    fn test_proto_initial_window_size_negative_delta_and_rapid_reset_guard() {
        let mut cfg = Http2Config::from_hardware(1, 2 * 1024 * 1024 * 1024);
        cfg.max_consecutive_resets = 3;

        let mut client = H2Connection::new(H2Role::Client, cfg);
        let uri: Uri = "https://velda.test/".parse().unwrap();
        let sid = client
            .send_request(&Method::GET, &uri, &HeaderWorkspace::empty(0), true)
            .unwrap();

        let mut srv_bytes = BytesMut::new();
        write_settings(
            &[SettingPair {
                id: setting_id::INITIAL_WINDOW_SIZE,
                value: 0,
            }],
            &mut srv_bytes,
        );
        client.feed_bytes(&srv_bytes).unwrap();
        assert_eq!(client.stream_send_window(sid), Some(0));

        let mut server = H2Connection::new(H2Role::Server, cfg);
        let mut c_enc = HpackEncoder::default();
        let mut req_buf = BytesMut::from(&CONNECTION_PREFACE[..]);
        write_settings(&[], &mut req_buf);
        for i in 0..4u32 {
            let stream_id = 1 + i * 2;
            let mut hblock = BytesMut::new();
            c_enc.encode_request(&Method::GET, &uri, &HeaderWorkspace::empty(0), &mut hblock);
            write_headers_frames(stream_id, &hblock, false, 16384, &mut req_buf);
            write_rst_stream(stream_id, H2ErrorCode::CANCEL, &mut req_buf);
        }
        let err = server.feed_bytes(&req_buf).unwrap_err();
        assert_eq!(err.code, H2ErrorCode::ENHANCE_YOUR_CALM);

        let s_out = server.take_outbound();
        let mut slice = &s_out[..];
        let mut saw_goaway = false;
        while let Some((f, n)) = decode_frame(slice, DEFAULT_MAX_FRAME_SIZE).unwrap() {
            slice = &slice[n..];
            if let H2Frame::GoAway { error_code, .. } = f {
                assert_eq!(error_code, H2ErrorCode::ENHANCE_YOUR_CALM);
                saw_goaway = true;
            }
        }
        assert!(saw_goaway);
    }

    #[test]
    fn test_proto_continuation_sequence_enforcement_and_goaway_drain() {
        let mut cfg = Http2Config::from_hardware(1, 2 * 1024 * 1024 * 1024);
        cfg.max_continuation_frames = 2;
        let mut server = H2Connection::new(H2Role::Server, cfg);

        let mut wire = BytesMut::from(&CONNECTION_PREFACE[..]);
        frame::write_frame_header(3, frame::frame_type::HEADERS, 0, 1, &mut wire);
        wire.extend_from_slice(&[0x82, 0x86, 0x84]);
        write_ping(false, b"12345678", &mut wire);
        assert_eq!(
            server.feed_bytes(&wire).unwrap_err().code,
            H2ErrorCode::PROTOCOL_ERROR
        );

        let mut server2 = H2Connection::new(H2Role::Server, cfg);
        let mut c_enc = HpackEncoder::default();
        let uri: Uri = "https://velda.test/one".parse().unwrap();
        let mut w1 = BytesMut::from(&CONNECTION_PREFACE[..]);
        let mut hb = BytesMut::new();
        c_enc.encode_request(&Method::GET, &uri, &HeaderWorkspace::empty(0), &mut hb);
        write_headers_frames(1, &hb, true, 16384, &mut w1);
        assert_eq!(server2.feed_bytes(&w1).unwrap().len(), 1);
        let _ = server2.take_outbound();

        server2.initiate_graceful_shutdown();
        let mut w2 = BytesMut::new();
        hb.clear();
        c_enc.encode_request(&Method::GET, &uri, &HeaderWorkspace::empty(0), &mut hb);
        write_headers_frames(3, &hb, true, 16384, &mut w2);
        assert!(server2.feed_bytes(&w2).unwrap().is_empty());

        let s_out = server2.take_outbound();
        let mut slice = &s_out[..];
        let mut saw_goaway = false;
        let mut saw_refused = false;
        while let Some((f, n)) = decode_frame(slice, DEFAULT_MAX_FRAME_SIZE).unwrap() {
            slice = &slice[n..];
            match f {
                H2Frame::GoAway {
                    last_stream_id: 1,
                    error_code: H2ErrorCode::NO_ERROR,
                    ..
                } => saw_goaway = true,
                H2Frame::RstStream {
                    stream_id: 3,
                    error_code: H2ErrorCode::REFUSED_STREAM,
                } => saw_refused = true,
                _ => {}
            }
        }
        assert!(saw_goaway && saw_refused);
    }

    #[tokio::test]
    async fn test_proto_full_interop_with_reference_h2_client_and_server() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (c_io, mut s_io) = tokio::io::duplex(64 * 1024);
        let cfg = Http2Config::auto();

        let srv_task = tokio::spawn(async move {
            let mut srv = H2Connection::new(H2Role::Server, cfg);
            s_io.write_all(&srv.take_outbound()).await.unwrap();
            let mut buf = [0u8; 4096];
            let mut body_acc = BytesMut::new();
            let mut responded = false;
            loop {
                let n = s_io.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                let events = srv
                    .feed_bytes_ingress(&buf[..n], |_sid| H2IngressEnrich {
                        request_id: HeaderValue::from_static("proto-req-id-1"),
                        forwarding: Some(H2ForwardingAddrs {
                            peer_ip: velda_core::lut::ip::v4_to_u128(0xC0A8_0002),
                            local_ip: velda_core::lut::ip::v4_to_u128(0x7F00_0001),
                            local_port_be: 8443u16.to_be(),
                            is_tls: true,
                        }),
                    })
                    .unwrap();
                for ev in events {
                    match ev {
                        H2Event::RequestHeaders {
                            stream_id,
                            decoded,
                            end_stream,
                        } => {
                            assert_eq!(
                                decoded.headers.get("x-request-id").unwrap(),
                                "proto-req-id-1"
                            );
                            if end_stream && !responded {
                                srv.send_response(
                                    stream_id,
                                    StatusCode::OK,
                                    &HeaderWorkspace::empty(0),
                                    true,
                                )
                                .unwrap();
                                responded = true;
                            }
                        }
                        H2Event::Data {
                            stream_id,
                            payload,
                            end_stream,
                        } => {
                            let plen = payload.len() as u32;
                            body_acc.extend_from_slice(&payload);
                            srv.release_recv_capacity(stream_id, plen);
                            if end_stream && !responded {
                                let mut rws = HeaderWorkspace::empty(1);
                                rws.insert(
                                    HeaderName::from_static("x-echo-len"),
                                    HeaderValue::from_str(&body_acc.len().to_string()).unwrap(),
                                );
                                srv.send_response(stream_id, StatusCode::OK, &rws, false)
                                    .unwrap();
                                srv.send_data(stream_id, body_acc.clone().freeze(), true)
                                    .unwrap();
                                responded = true;
                            }
                        }
                        _ => {}
                    }
                }
                let out = srv.take_outbound();
                if !out.is_empty() {
                    s_io.write_all(&out).await.unwrap();
                }
                if responded && srv.active_stream_count() == 0 {
                    break;
                }
            }
        });

        let (mut client, conn) = h2::client::handshake(c_io).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let req = http::Request::post("https://proto.velda.dev/echo")
            .header("x-request-id", "spoof-attempt")
            .body(())
            .unwrap();
        let (resp_fut, mut send_stream) = client.send_request(req, false).unwrap();
        send_stream
            .send_data(Bytes::from_static(b"hello sans-io h2!"), true)
            .unwrap();

        let resp = resp_fut.await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get("x-echo-len").unwrap(), "17");
        let mut recv_body = resp.into_body();
        let mut got = BytesMut::new();
        while let Some(chunk) = recv_body.data().await {
            got.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(&got[..], b"hello sans-io h2!");
        drop(client);
        srv_task.await.unwrap();

        // Client interop against reference h2::server::handshake
        let (mut cli_io, h2_srv_io) = tokio::io::duplex(64 * 1024);
        let h2_srv_task = tokio::spawn(async move {
            let mut h2_srv = h2::server::handshake(h2_srv_io).await.unwrap();
            while let Some(res) = h2_srv.accept().await {
                let (req, mut respond) = res.unwrap();
                let (parts, mut recv_body) = req.into_parts();
                tokio::spawn(async move {
                    let mut acc = BytesMut::new();
                    while let Some(chunk) = recv_body.data().await {
                        acc.extend_from_slice(&chunk.unwrap());
                    }
                    let mut resp = http::Response::new(());
                    *resp.status_mut() = StatusCode::OK;
                    resp.headers_mut().insert(
                        HeaderName::from_static("x-path-seen"),
                        HeaderValue::from_str(parts.uri.path()).unwrap(),
                    );
                    let mut send = respond.send_response(resp, false).unwrap();
                    send.send_data(acc.freeze(), false).unwrap();
                    let mut tr = http::HeaderMap::new();
                    tr.insert(
                        HeaderName::from_static("grpc-status"),
                        HeaderValue::from_static("0"),
                    );
                    send.send_trailers(tr).unwrap();
                });
            }
        });

        let mut cli = H2Connection::new(H2Role::Client, cfg);
        let mut hws = HeaderWorkspace::empty(2);
        hws.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/grpc"),
        );
        let s1 = cli
            .send_request(
                &Method::POST,
                &"https://grpc.velda.dev/svc.Echo/Say".parse().unwrap(),
                &hws,
                false,
            )
            .unwrap();
        let s2 = cli
            .send_request(
                &Method::POST,
                &"https://grpc.velda.dev/svc.Echo/Two".parse().unwrap(),
                &hws,
                false,
            )
            .unwrap();
        cli.send_data(s1, Bytes::from_static(b"payload-1"), true)
            .unwrap();
        cli.send_data(s2, Bytes::from_static(b"payload-2"), true)
            .unwrap();
        cli_io.write_all(&cli.take_outbound()).await.unwrap();

        let mut done_streams = 0usize;
        let mut s1_body = BytesMut::new();
        let mut s2_body = BytesMut::new();
        let mut s1_tr = false;
        let mut s2_tr = false;
        let mut rbuf = [0u8; 4096];
        while done_streams < 2 {
            let n = cli_io.read(&mut rbuf).await.unwrap();
            assert!(n > 0);
            for ev in cli.feed_bytes(&rbuf[..n]).unwrap() {
                match ev {
                    H2Event::ResponseHeaders {
                        stream_id, decoded, ..
                    } => {
                        assert_eq!(decoded.status, Some(StatusCode::OK));
                        if stream_id == s1 {
                            assert_eq!(
                                decoded.headers.get("x-path-seen").unwrap(),
                                "/svc.Echo/Say"
                            );
                        } else if stream_id == s2 {
                            assert_eq!(
                                decoded.headers.get("x-path-seen").unwrap(),
                                "/svc.Echo/Two"
                            );
                        }
                    }
                    H2Event::Data {
                        stream_id, payload, ..
                    } => {
                        let plen = payload.len() as u32;
                        if stream_id == s1 {
                            s1_body.extend_from_slice(&payload);
                        } else if stream_id == s2 {
                            s2_body.extend_from_slice(&payload);
                        }
                        cli.release_recv_capacity(stream_id, plen);
                    }
                    H2Event::Trailers {
                        stream_id,
                        trailers,
                    } => {
                        assert_eq!(trailers.get("grpc-status").unwrap(), "0");
                        if stream_id == s1 {
                            s1_tr = true;
                        } else if stream_id == s2 {
                            s2_tr = true;
                        }
                        done_streams += 1;
                    }
                    _ => {}
                }
            }
            let out = cli.take_outbound();
            if !out.is_empty() {
                cli_io.write_all(&out).await.unwrap();
            }
        }
        assert_eq!(&s1_body[..], b"payload-1");
        assert_eq!(&s2_body[..], b"payload-2");
        assert!(s1_tr && s2_tr);
        drop(cli_io);
        let _ = h2_srv_task.await;
    }

    #[test]
    fn test_proto_stateless_early_response_preserves_dynamic_table() {
        let cfg = Http2Config::auto();
        let mut client = H2Connection::new(H2Role::Client, cfg);
        let mut server = H2Connection::new(H2Role::Server, cfg);

        let _ = server.feed_bytes(&client.take_outbound()).unwrap();
        let _ = client.feed_bytes(&server.take_outbound()).unwrap();
        let _ = server.feed_bytes(&client.take_outbound()).unwrap();

        let uri1: Uri = "https://velda.test/ok1".parse().unwrap();
        let uri2: Uri = "https://velda.test/blocked".parse().unwrap();
        let uri3: Uri = "https://velda.test/ok2".parse().unwrap();
        let empty_ws = HeaderWorkspace::empty(2);

        let s1 = client
            .send_request(&Method::GET, &uri1, &empty_ws, true)
            .unwrap();
        let s2 = client
            .send_request(&Method::GET, &uri2, &empty_ws, true)
            .unwrap();
        let s3 = client
            .send_request(&Method::GET, &uri3, &empty_ws, true)
            .unwrap();
        let _ = server.feed_bytes(&client.take_outbound()).unwrap();

        // Stream 1: dynamic HPACK response populating server encoder table
        let mut r1_ws = HeaderWorkspace::empty(4);
        r1_ws.insert(
            HeaderName::from_static("x-dyn-shared"),
            HeaderValue::from_static("shared-dyn-value-1"),
        );
        server
            .send_response(s1, StatusCode::OK, &r1_ws, true)
            .unwrap();

        // Stream 2: stateless early error spliced between dynamic HPACK streams
        let raw_429 =
            crate::server::early_response::precompiled_early_hpack(StatusCode::TOO_MANY_REQUESTS);
        server
            .send_stateless_early_response(
                s2,
                StatusCode::TOO_MANY_REQUESTS,
                raw_429,
                Some(b"req-early-429"),
                Some(b"h3=\":443\""),
            )
            .unwrap();

        // Stream 3: reuses dynamic table entry from Stream 1
        server
            .send_response(s3, StatusCode::OK, &r1_ws, true)
            .unwrap();

        let evs = client.feed_bytes(&server.take_outbound()).unwrap();
        let mut seen_s1 = false;
        let mut seen_s2 = false;
        let mut seen_s3 = false;
        for ev in evs {
            if let H2Event::ResponseHeaders {
                stream_id,
                decoded,
                end_stream,
            } = ev
            {
                assert!(end_stream);
                if stream_id == s1 {
                    assert_eq!(decoded.status, Some(StatusCode::OK));
                    assert_eq!(
                        decoded.headers.get("x-dyn-shared").unwrap(),
                        "shared-dyn-value-1"
                    );
                    seen_s1 = true;
                } else if stream_id == s2 {
                    assert_eq!(decoded.status, Some(StatusCode::TOO_MANY_REQUESTS));
                    assert_eq!(decoded.headers.get("retry-after").unwrap(), "1");
                    assert_eq!(
                        decoded.headers.get("x-request-id").unwrap(),
                        "req-early-429"
                    );
                    assert_eq!(decoded.headers.get("alt-svc").unwrap(), "h3=\":443\"");
                    seen_s2 = true;
                } else if stream_id == s3 {
                    assert_eq!(decoded.status, Some(StatusCode::OK));
                    assert_eq!(
                        decoded.headers.get("x-dyn-shared").unwrap(),
                        "shared-dyn-value-1"
                    );
                    seen_s3 = true;
                }
            }
        }
        assert!(seen_s1 && seen_s2 && seen_s3);
    }
}
