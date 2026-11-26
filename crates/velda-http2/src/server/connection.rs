//! Downstream HTTP/2 Server Connection Driver (RFC 9113).
//!
//! ```text
//!  Network IO <──▶ Single-Task Driver (H2Connection::new(H2Role::Server, config))
//!                     ├─ 1-pass HPACK ingress decode -> HeaderWorkspace
//!                     ├─ 2-level recv/send flow control & Rapid Reset guard
//!                     └─ accepts streams -> accept_tx ──▶ Http2ServerConnection
//!                                        └─ body_tx ────▶ Http2StreamReceiver
//!                     ◀── cmd_rx (SendResponse / SendData / SendTrailers / Reset / Shutdown)
//! ```

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::{Buf, Bytes, BytesMut};
use http::StatusCode;
use http::header::HeaderValue;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};
use velda_core::L7Request;
use velda_core::buffer::header::HeaderWorkspace;

use super::request::{Http2ServerRequest, Http2ServerRequestHead, Http2StreamReceiver};
use super::response::Http2Responder;
use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::proto::frame::H2ErrorCode;
use crate::proto::hpack::{H2ForwardingAddrs, H2IngressEnrich};
use crate::proto::{H2Connection, H2Event, H2Role};

/// Incoming HTTP/2 streaming request tuple: request head, stream receiver, and dedicated responder.
pub type Http2StreamingParts = (Http2ServerRequestHead, Http2StreamReceiver, Http2Responder);

#[derive(Debug)]
pub(crate) enum ServerBodyEvent {
    Data { payload: Bytes, end_stream: bool },
    Trailers(HeaderWorkspace),
}

pub(crate) enum ServerCmd {
    SendResponse {
        stream_id: u32,
        status: StatusCode,
        headers: HeaderWorkspace,
        body: Bytes,
        end_stream: bool,
        cancel_upload: bool,
    },
    SendEarlyError {
        stream_id: u32,
        status: StatusCode,
        request_id: Option<HeaderValue>,
        alt_svc: Option<HeaderValue>,
        cancel_upload: bool,
    },
    StartStreamResponse {
        stream_id: u32,
        status: StatusCode,
        headers: HeaderWorkspace,
        queued_send_bytes: Arc<AtomicUsize>,
        send_notify: Arc<Notify>,
    },
    SendData {
        stream_id: u32,
        data: Bytes,
        end_stream: bool,
    },
    SendTrailers {
        stream_id: u32,
        trailers: HeaderWorkspace,
    },
    ReleaseRecvCapacity {
        stream_id: u32,
        bytes: u32,
    },
    ResetStream {
        stream_id: u32,
        code: H2ErrorCode,
    },
    GracefulShutdown,
}

/// Control frame flood tracker kept for standalone unit tests & telemetry hooks.
#[derive(Debug, Default)]
#[repr(C)]
pub struct Http2FloodTracker {
    pub total_bytes: AtomicU64,
    pub payload_bytes: AtomicU64,
}

impl Http2FloodTracker {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn on_bytes_read(&self, n: usize) {
        self.total_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn on_payload_read(&self, n: usize) {
        self.payload_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_flood_detected(&self) -> bool {
        let total = self.total_bytes.load(Ordering::Relaxed);
        let payload = self.payload_bytes.load(Ordering::Relaxed);
        (total >> 3) > payload.saturating_add(1_048_576)
    }
}

struct ActiveServerStream {
    body_tx: mpsc::UnboundedSender<Result<ServerBodyEvent, Http2Error>>,
    queued_send_bytes: Option<Arc<AtomicUsize>>,
    send_notify: Option<Arc<Notify>>,
}

/// Active HTTP/2 downstream connection handle consuming accepted multiplexed request streams.
pub struct Http2ServerConnection {
    accept_rx: mpsc::UnboundedReceiver<Result<Http2StreamingParts, Http2Error>>,
    cmd_tx: mpsc::UnboundedSender<ServerCmd>,
    pub config: Http2Config,
}

impl Http2ServerConnection {
    /// Performs HTTP/2 server setup over `stream` without proxy forwarding header injection.
    pub async fn handshake<IO>(stream: IO, config: Http2Config) -> Result<Self, Http2Error>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::handshake_internal(stream, config, None).await
    }

    /// Performs HTTP/2 server setup over `stream` with 1-pass proxy forwarding enrichment
    /// (`x-request-id`, `x-real-ip`, `x-forwarded-*`, `forwarded`) during HPACK ingress decode.
    pub async fn handshake_with_forwarding<IO>(
        stream: IO,
        config: Http2Config,
        forwarding: H2ForwardingAddrs,
    ) -> Result<Self, Http2Error>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::handshake_internal(stream, config, Some(forwarding)).await
    }

    async fn handshake_internal<IO>(
        stream: IO,
        config: Http2Config,
        forwarding: Option<H2ForwardingAddrs>,
    ) -> Result<Self, Http2Error>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut conn = H2Connection::new(H2Role::Server, config);
        let (rd, mut wr) = tokio::io::split(stream);

        let initial_out = conn.take_outbound();
        if !initial_out.is_empty() {
            wr.write_all(&initial_out).await?;
            wr.flush().await?;
        }

        let (accept_tx, accept_rx) = mpsc::unbounded_channel();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let driver_cmd_tx = cmd_tx.clone();

        tokio::spawn(async move {
            run_server_driver(
                conn,
                rd,
                wr,
                accept_tx,
                cmd_rx,
                driver_cmd_tx,
                config,
                forwarding,
            )
            .await;
        });

        Ok(Self {
            accept_rx,
            cmd_tx,
            config,
        })
    }

    /// Initiates graceful shutdown by sending `GOAWAY` while draining active streams.
    #[inline]
    pub fn graceful_shutdown(&mut self) {
        let _ = self.cmd_tx.send(ServerCmd::GracefulShutdown);
    }

    /// Non-blocking accept returning `Some(...)` if a parsed request stream is queued.
    #[inline]
    pub fn try_accept_streaming_request(
        &mut self,
    ) -> Option<Result<Option<Http2StreamingParts>, Http2Error>> {
        match self.accept_rx.try_recv() {
            Ok(Ok(parts)) => Some(Ok(Some(parts))),
            Ok(Err(e)) => Some(Err(e)),
            Err(mpsc::error::TryRecvError::Disconnected) => Some(Ok(None)),
            Err(mpsc::error::TryRecvError::Empty) => None,
        }
    }

    /// Accepts the next multiplexed streaming request head, body receiver, and responder.
    pub async fn accept_streaming_request(
        &mut self,
    ) -> Result<Option<Http2StreamingParts>, Http2Error> {
        match self.accept_rx.recv().await {
            Some(Ok(parts)) => Ok(Some(parts)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Accepts the next multiplexed request and buffers its body up to `config.max_body_size`.
    pub async fn accept_h2_request(
        &mut self,
    ) -> Result<Option<(Http2ServerRequest, Http2Responder)>, Http2Error> {
        let Some((head, mut receiver, responder)) = self.accept_streaming_request().await? else {
            return Ok(None);
        };
        let body = receiver.consume_all().await?;
        Ok(Some((Http2ServerRequest::new(head, body), responder)))
    }

    /// Accepts the next multiplexed request and converts it into canonical [`L7Request`].
    pub async fn accept_request(
        &mut self,
    ) -> Result<Option<(L7Request, Http2Responder)>, Http2Error> {
        let opt = self.accept_h2_request().await?;
        Ok(opt.map(|(req, responder)| (req.into_l7_request(), responder)))
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_server_driver<IO>(
    mut conn: H2Connection,
    mut rd: tokio::io::ReadHalf<IO>,
    mut wr: tokio::io::WriteHalf<IO>,
    accept_tx: mpsc::UnboundedSender<Result<Http2StreamingParts, Http2Error>>,
    mut cmd_rx: mpsc::UnboundedReceiver<ServerCmd>,
    cmd_tx: mpsc::UnboundedSender<ServerCmd>,
    config: Http2Config,
    forwarding: Option<H2ForwardingAddrs>,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut streams: HashMap<u32, ActiveServerStream> = HashMap::new();
    let mut read_buf = vec![0u8; 32 * 1024];
    let mut write_buf = BytesMut::with_capacity(16 * 1024);
    let mut stream_ids_meta: HashMap<u32, (u128, u64, HeaderValue)> = HashMap::new();

    loop {
        let out = conn.take_outbound();
        if !out.is_empty() {
            write_buf.extend_from_slice(&out);
        }

        if write_buf.is_empty()
            && ((conn.is_draining() && conn.active_stream_count() == 0)
                || (accept_tx.is_closed() && streams.is_empty() && conn.active_stream_count() == 0))
        {
            break;
        }

        tokio::select! {
            biased;

            write_res = write_pending_buf(&mut wr, &mut write_buf), if !write_buf.is_empty() => {
                if write_res.is_err() {
                    break;
                }
            }

            read_res = rd.read(&mut read_buf) => {
                match read_res {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let res = conn.feed_bytes_ingress(&read_buf[..n], |sid| {
                            let ingress_nanos = velda_core::clock::now_unix_nanos();
                            let (req_id, req_id_hdr) = velda_core::request_id::next_request_id(ingress_nanos);
                            stream_ids_meta.insert(sid, (req_id, ingress_nanos, req_id_hdr.clone()));
                            H2IngressEnrich {
                                request_id: req_id_hdr,
                                forwarding,
                            }
                        });
                        match res {
                            Ok(events) => {
                                dispatch_server_events(
                                    events,
                                    &mut streams,
                                    &mut stream_ids_meta,
                                    &accept_tx,
                                    &cmd_tx,
                                    &config,
                                );
                            }
                            Err(proto_err) => {
                                let err_out = conn.take_outbound();
                                if !err_out.is_empty() {
                                    write_buf.extend_from_slice(&err_out);
                                    let _ = wr.write_all(&write_buf).await;
                                }
                                let err = Http2Error::Protocol(proto_err);
                                fail_all_server_streams(&mut streams, &err);
                                let _ = accept_tx.send(Err(err));
                                return;
                            }
                        }
                    }
                }
            }

            cmd_opt = cmd_rx.recv() => {
                let Some(first) = cmd_opt else {
                    break;
                };
                handle_server_cmd(first, &mut conn, &mut streams);
                for _ in 0..64 {
                    match cmd_rx.try_recv() {
                        Ok(cmd) => handle_server_cmd(cmd, &mut conn, &mut streams),
                        Err(_) => break,
                    }
                }
            }
        }
    }

    if !write_buf.is_empty() {
        let _ = wr.write_all(&write_buf).await;
        let _ = wr.flush().await;
    }
    fail_all_server_streams(&mut streams, &Http2Error::ConnectionClosed);
}

#[inline]
async fn write_pending_buf<W: AsyncWrite + Unpin>(
    wr: &mut W,
    buf: &mut BytesMut,
) -> std::io::Result<()> {
    let n = std::future::poll_fn(|cx| Pin::new(&mut *wr).poll_write(cx, buf)).await?;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "zero byte write to HTTP/2 socket",
        ));
    }
    buf.advance(n);
    if buf.is_empty() {
        let _ = std::future::poll_fn(|cx| Pin::new(&mut *wr).poll_flush(cx)).await;
    }
    Ok(())
}

fn dispatch_server_events(
    events: Vec<H2Event>,
    streams: &mut HashMap<u32, ActiveServerStream>,
    meta_map: &mut HashMap<u32, (u128, u64, HeaderValue)>,
    accept_tx: &mpsc::UnboundedSender<Result<Http2StreamingParts, Http2Error>>,
    cmd_tx: &mpsc::UnboundedSender<ServerCmd>,
    config: &Http2Config,
) {
    let max_send_buf = config.max_send_buffer_size.max(64 * 1024);
    for ev in events {
        match ev {
            H2Event::RequestHeaders {
                stream_id,
                decoded,
                end_stream,
            } => {
                let method = decoded.method.unwrap_or(http::Method::GET);
                let uri = decoded.uri.unwrap_or_else(|| http::Uri::from_static("/"));
                let mut head =
                    Http2ServerRequestHead::new(method, uri, decoded.headers, Some(stream_id));
                let mut req_id_hdr = None;
                if let Some((req_id, ingress_nanos, hdr)) = meta_map.remove(&stream_id) {
                    head.request_id = req_id;
                    head.ingress_time_nanos = ingress_nanos;
                    req_id_hdr = Some(hdr);
                }

                let (body_tx, body_rx) = mpsc::unbounded_channel();
                if end_stream {
                    drop(body_tx);
                } else {
                    streams.insert(
                        stream_id,
                        ActiveServerStream {
                            body_tx,
                            queued_send_bytes: None,
                            send_notify: None,
                        },
                    );
                }

                let receiver = Http2StreamReceiver {
                    stream_id,
                    body_rx,
                    cmd_tx: cmd_tx.clone(),
                    max_body_size: config.max_body_size,
                    bytes_received: 0,
                    eof: end_stream,
                    trailers: None,
                    tee_tx: None,
                };
                let responder = Http2Responder {
                    cmd_tx: cmd_tx.clone(),
                    max_send_buffer_size: max_send_buf,
                    stream_id,
                    request_id: req_id_hdr,
                    alt_svc: None,
                    last_status: 0,
                    last_bytes_tx: 0,
                };
                let _ = accept_tx.send(Ok((head, receiver, responder)));
            }
            H2Event::Data {
                stream_id,
                payload,
                end_stream,
            } => {
                if let Some(st) = streams.get(&stream_id) {
                    let _ = st.body_tx.send(Ok(ServerBodyEvent::Data {
                        payload,
                        end_stream,
                    }));
                    if end_stream && st.queued_send_bytes.is_none() {
                        streams.remove(&stream_id);
                    }
                }
            }
            H2Event::Trailers {
                stream_id,
                trailers,
            } => {
                if let Some(st) = streams.get(&stream_id) {
                    let _ = st.body_tx.send(Ok(ServerBodyEvent::Trailers(trailers)));
                    if st.queued_send_bytes.is_none() {
                        streams.remove(&stream_id);
                    }
                }
            }
            H2Event::StreamReset {
                stream_id,
                error_code,
            } => {
                if let Some(st) = streams.remove(&stream_id) {
                    let _ = st.body_tx.send(Err(Http2Error::StreamReset(error_code)));
                    if let Some(notify) = st.send_notify {
                        notify.notify_waiters();
                    }
                }
            }
            H2Event::WindowUpdated { .. }
            | H2Event::GoAway { .. }
            | H2Event::ResponseHeaders { .. } => {}
        }
    }
}

fn handle_server_cmd(
    cmd: ServerCmd,
    conn: &mut H2Connection,
    streams: &mut HashMap<u32, ActiveServerStream>,
) {
    match cmd {
        ServerCmd::SendResponse {
            stream_id,
            status,
            headers,
            body,
            end_stream,
            cancel_upload,
        } => {
            let has_body = !body.is_empty();
            let head_end = end_stream && !has_body;
            let _ = conn.send_response(stream_id, status, &headers, head_end);
            if has_body {
                let _ = conn.send_data(stream_id, body, end_stream);
            }
            if cancel_upload {
                conn.send_reset(stream_id, H2ErrorCode::NO_ERROR);
                streams.remove(&stream_id);
            } else if end_stream {
                streams.remove(&stream_id);
            }
        }
        ServerCmd::SendEarlyError {
            stream_id,
            status,
            request_id,
            alt_svc,
            cancel_upload,
        } => {
            let preencoded = super::early_response::precompiled_early_hpack(status);
            let _ = conn.send_stateless_early_response(
                stream_id,
                status,
                preencoded,
                request_id.as_ref().map(|v| v.as_bytes()),
                alt_svc.as_ref().map(|v| v.as_bytes()),
            );
            if cancel_upload {
                conn.send_reset(stream_id, H2ErrorCode::NO_ERROR);
            }
            streams.remove(&stream_id);
        }
        ServerCmd::StartStreamResponse {
            stream_id,
            status,
            headers,
            queued_send_bytes,
            send_notify,
        } => {
            let _ = conn.send_response(stream_id, status, &headers, false);
            if let Some(st) = streams.get_mut(&stream_id) {
                st.queued_send_bytes = Some(queued_send_bytes);
                st.send_notify = Some(send_notify);
            } else {
                let (dead_tx, _) = mpsc::unbounded_channel();
                streams.insert(
                    stream_id,
                    ActiveServerStream {
                        body_tx: dead_tx,
                        queued_send_bytes: Some(queued_send_bytes),
                        send_notify: Some(send_notify),
                    },
                );
            }
        }
        ServerCmd::SendData {
            stream_id,
            data,
            end_stream,
        } => {
            let len = data.len();
            let _ = conn.send_data(stream_id, data, end_stream);
            if let Some(st) = streams.get(&stream_id)
                && let Some(q) = &st.queued_send_bytes
            {
                q.fetch_sub(len, Ordering::AcqRel);
                if let Some(n) = &st.send_notify {
                    n.notify_one();
                }
            }
            if end_stream {
                streams.remove(&stream_id);
            }
        }
        ServerCmd::SendTrailers {
            stream_id,
            trailers,
        } => {
            let _ = conn.send_trailers(stream_id, &trailers);
            streams.remove(&stream_id);
        }
        ServerCmd::ReleaseRecvCapacity { stream_id, bytes } => {
            conn.release_recv_capacity(stream_id, bytes);
        }
        ServerCmd::ResetStream { stream_id, code } => {
            conn.send_reset(stream_id, code);
            if let Some(st) = streams.remove(&stream_id)
                && let Some(n) = st.send_notify
            {
                n.notify_waiters();
            }
        }
        ServerCmd::GracefulShutdown => {
            conn.initiate_graceful_shutdown();
        }
    }
}

fn fail_all_server_streams(streams: &mut HashMap<u32, ActiveServerStream>, err: &Http2Error) {
    let msg = err.to_string();
    for (_sid, st) in streams.drain() {
        let cloned = match err {
            Http2Error::StreamReset(c) => Http2Error::StreamReset(*c),
            Http2Error::Protocol(p) => Http2Error::Protocol(p.clone()),
            _ => Http2Error::Parse(msg.clone()),
        };
        let _ = st.body_tx.send(Err(cloned));
        if let Some(n) = st.send_notify {
            n.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flood_tracker_heuristics() {
        let tracker = Http2FloodTracker::new();
        assert!(!tracker.is_flood_detected());

        tracker.on_bytes_read(1_000_000);
        assert!(!tracker.is_flood_detected());

        tracker.on_bytes_read(7_000_000);
        assert!(!tracker.is_flood_detected());

        tracker.on_bytes_read(1_000_000);
        assert!(tracker.is_flood_detected());

        tracker.on_payload_read(2 * 1_048_576);
        assert!(!tracker.is_flood_detected());
    }
}
