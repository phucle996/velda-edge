//! Upstream HTTP/2 Client Connection Driver & Stream Multiplexer (RFC 9113).
//!
//! ```text
//!  Caller Task(s)                                 Single-Task Connection Driver
//!  ┌───────────────────────────┐                  ┌───────────────────────────────────┐
//!  │ Http2Client::send_request │──▶ cmd_tx ─────▶ │ H2Connection(H2Role::Client)      │
//!  │ Http2ClientSendStream     │   (unbounded)    │  - tokio::select! on:             │
//!  │ Http2ClientRecvStream     │◀── body_rx ◀──── │     1) rd.read (feed_bytes)       │
//!  │ Http2ResponseFuture       │◀── head_tx ◀──── │     2) cmd_rx (batch up to 64)    │
//!  └───────────────────────────┘                  │     3) poll_write (flush outbound)│
//!                                                 └───────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use http::{Method, StatusCode, Uri};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};
use velda_core::buffer::header::HeaderWorkspace;

use crate::config::Http2Config;
use crate::error::Http2Error;
use crate::proto::frame::H2ErrorCode;
use crate::proto::{H2Connection, H2Event, H2Role};

#[derive(Debug)]
enum BodyEvent {
    Data(Bytes),
    Trailers(HeaderWorkspace),
}

type ClientHeadAck = (
    u32,
    oneshot::Receiver<Result<Http2ClientResponse, Http2Error>>,
);

enum ClientCmd {
    SendRequest {
        method: Method,
        uri: Uri,
        headers: HeaderWorkspace,
        end_stream: bool,
        sid_tx: oneshot::Sender<Result<ClientHeadAck, Http2Error>>,
        queued_send_bytes: Arc<AtomicUsize>,
        send_notify: Arc<Notify>,
        stream_max_body: usize,
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
}

struct ActiveClientStream {
    head_tx: Option<oneshot::Sender<Result<Http2ClientResponse, Http2Error>>>,
    body_rx: Option<mpsc::UnboundedReceiver<Result<BodyEvent, Http2Error>>>,
    body_tx: mpsc::UnboundedSender<Result<BodyEvent, Http2Error>>,
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
    queued_send_bytes: Arc<AtomicUsize>,
    send_notify: Arc<Notify>,
    max_body_size: usize,
}

/// Cloneable multiplexed HTTP/2 client connection handle.
#[derive(Clone, Debug)]
pub struct Http2Client {
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
    healthy: Arc<AtomicBool>,
    max_send_buffer_size: usize,
    default_max_body_size: usize,
}

impl Http2Client {
    /// Performs the HTTP/2 client preface + SETTINGS exchange and spawns the connection driver task.
    pub async fn handshake<IO>(stream: IO, config: &Http2Config) -> Result<Self, Http2Error>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut conn = H2Connection::new(H2Role::Client, *config);
        let (rd, mut wr) = tokio::io::split(stream);

        let initial_out = conn.take_outbound();
        if !initial_out.is_empty() {
            wr.write_all(&initial_out).await?;
            wr.flush().await?;
        }

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let healthy = Arc::new(AtomicBool::new(true));
        let driver_healthy = Arc::clone(&healthy);
        let driver_cmd_tx = cmd_tx.clone();
        let max_send_buffer_size = config.max_send_buffer_size.max(64 * 1024);
        let default_max_body_size = config.max_body_size;

        tokio::spawn(async move {
            run_client_driver(
                conn,
                rd,
                wr,
                cmd_rx,
                driver_cmd_tx,
                driver_healthy,
                max_send_buffer_size,
            )
            .await;
        });

        Ok(Self {
            cmd_tx,
            healthy,
            max_send_buffer_size,
            default_max_body_size,
        })
    }

    /// Returns true if the underlying HTTP/2 connection is open and not draining (`GOAWAY`).
    #[inline]
    pub fn is_healthy(&self) -> bool {
        !self.cmd_tx.is_closed() && self.healthy.load(Ordering::Acquire)
    }

    /// Opens a new multiplexed HTTP/2 stream and queues request HEADERS.
    pub async fn send_request(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderWorkspace,
        end_stream: bool,
    ) -> Result<(Http2ResponseFuture, Http2ClientSendStream), Http2Error> {
        if !self.is_healthy() {
            return Err(Http2Error::ConnectionClosed);
        }
        let (sid_tx, sid_rx) = oneshot::channel();
        let queued_send_bytes = Arc::new(AtomicUsize::new(0));
        let send_notify = Arc::new(Notify::new());
        self.cmd_tx
            .send(ClientCmd::SendRequest {
                method,
                uri,
                headers,
                end_stream,
                sid_tx,
                queued_send_bytes: Arc::clone(&queued_send_bytes),
                send_notify: Arc::clone(&send_notify),
                stream_max_body: self.default_max_body_size,
            })
            .map_err(|_| Http2Error::ConnectionClosed)?;

        let (stream_id, head_rx) = sid_rx.await.map_err(|_| Http2Error::ConnectionClosed)??;

        let send_stream = Http2ClientSendStream {
            stream_id,
            cmd_tx: self.cmd_tx.clone(),
            queued_send_bytes,
            send_notify,
            max_send_buffer_size: self.max_send_buffer_size,
            finished: end_stream,
        };
        Ok((Http2ResponseFuture { rx: head_rx }, send_stream))
    }
}

/// Future resolving to [`Http2ClientResponse`] once backend response `HEADERS` arrive.
pub struct Http2ResponseFuture {
    rx: oneshot::Receiver<Result<Http2ClientResponse, Http2Error>>,
}

impl Future for Http2ResponseFuture {
    type Output = Result<Http2ClientResponse, Http2Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Http2Error::ConnectionClosed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Decoded HTTP/2 backend response head and streaming body receiver backed by [`HeaderWorkspace`].
#[derive(Debug)]
pub struct Http2ClientResponse {
    pub status: StatusCode,
    pub headers: HeaderWorkspace,
    pub body: Http2ClientRecvStream,
}

/// Progressive HTTP/2 client stream body + trailers receiver.
#[derive(Debug)]
pub struct Http2ClientRecvStream {
    stream_id: u32,
    body_rx: mpsc::UnboundedReceiver<Result<BodyEvent, Http2Error>>,
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
    trailers: Option<HeaderWorkspace>,
    eof: bool,
    max_body_size: usize,
    bytes_received: usize,
}

impl Http2ClientRecvStream {
    /// Receives the next `DATA` payload chunk, returning `Ok(None)` at EOF / trailers.
    pub async fn recv_chunk(&mut self) -> Result<Option<Bytes>, Http2Error> {
        if self.eof {
            return Ok(None);
        }
        loop {
            match self.body_rx.recv().await {
                Some(Ok(BodyEvent::Data(chunk))) => {
                    let len = chunk.len();
                    if len > 0 {
                        self.bytes_received = self.bytes_received.saturating_add(len);
                        let _ = self.cmd_tx.send(ClientCmd::ReleaseRecvCapacity {
                            stream_id: self.stream_id,
                            bytes: len as u32,
                        });
                        if self.bytes_received > self.max_body_size {
                            self.eof = true;
                            return Err(Http2Error::PayloadTooLarge(self.bytes_received));
                        }
                        return Ok(Some(chunk));
                    }
                }
                Some(Ok(BodyEvent::Trailers(tr))) => {
                    self.trailers = Some(tr);
                    self.eof = true;
                    return Ok(None);
                }
                Some(Err(e)) => {
                    self.eof = true;
                    return Err(e);
                }
                None => {
                    self.eof = true;
                    return Ok(None);
                }
            }
        }
    }

    /// Non-blocking check for the next queued `DATA` chunk:
    /// `Ok(None)` = queue empty, `Ok(Some(Some(bytes)))` = chunk, `Ok(Some(None))` = EOF.
    #[inline]
    pub fn try_recv_chunk(&mut self) -> Result<Option<Option<Bytes>>, Http2Error> {
        if self.eof {
            return Ok(Some(None));
        }
        loop {
            match self.body_rx.try_recv() {
                Ok(Ok(BodyEvent::Data(chunk))) => {
                    let len = chunk.len();
                    if len > 0 {
                        self.bytes_received = self.bytes_received.saturating_add(len);
                        let _ = self.cmd_tx.send(ClientCmd::ReleaseRecvCapacity {
                            stream_id: self.stream_id,
                            bytes: len as u32,
                        });
                        if self.bytes_received > self.max_body_size {
                            self.eof = true;
                            return Err(Http2Error::PayloadTooLarge(self.bytes_received));
                        }
                        return Ok(Some(Some(chunk)));
                    }
                }
                Ok(Ok(BodyEvent::Trailers(tr))) => {
                    self.trailers = Some(tr);
                    self.eof = true;
                    return Ok(Some(None));
                }
                Ok(Err(e)) => {
                    self.eof = true;
                    return Err(e);
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.eof = true;
                    return Ok(Some(None));
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(None),
            }
        }
    }

    /// Consumes all remaining chunks into a contiguous [`Bytes`] buffer within `max_bytes`.
    pub async fn consume_all(&mut self, max_bytes: usize) -> Result<Bytes, Http2Error> {
        self.max_body_size = max_bytes;
        let mut out = BytesMut::new();
        while let Some(chunk) = self.recv_chunk().await? {
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }

    /// Takes trailing headers if received alongside stream EOF.
    #[inline]
    pub fn take_trailers(&mut self) -> Option<HeaderWorkspace> {
        self.trailers.take()
    }

    /// Awaits trailers after all DATA frames have been drained.
    pub async fn trailers(&mut self) -> Result<Option<HeaderWorkspace>, Http2Error> {
        if let Some(tr) = self.trailers.take() {
            return Ok(Some(tr));
        }
        if self.eof {
            return Ok(None);
        }
        while let Some(ev) = self.body_rx.recv().await {
            match ev? {
                BodyEvent::Data(_) => {}
                BodyEvent::Trailers(tr) => {
                    self.eof = true;
                    return Ok(Some(tr));
                }
            }
        }
        self.eof = true;
        Ok(None)
    }

    #[inline]
    pub fn is_end_stream(&self) -> bool {
        self.eof
    }
}

/// Multiplexed request body sender for an outbound HTTP/2 stream.
#[derive(Debug)]
pub struct Http2ClientSendStream {
    stream_id: u32,
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
    queued_send_bytes: Arc<AtomicUsize>,
    send_notify: Arc<Notify>,
    max_send_buffer_size: usize,
    finished: bool,
}

impl Http2ClientSendStream {
    /// Queues an outbound `DATA` payload (non-blocking) for transmission by the driver.
    #[inline]
    pub fn send_data(&mut self, data: Bytes, end_stream: bool) -> Result<(), Http2Error> {
        if self.finished {
            return Ok(());
        }
        self.finished = end_stream;
        self.queued_send_bytes
            .fetch_add(data.len(), Ordering::Release);
        self.cmd_tx
            .send(ClientCmd::SendData {
                stream_id: self.stream_id,
                data,
                end_stream,
            })
            .map_err(|_| Http2Error::ConnectionClosed)
    }

    /// Sends a `DATA` chunk applying backpressure when queued bytes exceed `max_send_buffer_size`.
    pub async fn send_chunk(&mut self, data: Bytes, end_stream: bool) -> Result<(), Http2Error> {
        if self.finished {
            return Ok(());
        }
        while self.queued_send_bytes.load(Ordering::Acquire) >= self.max_send_buffer_size {
            if self.cmd_tx.is_closed() {
                return Err(Http2Error::ConnectionClosed);
            }
            self.send_notify.notified().await;
        }
        self.send_data(data, end_stream)
    }

    /// Sends trailing headers and closes the local send side.
    #[inline]
    pub fn send_trailers(&mut self, trailers: HeaderWorkspace) -> Result<(), Http2Error> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.cmd_tx
            .send(ClientCmd::SendTrailers {
                stream_id: self.stream_id,
                trailers,
            })
            .map_err(|_| Http2Error::ConnectionClosed)
    }

    /// Sends `RST_STREAM` with `code`.
    #[inline]
    pub fn send_reset(&mut self, code: H2ErrorCode) {
        self.finished = true;
        let _ = self.cmd_tx.send(ClientCmd::ResetStream {
            stream_id: self.stream_id,
            code,
        });
    }
}

async fn run_client_driver<IO>(
    mut conn: H2Connection,
    mut rd: tokio::io::ReadHalf<IO>,
    mut wr: tokio::io::WriteHalf<IO>,
    mut cmd_rx: mpsc::UnboundedReceiver<ClientCmd>,
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
    healthy: Arc<AtomicBool>,
    _max_send_buffer_size: usize,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut streams: HashMap<u32, ActiveClientStream> = HashMap::new();
    let mut read_buf = vec![0u8; 32 * 1024];
    let mut write_buf = BytesMut::with_capacity(16 * 1024);

    loop {
        let out = conn.take_outbound();
        if !out.is_empty() {
            write_buf.extend_from_slice(&out);
        }

        if write_buf.is_empty() && conn.is_draining() && streams.is_empty() {
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
                        match conn.feed_bytes(&read_buf[..n]) {
                            Ok(events) => {
                                dispatch_client_events(
                                    events,
                                    &mut streams,
                                    &healthy,
                                );
                            }
                            Err(proto_err) => {
                                healthy.store(false, Ordering::Release);
                                let err_out = conn.take_outbound();
                                if !err_out.is_empty() {
                                    write_buf.extend_from_slice(&err_out);
                                    let _ = wr.write_all(&write_buf).await;
                                }
                                fail_all_client_streams(
                                    &mut streams,
                                    Http2Error::Protocol(proto_err),
                                );
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
                handle_client_cmd(first, &mut conn, &mut streams, &cmd_tx);
                for _ in 0..64 {
                    match cmd_rx.try_recv() {
                        Ok(cmd) => handle_client_cmd(cmd, &mut conn, &mut streams, &cmd_tx),
                        Err(_) => break,
                    }
                }
            }
        }
    }

    healthy.store(false, Ordering::Release);
    if !write_buf.is_empty() {
        let _ = wr.write_all(&write_buf).await;
        let _ = wr.flush().await;
    }
    fail_all_client_streams(&mut streams, Http2Error::ConnectionClosed);
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

fn handle_client_cmd(
    cmd: ClientCmd,
    conn: &mut H2Connection,
    streams: &mut HashMap<u32, ActiveClientStream>,
    cmd_tx: &mpsc::UnboundedSender<ClientCmd>,
) {
    match cmd {
        ClientCmd::SendRequest {
            method,
            uri,
            headers,
            end_stream,
            sid_tx,
            queued_send_bytes,
            send_notify,
            stream_max_body,
        } => {
            let res = conn.send_request(&method, &uri, &headers, end_stream);
            match res {
                Ok(stream_id) => {
                    let (head_tx, head_rx) = oneshot::channel();
                    let (body_tx, body_rx) = mpsc::unbounded_channel();
                    streams.insert(
                        stream_id,
                        ActiveClientStream {
                            head_tx: Some(head_tx),
                            body_rx: Some(body_rx),
                            body_tx,
                            cmd_tx: cmd_tx.clone(),
                            queued_send_bytes,
                            send_notify,
                            max_body_size: stream_max_body,
                        },
                    );
                    let _ = sid_tx.send(Ok((stream_id, head_rx)));
                }
                Err(pe) => {
                    let err = if pe.code == H2ErrorCode::REFUSED_STREAM {
                        if let Some(last) = conn.peer_goaway_last_stream_id() {
                            Http2Error::GoAway {
                                last_stream_id: last,
                                code: H2ErrorCode::NO_ERROR,
                            }
                        } else {
                            Http2Error::StreamReset(H2ErrorCode::REFUSED_STREAM)
                        }
                    } else {
                        Http2Error::Protocol(pe)
                    };
                    let _ = sid_tx.send(Err(err));
                }
            }
        }
        ClientCmd::SendData {
            stream_id,
            data,
            end_stream,
        } => {
            let len = data.len();
            let _ = conn.send_data(stream_id, data, end_stream);
            if let Some(st) = streams.get(&stream_id) {
                st.queued_send_bytes.fetch_sub(len, Ordering::AcqRel);
                st.send_notify.notify_one();
            }
        }
        ClientCmd::SendTrailers {
            stream_id,
            trailers,
        } => {
            let _ = conn.send_trailers(stream_id, &trailers);
        }
        ClientCmd::ReleaseRecvCapacity { stream_id, bytes } => {
            conn.release_recv_capacity(stream_id, bytes);
        }
        ClientCmd::ResetStream { stream_id, code } => {
            conn.send_reset(stream_id, code);
            if let Some(st) = streams.remove(&stream_id) {
                st.send_notify.notify_waiters();
            }
        }
    }
}

fn dispatch_client_events(
    events: Vec<H2Event>,
    streams: &mut HashMap<u32, ActiveClientStream>,
    healthy: &Arc<AtomicBool>,
) {
    for ev in events {
        match ev {
            H2Event::ResponseHeaders {
                stream_id,
                decoded,
                end_stream,
            } => {
                if let Some(st) = streams.get_mut(&stream_id) {
                    let status = decoded.status.unwrap_or(StatusCode::OK);
                    if status.is_informational() && !end_stream {
                        continue;
                    }
                    if let (Some(head_tx), Some(body_rx)) = (st.head_tx.take(), st.body_rx.take()) {
                        let resp = Http2ClientResponse {
                            status,
                            headers: decoded.headers,
                            body: Http2ClientRecvStream {
                                stream_id,
                                body_rx,
                                cmd_tx: st.cmd_tx.clone(),
                                trailers: None,
                                eof: end_stream,
                                max_body_size: st.max_body_size,
                                bytes_received: 0,
                            },
                        };
                        let _ = head_tx.send(Ok(resp));
                    }
                    if end_stream {
                        streams.remove(&stream_id);
                    }
                }
            }
            H2Event::Data {
                stream_id,
                payload,
                end_stream,
            } => {
                if let Some(st) = streams.get(&stream_id) {
                    if !payload.is_empty() {
                        let _ = st.body_tx.send(Ok(BodyEvent::Data(payload)));
                    }
                    if end_stream {
                        streams.remove(&stream_id);
                    }
                }
            }
            H2Event::Trailers {
                stream_id,
                trailers,
            } => {
                if let Some(st) = streams.remove(&stream_id) {
                    let _ = st.body_tx.send(Ok(BodyEvent::Trailers(trailers)));
                }
            }
            H2Event::StreamReset {
                stream_id,
                error_code,
            } => {
                if let Some(mut st) = streams.remove(&stream_id) {
                    st.send_notify.notify_waiters();
                    if let Some(head_tx) = st.head_tx.take() {
                        let _ = head_tx.send(Err(Http2Error::StreamReset(error_code)));
                    } else {
                        let _ = st.body_tx.send(Err(Http2Error::StreamReset(error_code)));
                    }
                }
            }
            H2Event::GoAway {
                last_stream_id,
                error_code,
                ..
            } => {
                healthy.store(false, Ordering::Release);
                let refused: Vec<u32> = streams
                    .keys()
                    .copied()
                    .filter(|&sid| sid > last_stream_id)
                    .collect();
                for sid in refused {
                    if let Some(mut st) = streams.remove(&sid) {
                        let err = Http2Error::GoAway {
                            last_stream_id,
                            code: error_code,
                        };
                        st.send_notify.notify_waiters();
                        if let Some(head_tx) = st.head_tx.take() {
                            let _ = head_tx.send(Err(err));
                        } else {
                            let _ = st.body_tx.send(Err(err));
                        }
                    }
                }
            }
            H2Event::WindowUpdated { .. } | H2Event::RequestHeaders { .. } => {}
        }
    }
}

fn fail_all_client_streams(streams: &mut HashMap<u32, ActiveClientStream>, err: Http2Error) {
    let msg = err.to_string();
    for (_sid, mut st) in streams.drain() {
        let cloned = match &err {
            Http2Error::GoAway {
                last_stream_id,
                code,
            } => Http2Error::GoAway {
                last_stream_id: *last_stream_id,
                code: *code,
            },
            Http2Error::StreamReset(c) => Http2Error::StreamReset(*c),
            Http2Error::Protocol(p) => Http2Error::Protocol(p.clone()),
            _ => Http2Error::Parse(msg.clone()),
        };
        st.send_notify.notify_waiters();
        if let Some(head_tx) = st.head_tx.take() {
            let _ = head_tx.send(Err(cloned));
        } else {
            let _ = st.body_tx.send(Err(cloned));
        }
    }
}
