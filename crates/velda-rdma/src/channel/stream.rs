//! # Tokio `AsyncRead` and `AsyncWrite` Stream Adapter for RDMA Channels
//!
//! ## Architectural Invariants & Stream Adaptation
//!
//! RDMA hardware natively exchanges discrete, message-oriented buffers bounded by Memory Region (MR)
//! slot allocations (typically 64 KB per slot). Conversely, higher-level streaming protocol engines
//! (HTTP/1.1 chunked bodies, HTTP/2 multiplexed streams, gRPC length-prefixed protocol buffers, and
//! bidirectional TCP byte streams) expect continuous, stream-oriented I/O interfaces characterized by
//! `tokio::io::AsyncRead` and `tokio::io::AsyncWrite`.
//!
//! `RdmaStream` bridges this gap, providing a seamless abstraction that adapts discrete RDMA message
//! transfers into a standard asynchronous stream.
//!
//! ### Decoupled Halves (`into_split` / `RdmaReadHalf` & `RdmaWriteHalf`)
//! Following the Hourglass Architecture & Flat Workflow (`AGENTS.md`), the inbound and outbound flows
//! are cleanly split into two independent halves:
//! - **`RdmaReadHalf` (`AsyncRead`)**: Exclusively polls the underlying `RdmaReceiver`, drains incoming
//!   chunks zero-copy, and replenishes RQ hardware slots.
//! - **`RdmaWriteHalf` (`AsyncWrite`)**: Exclusively stages outbound frames into contiguous chunks,
//!   dispatches Send Work Requests via `RdmaSender`, and coordinates credit backpressure.
//! - Both halves can be split via `into_split()` and moved into separate Tokio tasks without shared locks.

use bytes::{Buf, Bytes, BytesMut};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::channel::{RdmaChannel, RdmaReceiver, RdmaSender};

/// Maximum discrete payload chunk size dispatched per RDMA Send Work Request (64 KB).
pub const DEFAULT_RDMA_STREAM_MAX_CHUNK: usize = 65536;

enum ReadState {
    Idle,
    Pending(Pin<Box<dyn Future<Output = Result<Bytes, io::Error>> + Send>>),
}

enum WriteState {
    Idle,
    Pending(Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send>>),
    ShutdownPending(Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send>>),
    Closed,
}

/// The readable half of an `RdmaStream`.
pub struct RdmaReadHalf {
    pub receiver: RdmaReceiver,
    read_buf: Bytes,
    read_state: ReadState,
    timeout: Duration,
}

impl RdmaReadHalf {
    /// Constructs a new `RdmaReadHalf` from an `RdmaReceiver`.
    pub fn new(receiver: RdmaReceiver) -> Self {
        Self {
            receiver,
            read_buf: Bytes::new(),
            read_state: ReadState::Idle,
            timeout: Duration::from_secs(30),
        }
    }

    /// Sets the receive timeout for inbound RDMA completions.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }
}

impl AsyncRead for RdmaReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // 1. If we have buffered bytes from a previous receive, copy them directly
        if !self.read_buf.is_empty() {
            let to_copy = std::cmp::min(self.read_buf.len(), buf.remaining());
            buf.put_slice(&self.read_buf[..to_copy]);
            self.read_buf.advance(to_copy);
            return Poll::Ready(Ok(()));
        }

        // 2. Drive the async receive future
        loop {
            match &mut self.read_state {
                ReadState::Idle => {
                    let receiver = self.receiver.clone();
                    let timeout = self.timeout;
                    let fut = Box::pin(async move {
                        receiver
                            .recv(timeout)
                            .await
                            .map_err(|e| io::Error::other(e.to_string()))
                    });
                    self.read_state = ReadState::Pending(fut);
                }
                ReadState::Pending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(bytes)) => {
                        self.read_state = ReadState::Idle;
                        if bytes.is_empty() {
                            return Poll::Ready(Ok(())); // EOF
                        }
                        let to_copy = std::cmp::min(bytes.len(), buf.remaining());
                        buf.put_slice(&bytes[..to_copy]);
                        if to_copy < bytes.len() {
                            self.read_buf = bytes.slice(to_copy..);
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        self.read_state = ReadState::Idle;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

/// The writable half of an `RdmaStream`.
pub struct RdmaWriteHalf {
    pub sender: RdmaSender,
    write_buf: BytesMut,
    write_state: WriteState,
}

impl RdmaWriteHalf {
    /// Constructs a new `RdmaWriteHalf` from an `RdmaSender`.
    pub fn new(sender: RdmaSender) -> Self {
        Self {
            sender,
            write_buf: BytesMut::with_capacity(65536),
            write_state: WriteState::Idle,
        }
    }
}

impl AsyncWrite for RdmaWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if matches!(
            self.write_state,
            WriteState::ShutdownPending(_) | WriteState::Closed
        ) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "RDMA stream is shut down",
            )));
        }

        // If there's an ongoing flush, wait for it to complete first
        if let WriteState::Pending(fut) = &mut self.write_state {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    self.write_state = WriteState::Idle;
                }
                Poll::Ready(Err(e)) => {
                    self.write_state = WriteState::Idle;
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Proactively kick off flush if write buffer has accumulated a full RDMA chunk and we are idle
        if self.write_buf.len() >= DEFAULT_RDMA_STREAM_MAX_CHUNK
            && matches!(self.write_state, WriteState::Idle)
        {
            let _ = self.as_mut().poll_flush(cx);
        }

        // Backpressure when staged write buffer exceeds high water mark (128 KB)
        if self.write_buf.len() >= DEFAULT_RDMA_STREAM_MAX_CHUNK * 2 {
            let _ = self.as_mut().poll_flush(cx);
            if self.write_buf.len() >= DEFAULT_RDMA_STREAM_MAX_CHUNK * 2 {
                return Poll::Pending;
            }
        }

        self.write_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.write_state {
                WriteState::Idle => {
                    if self.write_buf.is_empty() {
                        return Poll::Ready(Ok(()));
                    }

                    // Chunk outbound payloads so discrete WRs never exceed hardware slot capacity
                    let chunk_len =
                        std::cmp::min(self.write_buf.len(), DEFAULT_RDMA_STREAM_MAX_CHUNK);
                    let data = self.write_buf.split_to(chunk_len).freeze();
                    let sender = self.sender.clone();
                    let fut = Box::pin(async move {
                        sender
                            .send(&data)
                            .await
                            .map_err(|e| io::Error::other(e.to_string()))
                    });
                    self.write_state = WriteState::Pending(fut);
                }
                WriteState::Pending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        self.write_state = WriteState::Idle;
                    }
                    Poll::Ready(Err(e)) => {
                        self.write_state = WriteState::Idle;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                WriteState::ShutdownPending(_) | WriteState::Closed => {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if matches!(self.write_state, WriteState::Closed) {
            return Poll::Ready(Ok(()));
        }

        // 1. Flush any pending buffered writes first
        if !self.write_buf.is_empty() || matches!(self.write_state, WriteState::Pending(_)) {
            match self.as_mut().poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        // 2. Drive explicit EOF shutdown frame to peer (Fixes Scenario 5)
        loop {
            match &mut self.write_state {
                WriteState::Idle => {
                    let sender = self.sender.clone();
                    let fut = Box::pin(async move {
                        sender
                            .shutdown()
                            .await
                            .map_err(|e| io::Error::other(e.to_string()))
                    });
                    self.write_state = WriteState::ShutdownPending(fut);
                }
                WriteState::ShutdownPending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        self.write_state = WriteState::Closed;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        self.write_state = WriteState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                WriteState::Pending(_) => unreachable!(),
                WriteState::Closed => return Poll::Ready(Ok(())),
            }
        }
    }
}

/// Stream adapter that implements `AsyncRead` + `AsyncWrite` on an underlying `RdmaChannel`.
pub struct RdmaStream {
    pub read_half: RdmaReadHalf,
    pub write_half: RdmaWriteHalf,
}

impl RdmaStream {
    /// Creates a new `RdmaStream` wrapping the given RDMA channel.
    pub fn new(channel: RdmaChannel) -> Self {
        let (sender, receiver) = channel.into_split();
        Self {
            read_half: RdmaReadHalf::new(receiver),
            write_half: RdmaWriteHalf::new(sender),
        }
    }

    /// Constructs an `RdmaStream` from decoupled read and write halves.
    pub fn from_halves(read_half: RdmaReadHalf, write_half: RdmaWriteHalf) -> Self {
        Self {
            read_half,
            write_half,
        }
    }

    /// Splits the stream into borrowed read and write halves.
    pub fn split(&mut self) -> (&mut RdmaReadHalf, &mut RdmaWriteHalf) {
        (&mut self.read_half, &mut self.write_half)
    }

    /// Consumes the stream, returning independent, owned read and write halves.
    pub fn into_split(self) -> (RdmaReadHalf, RdmaWriteHalf) {
        (self.read_half, self.write_half)
    }

    /// Reconstructs the underlying `RdmaChannel`.
    pub fn channel(&self) -> RdmaChannel {
        RdmaChannel::new(
            self.write_half.sender.clone(),
            self.read_half.receiver.clone(),
        )
    }
}

impl AsyncRead for RdmaStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.read_half).poll_read(cx, buf)
    }
}

impl AsyncWrite for RdmaStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.write_half).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write_half).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write_half).poll_shutdown(cx)
    }
}
