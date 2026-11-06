//! Tokio `AsyncRead` and `AsyncWrite` stream adapter for `RdmaChannel`.
//!
//! Enables any Velda Edge L7 protocol pipeline (HTTP/1.1, HTTP/2, HTTP/3, gRPC)
//! to run on top of RDMA kernel-bypass channels without changing protocol logic.

use bytes::{Buf, Bytes, BytesMut};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::channel::RdmaChannel;

enum ReadState {
    Idle,
    Pending(Pin<Box<dyn Future<Output = Result<Bytes, io::Error>> + Send>>),
}

enum WriteState {
    Idle,
    Pending(Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send>>),
}

/// Stream adapter that implements `AsyncRead` + `AsyncWrite` on an underlying `RdmaChannel`.
pub struct RdmaStream {
    channel: RdmaChannel,
    read_buf: Bytes,
    write_buf: BytesMut,
    read_state: ReadState,
    write_state: WriteState,
    timeout: Duration,
}

impl RdmaStream {
    /// Creates a new `RdmaStream` wrapping the given RDMA channel.
    pub fn new(channel: RdmaChannel) -> Self {
        Self {
            channel,
            read_buf: Bytes::new(),
            write_buf: BytesMut::with_capacity(65536),
            read_state: ReadState::Idle,
            write_state: WriteState::Idle,
            timeout: Duration::from_secs(30),
        }
    }

    /// Access to the underlying channel.
    pub fn channel(&self) -> &RdmaChannel {
        &self.channel
    }
}

impl AsyncRead for RdmaStream {
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
                    let channel = self.channel.clone();
                    let timeout = self.timeout;
                    let fut = Box::pin(async move {
                        channel
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

impl AsyncWrite for RdmaStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
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

                    let data = self.write_buf.split().freeze();
                    let channel = self.channel.clone();
                    let fut = Box::pin(async move {
                        channel
                            .send(&data)
                            .await
                            .map_err(|e| io::Error::other(e.to_string()))
                    });
                    self.write_state = WriteState::Pending(fut);
                }
                WriteState::Pending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        self.write_state = WriteState::Idle;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        self.write_state = WriteState::Idle;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
