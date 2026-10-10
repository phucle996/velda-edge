//! L4 TCP connection lifecycle and byte tracking.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use velda_core::{ConnectionContext, ConnectionId, L4Request, TransportProtocol};

use crate::error::{Result, TransportError};

/// An active L4 TCP connection.
///
/// Owns the underlying [`TcpStream`], its unique [`ConnectionId`],
/// socket endpoints, and tracks transferred byte counts.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`] so higher layers (such
/// as TLS or HTTP) can operate directly over it while byte counters
/// update automatically.
#[derive(Debug)]
pub struct Connection {
    pub id: ConnectionId,
    pub stream: TcpStream,
    pub peer: SocketAddr,
    pub local_addr: SocketAddr,
    pub bytes_read: u64,
    pub bytes_written: u64,
    /// Listener identifier shared across all connections of a listener (refcount clone, no alloc).
    pub listener_id: Option<Arc<str>>,
}

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
const ID_BATCH_SIZE: u64 = 512;

thread_local! {
    static LOCAL_ID_RANGE: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

/// Generates a globally monotonically increasing [`ConnectionId`].
///
/// Uses thread-local batching (512 IDs per batch) to eliminate atomic cache-line
/// contention (`MESI` invalidation ping-pong) across concurrent worker threads.
#[inline]
pub fn next_connection_id() -> ConnectionId {
    LOCAL_ID_RANGE.with(|range| {
        let (curr, max) = range.get();
        if curr < max {
            range.set((curr + 1, max));
            ConnectionId::new(curr)
        } else {
            let base = NEXT_CONNECTION_ID.fetch_add(ID_BATCH_SIZE, Ordering::Relaxed);
            range.set((base + 1, base + ID_BATCH_SIZE));
            ConnectionId::new(base)
        }
    })
}

impl Connection {
    /// Creates a new connection with explicit identifiers and addresses.
    #[inline]
    pub fn new(
        id: ConnectionId,
        stream: TcpStream,
        peer: SocketAddr,
        local_addr: SocketAddr,
    ) -> Self {
        Self {
            id,
            stream,
            peer,
            local_addr,
            bytes_read: 0,
            bytes_written: 0,
            listener_id: None,
        }
    }

    /// Creates a new connection from a raw [`TcpStream`], inspecting its peer
    /// and local socket addresses, and assigning a fresh [`ConnectionId`].
    pub fn from_stream(stream: TcpStream) -> Result<Self> {
        let peer = stream.peer_addr().map_err(TransportError::Io)?;
        let local_addr = stream.local_addr().map_err(TransportError::Io)?;
        Ok(Self::new(next_connection_id(), stream, peer, local_addr))
    }

    /// Returns the unique connection identifier.
    #[inline]
    pub const fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the client peer address.
    #[inline]
    pub const fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Returns the local address on which the connection was accepted.
    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Attaches an ingress listener identifier to this connection.
    #[inline]
    pub fn with_listener_id(mut self, id: impl Into<Arc<str>>) -> Self {
        self.listener_id = Some(id.into());
        self
    }

    /// Returns the ingress listener identifier that accepted this connection, if known.
    #[inline]
    pub fn listener_id(&self) -> Option<&str> {
        self.listener_id.as_deref()
    }

    /// Returns the total number of bytes read from this connection.
    #[inline]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Returns the total number of bytes written to this connection.
    #[inline]
    pub const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Increments the transferred byte counters after zero-copy or direct forwarding.
    #[inline]
    pub fn add_bytes_transferred(&mut self, read: u64, written: u64) {
        self.bytes_read += read;
        self.bytes_written += written;
    }

    /// Returns a reference to the underlying [`TcpStream`].
    #[inline]
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Returns a mutable reference to the underlying [`TcpStream`].
    #[inline]
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// Sets the `TCP_NODELAY` option on the underlying socket.
    #[inline]
    pub fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        self.stream.set_nodelay(nodelay).map_err(TransportError::Io)
    }

    /// Converts this connection into a [`L4Request`] metadata descriptor.
    pub fn to_l4_request(&self) -> L4Request {
        L4Request::new(self.id, TransportProtocol::Tcp, self.peer, self.local_addr)
    }

    /// Converts this connection into a [`ConnectionContext`].
    pub fn to_connection_context(&self) -> ConnectionContext {
        ConnectionContext::new(self.to_l4_request())
    }

    /// Reads incoming bytes into the provided buffer, tracking the read count.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = self.stream.read(buf).await.map_err(TransportError::Io)?;
        self.bytes_read += n as u64;
        Ok(n)
    }

    /// Writes outgoing bytes from the provided buffer, tracking the written count.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = self.stream.write(buf).await.map_err(TransportError::Io)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    /// Writes all outgoing bytes from the provided buffer.
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.stream
            .write_all(buf)
            .await
            .map_err(TransportError::Io)?;
        self.bytes_written += buf.len() as u64;
        Ok(())
    }

    /// Flushes any pending buffered writes to the socket.
    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await.map_err(TransportError::Io)
    }

    /// Shuts down the output stream of the socket.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown().await.map_err(TransportError::Io)
    }

    /// Consumes the connection, returning its constituent components
    /// for zero-copy handover to higher layers (e.g. TLS, HTTP).
    pub fn into_inner(self) -> (ConnectionId, TcpStream, SocketAddr, SocketAddr) {
        (self.id, self.stream, self.peer, self.local_addr)
    }

    /// Consumes the connection, returning the raw [`TcpStream`].
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }

    /// Splits the connection into independent read and write halves.
    pub fn split(self) -> (ConnectionReader, ConnectionWriter) {
        let (read_half, write_half) = self.stream.into_split();
        let reader = ConnectionReader {
            id: self.id,
            read_half,
            bytes_read: self.bytes_read,
        };
        let writer = ConnectionWriter {
            id: self.id,
            write_half,
            bytes_written: self.bytes_written,
        };
        (reader, writer)
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.stream).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let n = buf.filled().len() - before;
                this.bytes_read += n as u64;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.stream).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.bytes_written += n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_shutdown(cx)
    }
}

/// Owned read half of a split [`Connection`].
#[derive(Debug)]
pub struct ConnectionReader {
    id: ConnectionId,
    read_half: OwnedReadHalf,
    bytes_read: u64,
}

impl ConnectionReader {
    /// Returns the connection identifier.
    #[inline]
    pub const fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns total bytes read so far by this half.
    #[inline]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Returns a reference to the underlying [`OwnedReadHalf`].
    #[inline]
    pub fn read_half(&self) -> &OwnedReadHalf {
        &self.read_half
    }

    /// Consumes this reader and returns the underlying [`OwnedReadHalf`].
    pub fn into_inner(self) -> OwnedReadHalf {
        self.read_half
    }
}

impl AsyncRead for ConnectionReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.read_half).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let n = buf.filled().len() - before;
                this.bytes_read += n as u64;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Owned write half of a split [`Connection`].
#[derive(Debug)]
pub struct ConnectionWriter {
    id: ConnectionId,
    write_half: OwnedWriteHalf,
    bytes_written: u64,
}

impl ConnectionWriter {
    /// Returns the connection identifier.
    #[inline]
    pub const fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns total bytes written so far by this half.
    #[inline]
    pub const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Returns a reference to the underlying [`OwnedWriteHalf`].
    #[inline]
    pub fn write_half(&self) -> &OwnedWriteHalf {
        &self.write_half
    }

    /// Consumes this writer and returns the underlying [`OwnedWriteHalf`].
    pub fn into_inner(self) -> OwnedWriteHalf {
        self.write_half
    }
}

impl AsyncWrite for ConnectionWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.write_half).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.bytes_written += n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.write_half).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.write_half).poll_shutdown(cx)
    }
}
