//! High-performance UDP socket wrapper with atomic byte counting and socket options.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UdpSocket as TokioUdpSocket;

use super::config::UdpSocketConfig;
use super::datagram::Datagram;
use crate::error::{Result, TransportError};

/// High-performance UDP socket managing datagram transmission and receipt.
///
/// Tracks total bytes received and sent concurrently via atomic counters,
/// allowing multiple tasks to share an [`std::sync::Arc<UdpSocket>`] without lock contention.
#[derive(Debug)]
pub struct UdpSocket {
    socket: TokioUdpSocket,
    local_addr: SocketAddr,
    config: UdpSocketConfig,
    bytes_received: AtomicU64,
    bytes_sent: AtomicU64,
}

impl UdpSocket {
    /// Binds a new UDP socket to the given address with the specified configuration.
    pub fn bind(addr: SocketAddr, config: UdpSocketConfig) -> Result<Self> {
        let domain = if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };

        let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
            .map_err(TransportError::Io)?;

        // Allow immediate address reuse for quick daemon restarts
        sock.set_reuse_address(true).map_err(TransportError::Io)?;

        if let Some(recv_buf) = config.recv_buffer_size {
            let _ = sock.set_recv_buffer_size(recv_buf);
        }
        if let Some(send_buf) = config.send_buffer_size {
            let _ = sock.set_send_buffer_size(send_buf);
        }

        sock.set_nonblocking(true).map_err(TransportError::Io)?;

        sock.bind(&addr.into())
            .map_err(|e| TransportError::Bind { addr, source: e })?;

        let std_socket: std::net::UdpSocket = sock.into();
        let socket = TokioUdpSocket::from_std(std_socket)
            .map_err(|e| TransportError::Bind { addr, source: e })?;

        let local_addr = socket
            .local_addr()
            .map_err(|e| TransportError::Bind { addr, source: e })?;

        Ok(Self {
            socket,
            local_addr,
            config,
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        })
    }

    /// Wraps an existing Tokio [`TokioUdpSocket`] with the given configuration.
    pub fn from_tokio(socket: TokioUdpSocket, config: UdpSocketConfig) -> Result<Self> {
        let local_addr = socket.local_addr().map_err(TransportError::Io)?;
        Ok(Self {
            socket,
            local_addr,
            config,
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        })
    }

    /// Returns the local address this socket is bound to.
    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the configuration applied to this socket.
    #[inline]
    pub const fn config(&self) -> &UdpSocketConfig {
        &self.config
    }

    /// Returns the total bytes received by this socket.
    #[inline]
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    /// Returns the total bytes sent by this socket.
    #[inline]
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    /// Receives a datagram into the provided buffer, tracking the received byte count.
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let (n, peer) = self
            .socket
            .recv_from(buf)
            .await
            .map_err(TransportError::Io)?;
        self.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
        Ok((n, peer))
    }

    /// Receives an entire datagram dynamically allocated up to `max_size`.
    pub async fn recv_datagram(&self, max_size: usize) -> Result<Datagram> {
        let mut buf = vec![0u8; max_size];
        let (n, peer) = self.recv_from(&mut buf).await?;
        buf.truncate(n);
        Ok(Datagram::new(peer, self.local_addr, buf))
    }

    /// Sends a datagram buffer to the specified remote address, tracking the sent byte count.
    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        let n = self
            .socket
            .send_to(buf, target)
            .await
            .map_err(TransportError::Io)?;
        self.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    /// Sends a [`Datagram`] to its destination peer, tracking the sent byte count.
    pub async fn send_datagram(&self, datagram: &Datagram) -> Result<usize> {
        self.send_to(datagram.data(), datagram.peer()).await
    }

    /// Returns a reference to the underlying Tokio [`TokioUdpSocket`].
    #[inline]
    pub fn inner(&self) -> &TokioUdpSocket {
        &self.socket
    }

    /// Consumes this wrapper and returns the raw Tokio [`TokioUdpSocket`].
    pub fn into_inner(self) -> TokioUdpSocket {
        self.socket
    }

    /// Spawns a UDP receive loop on the provided `JoinSet`, dispatching to L4 or L7 handoff based on path kind.
    pub fn spawn_receive_loop<UdpL4H, UdpL7H, FutL4, FutL7>(
        socket: std::sync::Arc<Self>,
        binding: crate::ingress::listener::IngressBinding,
        tasks: &mut tokio::task::JoinSet<()>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        udp_l4_fn: UdpL4H,
        udp_l7_fn: UdpL7H,
    ) where
        UdpL4H: Fn(String, std::sync::Arc<Self>, Datagram) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        UdpL7H: Fn(crate::forwarding::l7::UdpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        tasks.spawn(async move {
            let mut shutdown = shutdown;
            let local_addr = socket.local_addr();
            tracing::info!(
                listener_id = %binding.id,
                listen_addr = %local_addr,
                path = %binding.path,
                "UDP ingress loop running"
            );

            let mut buf = [0u8; 65535];

            loop {
                if *shutdown.borrow() {
                    break;
                }

                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            tracing::info!(
                                listener_id = %binding.id,
                                listen_addr = %local_addr,
                                "UDP ingress loop shutting down"
                            );
                            break;
                        }
                    }
                    res = socket.recv_from(&mut buf) => {
                        match res {
                            Ok((n, peer)) => {
                                let dgram = Datagram::new(peer, local_addr, buf[..n].to_vec());
                                match binding.path {
                                    crate::ingress::classifier::PathKind::Http3
                                    | crate::ingress::classifier::PathKind::Quic => {
                                        let handoff = crate::forwarding::l7::UdpL7Handoff::new(
                                            dgram,
                                            std::sync::Arc::clone(&socket),
                                            binding.path,
                                            binding.id.clone(),
                                            binding.tls_enabled,
                                        );
                                        tokio::spawn(udp_l7_fn(handoff));
                                    }
                                    _ => {
                                        let sock_clone = std::sync::Arc::clone(&socket);
                                        let id_clone = binding.id.clone();
                                        tokio::spawn(udp_l4_fn(id_clone, sock_clone, dgram));
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    listener_id = %binding.id,
                                    listen_addr = %local_addr,
                                    error = %err,
                                    "UDP recv error"
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}
