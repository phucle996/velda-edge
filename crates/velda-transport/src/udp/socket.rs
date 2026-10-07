//! High-performance UDP socket wrapper with atomic byte counting and socket options.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UdpSocket as TokioUdpSocket;

use super::config::UdpSocketConfig;
use super::datagram::Datagram;
use crate::error::{Result, TransportError};

/// Cache-line aligned 64-bit atomic counter to prevent false sharing under high concurrency.
#[repr(align(64))]
#[derive(Debug)]
struct CacheAlignedAtomicU64(AtomicU64);

impl CacheAlignedAtomicU64 {
    const fn new(val: u64) -> Self {
        Self(AtomicU64::new(val))
    }
}

/// High-performance UDP socket managing datagram transmission and receipt.
///
/// Tracks total bytes received and sent concurrently via cache-aligned atomic counters,
/// eliminating false sharing when read and write workers operate on different cores.
#[derive(Debug)]
pub struct UdpSocket {
    socket: TokioUdpSocket,
    local_addr: SocketAddr,
    config: UdpSocketConfig,
    bytes_received: CacheAlignedAtomicU64,
    bytes_sent: CacheAlignedAtomicU64,
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

        #[cfg(unix)]
        if config.reuseport {
            use std::os::fd::AsRawFd;
            let fd = sock.as_raw_fd();
            let val: libc::c_int = 1;
            let ret = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_REUSEPORT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                )
            };
            if ret != 0 {
                return Err(TransportError::Io(std::io::Error::last_os_error()));
            }
        }

        #[cfg(target_os = "linux")]
        if config.freebind {
            use std::os::fd::AsRawFd;
            let fd = sock.as_raw_fd();
            let val: libc::c_int = 1;
            let (level, optname) = if addr.is_ipv4() {
                (libc::IPPROTO_IP, libc::IP_FREEBIND)
            } else {
                (libc::IPPROTO_IPV6, libc::IPV6_FREEBIND)
            };
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    level,
                    optname,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "IP_FREEBIND socket option skipped on UDP socket"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if config.gro {
            use std::os::fd::AsRawFd;
            let fd = sock.as_raw_fd();
            let val: libc::c_int = 1;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_UDP,
                    libc::UDP_GRO,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "UDP_GRO socket option skipped or unsupported by kernel"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if config.rxq_ovfl {
            use std::os::fd::AsRawFd;
            let fd = sock.as_raw_fd();
            let val: libc::c_int = 1;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RXQ_OVFL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "SO_RXQ_OVFL socket option skipped or unsupported by kernel"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if config.gso {
            use std::os::fd::AsRawFd;
            let fd = sock.as_raw_fd();
            const UDP_SEGMENT: libc::c_int = 103;
            let val: libc::c_int = 1472;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_UDP,
                    UDP_SEGMENT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "UDP_SEGMENT (GSO) socket option skipped or unsupported by kernel"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if config.prefer_busy_poll {
            use std::os::fd::AsRawFd;
            let fd = sock.as_raw_fd();
            const SO_PREFER_BUSY_POLL: libc::c_int = 69;
            const SO_BUSY_POLL_BUDGET: libc::c_int = 70;
            let val: libc::c_int = 1;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    SO_PREFER_BUSY_POLL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "SO_PREFER_BUSY_POLL skipped on UDP socket"
                    );
                }
                let budget: libc::c_int = 8;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    SO_BUSY_POLL_BUDGET,
                    &budget as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&budget) as libc::socklen_t,
                );
            }
        }

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
            bytes_received: CacheAlignedAtomicU64::new(0),
            bytes_sent: CacheAlignedAtomicU64::new(0),
        })
    }

    /// Binds multiple UDP socket shards to the specified address with `SO_REUSEPORT`.
    ///
    /// If `config.concurrency_shards > 1` and `config.reuseport` is enabled, binds up to
    /// `config.concurrency_shards` sockets to the same port.
    /// The first socket determines the exact assigned address (critical when `addr.port() == 0`).
    pub fn bind_shards(addr: SocketAddr, config: UdpSocketConfig) -> Result<Vec<Self>> {
        let first = Self::bind(addr, config.clone())?;
        let actual_addr = first.local_addr();
        let target_shards = if config.reuseport {
            config.concurrency_shards.max(1)
        } else {
            1
        };

        if target_shards <= 1 {
            return Ok(vec![first]);
        }

        let mut shards = Vec::with_capacity(target_shards);
        shards.push(first);

        for shard_idx in 1..target_shards {
            match Self::bind(actual_addr, config.clone()) {
                Ok(shard) => shards.push(shard),
                Err(err) => {
                    tracing::warn!(
                        shard = shard_idx,
                        listen_addr = %actual_addr,
                        error = %err,
                        "Failed to bind SO_REUSEPORT UDP socket shard; continuing with existing shards"
                    );
                    break;
                }
            }
        }

        Ok(shards)
    }

    /// Wraps an existing Tokio [`TokioUdpSocket`] with the given configuration.
    pub fn from_tokio(socket: TokioUdpSocket, config: UdpSocketConfig) -> Result<Self> {
        let local_addr = socket.local_addr().map_err(TransportError::Io)?;
        Ok(Self {
            socket,
            local_addr,
            config,
            bytes_received: CacheAlignedAtomicU64::new(0),
            bytes_sent: CacheAlignedAtomicU64::new(0),
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
        self.bytes_received.0.load(Ordering::Relaxed)
    }

    /// Returns the total bytes sent by this socket.
    #[inline]
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.0.load(Ordering::Relaxed)
    }

    /// Receives a datagram into the provided buffer, tracking the received byte count.
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let (n, peer) = self
            .socket
            .recv_from(buf)
            .await
            .map_err(TransportError::Io)?;
        self.bytes_received.0.fetch_add(n as u64, Ordering::Relaxed);
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
        self.bytes_sent.0.fetch_add(n as u64, Ordering::Relaxed);
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
}
