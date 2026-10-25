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
    pub worker_id: usize,
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
                tracing::trace!(
                    errno = std::io::Error::last_os_error().raw_os_error(),
                    "SO_REUSEPORT socket option skipped or unsupported on UDP socket"
                );
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

        // Note: UDP_GRO requires recvmsg with UDP_GRO cmsg segment size demuxing.
        // Standard recvfrom returns coalesced buffers which corrupts QUIC packet framing.

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
            // Kernel GSO capability acknowledged. Datagrams are segmented dynamically per-packet
            // via sendmsg control messages (cmsg: UDP_SEGMENT) in `try_send_gso_to` / `send_gso_to`,
            // avoiding socket-level global segment enforcement that conflicts with discrete packets.
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
                let busy_poll_us: libc::c_int = 50;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &busy_poll_us as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&busy_poll_us) as libc::socklen_t,
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
            worker_id: 0,
            bytes_received: CacheAlignedAtomicU64::new(0),
            bytes_sent: CacheAlignedAtomicU64::new(0),
        })
    }

    /// Binds a single UDP socket shard affined to a specific worker/CPU core.
    pub fn bind_shard(addr: SocketAddr, config: UdpSocketConfig, worker_id: usize) -> Result<Self> {
        let mut socket = Self::bind(addr, config.clone())?;
        socket.worker_id = worker_id;
        #[cfg(target_os = "linux")]
        {
            let available_cpus = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            socket.apply_incoming_cpu(worker_id % available_cpus);
        }
        Ok(socket)
    }

    /// Binds multiple UDP socket shards to the specified address with `SO_REUSEPORT`.
    ///
    /// If `config.concurrency_shards > 1` and `config.reuseport` is enabled, binds up to
    /// `config.concurrency_shards` sockets to the same port.
    /// The first socket determines the exact assigned address (critical when `addr.port() == 0`).
    pub fn bind_shards(addr: SocketAddr, config: UdpSocketConfig) -> Result<Vec<Self>> {
        let first = Self::bind_shard(addr, config.clone(), 0)?;
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
            match Self::bind_shard(actual_addr, config.clone(), shard_idx) {
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

    /// Affines the UDP socket queue to a specific CPU core (`SO_INCOMING_CPU`) on Linux.
    ///
    /// When combined with `SO_REUSEPORT`, Linux kernel steers incoming UDP datagrams
    /// processed on that CPU's NAPI/softirq handler directly to this socket shard,
    /// eliminating cross-core cache invalidation and IPC thrashing.
    pub fn apply_incoming_cpu(&self, cpu: usize) {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let fd = self.socket.as_raw_fd();
            let val: libc::c_int = cpu as libc::c_int;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_INCOMING_CPU,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        cpu,
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "SO_INCOMING_CPU socket option skipped or unsupported on UDP socket"
                    );
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = cpu;
        }
    }

    /// Wraps an existing Tokio [`TokioUdpSocket`] with the given configuration.
    pub fn from_tokio(socket: TokioUdpSocket, config: UdpSocketConfig) -> Result<Self> {
        let local_addr = socket.local_addr().map_err(TransportError::Io)?;
        Ok(Self {
            socket,
            local_addr,
            config,
            worker_id: 0,
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

    /// Attempts non-blocking datagram receipt via `try_recv_from`.
    ///
    /// Returns:
    /// - `Some(Ok((bytes, peer)))` on immediate non-blocking kernel delivery (zero epoll overhead).
    /// - `Some(Err(e))` on permanent I/O error.
    /// - `None` if the socket ingress buffer is empty (`WouldBlock`), requiring asynchronous waiting.
    #[inline]
    pub fn try_recv_from(&self, buf: &mut [u8]) -> Option<Result<(usize, SocketAddr)>> {
        match self.socket.try_recv_from(buf) {
            Ok((n, peer)) => {
                self.bytes_received.0.fetch_add(n as u64, Ordering::Relaxed);
                Some(Ok((n, peer)))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(e) => Some(Err(TransportError::Io(e))),
        }
    }

    /// Waits asynchronously until the underlying socket has pending datagrams ready in kernel buffers.
    ///
    /// # Performance Rationale
    /// Paired with `try_recv_batch_slab`, this allows the ingress event loop to wait on epoll readiness
    /// and immediately drain entire batches of packets via `recvmmsg` without issuing an initial 1-packet
    /// `recv_from` syscall on every event loop turn.
    #[inline]
    pub async fn readable(&self) -> Result<()> {
        self.socket.readable().await.map_err(TransportError::Io)
    }

    /// Receives an entire datagram dynamically allocated up to `max_size`.
    pub async fn recv_datagram(&self, max_size: usize) -> Result<Datagram> {
        let mut buf = vec![0u8; max_size];
        let (n, peer) = self.recv_from(&mut buf).await?;
        buf.truncate(n);
        Ok(Datagram::new(peer, self.local_addr, buf))
    }

    /// Attempts non-blocking datagram transmission via `try_send_to`.
    ///
    /// Returns:
    /// - `Some(Ok(bytes))` on immediate non-blocking kernel delivery (zero epoll overhead).
    /// - `Some(Err(e))` on permanent I/O error.
    /// - `None` if the socket egress buffer is full (`WouldBlock`), requiring asynchronous waiting.
    #[inline]
    pub fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> Option<Result<usize>> {
        match self.socket.try_send_to(buf, target) {
            Ok(n) => {
                self.bytes_sent.0.fetch_add(n as u64, Ordering::Relaxed);
                Some(Ok(n))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(e) => Some(Err(TransportError::Io(e))),
        }
    }

    /// Sends a datagram buffer to the specified remote address, tracking the sent byte count.
    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        if let Some(res) = self.try_send_to(buf, target) {
            return res;
        }
        let n = self
            .socket
            .send_to(buf, target)
            .await
            .map_err(TransportError::Io)?;
        self.bytes_sent.0.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    /// Attempts non-blocking GSO datagram transmission via Linux kernel `UDP_SEGMENT` cmsg.
    ///
    /// If `segment_size == 0` or unsupported on host OS, degrades gracefully to standard datagram send.
    #[inline]
    pub fn try_send_gso_to(
        &self,
        buf: &[u8],
        segment_size: u16,
        target: SocketAddr,
    ) -> Option<Result<usize>> {
        #[cfg(target_os = "linux")]
        {
            if segment_size == 0 {
                return self.try_send_to(buf, target);
            }

            use std::os::fd::AsRawFd;
            let fd = self.socket.as_raw_fd();
            let sock_addr = socket2::SockAddr::from(target);

            let mut iov = libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            };

            let mut cmsg_buf = [0u8; 64];
            let mut msg = unsafe { std::mem::zeroed::<libc::msghdr>() };
            msg.msg_name = sock_addr.as_ptr() as *mut libc::c_void;
            msg.msg_namelen = sock_addr.len();
            msg.msg_iov = &mut iov;
            msg.msg_iovlen = 1;
            msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
            msg.msg_controllen =
                unsafe { libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) as _ };

            const UDP_SEGMENT: libc::c_int = 103;
            let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
            if !cmsg.is_null() {
                unsafe {
                    (*cmsg).cmsg_level = libc::IPPROTO_UDP;
                    (*cmsg).cmsg_type = UDP_SEGMENT;
                    (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
                    let data_ptr = libc::CMSG_DATA(cmsg) as *mut u16;
                    std::ptr::write_unaligned(data_ptr, segment_size);
                }
            }

            let ret = unsafe { libc::sendmsg(fd, &msg, libc::MSG_DONTWAIT) };
            if ret >= 0 {
                self.bytes_sent.0.fetch_add(ret as u64, Ordering::Relaxed);
                Some(Ok(ret as usize))
            } else {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    None
                } else {
                    Some(Err(TransportError::Io(err)))
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = segment_size;
            self.try_send_to(buf, target)
        }
    }

    /// Sends a GSO datagram batch to the destination peer with automatic async backpressure.
    pub async fn send_gso_to(
        &self,
        buf: &[u8],
        segment_size: u16,
        target: SocketAddr,
    ) -> Result<usize> {
        if let Some(res) = self.try_send_gso_to(buf, segment_size, target) {
            return res;
        }
        loop {
            self.socket.writable().await.map_err(TransportError::Io)?;
            if let Some(res) = self.try_send_gso_to(buf, segment_size, target) {
                return res;
            }
        }
    }

    /// Sends a [`Datagram`] to its destination peer, tracking the sent byte count.
    pub async fn send_datagram(&self, datagram: &Datagram) -> Result<usize> {
        self.send_to(datagram.data(), datagram.peer()).await
    }

    /// Sends a batch of outgoing UDP datagrams using `sendmmsg` (Linux) for minimal syscall overhead.
    ///
    /// Each entry is `(payload, target)`. Falls back to per-packet `try_send_to` on non-Linux
    /// or if `sendmmsg` is unavailable. Stack-allocated buffers eliminate heap allocation.
    #[cfg(target_os = "linux")]
    pub fn try_send_batch(&self, packets: &[(&[u8], SocketAddr)]) -> usize {
        if packets.is_empty() {
            return 0;
        }
        if packets.len() == 1 {
            return match self.try_send_to(packets[0].0, packets[0].1) {
                Some(Ok(_)) => 1,
                _ => 0,
            };
        }

        use std::os::fd::AsRawFd;
        let fd = self.socket.as_raw_fd();
        let batch_len = packets.len().min(64); // kernel caps at UIO_MAXIOV

        let mut sock_addrs: [std::mem::MaybeUninit<socket2::SockAddr>; 64] =
            [const { std::mem::MaybeUninit::uninit() }; 64];
        let mut iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }; 64];
        let mut msgs = unsafe { std::mem::zeroed::<[libc::mmsghdr; 64]>() };

        for i in 0..batch_len {
            sock_addrs[i].write(socket2::SockAddr::from(packets[i].1));
            iovecs[i].iov_base = packets[i].0.as_ptr() as *mut libc::c_void;
            iovecs[i].iov_len = packets[i].0.len();

            let sa = unsafe { sock_addrs[i].assume_init_ref() };
            msgs[i].msg_hdr.msg_name = sa.as_ptr() as *mut libc::c_void;
            msgs[i].msg_hdr.msg_namelen = sa.len();
            msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
            msgs[i].msg_hdr.msg_iovlen = 1;
        }

        let ret =
            unsafe { libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_len as u32, libc::MSG_DONTWAIT) };
        if ret > 0 {
            let sent = ret as usize;
            let total_bytes: u64 = msgs[..sent].iter().map(|m| m.msg_len as u64).sum();
            self.bytes_sent.0.fetch_add(total_bytes, Ordering::Relaxed);
            sent
        } else {
            0
        }
    }

    /// Fallback for non-Linux platforms sending sequentially.
    #[cfg(not(target_os = "linux"))]
    pub fn try_send_batch(&self, packets: &[(&[u8], SocketAddr)]) -> usize {
        let mut sent = 0;
        for (buf, target) in packets {
            if let Some(Ok(_)) = self.try_send_to(buf, *target) {
                sent += 1;
            } else {
                break;
            }
        }
        sent
    }

    /// Receives a batch of incoming UDP datagrams using `recvmmsg` (Linux) for minimal syscall overhead.
    ///
    /// Reads up to `max_batch` packets (capped at 32) into the provided buffers in a single system call.
    /// Stack-allocated structures ensure zero heap allocations on the ingress hot path.
    #[cfg(target_os = "linux")]
    pub fn try_recv_batch(
        &self,
        buffers: &mut [[u8; 2048]],
        out: &mut [(usize, SocketAddr)],
    ) -> usize {
        let max_batch = buffers.len().min(out.len()).min(32);
        if max_batch == 0 {
            return 0;
        }

        use std::os::fd::AsRawFd;
        let fd = self.socket.as_raw_fd();

        let mut sock_storage: [libc::sockaddr_storage; 32] = unsafe { std::mem::zeroed() };
        let mut iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }; 32];
        let mut msgs = unsafe { std::mem::zeroed::<[libc::mmsghdr; 32]>() };

        for i in 0..max_batch {
            iovecs[i].iov_base = buffers[i].as_mut_ptr() as *mut libc::c_void;
            iovecs[i].iov_len = 2048;

            msgs[i].msg_hdr.msg_name = &mut sock_storage[i] as *mut _ as *mut libc::c_void;
            msgs[i].msg_hdr.msg_namelen =
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
            msgs[i].msg_hdr.msg_iovlen = 1;
        }

        let ret = self.socket.try_io(tokio::io::Interest::READABLE, || {
            let res = unsafe {
                libc::recvmmsg(
                    fd,
                    msgs.as_mut_ptr(),
                    max_batch as u32,
                    libc::MSG_DONTWAIT,
                    std::ptr::null_mut(),
                )
            };
            if res >= 0 {
                Ok(res)
            } else {
                Err(std::io::Error::last_os_error())
            }
        });

        if let Ok(res_count) = ret
            && res_count > 0
        {
            let count = res_count as usize;
            let mut total_bytes = 0u64;
            for i in 0..count {
                let sa = &sock_storage[i];
                let addr = match sa.ss_family as libc::c_int {
                    libc::AF_INET => {
                        let sin = unsafe { &*(sa as *const _ as *const libc::sockaddr_in) };
                        let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                        let port = u16::from_be(sin.sin_port);
                        SocketAddr::V4(std::net::SocketAddrV4::new(ip, port))
                    }
                    libc::AF_INET6 => {
                        let sin6 = unsafe { &*(sa as *const _ as *const libc::sockaddr_in6) };
                        let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
                        let port = u16::from_be(sin6.sin6_port);
                        SocketAddr::V6(std::net::SocketAddrV6::new(
                            ip,
                            port,
                            sin6.sin6_flowinfo,
                            sin6.sin6_scope_id,
                        ))
                    }
                    _ => SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0),
                };

                if (msgs[i].msg_hdr.msg_flags & libc::MSG_TRUNC) != 0 {
                    tracing::trace!("Dropping truncated UDP datagram exceeding slab slot");
                    out[i] = (0, addr);
                    continue;
                }

                let n = msgs[i].msg_len as usize;
                total_bytes += n as u64;
                out[i] = (n, addr);
            }
            self.bytes_received
                .0
                .fetch_add(total_bytes, Ordering::Relaxed);
            count
        } else {
            0
        }
    }

    /// Fallback for non-Linux platforms receiving sequentially.
    #[cfg(not(target_os = "linux"))]
    pub fn try_recv_batch(
        &self,
        buffers: &mut [[u8; 2048]],
        out: &mut [(usize, SocketAddr)],
    ) -> usize {
        let max_batch = buffers.len().min(out.len());
        let mut count = 0;
        for i in 0..max_batch {
            match self.try_recv_from(&mut buffers[i]) {
                Some(Ok(res)) => {
                    out[i] = res;
                    count += 1;
                }
                _ => break,
            }
        }
        count
    }

    /// Receives a batch of incoming UDP datagrams directly into a contiguous slab buffer via `recvmmsg` (Linux).
    ///
    /// Reads up to `max_batch` packets (capped at 32) in a single non-blocking system call,
    /// placing each packet into an aligned `packet_len` slot within the contiguous `slab`.
    ///
    /// # Performance & Hardware Rationale
    /// 1. **Syscall Reduction**: Traditional `recv_from` issues 1 system call per datagram. Under heavy load
    ///    (e.g. 1M requests, 200 concurrent clients), this causes thousands of context switches. `recvmmsg`
    ///    drains up to 32 datagrams in a single context switch.
    /// 2. **Zero-Copy & Zero-Alloc Ingress**: Writing directly into a single contiguous slab allows the caller
    ///    to freeze the slab and extract datagrams using `Bytes::slice(offset..offset + len)`. This reduces
    ///    heap allocations by up to 97% (1 buffer instead of 32 separate allocations) and eliminates all
    ///    userspace `memcpy` operations on the ingress packet path.
    /// 3. **Cache Line Locality**: Placing incoming packet buffers in contiguous memory maximizes L1/L2 data
    ///    cache prefetching efficiency on modern vector-capable CPUs.
    #[cfg(target_os = "linux")]
    pub fn try_recv_batch_slab(
        &self,
        slab: &mut [u8],
        packet_len: usize,
        out: &mut [(usize, SocketAddr)],
    ) -> usize {
        let max_batch = (slab.len() / packet_len).min(out.len()).min(32);
        if max_batch == 0 {
            return 0;
        }

        use std::os::fd::AsRawFd;
        let fd = self.socket.as_raw_fd();

        let mut sock_storage: [libc::sockaddr_storage; 32] = unsafe { std::mem::zeroed() };
        let mut iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }; 32];
        let mut msgs = unsafe { std::mem::zeroed::<[libc::mmsghdr; 32]>() };

        for i in 0..max_batch {
            iovecs[i].iov_base =
                unsafe { slab.as_mut_ptr().add(i * packet_len) as *mut libc::c_void };
            iovecs[i].iov_len = packet_len;

            msgs[i].msg_hdr.msg_name = &mut sock_storage[i] as *mut _ as *mut libc::c_void;
            msgs[i].msg_hdr.msg_namelen =
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
            msgs[i].msg_hdr.msg_iovlen = 1;
        }

        let ret = self.socket.try_io(tokio::io::Interest::READABLE, || {
            let res = unsafe {
                libc::recvmmsg(
                    fd,
                    msgs.as_mut_ptr(),
                    max_batch as u32,
                    libc::MSG_DONTWAIT,
                    std::ptr::null_mut(),
                )
            };
            if res >= 0 {
                Ok(res)
            } else {
                Err(std::io::Error::last_os_error())
            }
        });

        if let Ok(res_count) = ret
            && res_count > 0
        {
            let count = res_count as usize;
            let mut total_bytes = 0u64;
            for i in 0..count {
                let sa = &sock_storage[i];
                let addr = match sa.ss_family as libc::c_int {
                    libc::AF_INET => {
                        let sin = unsafe { &*(sa as *const _ as *const libc::sockaddr_in) };
                        let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                        let port = u16::from_be(sin.sin_port);
                        SocketAddr::V4(std::net::SocketAddrV4::new(ip, port))
                    }
                    libc::AF_INET6 => {
                        let sin6 = unsafe { &*(sa as *const _ as *const libc::sockaddr_in6) };
                        let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
                        let port = u16::from_be(sin6.sin6_port);
                        SocketAddr::V6(std::net::SocketAddrV6::new(
                            ip,
                            port,
                            sin6.sin6_flowinfo,
                            sin6.sin6_scope_id,
                        ))
                    }
                    _ => SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0),
                };

                if (msgs[i].msg_hdr.msg_flags & libc::MSG_TRUNC) != 0 {
                    tracing::trace!("Dropping truncated UDP datagram exceeding slab slot");
                    out[i] = (0, addr);
                    continue;
                }

                let n = msgs[i].msg_len as usize;
                total_bytes += n as u64;
                out[i] = (n, addr);
            }
            self.bytes_received
                .0
                .fetch_add(total_bytes, Ordering::Relaxed);
            count
        } else {
            0
        }
    }

    /// Sequential slab receipt fallback for non-Linux platforms.
    #[cfg(not(target_os = "linux"))]
    pub fn try_recv_batch_slab(
        &self,
        slab: &mut [u8],
        packet_len: usize,
        out: &mut [(usize, SocketAddr)],
    ) -> usize {
        let max_batch = (slab.len() / packet_len).min(out.len());
        let mut count = 0;
        for i in 0..max_batch {
            let start = i * packet_len;
            let end = start + packet_len;
            match self.try_recv_from(&mut slab[start..end]) {
                Some(Ok(res)) => {
                    out[i] = res;
                    count += 1;
                }
                _ => break,
            }
        }
        count
    }

    /// Sends a GSO-segmented datagram with proper `UDP_SEGMENT` cmsg (Linux).
    ///
    /// When `segment_size` is `Some(size)`, the kernel segments the large buffer into
    /// individual UDP datagrams of `size` bytes each in a single `sendmsg` syscall.
    /// If the kernel NIC does not support GSO (returns `EMSGSIZE`), this automatically
    /// falls back to `sendmmsg` batching to ensure zero packet drops and maximum throughput.
    #[inline]
    pub fn try_send_gso_aware(
        &self,
        buf: &[u8],
        segment_size: Option<u16>,
        target: SocketAddr,
    ) -> Option<Result<usize>> {
        match segment_size {
            Some(seg) if seg > 0 && buf.len() > seg as usize => {
                if self.config.gso {
                    match self.try_send_gso_to(buf, seg, target) {
                        Some(Err(TransportError::Io(ref e)))
                            if e.raw_os_error() == Some(libc::EMSGSIZE) =>
                        {
                            // Kernel NIC does not support GSO. Fall through to batch send chunks!
                        }
                        other => return other,
                    }
                }

                // Batch send chunks of size `seg` with zero heap allocation
                let chunk_size = seg as usize;
                let mut offset = 0;
                while offset < buf.len() {
                    let mut batch: [(&[u8], SocketAddr); 64] = [(&[], target); 64];
                    let mut count = 0;
                    while count < 64 && offset < buf.len() {
                        let end = (offset + chunk_size).min(buf.len());
                        batch[count] = (&buf[offset..end], target);
                        offset = end;
                        count += 1;
                    }
                    let mut batch_sent = 0;
                    while batch_sent < count {
                        let n = self.try_send_batch(&batch[batch_sent..count]);
                        if n > 0 {
                            batch_sent += n;
                        } else {
                            return None;
                        }
                    }
                }
                Some(Ok(buf.len()))
            }
            _ => self.try_send_to(buf, target),
        }
    }

    /// Async version of [`try_send_gso_aware`] with backpressure.
    pub async fn send_gso_aware(
        &self,
        buf: &[u8],
        segment_size: Option<u16>,
        target: SocketAddr,
    ) -> Result<usize> {
        match segment_size {
            Some(seg) if seg > 0 && buf.len() > seg as usize => {
                if self.config.gso {
                    match self.try_send_gso_to(buf, seg, target) {
                        Some(Err(TransportError::Io(ref e)))
                            if e.raw_os_error() == Some(libc::EMSGSIZE) =>
                        {
                            // Fall through to batch chunk send
                        }
                        Some(Ok(n)) => return Ok(n),
                        Some(Err(e)) => return Err(e),
                        None => {
                            loop {
                                self.socket.writable().await.map_err(TransportError::Io)?;
                                match self.try_send_gso_to(buf, seg, target) {
                                    Some(Err(TransportError::Io(ref e)))
                                        if e.raw_os_error() == Some(libc::EMSGSIZE) =>
                                    {
                                        break; // Fall through to batch send
                                    }
                                    Some(res) => return res,
                                    None => continue,
                                }
                            }
                        }
                    }
                }

                // Batch send chunks of size `seg` with zero heap allocation & async backpressure
                let chunk_size = seg as usize;
                let mut offset = 0;
                while offset < buf.len() {
                    let mut batch: [(&[u8], SocketAddr); 64] = [(&[], target); 64];
                    let mut count = 0;
                    while count < 64 && offset < buf.len() {
                        let end = (offset + chunk_size).min(buf.len());
                        batch[count] = (&buf[offset..end], target);
                        offset = end;
                        count += 1;
                    }
                    let mut batch_sent = 0;
                    while batch_sent < count {
                        let n = self.try_send_batch(&batch[batch_sent..count]);
                        if n > 0 {
                            batch_sent += n;
                        } else {
                            self.socket.writable().await.map_err(TransportError::Io)?;
                        }
                    }
                }
                Ok(buf.len())
            }
            _ => self.send_to(buf, target).await,
        }
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
