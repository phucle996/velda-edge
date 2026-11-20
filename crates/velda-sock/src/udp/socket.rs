//! High-performance UDP socket wrapper with atomic byte counting and socket options.

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UdpSocket as TokioUdpSocket;
use velda_core::lut::ip::is_v4_u128;

use super::config::UdpSocketConfig;
use super::datagram::Datagram;
use crate::connect::{
    format_raw_addr, raw_to_sockaddr_storage_u128, raw_to_socket_addr, sockaddr_storage_to_raw,
    socket_addr_to_raw,
};
use crate::error::{Result, SockError};

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
    pub local_ip: u128,
    pub local_port_be: u16,
    config: UdpSocketConfig,
    pub worker_id: usize,
    bytes_received: CacheAlignedAtomicU64,
    bytes_sent: CacheAlignedAtomicU64,
}

impl UdpSocket {
    /// Binds a new UDP socket to `(ip, port_be)` with the specified configuration.
    pub fn bind(ip: u128, port_be: u16, config: UdpSocketConfig) -> Result<Self> {
        let domain = if is_v4_u128(ip) {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };

        let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
            .map_err(SockError::Io)?;

        // Allow immediate address reuse for quick daemon restarts
        sock.set_reuse_address(true).map_err(SockError::Io)?;

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
            let (level, optname) = if is_v4_u128(ip) {
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

        sock.set_nonblocking(true).map_err(SockError::Io)?;

        let addr = raw_to_socket_addr(ip, port_be);
        sock.bind(&addr.into()).map_err(|e| SockError::Bind {
            ip,
            port_be,
            source: e,
        })?;

        let std_socket: std::net::UdpSocket = sock.into();
        let socket = TokioUdpSocket::from_std(std_socket).map_err(|e| SockError::Bind {
            ip,
            port_be,
            source: e,
        })?;

        let local_addr = socket.local_addr().map_err(|e| SockError::Bind {
            ip,
            port_be,
            source: e,
        })?;
        let (local_ip, local_port_be) = socket_addr_to_raw(local_addr);

        Ok(Self {
            socket,
            local_ip,
            local_port_be,
            config,
            worker_id: 0,
            bytes_received: CacheAlignedAtomicU64::new(0),
            bytes_sent: CacheAlignedAtomicU64::new(0),
        })
    }

    /// Binds a single UDP socket shard affined to a specific worker/CPU core.
    pub fn bind_shard(
        ip: u128,
        port_be: u16,
        config: UdpSocketConfig,
        worker_id: usize,
    ) -> Result<Self> {
        let mut socket = Self::bind(ip, port_be, config.clone())?;
        socket.worker_id = worker_id;
        // See `TcpListener::bind_shard`: hardware RSS authority is applied only when proven;
        // otherwise the kernel software hash owns flow distribution.
        let is_loopback = ip == velda_core::lut::ip::v4_to_u128(0x7F00_0001) || ip == 1;
        if !is_loopback && let Some(core) = velda_core::hardware::ingress_steering_cpu(worker_id) {
            socket.apply_incoming_cpu(core);
        }
        Ok(socket)
    }

    /// Binds multiple UDP socket shards to `(ip, port_be)` with `SO_REUSEPORT`.
    ///
    /// If `config.concurrency_shards > 1` and `config.reuseport` is enabled, binds up to
    /// `config.concurrency_shards` sockets to the same port.
    /// The first socket determines the exact assigned address (critical when `port_be == 0`).
    pub fn bind_shards(ip: u128, port_be: u16, config: UdpSocketConfig) -> Result<Vec<Self>> {
        let first = Self::bind_shard(ip, port_be, config.clone(), 0)?;
        let actual_ip = first.local_ip;
        let actual_port_be = first.local_port_be;
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
            match Self::bind_shard(actual_ip, actual_port_be, config.clone(), shard_idx) {
                Ok(shard) => shards.push(shard),
                Err(err) => {
                    tracing::warn!(
                        shard = shard_idx,
                        listen_addr = %format_raw_addr(actual_ip, actual_port_be),
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
        let local_addr = socket.local_addr().map_err(SockError::Io)?;
        let (local_ip, local_port_be) = socket_addr_to_raw(local_addr);
        Ok(Self {
            socket,
            local_ip,
            local_port_be,
            config,
            worker_id: 0,
            bytes_received: CacheAlignedAtomicU64::new(0),
            bytes_sent: CacheAlignedAtomicU64::new(0),
        })
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
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, u128, u16)> {
        let (n, peer) = self.socket.recv_from(buf).await.map_err(SockError::Io)?;
        self.bytes_received.0.fetch_add(n as u64, Ordering::Relaxed);
        let (peer_ip, peer_port_be) = socket_addr_to_raw(peer);
        Ok((n, peer_ip, peer_port_be))
    }

    /// Attempts non-blocking datagram receipt via `try_recv_from`.
    #[inline]
    pub fn try_recv_from(&self, buf: &mut [u8]) -> Option<Result<(usize, u128, u16)>> {
        match self.socket.try_recv_from(buf) {
            Ok((n, peer)) => {
                self.bytes_received.0.fetch_add(n as u64, Ordering::Relaxed);
                let (peer_ip, peer_port_be) = socket_addr_to_raw(peer);
                Some(Ok((n, peer_ip, peer_port_be)))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(e) => Some(Err(SockError::Io(e))),
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
        self.socket.readable().await.map_err(SockError::Io)
    }

    /// Receives an entire datagram dynamically allocated up to `max_size`.
    pub async fn recv_datagram(&self, max_size: usize) -> Result<Datagram> {
        let mut buf = vec![0u8; max_size];
        let (n, peer_ip, peer_port_be) = self.recv_from(&mut buf).await?;
        buf.truncate(n);
        Ok(Datagram::new(
            peer_ip,
            peer_port_be,
            self.local_ip,
            self.local_port_be,
            buf,
        ))
    }

    /// Attempts non-blocking datagram transmission via `try_send_to`.
    #[inline]
    pub fn try_send_to(
        &self,
        buf: &[u8],
        target_ip: u128,
        target_port_be: u16,
    ) -> Option<Result<usize>> {
        let target = raw_to_socket_addr(target_ip, target_port_be);
        match self.socket.try_send_to(buf, target) {
            Ok(n) => {
                self.bytes_sent.0.fetch_add(n as u64, Ordering::Relaxed);
                Some(Ok(n))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(e) => Some(Err(SockError::Io(e))),
        }
    }

    /// Sends a datagram buffer to `(target_ip, target_port_be)`, tracking the sent byte count.
    pub async fn send_to(&self, buf: &[u8], target_ip: u128, target_port_be: u16) -> Result<usize> {
        if let Some(res) = self.try_send_to(buf, target_ip, target_port_be) {
            return res;
        }
        let target = raw_to_socket_addr(target_ip, target_port_be);
        let n = self
            .socket
            .send_to(buf, target)
            .await
            .map_err(SockError::Io)?;
        self.bytes_sent.0.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    /// Attempts non-blocking GSO datagram transmission via Linux kernel `UDP_SEGMENT` cmsg.
    #[inline]
    pub fn try_send_gso_to(
        &self,
        buf: &[u8],
        segment_size: u16,
        target_ip: u128,
        target_port_be: u16,
    ) -> Option<Result<usize>> {
        #[cfg(target_os = "linux")]
        {
            if segment_size == 0 {
                return self.try_send_to(buf, target_ip, target_port_be);
            }

            use std::os::fd::AsRawFd;
            let fd = self.socket.as_raw_fd();
            let (mut storage, addr_len) = raw_to_sockaddr_storage_u128(target_ip, target_port_be);

            let mut iov = libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            };

            let mut cmsg_buf = [0u8; 64];
            let mut msg = unsafe { std::mem::zeroed::<libc::msghdr>() };
            msg.msg_name = &mut storage as *mut _ as *mut libc::c_void;
            msg.msg_namelen = addr_len;
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
                    Some(Err(SockError::Io(err)))
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = segment_size;
            self.try_send_to(buf, target_ip, target_port_be)
        }
    }

    /// Sends a GSO datagram batch to the destination peer with automatic async backpressure.
    pub async fn send_gso_to(
        &self,
        buf: &[u8],
        segment_size: u16,
        target_ip: u128,
        target_port_be: u16,
    ) -> Result<usize> {
        if let Some(res) = self.try_send_gso_to(buf, segment_size, target_ip, target_port_be) {
            return res;
        }
        loop {
            self.socket.writable().await.map_err(SockError::Io)?;
            if let Some(res) = self.try_send_gso_to(buf, segment_size, target_ip, target_port_be) {
                return res;
            }
        }
    }

    /// Sends a [`Datagram`] to its destination peer, tracking the sent byte count.
    pub async fn send_datagram(&self, datagram: &Datagram) -> Result<usize> {
        self.send_to(datagram.data(), datagram.peer_ip, datagram.peer_port_be)
            .await
    }

    /// Sends a batch of outgoing UDP datagrams using `sendmmsg` (Linux) for minimal syscall overhead.
    ///
    /// Each entry is `(payload, target_ip, target_port_be)`.
    #[cfg(target_os = "linux")]
    pub fn try_send_batch(&self, packets: &[(&[u8], u128, u16)]) -> usize {
        if packets.is_empty() {
            return 0;
        }
        if packets.len() == 1 {
            return match self.try_send_to(packets[0].0, packets[0].1, packets[0].2) {
                Some(Ok(_)) => 1,
                _ => 0,
            };
        }

        use std::os::fd::AsRawFd;
        let fd = self.socket.as_raw_fd();
        let batch_len = packets.len().min(64); // kernel caps at UIO_MAXIOV

        let mut storages: [libc::sockaddr_storage; 64] = unsafe { std::mem::zeroed() };
        let mut iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }; 64];
        let mut msgs = unsafe { std::mem::zeroed::<[libc::mmsghdr; 64]>() };

        for i in 0..batch_len {
            let (storage, addr_len) = raw_to_sockaddr_storage_u128(packets[i].1, packets[i].2);
            storages[i] = storage;
            iovecs[i].iov_base = packets[i].0.as_ptr() as *mut libc::c_void;
            iovecs[i].iov_len = packets[i].0.len();

            msgs[i].msg_hdr.msg_name = &mut storages[i] as *mut _ as *mut libc::c_void;
            msgs[i].msg_hdr.msg_namelen = addr_len;
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
    pub fn try_send_batch(&self, packets: &[(&[u8], u128, u16)]) -> usize {
        let mut sent = 0;
        for &(buf, ip, port_be) in packets {
            if let Some(Ok(_)) = self.try_send_to(buf, ip, port_be) {
                sent += 1;
            } else {
                break;
            }
        }
        sent
    }

    /// Receives a batch of incoming UDP datagrams using `recvmmsg` (Linux) for minimal syscall overhead.
    #[cfg(target_os = "linux")]
    pub fn try_recv_batch(
        &self,
        buffers: &mut [[u8; 2048]],
        out: &mut [(usize, u128, u16)],
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
                let (peer_ip, peer_port_be) = sockaddr_storage_to_raw(&sock_storage[i]);

                if (msgs[i].msg_hdr.msg_flags & libc::MSG_TRUNC) != 0 {
                    tracing::trace!("Dropping truncated UDP datagram exceeding slab slot");
                    out[i] = (0, peer_ip, peer_port_be);
                    continue;
                }

                let n = msgs[i].msg_len as usize;
                total_bytes += n as u64;
                out[i] = (n, peer_ip, peer_port_be);
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
        out: &mut [(usize, u128, u16)],
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
    #[cfg(target_os = "linux")]
    pub fn try_recv_batch_slab(
        &self,
        slab: &mut [u8],
        packet_len: usize,
        out: &mut [(usize, u128, u16)],
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
                let (peer_ip, peer_port_be) = sockaddr_storage_to_raw(&sock_storage[i]);

                if (msgs[i].msg_hdr.msg_flags & libc::MSG_TRUNC) != 0 {
                    tracing::trace!("Dropping truncated UDP datagram exceeding slab slot");
                    out[i] = (0, peer_ip, peer_port_be);
                    continue;
                }

                let n = msgs[i].msg_len as usize;
                total_bytes += n as u64;
                out[i] = (n, peer_ip, peer_port_be);
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
        out: &mut [(usize, u128, u16)],
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
    #[inline]
    pub fn try_send_gso_aware(
        &self,
        buf: &[u8],
        segment_size: Option<u16>,
        target_ip: u128,
        target_port_be: u16,
    ) -> Option<Result<usize>> {
        match segment_size {
            Some(seg) if seg > 0 && buf.len() > seg as usize => {
                if self.config.gso {
                    match self.try_send_gso_to(buf, seg, target_ip, target_port_be) {
                        Some(Err(SockError::Io(ref e)))
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
                    let mut batch: [(&[u8], u128, u16); 64] =
                        [(&[], target_ip, target_port_be); 64];
                    let mut count = 0;
                    while count < 64 && offset < buf.len() {
                        let end = (offset + chunk_size).min(buf.len());
                        batch[count] = (&buf[offset..end], target_ip, target_port_be);
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
            _ => self.try_send_to(buf, target_ip, target_port_be),
        }
    }

    /// Async version of [`try_send_gso_aware`] with backpressure.
    pub async fn send_gso_aware(
        &self,
        buf: &[u8],
        segment_size: Option<u16>,
        target_ip: u128,
        target_port_be: u16,
    ) -> Result<usize> {
        match segment_size {
            Some(seg) if seg > 0 && buf.len() > seg as usize => {
                if self.config.gso {
                    match self.try_send_gso_to(buf, seg, target_ip, target_port_be) {
                        Some(Err(SockError::Io(ref e)))
                            if e.raw_os_error() == Some(libc::EMSGSIZE) =>
                        {
                            // Fall through to batch chunk send
                        }
                        Some(Ok(n)) => return Ok(n),
                        Some(Err(e)) => return Err(e),
                        None => {
                            loop {
                                self.socket.writable().await.map_err(SockError::Io)?;
                                match self.try_send_gso_to(buf, seg, target_ip, target_port_be) {
                                    Some(Err(SockError::Io(ref e)))
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
                    let mut batch: [(&[u8], u128, u16); 64] =
                        [(&[], target_ip, target_port_be); 64];
                    let mut count = 0;
                    while count < 64 && offset < buf.len() {
                        let end = (offset + chunk_size).min(buf.len());
                        batch[count] = (&buf[offset..end], target_ip, target_port_be);
                        offset = end;
                        count += 1;
                    }
                    let mut batch_sent = 0;
                    while batch_sent < count {
                        let n = self.try_send_batch(&batch[batch_sent..count]);
                        if n > 0 {
                            batch_sent += n;
                        } else {
                            self.socket.writable().await.map_err(SockError::Io)?;
                        }
                    }
                }
                Ok(buf.len())
            }
            _ => self.send_to(buf, target_ip, target_port_be).await,
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
