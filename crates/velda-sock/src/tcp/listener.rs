//! High-performance TCP listener implementation.

use tokio::net::{TcpListener as TokioTcpListener, TcpSocket};
use tokio::sync::watch;
use velda_core::lut::ip::is_v4_u128;

use super::config::TcpListenerConfig;
use crate::connect::{
    format_raw_addr, raw_to_socket_addr, sockaddr_storage_to_raw, socket_addr_to_raw,
};
use crate::connection::Connection;
use crate::error::{Result, SockError};

/// High-performance TCP listener managing incoming client connections.
#[derive(Debug)]
pub struct TcpListener {
    listener: TokioTcpListener,
    pub local_ip: u128,
    pub local_port_be: u16,
    config: TcpListenerConfig,
}

impl TcpListener {
    /// Binds a new TCP listener to `(ip, port_be)` with the given configuration.
    pub fn bind(ip: u128, port_be: u16, config: TcpListenerConfig) -> Result<Self> {
        let is_v4 = is_v4_u128(ip);
        let socket = if is_v4 {
            TcpSocket::new_v4().map_err(SockError::Io)?
        } else {
            TcpSocket::new_v6().map_err(SockError::Io)?
        };

        // Enable SO_REUSEADDR for rapid port recycling on restarts
        socket.set_reuseaddr(true).map_err(SockError::Io)?;

        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        if config.reuseport
            && let Err(err) = socket.set_reuseport(true)
        {
            tracing::trace!(
                error = %err,
                "SO_REUSEPORT socket option skipped or unsupported on TCP listener"
            );
        }

        #[cfg(target_os = "linux")]
        if config.freebind {
            use std::os::fd::AsRawFd;
            let fd = socket.as_raw_fd();
            let val: libc::c_int = 1;
            let (level, optname) = if is_v4 {
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
                        "IP_FREEBIND socket option skipped or unsupported"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(secs) = config.defer_accept_secs {
            use std::os::fd::AsRawFd;
            let fd = socket.as_raw_fd();
            let val: libc::c_int = secs as libc::c_int;
            unsafe {
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_DEFER_ACCEPT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(backlog) = config.fastopen_backlog {
            use std::os::fd::AsRawFd;
            let fd = socket.as_raw_fd();
            let val: libc::c_int = backlog as libc::c_int;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_FASTOPEN,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "TCP_FASTOPEN server backlog skipped"
                    );
                }
            }
        }

        if let Some(recv_buf) = config.recv_buffer_size {
            let _ = socket.set_recv_buffer_size(recv_buf as u32);
        }
        if let Some(send_buf) = config.send_buffer_size {
            let _ = socket.set_send_buffer_size(send_buf as u32);
        }

        let addr = raw_to_socket_addr(ip, port_be);
        socket.bind(addr).map_err(|e| SockError::Bind {
            ip,
            port_be,
            source: e,
        })?;

        let listener = socket.listen(config.backlog).map_err(|e| SockError::Bind {
            ip,
            port_be,
            source: e,
        })?;

        let local_addr = listener.local_addr().map_err(|e| SockError::Bind {
            ip,
            port_be,
            source: e,
        })?;
        let (local_ip, local_port_be) = socket_addr_to_raw(local_addr);

        Ok(Self {
            listener,
            local_ip,
            local_port_be,
            config,
        })
    }

    /// Affines the listener socket queue to a specific CPU core (`SO_INCOMING_CPU`) on Linux.
    ///
    /// If the kernel does not support `SO_INCOMING_CPU` or running under a restrictive
    /// container seccomp filter, fails silently with a trace log and preserves normal socket operation.
    pub fn apply_incoming_cpu(&self, cpu: usize) {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let fd = self.listener.as_raw_fd();
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
                        "SO_INCOMING_CPU socket option skipped or unsupported by kernel"
                    );
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = cpu;
        }
    }

    /// Binds a single TCP listener shard affined to a specific worker/CPU core.
    pub fn bind_shard(
        ip: u128,
        port_be: u16,
        config: TcpListenerConfig,
        worker_id: usize,
    ) -> Result<Self> {
        let listener = Self::bind(ip, port_be, config.clone())?;
        // Only when NIC hardware RSS is proven to cover every worker core do we let the NIC's
        // queue->CPU choice win (SO_INCOMING_CPU = real physical core). Otherwise the kernel
        // SO_REUSEPORT software hash distributes flows; binding SO_INCOMING_CPU on a NIC that does
        // not spread flows would funnel all connections into one worker.
        let is_loopback = ip == velda_core::lut::ip::v4_to_u128(0x7F00_0001) || ip == 1;
        if config.incoming_cpu
            && !is_loopback
            && let Some(core) = velda_core::hardware::ingress_steering_cpu(worker_id)
        {
            listener.apply_incoming_cpu(core);
        }
        Ok(listener)
    }

    /// Binds multiple TCP listener shards to `(ip, port_be)` with `SO_REUSEPORT`.
    ///
    /// If `config.concurrency_shards > 1` and `config.reuseport` is enabled, binds up to
    /// `config.concurrency_shards` sockets to the same port.
    /// The first socket determines the exact assigned address (critical when `port_be == 0`).
    pub fn bind_shards(ip: u128, port_be: u16, config: TcpListenerConfig) -> Result<Vec<Self>> {
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
                        "Failed to bind SO_REUSEPORT listener shard; continuing with existing shards"
                    );
                    break;
                }
            }
        }

        Ok(shards)
    }

    /// Wraps an existing Tokio [`TokioTcpListener`] with configuration.
    pub fn from_tokio(listener: TokioTcpListener, config: TcpListenerConfig) -> Result<Self> {
        let local_addr = listener.local_addr().map_err(SockError::Io)?;
        let (local_ip, local_port_be) = socket_addr_to_raw(local_addr);
        Ok(Self {
            listener,
            local_ip,
            local_port_be,
            config,
        })
    }

    /// Returns the configuration applied to this listener and its accepted sockets.
    #[inline]
    pub const fn config(&self) -> &TcpListenerConfig {
        &self.config
    }

    /// Accepts an incoming connection and applies socket options.
    pub async fn accept(&self) -> Result<Connection> {
        let (stream, peer_addr) = self.listener.accept().await.map_err(SockError::Accept)?;
        let (peer_ip, peer_port_be) = socket_addr_to_raw(peer_addr);
        self.setup_accepted_stream(stream, peer_ip, peer_port_be)
    }

    /// Attempts to accept a pending connection without waiting.
    ///
    /// On Linux, executes `libc::accept4` with `SOCK_NONBLOCK | SOCK_CLOEXEC` directly
    /// to drain the kernel SYN backlog without epoll churn.
    pub fn try_accept(&self) -> Result<Option<Connection>> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            let fd = self.listener.as_raw_fd();
            let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of_val(&storage) as libc::socklen_t;
            let res = unsafe {
                libc::accept4(
                    fd,
                    &mut storage as *mut _ as *mut libc::sockaddr,
                    &mut len,
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            };
            if res < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    return Ok(None);
                }
                return Err(SockError::Accept(err));
            }
            let std_stream = unsafe { std::net::TcpStream::from_raw_fd(res) };
            let stream = tokio::net::TcpStream::from_std(std_stream).map_err(SockError::Accept)?;
            let (peer_ip, peer_port_be) = sockaddr_storage_to_raw(&storage);
            self.setup_accepted_stream(stream, peer_ip, peer_port_be)
                .map(Some)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(None)
        }
    }

    #[inline]
    fn setup_accepted_stream(
        &self,
        stream: tokio::net::TcpStream,
        peer_ip: u128,
        peer_port_be: u16,
    ) -> Result<Connection> {
        if self.config.nodelay {
            let _ = stream.set_nodelay(true);
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();

            if let Some(rcv) = self.config.recv_buffer_size {
                let val: libc::c_int = rcv as libc::c_int;
                unsafe {
                    let _ = libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_RCVBUF,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                }
            }

            if let Some(snd) = self.config.send_buffer_size {
                let val: libc::c_int = snd as libc::c_int;
                unsafe {
                    let _ = libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_SNDBUF,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if self.config.quickack {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            let val: libc::c_int = 1;
            unsafe {
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
            }
        }

        #[cfg(target_os = "linux")]
        if self.config.bbr {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            let bbr_name = b"bbr\0";
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_CONGESTION,
                    bbr_name.as_ptr() as *const libc::c_void,
                    bbr_name.len() as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "TCP_CONGESTION bbr skipped on accepted downstream socket"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(busy_poll) = self.config.busy_poll_us {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            let val: libc::c_int = busy_poll as libc::c_int;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "SO_BUSY_POLL skipped on accepted socket"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(lowat) = self.config.notsent_lowat {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            let val: libc::c_uint = lowat as libc::c_uint;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NOTSENT_LOWAT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "TCP_NOTSENT_LOWAT skipped on accepted downstream socket"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(user_timeout) = self.config.user_timeout {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            let val: libc::c_uint = user_timeout.as_millis().clamp(1000, 300_000) as libc::c_uint;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_USER_TIMEOUT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "TCP_USER_TIMEOUT skipped on accepted downstream socket"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if self.config.prefer_busy_poll {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
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
                        "SO_PREFER_BUSY_POLL skipped on accepted socket"
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

        #[cfg(target_os = "linux")]
        if let Some(rto_min) = self.config.rto_min_us {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            const TCP_RTO_MIN_US: libc::c_int = 45;
            let val = rto_min as libc::c_uint;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    TCP_RTO_MIN_US,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "TCP_RTO_MIN_US skipped on accepted downstream socket"
                    );
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(delack) = self.config.delack_max_us {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            const TCP_DELACK_MAX_US: libc::c_int = 46;
            let val = delack as libc::c_uint;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    TCP_DELACK_MAX_US,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "TCP_DELACK_MAX_US skipped on accepted downstream socket"
                    );
                }
            }
        }

        // Avoid a getsockname() syscall if the bound address is already specific (non-unspecified).
        let is_unspecified =
            self.local_ip == velda_core::lut::ip::v4_to_u128(0) || self.local_ip == 0;
        let (local_ip, local_port_be) = if is_unspecified {
            stream
                .local_addr()
                .map(socket_addr_to_raw)
                .unwrap_or((self.local_ip, self.local_port_be))
        } else {
            (self.local_ip, self.local_port_be)
        };
        let id = crate::connection::next_connection_id();

        Ok(Connection::new(
            id,
            stream,
            peer_ip,
            peer_port_be,
            local_ip,
            local_port_be,
        ))
    }

    /// Accepts an incoming connection, or returns `None` if a graceful shutdown
    /// was signaled via the watch receiver.
    pub async fn accept_with_shutdown(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<Connection>> {
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }

            tokio::select! {
                res = shutdown.changed() => {
                    match res {
                        Ok(()) => {
                            if *shutdown.borrow() {
                                return Ok(None);
                            }
                            // Spurious change without true shutdown, continue loop in select!
                        }
                        Err(_) => return Ok(None), // Sender dropped, treat as shutdown
                    }
                }
                res = self.accept() => {
                    return res.map(Some);
                }
            }
        }
    }

    /// Runs the accept loop, spawning each accepted connection onto Tokio
    /// with the provided handler, until the shutdown watch signal is triggered.
    pub async fn serve<F, Fut>(&self, mut shutdown: watch::Receiver<bool>, handler: F) -> Result<()>
    where
        F: Fn(Connection) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        tracing::info!(
            listen_addr = %format_raw_addr(self.local_ip, self.local_port_be),
            "TCP listener started serving"
        );

        // Exponential backoff on accept errors (e.g. EMFILE) to avoid a CPU-spinning error loop.
        let mut backoff = std::time::Duration::from_millis(5);

        loop {
            match self.accept_with_shutdown(&mut shutdown).await {
                Ok(Some(conn)) => {
                    backoff = std::time::Duration::from_millis(5);
                    tracing::debug!(
                        connection_id = %conn.id(),
                        peer = %format_raw_addr(conn.peer_ip, conn.peer_port_be),
                        "Accepted TCP connection"
                    );
                    tokio::spawn(handler(conn));

                    let mut batch = 0;
                    while batch < 32 {
                        if *shutdown.borrow() {
                            break;
                        }
                        match self.try_accept() {
                            Ok(Some(conn)) => {
                                batch += 1;
                                tokio::spawn(handler(conn));
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        listen_addr = %format_raw_addr(self.local_ip, self.local_port_be),
                        "TCP listener shutting down gracefully"
                    );
                    break;
                }
                Err(err) => {
                    tracing::error!(
                        listen_addr = %format_raw_addr(self.local_ip, self.local_port_be),
                        error = %err,
                        "Accept loop error encountered"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.changed() => {}
                    }
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(1));
                }
            }
        }

        Ok(())
    }
}
