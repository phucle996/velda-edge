//! High-performance TCP listener implementation.

use std::net::SocketAddr;
use tokio::net::{TcpListener as TokioTcpListener, TcpSocket};
use tokio::sync::watch;

use super::config::TcpListenerConfig;
use crate::connection::Connection;
use crate::error::{Result, TransportError};

/// High-performance TCP listener managing incoming client connections.
#[derive(Debug)]
pub struct TcpListener {
    listener: TokioTcpListener,
    local_addr: SocketAddr,
    config: TcpListenerConfig,
}

impl TcpListener {
    /// Binds a new TCP listener to the specified address with the given configuration.
    pub fn bind(addr: SocketAddr, config: TcpListenerConfig) -> Result<Self> {
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4().map_err(TransportError::Io)?
        } else {
            TcpSocket::new_v6().map_err(TransportError::Io)?
        };

        // Enable SO_REUSEADDR for rapid port recycling on restarts
        socket.set_reuseaddr(true).map_err(TransportError::Io)?;

        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        if config.reuseport {
            socket.set_reuseport(true).map_err(TransportError::Io)?;
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

        socket
            .bind(addr)
            .map_err(|e| TransportError::Bind { addr, source: e })?;

        let listener = socket
            .listen(config.backlog)
            .map_err(|e| TransportError::Bind { addr, source: e })?;

        let local_addr = listener
            .local_addr()
            .map_err(|e| TransportError::Bind { addr, source: e })?;

        Ok(Self {
            listener,
            local_addr,
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

    /// Binds multiple TCP listener shards to the specified address with `SO_REUSEPORT`.
    ///
    /// If `config.concurrency_shards > 1` and `config.reuseport` is enabled, binds up to
    /// `config.concurrency_shards` sockets to the same port.
    /// The first socket determines the exact assigned address (critical when `addr.port() == 0`).
    pub fn bind_shards(addr: SocketAddr, config: TcpListenerConfig) -> Result<Vec<Self>> {
        let first = Self::bind(addr, config.clone())?;
        let actual_addr = first.local_addr();
        let target_shards = if config.reuseport {
            config.concurrency_shards.max(1)
        } else {
            1
        };

        let available_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        if config.incoming_cpu {
            first.apply_incoming_cpu(0);
        }

        if target_shards <= 1 {
            return Ok(vec![first]);
        }

        let mut shards = Vec::with_capacity(target_shards);
        shards.push(first);

        for shard_idx in 1..target_shards {
            match Self::bind(actual_addr, config.clone()) {
                Ok(shard) => {
                    if config.incoming_cpu {
                        shard.apply_incoming_cpu(shard_idx % available_cpus);
                    }
                    shards.push(shard);
                }
                Err(err) => {
                    tracing::warn!(
                        shard = shard_idx,
                        listen_addr = %actual_addr,
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
        let local_addr = listener.local_addr().map_err(TransportError::Io)?;
        Ok(Self {
            listener,
            local_addr,
            config,
        })
    }

    /// Returns the local address this listener is bound to.
    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the configuration applied to this listener and its accepted sockets.
    #[inline]
    pub const fn config(&self) -> &TcpListenerConfig {
        &self.config
    }

    /// Accepts an incoming connection and applies socket options.
    pub async fn accept(&self) -> Result<Connection> {
        let (stream, peer_addr) = self
            .listener
            .accept()
            .await
            .map_err(TransportError::Accept)?;

        if self.config.nodelay {
            let _ = stream.set_nodelay(true);
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

        let local_addr = stream.local_addr().unwrap_or(self.local_addr);
        let id = crate::connection::next_connection_id();

        Ok(Connection::new(id, stream, peer_addr, local_addr))
    }

    /// Accepts an incoming connection, or returns `None` if a graceful shutdown
    /// was signaled via the watch receiver.
    pub async fn accept_with_shutdown(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<Connection>> {
        if *shutdown.borrow() {
            return Ok(None);
        }

        tokio::select! {
            res = shutdown.changed() => {
                match res {
                    Ok(()) => {
                        if *shutdown.borrow() {
                            Ok(None)
                        } else {
                            // Spurious wake-up or non-shutdown change; accept normally
                            self.accept().await.map(Some)
                        }
                    }
                    Err(_) => {
                        // Sender dropped, treat as shutdown signal
                        Ok(None)
                    }
                }
            }
            res = self.accept() => {
                res.map(Some)
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
        tracing::info!(listen_addr = %self.local_addr, "TCP listener started serving");

        // Exponential backoff on accept errors (e.g. EMFILE) to avoid a CPU-spinning error loop.
        let mut backoff = std::time::Duration::from_millis(5);

        loop {
            match self.accept_with_shutdown(&mut shutdown).await {
                Ok(Some(conn)) => {
                    backoff = std::time::Duration::from_millis(5);
                    tracing::debug!(
                        connection_id = %conn.id(),
                        peer = %conn.peer(),
                        "Accepted TCP connection"
                    );
                    tokio::spawn(handler(conn));
                }
                Ok(None) => {
                    tracing::info!(listen_addr = %self.local_addr, "TCP listener shutting down gracefully");
                    break;
                }
                Err(err) => {
                    tracing::error!(listen_addr = %self.local_addr, error = %err, "Accept loop error encountered");
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
