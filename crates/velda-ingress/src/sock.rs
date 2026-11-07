//! OS Socket Ingress Management (`velda-ingress::sock`).
//!
//! Manages Linux OS socket listener lifecycles and frontline traffic arrival loops
//! for stateful TCP connections and stateless UDP datagrams.
//!
//! # Protocol-Agnostic Invariant
//! This module has zero knowledge of application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC).
//! It strictly operates as a traffic intake manager: accepting raw connections or receiving raw
//! datagrams, affixing `listener_id`, and immediately dispatching opaque byte streams to caller closures.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinSet;

use velda_sock::{
    Connection, Datagram, TcpListener, TcpListenerConfig, UdpSocket, UdpSocketConfig,
};

use crate::error::Result;

const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(5);
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Ingress binding configuration for stateful TCP listeners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpBinding {
    /// Unique listener identifier declared in configuration.
    pub id: String,
    /// Local address to bind and listen on.
    pub addr: SocketAddr,
    /// Whether downstream TLS is enabled on this listener.
    pub tls_enabled: bool,
    /// TCP socket listener options (backlog, nodelay, buffer sizing, reuseport).
    pub config: TcpListenerConfig,
}

impl TcpBinding {
    /// Creates a new TCP ingress binding.
    pub fn new(id: impl Into<String>, addr: SocketAddr, tls_enabled: bool) -> Self {
        Self {
            id: id.into(),
            addr,
            tls_enabled,
            config: TcpListenerConfig::default(),
        }
    }

    /// Sets the TCP socket configuration.
    pub fn with_config(mut self, config: TcpListenerConfig) -> Self {
        self.config = config;
        self
    }
}

/// Stateful TCP ingress listener managing connection arrival across socket shards.
#[derive(Debug)]
pub struct TcpIngress {
    listeners: Vec<TcpListener>,
    binding: TcpBinding,
    /// Shared listener id stamped onto every accepted connection (refcount clone per accept).
    listener_id: Arc<str>,
}

impl TcpIngress {
    /// Binds a TCP ingress listener to the address configured in its binding.
    pub fn bind(binding: TcpBinding) -> Result<Self> {
        let listeners = TcpListener::bind_shards(binding.addr, binding.config.clone())?;
        let listener_id = Arc::from(binding.id.as_str());
        Ok(Self {
            listeners,
            binding,
            listener_id,
        })
    }

    /// Binds a single-shard TCP ingress listener affined to a specific core shard.
    pub fn bind_shard(
        binding: TcpBinding,
        shard_id: usize,
        addr_override: Option<SocketAddr>,
    ) -> Result<Self> {
        let addr = addr_override.unwrap_or(binding.addr);
        let listener = TcpListener::bind_shard(addr, binding.config.clone(), shard_id)?;
        let listener_id = Arc::from(binding.id.as_str());
        Ok(Self {
            listeners: vec![listener],
            binding,
            listener_id,
        })
    }

    /// Returns the bound local socket address.
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.listeners[0].local_addr()
    }

    /// Returns the number of listener shards currently bound.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.listeners.len()
    }

    /// Returns a reference to the binding configuration.
    #[inline]
    pub const fn binding(&self) -> &TcpBinding {
        &self.binding
    }

    /// Returns the unique listener identifier.
    #[inline]
    pub fn id(&self) -> &str {
        &self.binding.id
    }

    /// Accepts an incoming connection from the first shard, tagged with this listener's id.
    pub async fn accept(&self) -> Result<Connection> {
        let conn = self.listeners[0].accept().await?;
        Ok(conn.with_listener_id(Arc::clone(&self.listener_id)))
    }

    /// Attempts to accept an incoming connection from the first shard without waiting.
    pub fn try_accept(&self) -> Result<Option<Connection>> {
        self.listeners[0]
            .try_accept()
            .map(|opt| opt.map(|conn| conn.with_listener_id(Arc::clone(&self.listener_id))))
            .map_err(Into::into)
    }

    /// Spawns an ingress accept loop for each listener shard on the provided `JoinSet`.
    pub fn spawn_accept_loop<H, Fut>(
        ingress: Arc<Self>,
        tasks: &mut JoinSet<()>,
        shutdown: watch::Receiver<bool>,
        handler: H,
    ) where
        H: Fn(Connection) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        for shard_idx in 0..ingress.listeners.len() {
            let ingress_clone = Arc::clone(&ingress);
            let shutdown_clone = shutdown.clone();
            let handler_clone = handler.clone();
            tasks.spawn(async move {
                Self::run_shard_accept_loop(
                    ingress_clone,
                    shard_idx,
                    shutdown_clone,
                    handler_clone,
                )
                .await;
            });
        }
    }

    /// Internal accept loop driving connection ingress for a specific listener shard.
    pub async fn run_shard_accept_loop<H, Fut>(
        ingress: Arc<Self>,
        shard_idx: usize,
        mut shutdown: watch::Receiver<bool>,
        handler: H,
    ) where
        H: Fn(Connection) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let local_addr = ingress.local_addr();
        let listener_id = Arc::clone(&ingress.listener_id);
        let total_shards = ingress.listeners.len();
        tracing::info!(
            listener_id = %listener_id,
            listen_addr = %local_addr,
            shard = shard_idx,
            total_shards,
            "TCP ingress listener shard bound and serving"
        );

        let mut backoff = ACCEPT_BACKOFF_MIN;

        loop {
            if *shutdown.borrow() {
                break;
            }

            match ingress.listeners[shard_idx]
                .accept_with_shutdown(&mut shutdown)
                .await
            {
                Ok(Some(conn)) => {
                    backoff = ACCEPT_BACKOFF_MIN;
                    tracing::debug!(
                        listener_id = %listener_id,
                        conn_id = %conn.id(),
                        shard = shard_idx,
                        peer = %conn.peer(),
                        "TCP ingress accepted connection"
                    );
                    tokio::spawn(handler(conn.with_listener_id(Arc::clone(&listener_id))));

                    // Multi-accept drain loop: drain up to 32 pending connections immediately
                    // from kernel backlog without re-registering epoll interest
                    let mut batch = 0;
                    while batch < 32 {
                        if *shutdown.borrow() {
                            break;
                        }
                        match ingress.listeners[shard_idx].try_accept() {
                            Ok(Some(conn)) => {
                                batch += 1;
                                tracing::debug!(
                                    listener_id = %listener_id,
                                    conn_id = %conn.id(),
                                    shard = shard_idx,
                                    peer = %conn.peer(),
                                    batch,
                                    "TCP ingress batch-accepted connection"
                                );
                                tokio::spawn(handler(
                                    conn.with_listener_id(Arc::clone(&listener_id)),
                                ));
                            }
                            Ok(None) => break,
                            Err(err) => {
                                tracing::warn!(
                                    listener_id = %listener_id,
                                    shard = shard_idx,
                                    error = %err,
                                    "TCP try_accept error during batch drain"
                                );
                                break;
                            }
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        listener_id = %listener_id,
                        listen_addr = %local_addr,
                        shard = shard_idx,
                        "TCP ingress loop shutting down"
                    );
                    break;
                }
                Err(err) => {
                    tracing::error!(
                        listener_id = %listener_id,
                        listen_addr = %local_addr,
                        shard = shard_idx,
                        error = %err,
                        backoff_ms = backoff.as_millis() as u64,
                        "TCP accept error"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.changed() => {}
                    }
                    backoff = (backoff * 2).min(ACCEPT_BACKOFF_MAX);
                }
            }
        }
    }
}

/// Ingress binding configuration for stateless UDP listeners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpBinding {
    /// Unique listener identifier declared in configuration.
    pub id: String,
    /// Local address to bind and listen on.
    pub addr: SocketAddr,
    /// Whether downstream TLS (QUIC/DTLS) is enabled on this listener.
    pub tls_enabled: bool,
    /// UDP socket options (buffer sizing, reuseport, concurrency shards).
    pub config: UdpSocketConfig,
}

impl UdpBinding {
    /// Creates a new UDP ingress binding.
    pub fn new(id: impl Into<String>, addr: SocketAddr, tls_enabled: bool) -> Self {
        Self {
            id: id.into(),
            addr,
            tls_enabled,
            config: UdpSocketConfig::default(),
        }
    }

    /// Sets the UDP socket configuration.
    pub fn with_config(mut self, config: UdpSocketConfig) -> Self {
        self.config = config;
        self
    }
}

/// Stateless UDP ingress listener managing datagram reception across socket shards.
#[derive(Debug)]
pub struct UdpIngress {
    sockets: Vec<Arc<UdpSocket>>,
    binding: UdpBinding,
    /// Shared listener id handed to every datagram handler call (refcount clone, no alloc).
    listener_id: Arc<str>,
}

impl UdpIngress {
    /// Binds a UDP ingress listener to the address configured in its binding.
    pub fn bind(binding: UdpBinding) -> Result<Self> {
        let sockets = UdpSocket::bind_shards(binding.addr, binding.config.clone())?
            .into_iter()
            .map(Arc::new)
            .collect();
        let listener_id = Arc::from(binding.id.as_str());
        Ok(Self {
            sockets,
            binding,
            listener_id,
        })
    }

    /// Binds a single-shard UDP ingress listener affined to a specific core shard.
    pub fn bind_shard(
        binding: UdpBinding,
        shard_id: usize,
        addr_override: Option<SocketAddr>,
    ) -> Result<Self> {
        let addr = addr_override.unwrap_or(binding.addr);
        let socket = UdpSocket::bind_shard(addr, binding.config.clone(), shard_id)?;
        let listener_id = Arc::from(binding.id.as_str());
        Ok(Self {
            sockets: vec![Arc::new(socket)],
            binding,
            listener_id,
        })
    }

    /// Returns the bound local socket address.
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.sockets[0].local_addr()
    }

    /// Returns the number of socket shards currently bound.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.sockets.len()
    }

    /// Returns a reference to the binding configuration.
    #[inline]
    pub const fn binding(&self) -> &UdpBinding {
        &self.binding
    }

    /// Returns the unique listener identifier.
    #[inline]
    pub fn id(&self) -> &str {
        &self.binding.id
    }

    /// Spawns an ingress receive loop for each socket shard on the provided `JoinSet`.
    pub fn spawn_receive_loop<H, Fut>(
        ingress: Arc<Self>,
        tasks: &mut JoinSet<()>,
        shutdown: watch::Receiver<bool>,
        handler: H,
    ) where
        H: Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        for shard_idx in 0..ingress.sockets.len() {
            let ingress_clone = Arc::clone(&ingress);
            let shutdown_clone = shutdown.clone();
            let handler_clone = handler.clone();
            tasks.spawn(async move {
                Self::run_shard_receive_loop(
                    ingress_clone,
                    shard_idx,
                    shutdown_clone,
                    handler_clone,
                )
                .await;
            });
        }
    }

    /// Internal receive loop driving datagram ingress for a specific socket shard.
    pub async fn run_shard_receive_loop<H, Fut>(
        ingress: Arc<Self>,
        shard_idx: usize,
        mut shutdown: watch::Receiver<bool>,
        handler: H,
    ) where
        H: Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let socket = Arc::clone(&ingress.sockets[shard_idx]);
        let local_addr = ingress.local_addr();
        let listener_id = Arc::clone(&ingress.listener_id);
        let total_shards = ingress.sockets.len();
        tracing::info!(
            listener_id = %listener_id,
            listen_addr = %local_addr,
            shard = shard_idx,
            total_shards,
            "UDP ingress socket shard bound and receiving"
        );

        const BATCH_SIZE: usize = 32;
        const SLOT_SIZE: usize = 2048;
        const SLAB_SIZE: usize = BATCH_SIZE * SLOT_SIZE;

        let mut slab = vec![0u8; SLAB_SIZE];
        let mut batch_out = [(0usize, local_addr); BATCH_SIZE];

        loop {
            if *shutdown.borrow() {
                break;
            }

            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!(
                            listener_id = %listener_id,
                            listen_addr = %local_addr,
                            shard = shard_idx,
                            "UDP ingress loop shutting down"
                        );
                        break;
                    }
                }
                ready_res = socket.readable() => {
                    if let Err(err) = ready_res {
                        tracing::error!(
                            listener_id = %listener_id,
                            listen_addr = %local_addr,
                            shard = shard_idx,
                            error = %err,
                            "UDP socket readiness error"
                        );
                        continue;
                    }

                    let mut drain_loops = 0;
                    while drain_loops < 16 {
                        let count = socket.try_recv_batch_slab(&mut slab, SLOT_SIZE, &mut batch_out);
                        if count == 0 {
                            break;
                        }
                        drain_loops += 1;

                        for (i, &(n, peer)) in batch_out[..count].iter().enumerate() {
                            if n == 0 {
                                continue;
                            }
                            let offset = i * SLOT_SIZE;
                            let dgram_bytes = bytes::Bytes::copy_from_slice(&slab[offset..offset + n]);
                            let dgram = Datagram::new(peer, local_addr, dgram_bytes);

                            handler(Arc::clone(&listener_id), Arc::clone(&socket), dgram).await;
                        }
                    }
                }
            }
        }
    }
}
