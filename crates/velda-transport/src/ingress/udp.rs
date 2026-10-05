//! Stateless UDP ingress listener and datagram dispatch.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::error::Result;
use crate::udp::config::UdpSocketConfig;
use crate::udp::datagram::Datagram;
use crate::udp::socket::UdpSocket;

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

        let mut buf = vec![0u8; 65535];
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
                res = socket.recv_from(&mut buf) => {
                    match res {
                        Ok((n, peer)) => {
                            let dgram = Datagram::new(peer, local_addr, buf[..n].to_vec());
                            // Handlers run inline, not in a task per datagram: this keeps packets
                            // of one flow ordered (SO_REUSEPORT pins a 4-tuple to one shard) and
                            // avoids a task spawn per packet. Handlers must stay short; long work
                            // (e.g. HTTP/3 request processing, L4 session pumps) is spawned by them.
                            handler(Arc::clone(&listener_id), Arc::clone(&socket), dgram).await;
                        }
                        Err(err) => {
                            tracing::error!(
                                listener_id = %listener_id,
                                listen_addr = %local_addr,
                                shard = shard_idx,
                                error = %err,
                                "UDP recv error"
                            );
                        }
                    }
                }
            }
        }
    }
}
