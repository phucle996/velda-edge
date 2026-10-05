//! Stateful TCP ingress listener and connection dispatch.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::connection::Connection;
use crate::error::Result;
use crate::tcp::config::TcpListenerConfig;
use crate::tcp::listener::TcpListener;

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
}

impl TcpIngress {
    /// Binds a TCP ingress listener to the address configured in its binding.
    pub fn bind(binding: TcpBinding) -> Result<Self> {
        let listeners = TcpListener::bind_shards(binding.addr, binding.config.clone())?;
        Ok(Self { listeners, binding })
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
        Ok(conn.with_listener_id(self.binding.id.clone()))
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
        let listener_id = ingress.binding.id.clone();
        let total_shards = ingress.listeners.len();
        tracing::info!(
            listener_id = %listener_id,
            listen_addr = %local_addr,
            shard = shard_idx,
            total_shards,
            "TCP ingress listener shard bound and serving"
        );

        loop {
            if *shutdown.borrow() {
                break;
            }

            match ingress.listeners[shard_idx]
                .accept_with_shutdown(&mut shutdown)
                .await
            {
                Ok(Some(conn)) => {
                    tracing::debug!(
                        listener_id = %listener_id,
                        conn_id = %conn.id(),
                        shard = shard_idx,
                        peer = %conn.peer(),
                        "TCP ingress accepted connection"
                    );
                    tokio::spawn(handler(conn.with_listener_id(listener_id.clone())));
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
                        "TCP accept error"
                    );
                }
            }
        }
    }
}
