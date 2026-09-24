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

        loop {
            match self.accept_with_shutdown(&mut shutdown).await {
                Ok(Some(conn)) => {
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
                }
            }
        }

        Ok(())
    }
}
