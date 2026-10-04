//! Ingress binding configuration and listener wrapper aligned with listeners.json schema.

use std::net::SocketAddr;

use super::classifier::PathKind;
use crate::connection::Connection;
use crate::error::{Result, TransportError};
use crate::tcp::config::TcpListenerConfig;
use crate::tcp::listener::TcpListener;
use crate::udp::config::UdpSocketConfig;

/// Ingress binding configuration matching the user's declared listener definition in listeners.json.
///
/// Strictly separates transport (L4) and application (L7) dimensions:
/// - `transport.protocol`: "tcp" or "udp" (determines socket type)
/// - `application.protocol`: "raw" -> [`PathKind::L4Direct`], != "raw" -> [`PathKind::L7Handoff`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressBinding {
    /// Unique listener identifier declared in listeners.json.
    pub id: String,
    /// Local address to bind and listen on.
    pub addr: SocketAddr,
    /// Declared transport protocol ("tcp" or "udp").
    pub protocol: String,
    /// Whether TLS is enabled for this listener.
    pub tls_enabled: bool,
    /// Resolved dispatch path (L4Direct for raw proxying, L7Handoff for Composer).
    pub path: PathKind,
    /// TCP socket listener options (e.g. nodelay, backlog, buffer sizes).
    pub tcp_config: TcpListenerConfig,
    /// UDP socket options (e.g. receive/send buffer sizes).
    pub udp_config: UdpSocketConfig,
}

impl IngressBinding {
    /// Creates a new ingress binding matching explicit transport protocol and dispatch path.
    pub fn new(
        id: impl Into<String>,
        addr: SocketAddr,
        transport_protocol: impl AsRef<str>,
        path: PathKind,
        tls_enabled: bool,
    ) -> Result<Self> {
        let tp = transport_protocol.as_ref();
        let protocol = if tp.eq_ignore_ascii_case("tcp") {
            "tcp".to_string()
        } else if tp.eq_ignore_ascii_case("udp") {
            "udp".to_string()
        } else {
            let id_str = id.into();
            return Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "unsupported transport protocol '{tp}' for listener '{id_str}' (must be 'tcp' or 'udp')"
                ),
            )));
        };

        Ok(Self {
            id: id.into(),
            addr,
            protocol,
            tls_enabled,
            path,
            tcp_config: TcpListenerConfig::default(),
            udp_config: UdpSocketConfig::default(),
        })
    }

    /// Creates an ingress binding directly from declared transport and application protocol dimensions:
    /// - `application_protocol == "raw"` -> [`PathKind::L4Direct`]
    /// - `application_protocol != "raw"` -> [`PathKind::L7Handoff`]
    pub fn from_protocols(
        id: impl Into<String>,
        addr: SocketAddr,
        transport_protocol: impl AsRef<str>,
        application_protocol: impl AsRef<str>,
        tls_enabled: bool,
    ) -> Result<Self> {
        let path = if application_protocol.as_ref().eq_ignore_ascii_case("raw") {
            PathKind::L4Direct
        } else {
            PathKind::L7Handoff
        };
        Self::new(id, addr, transport_protocol, path, tls_enabled)
    }

    /// Returns whether this binding is for UDP-based transport.
    #[inline]
    pub fn is_udp(&self) -> bool {
        self.protocol.eq_ignore_ascii_case("udp")
    }

    /// Returns whether this binding is for TCP-based transport.
    #[inline]
    pub fn is_tcp(&self) -> bool {
        self.protocol.eq_ignore_ascii_case("tcp")
    }

    /// Returns whether this binding is pure L4 direct proxying.
    #[inline]
    pub fn is_l4(&self) -> bool {
        self.path == PathKind::L4Direct
    }

    /// Returns whether this binding requires L7 handoff.
    #[inline]
    pub fn is_l7(&self) -> bool {
        self.path == PathKind::L7Handoff
    }

    /// Configures the TCP listener socket parameters.
    pub fn with_tcp_config(mut self, config: TcpListenerConfig) -> Self {
        self.tcp_config = config;
        self
    }

    /// Configures the UDP socket parameters.
    pub fn with_udp_config(mut self, config: UdpSocketConfig) -> Self {
        self.udp_config = config;
        self
    }
}

/// Ingress listener managing client connection arrival according to user configuration.
#[derive(Debug)]
pub struct IngressListener {
    listeners: Vec<TcpListener>,
    binding: IngressBinding,
}

impl IngressListener {
    /// Binds an ingress listener to the address configured in its binding.
    ///
    /// If `tcp_config.concurrency_shards > 1` and `tcp_config.reuseport` is enabled,
    /// binds multiple socket shards to the same address using `SO_REUSEPORT`.
    pub fn bind(binding: IngressBinding) -> Result<Self> {
        let listeners = TcpListener::bind_shards(binding.addr, binding.tcp_config.clone())?;
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
    pub const fn binding(&self) -> &IngressBinding {
        &self.binding
    }

    /// Returns the unique listener identifier.
    #[inline]
    pub fn id(&self) -> &str {
        &self.binding.id
    }

    /// Returns the resolved path kind for connections on this listener.
    #[inline]
    pub const fn path(&self) -> PathKind {
        self.binding.path
    }

    /// Accepts an incoming connection from the first shard and returns its strictly declared traffic path.
    pub async fn accept(&self) -> Result<(Connection, PathKind)> {
        let conn = self.listeners[0].accept().await?;
        Ok((conn, self.binding.path))
    }

    /// Spawns an ingress accept loop for each listener shard on the provided `JoinSet`.
    pub fn spawn_accept_loop<L4H, L7H, FutL4, FutL7>(
        ingress: std::sync::Arc<Self>,
        tasks: &mut tokio::task::JoinSet<()>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        l4_fn: L4H,
        l7_fn: L7H,
    ) where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(crate::handoff::TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        for shard_idx in 0..ingress.listeners.len() {
            let ingress_clone = std::sync::Arc::clone(&ingress);
            let shutdown_clone = shutdown.clone();
            let l4_clone = l4_fn.clone();
            let l7_clone = l7_fn.clone();
            tasks.spawn(async move {
                Self::run_shard_accept_loop(
                    ingress_clone,
                    shard_idx,
                    shutdown_clone,
                    l4_clone,
                    l7_clone,
                )
                .await;
            });
        }
    }

    /// Internal accept loop driving connection ingress and dispatch for a specific listener shard.
    pub async fn run_shard_accept_loop<L4H, L7H, FutL4, FutL7>(
        ingress: std::sync::Arc<Self>,
        shard_idx: usize,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        l4_fn: L4H,
        l7_fn: L7H,
    ) where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(crate::handoff::TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        let local_addr = ingress.local_addr();
        let listener_id = ingress.binding().id.clone();
        let total_shards = ingress.listeners.len();
        tracing::info!(
            listener_id = %ingress.id(),
            listen_addr = %local_addr,
            shard = shard_idx,
            total_shards,
            path = %ingress.path(),
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
                        listener_id = %ingress.id(),
                        conn_id = %conn.id(),
                        shard = shard_idx,
                        path = %ingress.path(),
                        peer = %conn.peer(),
                        "Ingress accepted connection"
                    );
                    match ingress.path() {
                        PathKind::L4Direct => {
                            let conn = conn.with_listener_id(listener_id.clone());
                            tokio::spawn(l4_fn(conn));
                        }
                        PathKind::L7Handoff => {
                            let handoff =
                                crate::handoff::TcpL7Handoff::new(conn, listener_id.clone());
                            tokio::spawn(l7_fn(handoff));
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        listener_id = %ingress.id(),
                        listen_addr = %local_addr,
                        shard = shard_idx,
                        "TCP ingress loop shutting down"
                    );
                    break;
                }
                Err(err) => {
                    tracing::error!(
                        listener_id = %ingress.id(),
                        listen_addr = %local_addr,
                        shard = shard_idx,
                        error = %err,
                        "TCP accept error"
                    );
                }
            }
        }
    }

    /// Internal accept loop driving connection ingress and dispatch (runs shard 0).
    pub async fn run_tcp_accept_loop<L4H, L7H, FutL4, FutL7>(
        ingress: std::sync::Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        l4_fn: L4H,
        l7_fn: L7H,
    ) where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(crate::handoff::TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        Self::run_shard_accept_loop(ingress, 0, shutdown, l4_fn, l7_fn).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingress_binding_from_user_json_schema() {
        // Matches listeners.json "http": application "http1" -> L7Handoff
        let http_binding = IngressBinding::from_protocols(
            "http",
            "0.0.0.0:80".parse().unwrap(),
            "tcp",
            "http1",
            false,
        )
        .unwrap();
        assert_eq!(http_binding.path, PathKind::L7Handoff);
        assert!(!http_binding.tls_enabled);
        assert!(http_binding.is_tcp());
        assert!(!http_binding.is_udp());

        // Matches listeners.json "https": application "http2", tls true -> L7Handoff
        let https_binding = IngressBinding::from_protocols(
            "https",
            "0.0.0.0:443".parse().unwrap(),
            "tcp",
            "http2",
            true,
        )
        .unwrap();
        assert_eq!(https_binding.path, PathKind::L7Handoff);
        assert!(https_binding.tls_enabled);
        assert!(https_binding.is_tcp());

        // Matches listeners.json "tcp-ingress": application "raw" -> L4Direct
        let tcp_binding = IngressBinding::from_protocols(
            "tcp-ingress",
            "0.0.0.0:9000".parse().unwrap(),
            "tcp",
            "raw",
            false,
        )
        .unwrap();
        assert_eq!(tcp_binding.path, PathKind::L4Direct);
        assert!(tcp_binding.is_tcp());
        assert!(!tcp_binding.is_udp());

        // UDP L4 binding: application "raw" -> L4Direct
        let udp_binding = IngressBinding::from_protocols(
            "udp-ingress",
            "0.0.0.0:53".parse().unwrap(),
            "udp",
            "raw",
            false,
        )
        .unwrap();
        assert_eq!(udp_binding.path, PathKind::L4Direct);
        assert!(udp_binding.is_udp());
        assert!(!udp_binding.is_tcp());

        // UDP HTTP/3 binding: application "http3" -> L7Handoff
        let h3_binding = IngressBinding::from_protocols(
            "h3-ingress",
            "0.0.0.0:8443".parse().unwrap(),
            "udp",
            "http3",
            true,
        )
        .unwrap();
        assert_eq!(h3_binding.path, PathKind::L7Handoff);
        assert!(h3_binding.is_udp());
    }

    #[test]
    fn test_unsupported_transport_protocol_fails() {
        let result = IngressBinding::new(
            "bad",
            "0.0.0.0:80".parse().unwrap(),
            "unsupported_proto",
            PathKind::L7Handoff,
            false,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ingress_listener_binds_ephemeral_socket() {
        let binding = IngressBinding::new(
            "active",
            "127.0.0.1:0".parse().unwrap(),
            "tcp",
            PathKind::L4Direct,
            false,
        )
        .unwrap();

        let listener = IngressListener::bind(binding).unwrap();
        assert_ne!(listener.local_addr().port(), 0);
        assert_eq!(listener.id(), "active");
        assert_eq!(listener.path(), PathKind::L4Direct);
    }
}
