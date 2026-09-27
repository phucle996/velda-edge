//! Ingress binding configuration and listener wrapper aligned with listeners.json schema.

use std::net::SocketAddr;

use super::classifier::PathKind;
use crate::connection::Connection;
use crate::error::{Result, TransportError};
use crate::tcp::config::TcpListenerConfig;
use crate::tcp::listener::TcpListener;

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
}

impl IngressBinding {
    /// Creates a new ingress binding matching explicit transport protocol and dispatch path.
    pub fn new(
        id: impl Into<String>,
        addr: SocketAddr,
        transport_protocol: impl Into<String>,
        path: PathKind,
        tls_enabled: bool,
    ) -> Result<Self> {
        let id = id.into();
        let protocol = transport_protocol.into();
        let proto_lower = protocol.to_ascii_lowercase();

        if proto_lower != "tcp" && proto_lower != "udp" {
            return Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "unsupported transport protocol '{protocol}' for listener '{id}' (must be 'tcp' or 'udp')"
                ),
            )));
        }

        Ok(Self {
            id,
            addr,
            protocol: proto_lower,
            tls_enabled,
            path,
            tcp_config: TcpListenerConfig::default(),
        })
    }

    /// Creates an ingress binding directly from declared transport and application protocol dimensions:
    /// - `application_protocol == "raw"` -> [`PathKind::L4Direct`]
    /// - `application_protocol != "raw"` -> [`PathKind::L7Handoff`]
    pub fn from_protocols(
        id: impl Into<String>,
        addr: SocketAddr,
        transport_protocol: impl Into<String>,
        application_protocol: impl Into<String>,
        tls_enabled: bool,
    ) -> Result<Self> {
        let app_proto = application_protocol.into();
        let path = if app_proto.eq_ignore_ascii_case("raw") {
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
}

/// Ingress listener managing client connection arrival according to user configuration.
#[derive(Debug)]
pub struct IngressListener {
    listener: TcpListener,
    binding: IngressBinding,
}

impl IngressListener {
    /// Binds an ingress listener to the address configured in its binding.
    pub fn bind(binding: IngressBinding) -> Result<Self> {
        let listener = TcpListener::bind(binding.addr, binding.tcp_config.clone())?;
        Ok(Self { listener, binding })
    }

    /// Returns the bound local socket address.
    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
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

    /// Accepts an incoming connection and returns its strictly declared traffic path.
    pub async fn accept(&self) -> Result<(Connection, PathKind)> {
        let conn = self.listener.accept().await?;
        Ok((conn, self.binding.path))
    }

    /// Spawns an ingress accept loop on the provided `JoinSet`.
    pub fn spawn_accept_loop<L4H, L7H, FutL4, FutL7>(
        ingress: std::sync::Arc<Self>,
        tasks: &mut tokio::task::JoinSet<()>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        l4_fn: L4H,
        l7_fn: L7H,
    ) where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(crate::forwarding::l7::TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        tasks.spawn(async move {
            Self::run_tcp_accept_loop(ingress, shutdown, l4_fn, l7_fn).await;
        });
    }

    /// Internal accept loop driving connection ingress and dispatch.
    pub async fn run_tcp_accept_loop<L4H, L7H, FutL4, FutL7>(
        ingress: std::sync::Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        l4_fn: L4H,
        l7_fn: L7H,
    ) where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(crate::forwarding::l7::TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        let local_addr = ingress.local_addr();
        tokio::spawn(async move {
            tracing::info!(
                listener_id = %ingress.id(),
                listen_addr = %local_addr,
                path = %ingress.path(),
                "TCP ingress listener bound and serving"
            );

            loop {
                if *shutdown.borrow() {
                    break;
                }

                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            tracing::info!(
                                listener_id = %ingress.id(),
                                listen_addr = %local_addr,
                                "TCP ingress loop shutting down"
                            );
                            break;
                        }
                    }
                    res = ingress.accept() => {
                        match res {
                            Ok((conn, path)) => {
                                tracing::debug!(
                                    listener_id = %ingress.id(),
                                    conn_id = %conn.id(),
                                    path = %path,
                                    peer = %conn.peer(),
                                    "Ingress accepted connection"
                                );
                                match path {
                                    PathKind::L4Direct => {
                                        tokio::spawn(l4_fn(conn));
                                    }
                                    PathKind::L7Handoff => {
                                        let handoff = crate::forwarding::l7::TcpL7Handoff::new(
                                            conn,
                                            ingress.binding().id.clone(),
                                        );
                                        tokio::spawn(l7_fn(handoff));
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    listener_id = %ingress.id(),
                                    listen_addr = %local_addr,
                                    error = %err,
                                    "TCP accept error"
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingress_binding_from_user_json_schema() {
        // Matches listeners.json "http": application "http" -> L7Handoff
        let http_binding = IngressBinding::from_protocols(
            "http",
            "0.0.0.0:80".parse().unwrap(),
            "tcp",
            "http",
            false,
        )
        .unwrap();
        assert_eq!(http_binding.path, PathKind::L7Handoff);
        assert!(!http_binding.tls_enabled);
        assert!(http_binding.is_tcp());
        assert!(!http_binding.is_udp());

        // Matches listeners.json "https": application "http", tls true -> L7Handoff
        let https_binding = IngressBinding::from_protocols(
            "https",
            "0.0.0.0:443".parse().unwrap(),
            "tcp",
            "http",
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

        // UDP HTTP/3 binding: application "http" -> L7Handoff
        let h3_binding = IngressBinding::from_protocols(
            "h3-ingress",
            "0.0.0.0:8443".parse().unwrap(),
            "udp",
            "http",
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
